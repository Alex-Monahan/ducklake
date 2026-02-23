#!/bin/bash
#
# setup_benchmark.sh
#
# Sets up the environment for the DuckLake Parquet vs DuckDB benchmark.
# This script configures submodules, builds DuckDB with the DuckLake extension,
# builds the gofakes3 S3 server, and verifies everything is ready.
#
# Usage:
#   ./scripts/setup_benchmark.sh
#
# Must be run from the repository root directory.

set -e

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

print_step() {
    echo ""
    echo "================================================================"
    echo "  $1"
    echo "================================================================"
}

print_info() {
    echo "  [INFO] $1"
}

print_ok() {
    echo "  [OK]   $1"
}

print_error() {
    echo "  [ERROR] $1" >&2
}

die() {
    print_error "$1"
    exit 1
}

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------

print_step "Pre-flight checks"

# Ensure we are in the repo root (check for key files)
if [ ! -f "extension_config.cmake" ] || [ ! -f "Makefile" ] || [ ! -d "src" ]; then
    die "This script must be run from the DuckLake repository root directory."
fi
print_ok "Running from repository root: $(pwd)"

# Verify we are on the correct DuckLake branch
DUCKLAKE_BRANCH=$(git branch --show-current 2>/dev/null || true)
EXPECTED_BRANCH="claude/duckdb-s3-writes-optimization-9StJ5"

if [ "$DUCKLAKE_BRANCH" = "$EXPECTED_BRANCH" ]; then
    print_ok "On expected DuckLake branch: $DUCKLAKE_BRANCH"
else
    echo "  [WARN] Current DuckLake branch is '$DUCKLAKE_BRANCH', expected '$EXPECTED_BRANCH'."
    echo "         Continuing anyway -- the extension_config.cmake will be verified separately."
fi

# ---------------------------------------------------------------------------
# Step 1: Initialize submodules if needed
# ---------------------------------------------------------------------------

print_step "Step 1: Initializing submodules (if needed)"

if [ ! -d "duckdb/.git" ] && [ ! -f "duckdb/.git" ]; then
    print_info "Submodules not initialized. Running git submodule update --init --recursive..."
    git submodule update --init --recursive
    print_ok "Submodules initialized."
else
    print_ok "Submodules already initialized."
fi

# ---------------------------------------------------------------------------
# Step 2: Configure DuckDB submodule
# ---------------------------------------------------------------------------

print_step "Step 2: Configuring DuckDB submodule"

DUCKDB_BRANCH="claude/increase-duckdb-block-size-bJaNw"

print_info "Fetching branch '$DUCKDB_BRANCH' in duckdb submodule..."
cd duckdb
git fetch origin "$DUCKDB_BRANCH"

print_info "Checking out branch '$DUCKDB_BRANCH'..."
git checkout "$DUCKDB_BRANCH"

CURRENT_DUCKDB_BRANCH=$(git branch --show-current 2>/dev/null || git rev-parse --short HEAD)
if [ "$CURRENT_DUCKDB_BRANCH" = "$DUCKDB_BRANCH" ]; then
    print_ok "DuckDB submodule is on branch: $CURRENT_DUCKDB_BRANCH"
else
    die "Failed to checkout DuckDB branch '$DUCKDB_BRANCH'. Current: '$CURRENT_DUCKDB_BRANCH'"
fi

cd ..

# ---------------------------------------------------------------------------
# Step 3: Configure extension-ci-tools submodule
# ---------------------------------------------------------------------------

print_step "Step 3: Configuring extension-ci-tools submodule"

print_info "Checking out 'main' branch in extension-ci-tools submodule..."
cd extension-ci-tools
git checkout main

CURRENT_ECIT_BRANCH=$(git branch --show-current 2>/dev/null || git rev-parse --short HEAD)
print_ok "extension-ci-tools is on branch: $CURRENT_ECIT_BRANCH"

cd ..

# ---------------------------------------------------------------------------
# Step 4: Verify extension_config.cmake
# ---------------------------------------------------------------------------

print_step "Step 4: Verifying extension_config.cmake"

EXPECTED_HTTPFS_TAG="claude/duckdb-s3-streaming-writes-WCPED"

if grep -q "GIT_TAG ${EXPECTED_HTTPFS_TAG}" extension_config.cmake; then
    print_ok "extension_config.cmake references the correct httpfs branch: $EXPECTED_HTTPFS_TAG"
else
    die "extension_config.cmake does NOT contain 'GIT_TAG ${EXPECTED_HTTPFS_TAG}'." \
        "Make sure you are on the correct DuckLake branch."
fi

# Also verify tpch extension is included (needed for benchmark)
if grep -q "duckdb_extension_load(tpch)" extension_config.cmake; then
    print_ok "extension_config.cmake includes the tpch extension."
else
    die "extension_config.cmake does not include the tpch extension, which is required for the benchmark."
fi

# ---------------------------------------------------------------------------
# Step 5: Build DuckDB with the DuckLake extension (release mode)
# ---------------------------------------------------------------------------

print_step "Step 5: Building DuckDB with DuckLake extension (release mode)"

print_info "This may take a while..."
GEN=ninja make release

if [ -f "./build/release/duckdb" ]; then
    print_ok "DuckDB binary built successfully: ./build/release/duckdb"
else
    die "DuckDB binary not found at ./build/release/duckdb after build."
fi

# ---------------------------------------------------------------------------
# Step 6: Build the gofakes3 S3 server
# ---------------------------------------------------------------------------

print_step "Step 6: Building gofakes3 S3 server"

if [ ! -d "scripts/gofakes3" ]; then
    die "scripts/gofakes3 directory not found. Cannot build gofakes3."
fi

# Check that Go is available
if ! command -v go &>/dev/null; then
    die "Go is not installed or not in PATH. Please install Go to build gofakes3."
fi

print_info "Building gofakes3..."
cd scripts/gofakes3
go build -o ../../build/gofakes3 .
cd ../..

if [ -f "./build/gofakes3" ]; then
    print_ok "gofakes3 binary built successfully: ./build/gofakes3"
else
    die "gofakes3 binary not found at ./build/gofakes3 after build."
fi

# ---------------------------------------------------------------------------
# Step 7: Verify the build
# ---------------------------------------------------------------------------

print_step "Step 7: Verifying the build"

# Test DuckDB binary
print_info "Testing DuckDB binary..."
DUCKDB_OUTPUT=$(./build/release/duckdb -c "SELECT 42;" 2>&1)
if echo "$DUCKDB_OUTPUT" | grep -q "42"; then
    print_ok "DuckDB binary is working (SELECT 42 returned expected result)."
else
    die "DuckDB binary test failed. Output: $DUCKDB_OUTPUT"
fi

# Check that the gofakes3 binary exists and is executable
if [ -x "./build/gofakes3" ]; then
    print_ok "gofakes3 binary exists and is executable."
else
    die "gofakes3 binary is not executable."
fi

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------

print_step "Setup Complete"

echo ""
echo "  All components have been built and verified successfully."
echo ""
echo "  DuckDB binary:   ./build/release/duckdb"
echo "  gofakes3 binary: ./build/gofakes3"
echo ""
echo "  Run the benchmark with:"
echo "    python3 benchmark/tpch_block_rowgroup_benchmark.py --sf 1"
echo ""
