## Core tools / skills
* Build: When building the project, use `GEN=ninja make release`
* Test: When testing the project, use `./build/release/test/unittest "test/path/*"` with `test/path/*` replaced with the desired test directory. Multiple sets of tests can be run by subagents in parallel.
* Format: After all tests pass, format the code. Activate the Python environment in .venv and run make format with `source .venv/bin/activate && make format`

## Testing Guidelines

* Write many tests.
* Test with different types, especially numerics, strings and complex nested types.
* Try to test unexpected/incorrect usage as well, instead of only the happy path.
* Look at the code coverage report of your branch and attempt to cover all code paths in the fast unit tests. Attempt to trigger exceptions as well. It is acceptable to have some exceptions not triggered (e.g. out of memory exceptions or type switch exceptions), but large branches of code should always be either covered or removed.
* If testing sort logic, include the sort expression as an output column to make validation easier.

## C++ Guidelines

* C++ must be backward compatible to C++11 and must be forward compatible to C++17. 
* Prefer the use of smart pointers.
* Strongly prefer the use of `unique_ptr`. Only use `shared_ptr` if you **absolutely** have to.
* Use `const` whenever possible.
* Use `#include ` when bringing in other files
* All functions in source files in the core (`src` directory) should be part of the `duckdb` namespace.
* When overriding a virtual method, avoid repeating virtual and always use `override` or `final`.
* Use `[u]int(8|16|32|64)_t` like `uint8_t` or `int64_t`. Use `idx_t` for offsets/indices/counts of any kind.
* Prefer using references as arguments.
* Use `const` references for arguments of non-trivial objects (e.g. `std::vector`, ...).
* Use C++11 for loops when possible: `for (const auto& item : items) {...}`
* Use braces for indenting `if` statements and loops. Avoid single-line if statements and loops, especially nested ones.
* **Class Layout:** Start out with a `public` block containing the constructor and public variables, followed by a `public` block containing public methods of the class. After that follow any private functions and private variables. For example:
    ```cpp
    class MyClass {
    public:
    	MyClass();

    	int my_public_variable;

    public:
    	void MyFunction();

    private:
    	void MyPrivateFunction();

    private:
    	int my_private_variable;
    };
    ```
* Avoid [unnamed magic numbers](https://en.wikipedia.org/wiki/Magic_number_(programming)). Instead, use named variables that are stored in a `constexpr`.
* [Return early](https://medium.com/swlh/return-early-pattern-3d18a41bba8). Avoid deep nested branches.
* Do not include commented out code blocks in pull requests.

## Error Handling

* Use exceptions **only** when an error is encountered that terminates a query (e.g. parser error, table not found). Exceptions should only be used for **exceptional** situations. For regular errors that do not break the execution flow (e.g. errors you **expect** might occur) use a return value instead.
* Try to add test cases that trigger exceptions. If an exception cannot be easily triggered using a test case then it should probably be an assertion. This is not always true (e.g. out of memory errors are exceptions, but are very hard to trigger).
* Use `D_ASSERT` to assert. Use **assert** only when failing the assert means a programmer error. Assert should never be triggered by user input. Avoid code like `D_ASSERT(a > b + 3);` without comments or context.
* Assert liberally, but make it clear with comments next to the assert what went wrong when the assert is triggered.

## Naming Conventions

* Choose descriptive names. Use multi-letter variable names.
* Files: lowercase separated by underscores, e.g., abstract_operator.cpp
* Types (classes, structs, enums, typedefs, using): CamelCase starting with uppercase letter, e.g., BaseColumn
* Variables: lowercase separated by underscores, e.g., chunk_size
* Functions: CamelCase starting with uppercase letter, e.g., GetChunk
* Prefer to use e.g. **column_idx**, **check_idx** or similar for indexes in nested loops. In a **non-nested** loop it is permissible to use **i** as iterator index.
* These rules are partially enforced by `clang-tidy`.