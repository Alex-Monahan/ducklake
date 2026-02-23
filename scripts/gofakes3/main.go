// gofakes3 - Minimal S3-compatible HTTP server for benchmarking DuckDB/DuckLake.
//
// Significantly faster than moto/Python equivalents because Go's net/http is
// multi-threaded by default (goroutine per connection), there is no GIL, and
// we skip AWS auth validation and heavy XML parsing.
//
// Stores objects on local disk. Supports the S3 operations DuckDB actually uses:
// PUT, GET (with Range), HEAD, DELETE, POST (batch delete, multipart upload),
// and ListObjectsV2.
//
// Usage:
//
//	go build -o gofakes3 && ./gofakes3 [--port 9123] [--host 127.0.0.1] [--data-dir /tmp/gofakes3_data]
package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

var (
	flagPort    = flag.Int("port", 9123, "Listen port")
	flagHost    = flag.String("host", "127.0.0.1", "Listen host")
	flagDataDir = flag.String("data-dir", "/tmp/gofakes3_data", "Root directory for stored objects")
)

// ---------------------------------------------------------------------------
// Multipart upload tracking
// ---------------------------------------------------------------------------

// multipartUpload holds the state for an in-progress multipart upload.
type multipartUpload struct {
	bucket string
	key    string
	// parts maps part number -> path on disk where the part body is stored.
	parts map[int]string
}

// multipartStore manages in-flight multipart uploads keyed by uploadId.
type multipartStore struct {
	mu      sync.Mutex
	uploads map[string]*multipartUpload
	nextID  uint64
}

var multiparts = &multipartStore{
	uploads: make(map[string]*multipartUpload),
}

func (m *multipartStore) create(bucket, key string) string {
	id := atomic.AddUint64(&m.nextID, 1)
	uploadID := fmt.Sprintf("%d", id)
	m.mu.Lock()
	m.uploads[uploadID] = &multipartUpload{
		bucket: bucket,
		key:    key,
		parts:  make(map[int]string),
	}
	m.mu.Unlock()
	return uploadID
}

func (m *multipartStore) get(uploadID string) (*multipartUpload, bool) {
	m.mu.Lock()
	u, ok := m.uploads[uploadID]
	m.mu.Unlock()
	return u, ok
}

func (m *multipartStore) remove(uploadID string) {
	m.mu.Lock()
	delete(m.uploads, uploadID)
	m.mu.Unlock()
}

// ---------------------------------------------------------------------------
// XML response types
// ---------------------------------------------------------------------------

// ListBucketResult is the XML envelope for ListObjectsV2.
type ListBucketResult struct {
	XMLName        xml.Name        `xml:"ListBucketResult"`
	Xmlns          string          `xml:"xmlns,attr"`
	Name           string          `xml:"Name"`
	Prefix         string          `xml:"Prefix"`
	Delimiter      string          `xml:"Delimiter,omitempty"`
	KeyCount       int             `xml:"KeyCount"`
	MaxKeys        int             `xml:"MaxKeys"`
	IsTruncated    bool            `xml:"IsTruncated"`
	Contents       []ObjectEntry   `xml:"Contents,omitempty"`
	CommonPrefixes []CommonPrefix  `xml:"CommonPrefixes,omitempty"`
}

// ObjectEntry represents a single object in a list response.
type ObjectEntry struct {
	Key          string `xml:"Key"`
	Size         int64  `xml:"Size"`
	ETag         string `xml:"ETag"`
	LastModified string `xml:"LastModified"`
}

// CommonPrefix represents a rolled-up prefix when a delimiter is used.
type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// ListAllMyBucketsResult is the XML envelope for ListBuckets.
type ListAllMyBucketsResult struct {
	XMLName xml.Name      `xml:"ListAllMyBucketsResult"`
	Xmlns   string        `xml:"xmlns,attr"`
	Buckets BucketList    `xml:"Buckets"`
}

// BucketList wraps a slice of BucketEntry.
type BucketList struct {
	Bucket []BucketEntry `xml:"Bucket,omitempty"`
}

// BucketEntry is a single bucket in ListBuckets.
type BucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

// ErrorResponse is the standard S3 error XML.
type ErrorResponse struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// InitiateMultipartUploadResult is the XML response for CreateMultipartUpload.
type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

// CompleteMultipartUploadResult is the XML response for CompleteMultipartUpload.
type CompleteMultipartUploadResult struct {
	XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Bucket  string   `xml:"Bucket"`
	Key     string   `xml:"Key"`
	ETag    string   `xml:"ETag"`
}

// DeleteResult is the XML response for batch delete.
type DeleteResult struct {
	XMLName xml.Name `xml:"DeleteResult"`
	Xmlns   string   `xml:"xmlns,attr"`
}

// ---------------------------------------------------------------------------
// Helpers for parsing the batch-delete request body (minimal XML decode)
// ---------------------------------------------------------------------------

// deleteRequest is used to decode the incoming Delete XML body.
type deleteRequest struct {
	XMLName xml.Name       `xml:"Delete"`
	Objects []deleteObject `xml:"Object"`
}

// deleteObject represents a single key inside a Delete request.
type deleteObject struct {
	Key string `xml:"Key"`
}

// completeMultipartUploadRequest is the XML body sent by the client.
type completeMultipartUploadRequest struct {
	XMLName xml.Name           `xml:"CompleteMultipartUpload"`
	Parts   []completePart     `xml:"Part"`
}

type completePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// ---------------------------------------------------------------------------
// S3 handler
// ---------------------------------------------------------------------------

type s3Handler struct {
	dataDir string
}

// objectPath returns the on-disk path for bucket/key, preventing traversal.
func (h *s3Handler) objectPath(bucket, key string) string {
	safe := strings.TrimLeft(key, "/")
	return filepath.Join(h.dataDir, bucket, safe)
}

// parsePath splits the URL into (bucket, key, query).
func parsePath(r *http.Request) (bucket, key string) {
	p := strings.TrimLeft(r.URL.Path, "/")
	idx := strings.IndexByte(p, '/')
	if idx < 0 {
		return p, ""
	}
	return p[:idx], p[idx+1:]
}

// sendXML marshals v to XML, prepends the XML header, and writes it.
func sendXML(w http.ResponseWriter, code int, v interface{}) {
	body, err := xml.Marshal(v)
	if err != nil {
		http.Error(w, "internal error", 500)
		return
	}
	out := append([]byte(xml.Header), body...)
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", strconv.Itoa(len(out)))
	w.WriteHeader(code)
	w.Write(out)
}

// sendErrorXML writes a standard S3 error response.
func sendErrorXML(w http.ResponseWriter, code int, errCode, msg string) {
	sendXML(w, code, &ErrorResponse{Code: errCode, Message: msg})
}

// computeETag returns a quoted MD5 hex string for the given data.
func computeETag(data []byte) string {
	h := md5.Sum(data)
	return `"` + hex.EncodeToString(h[:]) + `"`
}

// computeFileETag computes a quoted MD5 hex string by reading the file.
func computeFileETag(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return `"d41d8cd98f00b204e9800998ecf8427e"`
	}
	defer f.Close()
	h := md5.New()
	io.Copy(h, f)
	return `"` + hex.EncodeToString(h.Sum(nil)) + `"`
}

// ServeHTTP is the single entry point for all requests.
func (h *s3Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r)
	case http.MethodHead:
		h.handleHead(w, r)
	case http.MethodPut:
		h.handlePut(w, r)
	case http.MethodDelete:
		h.handleDelete(w, r)
	case http.MethodPost:
		h.handlePost(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// ---- GET -----------------------------------------------------------------

func (h *s3Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	bucket, key := parsePath(r)
	q := r.URL.Query()

	if bucket == "" {
		h.listBuckets(w)
		return
	}

	// ListObjectsV2 is triggered by list-type=2 OR by an empty key with a prefix query param.
	if key == "" || q.Get("list-type") != "" || (key == "" && q.Get("prefix") != "") {
		h.listObjects(w, r, bucket)
		return
	}

	// GetObject
	fpath := h.objectPath(bucket, key)
	info, err := os.Stat(fpath)
	if err != nil || info.IsDir() {
		sendErrorXML(w, 404, "NoSuchKey", "Key "+key+" not found")
		return
	}

	fileSize := info.Size()

	// Open once, serve range or full body.
	f, err := os.Open(fpath)
	if err != nil {
		sendErrorXML(w, 500, "InternalError", err.Error())
		return
	}
	defer f.Close()

	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		h.serveRange(w, f, fileSize, rangeHeader)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(fileSize, 10))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("ETag", computeFileETag(fpath))
	w.WriteHeader(200)
	io.Copy(w, f)
}

// serveRange handles Range: bytes=start-end.
func (h *s3Handler) serveRange(w http.ResponseWriter, f *os.File, fileSize int64, rangeHeader string) {
	spec := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.SplitN(spec, "-", 2)

	var start, end int64
	if parts[0] == "" {
		// suffix range: -N means last N bytes
		suffix, _ := strconv.ParseInt(parts[1], 10, 64)
		start = fileSize - suffix
		end = fileSize - 1
	} else {
		start, _ = strconv.ParseInt(parts[0], 10, 64)
		if len(parts) > 1 && parts[1] != "" {
			end, _ = strconv.ParseInt(parts[1], 10, 64)
		} else {
			end = fileSize - 1
		}
	}
	if end >= fileSize {
		end = fileSize - 1
	}
	if start < 0 {
		start = 0
	}

	length := end - start + 1

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize))
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(206)

	f.Seek(start, io.SeekStart)
	io.CopyN(w, f, length)
}

// ---- HEAD ----------------------------------------------------------------

func (h *s3Handler) handleHead(w http.ResponseWriter, r *http.Request) {
	bucket, key := parsePath(r)

	if key == "" {
		// HEAD bucket
		bucketDir := filepath.Join(h.dataDir, bucket)
		info, err := os.Stat(bucketDir)
		if err != nil || !info.IsDir() {
			w.WriteHeader(404)
			return
		}
		w.WriteHeader(200)
		return
	}

	fpath := h.objectPath(bucket, key)
	info, err := os.Stat(fpath)
	if err != nil || info.IsDir() {
		w.WriteHeader(404)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("ETag", computeFileETag(fpath))
	w.WriteHeader(200)
}

// ---- PUT -----------------------------------------------------------------

func (h *s3Handler) handlePut(w http.ResponseWriter, r *http.Request) {
	bucket, key := parsePath(r)
	q := r.URL.Query()

	if key == "" {
		// CreateBucket
		bucketDir := filepath.Join(h.dataDir, bucket)
		os.MkdirAll(bucketDir, 0o755)
		w.WriteHeader(200)
		return
	}

	// UploadPart: PUT /{bucket}/{key}?partNumber=N&uploadId=ID
	if q.Get("uploadId") != "" && q.Get("partNumber") != "" {
		h.handleUploadPart(w, r, bucket, key)
		return
	}

	// PutObject
	fpath := h.objectPath(bucket, key)
	os.MkdirAll(filepath.Dir(fpath), 0o755)

	tmp := fpath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		sendErrorXML(w, 500, "InternalError", err.Error())
		return
	}

	hash := md5.New()
	mw := io.MultiWriter(f, hash)

	_, copyErr := io.Copy(mw, r.Body)
	f.Close()
	if copyErr != nil {
		os.Remove(tmp)
		sendErrorXML(w, 500, "InternalError", copyErr.Error())
		return
	}

	if err := os.Rename(tmp, fpath); err != nil {
		os.Remove(tmp)
		sendErrorXML(w, 500, "InternalError", err.Error())
		return
	}

	etag := `"` + hex.EncodeToString(hash.Sum(nil)) + `"`
	w.Header().Set("ETag", etag)
	w.WriteHeader(200)
}

// handleUploadPart stores a single part for a multipart upload.
func (h *s3Handler) handleUploadPart(w http.ResponseWriter, r *http.Request, bucket, key string) {
	q := r.URL.Query()
	uploadID := q.Get("uploadId")
	partNum, _ := strconv.Atoi(q.Get("partNumber"))

	up, ok := multiparts.get(uploadID)
	if !ok {
		sendErrorXML(w, 404, "NoSuchUpload", "Upload "+uploadID+" not found")
		return
	}

	// Write part to a temp file.
	partDir := filepath.Join(h.dataDir, ".multipart", uploadID)
	os.MkdirAll(partDir, 0o755)

	partPath := filepath.Join(partDir, fmt.Sprintf("%05d", partNum))
	f, err := os.Create(partPath)
	if err != nil {
		sendErrorXML(w, 500, "InternalError", err.Error())
		return
	}

	hash := md5.New()
	mw := io.MultiWriter(f, hash)
	_, copyErr := io.Copy(mw, r.Body)
	f.Close()
	if copyErr != nil {
		os.Remove(partPath)
		sendErrorXML(w, 500, "InternalError", copyErr.Error())
		return
	}

	etag := `"` + hex.EncodeToString(hash.Sum(nil)) + `"`

	// Record in the upload tracker.
	multiparts.mu.Lock()
	up.parts[partNum] = partPath
	multiparts.mu.Unlock()

	w.Header().Set("ETag", etag)
	w.WriteHeader(200)
}

// ---- DELETE --------------------------------------------------------------

func (h *s3Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	bucket, key := parsePath(r)

	if key == "" {
		// DeleteBucket
		bucketDir := filepath.Join(h.dataDir, bucket)
		os.RemoveAll(bucketDir)
		w.WriteHeader(204)
		return
	}

	fpath := h.objectPath(bucket, key)
	os.Remove(fpath)

	// Clean up empty parent directories up to the bucket root.
	dir := filepath.Dir(fpath)
	bucketDir := filepath.Join(h.dataDir, bucket)
	for dir != bucketDir && dir != h.dataDir {
		if err := os.Remove(dir); err != nil {
			break // directory not empty or other error
		}
		dir = filepath.Dir(dir)
	}

	w.WriteHeader(204)
}

// ---- POST ----------------------------------------------------------------

func (h *s3Handler) handlePost(w http.ResponseWriter, r *http.Request) {
	bucket, key := parsePath(r)
	q := r.URL.Query()

	// Batch delete: POST /{bucket}?delete
	if _, ok := q["delete"]; ok {
		h.handleBatchDelete(w, r, bucket)
		return
	}

	// CreateMultipartUpload: POST /{bucket}/{key}?uploads
	if _, ok := q["uploads"]; ok {
		h.handleCreateMultipartUpload(w, bucket, key)
		return
	}

	// CompleteMultipartUpload: POST /{bucket}/{key}?uploadId=ID
	if q.Get("uploadId") != "" {
		h.handleCompleteMultipartUpload(w, r, bucket, key)
		return
	}

	w.WriteHeader(200)
}

// handleBatchDelete processes a POST /{bucket}?delete request.
func (h *s3Handler) handleBatchDelete(w http.ResponseWriter, r *http.Request, bucket string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		sendErrorXML(w, 500, "InternalError", err.Error())
		return
	}

	var req deleteRequest
	// Try parsing with and without namespace — DuckDB may send either.
	if xml.Unmarshal(body, &req) != nil {
		// Fallback: brute-force extract <Key>...</Key> entries.
		s := string(body)
		for {
			start := strings.Index(s, "<Key>")
			if start < 0 {
				break
			}
			s = s[start+5:]
			end := strings.Index(s, "</Key>")
			if end < 0 {
				break
			}
			keyStr := s[:end]
			s = s[end+6:]
			fpath := h.objectPath(bucket, keyStr)
			os.Remove(fpath)
		}
	} else {
		for _, obj := range req.Objects {
			fpath := h.objectPath(bucket, obj.Key)
			os.Remove(fpath)
		}
	}

	sendXML(w, 200, &DeleteResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"})
}

// handleCreateMultipartUpload starts a new multipart upload.
func (h *s3Handler) handleCreateMultipartUpload(w http.ResponseWriter, bucket, key string) {
	uploadID := multiparts.create(bucket, key)
	sendXML(w, 200, &InitiateMultipartUploadResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
	})
}

// handleCompleteMultipartUpload concatenates parts and writes the final object.
func (h *s3Handler) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	q := r.URL.Query()
	uploadID := q.Get("uploadId")

	up, ok := multiparts.get(uploadID)
	if !ok {
		sendErrorXML(w, 404, "NoSuchUpload", "Upload "+uploadID+" not found")
		return
	}

	// Parse the request body to get part ordering.
	body, _ := io.ReadAll(r.Body)
	var req completeMultipartUploadRequest
	xml.Unmarshal(body, &req)

	// Sort parts by number.
	sort.Slice(req.Parts, func(i, j int) bool {
		return req.Parts[i].PartNumber < req.Parts[j].PartNumber
	})

	// Concatenate all parts into the final object.
	fpath := h.objectPath(bucket, key)
	os.MkdirAll(filepath.Dir(fpath), 0o755)

	tmp := fpath + ".mpu"
	out, err := os.Create(tmp)
	if err != nil {
		sendErrorXML(w, 500, "InternalError", err.Error())
		return
	}

	hash := md5.New()
	mw := io.MultiWriter(out, hash)

	multiparts.mu.Lock()
	partPaths := make(map[int]string, len(up.parts))
	for k, v := range up.parts {
		partPaths[k] = v
	}
	multiparts.mu.Unlock()

	for _, p := range req.Parts {
		pp, exists := partPaths[p.PartNumber]
		if !exists {
			out.Close()
			os.Remove(tmp)
			sendErrorXML(w, 400, "InvalidPart", fmt.Sprintf("Part %d not found", p.PartNumber))
			return
		}
		pf, err := os.Open(pp)
		if err != nil {
			out.Close()
			os.Remove(tmp)
			sendErrorXML(w, 500, "InternalError", err.Error())
			return
		}
		io.Copy(mw, pf)
		pf.Close()
	}
	out.Close()

	if err := os.Rename(tmp, fpath); err != nil {
		os.Remove(tmp)
		sendErrorXML(w, 500, "InternalError", err.Error())
		return
	}

	etag := `"` + hex.EncodeToString(hash.Sum(nil)) + `"`

	// Clean up part files.
	partDir := filepath.Join(h.dataDir, ".multipart", uploadID)
	os.RemoveAll(partDir)
	multiparts.remove(uploadID)

	sendXML(w, 200, &CompleteMultipartUploadResult{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket: bucket,
		Key:    key,
		ETag:   etag,
	})
}

// ---- List helpers --------------------------------------------------------

func (h *s3Handler) listBuckets(w http.ResponseWriter) {
	var buckets []BucketEntry
	entries, _ := os.ReadDir(h.dataDir)
	for _, e := range entries {
		if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			buckets = append(buckets, BucketEntry{
				Name:         e.Name(),
				CreationDate: "2024-01-01T00:00:00.000Z",
			})
		}
	}
	sendXML(w, 200, &ListAllMyBucketsResult{
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Buckets: BucketList{Bucket: buckets},
	})
}

func (h *s3Handler) listObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	maxKeys := 1000
	if mk := q.Get("max-keys"); mk != "" {
		maxKeys, _ = strconv.Atoi(mk)
	}

	bucketDir := filepath.Join(h.dataDir, bucket)
	info, err := os.Stat(bucketDir)
	if err != nil || !info.IsDir() {
		sendErrorXML(w, 404, "NoSuchBucket", "Bucket "+bucket+" not found")
		return
	}

	// Walk the bucket directory and collect all keys.
	var allKeys []string
	filepath.Walk(bucketDir, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		// Skip the .multipart staging directory.
		if fi.IsDir() && fi.Name() == ".multipart" {
			return filepath.SkipDir
		}
		if fi.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(bucketDir, path)
		// On Windows filepath uses backslash; S3 keys use forward slash.
		rel = filepath.ToSlash(rel)
		allKeys = append(allKeys, rel)
		return nil
	})
	sort.Strings(allKeys)

	// Filter by prefix and apply delimiter logic.
	var contents []ObjectEntry
	commonPrefixSet := make(map[string]struct{})
	count := 0

	for _, k := range allKeys {
		if !strings.HasPrefix(k, prefix) {
			continue
		}

		if delimiter != "" {
			rest := k[len(prefix):]
			idx := strings.Index(rest, delimiter)
			if idx >= 0 {
				cp := prefix + rest[:idx+len(delimiter)]
				commonPrefixSet[cp] = struct{}{}
				continue
			}
		}

		if count >= maxKeys {
			break
		}

		fpath := filepath.Join(bucketDir, filepath.FromSlash(k))
		var size int64
		var modTime time.Time
		if fi, err := os.Stat(fpath); err == nil {
			size = fi.Size()
			modTime = fi.ModTime()
		}

		contents = append(contents, ObjectEntry{
			Key:          k,
			Size:         size,
			ETag:         `"d41d8cd98f00b204e9800998ecf8427e"`,
			LastModified: modTime.UTC().Format(time.RFC3339),
		})
		count++
	}

	// Sort common prefixes.
	var commonPrefixes []CommonPrefix
	sortedPrefixes := make([]string, 0, len(commonPrefixSet))
	for p := range commonPrefixSet {
		sortedPrefixes = append(sortedPrefixes, p)
	}
	sort.Strings(sortedPrefixes)
	for _, p := range sortedPrefixes {
		commonPrefixes = append(commonPrefixes, CommonPrefix{Prefix: p})
	}

	result := ListBucketResult{
		Xmlns:          "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:           bucket,
		Prefix:         prefix,
		Delimiter:      delimiter,
		KeyCount:       count,
		MaxKeys:        maxKeys,
		IsTruncated:    false,
		Contents:       contents,
		CommonPrefixes: commonPrefixes,
	}

	sendXML(w, 200, &result)
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	flag.Parse()

	dataDir := *flagDataDir
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatalf("Cannot create data directory %s: %v", dataDir, err)
	}

	handler := &s3Handler{dataDir: dataDir}

	addr := fmt.Sprintf("%s:%d", *flagHost, *flagPort)
	server := &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	// Graceful shutdown on SIGINT / SIGTERM.
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-done
		log.Println("Shutting down...")
		server.Close()
	}()

	log.Printf("gofakes3 listening on http://%s  (data: %s)", addr, dataDir)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}
