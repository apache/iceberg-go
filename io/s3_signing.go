package io

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// RemoteSigningRequest represents the request sent to the remote signer
type RemoteSigningRequest struct {
	Method  string              `json:"method"`
	URI     string              `json:"uri"`
	Headers map[string][]string `json:"headers,omitempty"`
	Region  string              `json:"region"`
}

// RemoteSigningResponse represents the response from the remote signer
type RemoteSigningResponse struct {
	Headers map[string][]string `json:"headers"`
}

// RemoteSigningTransport wraps an HTTP transport to handle remote signing
type RemoteSigningTransport struct {
	base           http.RoundTripper
	signerURI      string
	signerEndpoint string
	region         string
	authToken      string
	client         *http.Client
}

// NewRemoteSigningTransport creates a new remote signing transport
func NewRemoteSigningTransport(base http.RoundTripper, signerURI, signerEndpoint, region, authToken, timeoutStr string) *RemoteSigningTransport {

	timeout := 30 // default timeout in seconds
	if t, err := strconv.Atoi(timeoutStr); timeoutStr != "" && err == nil {
		timeout = t
	}

	return &RemoteSigningTransport{
		base:           base,
		signerURI:      signerURI,
		signerEndpoint: signerEndpoint,
		region:         region,
		authToken:      authToken,
		client: &http.Client{
			Timeout: time.Duration(timeout) * time.Second,
		},
	}
}

// RoundTrip implements http.RoundTripper
func (r *RemoteSigningTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	log.Printf("RoundTrip: Processing request to %s %s\n", req.Method, req.URL.String())

	isS3 := r.isS3Request(req)
	log.Printf("RoundTrip: isS3Request result: %v\n", isS3)

	// Only handle S3 requests
	if !isS3 {
		log.Printf("RoundTrip: Not an S3 request, using base transport\n")
		return r.base.RoundTrip(req)
	}

	log.Printf("RoundTrip: Handling S3 request with remote signing\n")

	// Check if this is a chunked upload that might cause compatibility issues
	originalHeaders := r.extractHeaders(req)
	log.Printf("RoundTrip: Original headers count: %d\n", len(originalHeaders))

	if contentEncoding, ok := originalHeaders["Content-Encoding"]; ok && len(contentEncoding) > 0 && contentEncoding[0] == "aws-chunked" {
		log.Printf("RoundTrip: Detected AWS chunked encoding, processing...\n")
		// The problem is that the AWS SDK has already applied chunked encoding to the body.
		// We need to decode the chunked body and create a new request with the original content.

		// Read the entire chunked body
		bodyBytes, err := io.ReadAll(req.Body)
		if err != nil {
			log.Printf("RoundTrip: ERROR reading chunked body: %v\n", err)
			return nil, fmt.Errorf("failed to read chunked body: %w", err)
		}
		req.Body.Close()

		// Decode the AWS chunked body
		decodedBody, err := decodeAWSChunkedBody(bodyBytes)
		if err != nil {
			log.Printf("RoundTrip: WARNING: Failed to decode chunked body, using as-is: %v\n", err)
			// If decoding fails, try to use the body as-is
			decodedBody = bodyBytes
		}

		log.Printf("RoundTrip: Decoded body length: %d\n", len(decodedBody))

		// Clone the request with the decoded body
		newReq := req.Clone(req.Context())
		newReq.Body = io.NopCloser(bytes.NewReader(decodedBody))
		newReq.ContentLength = int64(len(decodedBody))

		// Remove chunked-specific headers
		newReq.Header.Del("Content-Encoding")
		newReq.Header.Del("X-Amz-Decoded-Content-Length")
		newReq.Header.Del("X-Amz-Trailer")
		newReq.Header.Set("Content-Length", strconv.Itoa(len(decodedBody)))

		// Set UNSIGNED-PAYLOAD
		newReq.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

		// Get headers for signing
		headersForSigning := r.extractHeaders(newReq)

		log.Printf("RoundTrip: Getting remote signature for chunked request\n")
		// Get remote signature
		signedHeaders, err := r.getRemoteSignature(newReq.Context(), newReq.Method, newReq.URL.String(), headersForSigning)
		if err != nil {
			log.Printf("RoundTrip: ERROR: Failed to get remote signature: %s\n", err.Error())
			return nil, fmt.Errorf("failed to get remote signature: %w", err)
		}

		log.Printf("RoundTrip: Received %d signed headers\n", len(signedHeaders))

		// Apply signed headers
		for key, value := range signedHeaders {
			canonicalKey := http.CanonicalHeaderKey(key)
			newReq.Header.Set(canonicalKey, value)
		}

		// Execute the new non-chunked request
		log.Printf("RoundTrip: Executing decoded request\n")
		return r.base.RoundTrip(newReq)
	}

	log.Printf("RoundTrip: Processing non-chunked request\n")
	// For non-chunked requests, use the normal flow with header preprocessing
	headersForSigning := make(map[string][]string)
	for key, values := range originalHeaders {
		headersForSigning[key] = values
	}

	log.Printf("RoundTrip: Getting remote signature for non-chunked request\n")
	signedHeaders, err := r.getRemoteSignature(req.Context(), req.Method, req.URL.String(), headersForSigning)
	if err != nil {
		log.Printf("RoundTrip: ERROR: Failed to get remote signature: %s\n", err.Error())
		return nil, fmt.Errorf("failed to get remote signature: %w", err)
	}

	log.Printf("RoundTrip: Received %d signed headers\n", len(signedHeaders))

	// Clone the request and apply signed headers
	newReq := req.Clone(req.Context())

	// Apply signed headers
	for key, value := range signedHeaders {
		canonicalKey := http.CanonicalHeaderKey(key)
		newReq.Header.Set(canonicalKey, value)
	}

	// Execute the request and check for errors
	log.Printf("RoundTrip: Executing signed request\n")
	resp, err := r.base.RoundTrip(newReq)
	if err != nil {
		log.Printf("RoundTrip: ERROR executing request: %v\n", err)
		return nil, err
	}

	log.Printf("RoundTrip: Request completed with status: %s\n", resp.Status)
	return resp, nil
}

// isS3Request checks if the request is destined for S3
func (r *RemoteSigningTransport) isS3Request(req *http.Request) bool {
	// Check if the host contains typical S3 patterns
	host := req.URL.Host
	log.Printf("isS3Request: Checking host: %s, URL: %s\n", host, req.URL.String())

	// Don't sign requests to the remote signer itself to avoid circular dependency
	if r.signerURI != "" {
		signerHost := ""
		if signerURL, err := url.Parse(r.signerURI); err == nil {
			signerHost = signerURL.Host
		}
		if host == signerHost {
			log.Printf("isS3Request: Host matches signer host (%s), not signing\n", signerHost)
			return false
		}
	}

	if host == "" {
		log.Printf("isS3Request: Empty host, not signing\n")
		return false
	}

	// S3 compatible storage might be hosted on a different TLD
	isAmazon := strings.HasSuffix(host, ".amazonaws.com")
	isCloudflare := strings.HasSuffix(host, ".r2.cloudflarestorage.com")
	isGoogleCloudStorage := host == "storage.googleapis.com" || strings.HasSuffix(host, ".storage.googleapis.com")

	log.Printf("isS3Request: Checks - isAmazon: %v, isCloudflare: %v, isGoogleCloudStorage: %v\n", isAmazon, isCloudflare, isGoogleCloudStorage)

	if isCloudflare || isGoogleCloudStorage {
		log.Printf("isS3Request: Recognized as S3-compatible storage (Cloudflare or GCS)\n")
		return true
	}

	if isAmazon {
		// More robust check for various S3 endpoint formats
		// - s3.amazonaws.com (global)
		// - s3.<region>.amazonaws.com (regional path-style)
		// - <bucket>.s3.amazonaws.com (virtual-hosted, us-east-1)
		// - <bucket>.s3.<region>.amazonaws.com (virtual-hosted, other regions)
		// - <bucket>.s3-accelerate.amazonaws.com (transfer acceleration)
		// - s3.dualstack.<region>.amazonaws.com (dual-stack path-style)
		// - <bucket>.s3.dualstack.<region>.amazonaws.com (dual-stack virtual-hosted)
		isS3Pattern := strings.Contains(host, ".s3") || strings.HasPrefix(host, "s3.")
		log.Printf("isS3Request: Amazon host detected, S3 pattern check: %v\n", isS3Pattern)
		return isS3Pattern
	}

	// MinIO or other custom S3-compatible endpoints (be more conservative)
	if host == "localhost:9000" || host == "127.0.0.1:9000" {
		log.Printf("isS3Request: Recognized as MinIO endpoint\n")
		return true
	}

	log.Printf("isS3Request: Host not recognized as S3-compatible: %s\n", host)
	return false
}

// extractHeaders extracts relevant headers from the request
func (r *RemoteSigningTransport) extractHeaders(req *http.Request) map[string][]string {
	headers := make(map[string][]string)
	for key, values := range req.Header {
		if len(values) > 0 {
			headers[key] = values
		}
	}
	return headers
}

// decodeAWSChunkedBody decodes AWS chunked transfer encoding
func decodeAWSChunkedBody(chunkedData []byte) ([]byte, error) {
	// AWS chunked format starts with hex size followed by ";chunk-signature="
	// Example: "8a2;chunk-signature=..."
	// But sometimes it's just hex size followed by \r\n
	str := string(chunkedData)

	// Check for simple chunked format (no signature)
	if len(chunkedData) > 5 {
		// Look for pattern like "8a0\r\n"
		firstLine := ""
		for i, b := range chunkedData {
			if b == '\r' && i+1 < len(chunkedData) && chunkedData[i+1] == '\n' {
				firstLine = string(chunkedData[:i])
				break
			}
			if i > 10 {
				break
			}
		}

		// Try to parse as hex
		if firstLine != "" {
			if _, err := strconv.ParseInt(firstLine, 16, 64); err == nil {
				// fmt.Printf("decodeAWSChunkedBody: Detected simple chunk format, first chunk size: %d (0x%s)\n", size, firstLine)
				// This is a simple chunked format
				return decodeSimpleChunkedBody(chunkedData)
			}
		}
	}

	if !strings.Contains(str, ";chunk-signature=") {
		return nil, fmt.Errorf("data does not appear to be AWS chunked format")
	}

	var decoded bytes.Buffer
	reader := bytes.NewReader(chunkedData)

	for {
		// Read the chunk header line
		headerLine, err := readLine(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk header: %w", err)
		}

		// Parse chunk size line (format: <size-in-hex>;chunk-signature=<signature>)
		if !strings.Contains(headerLine, ";") {
			// Not a valid chunk header
			continue
		}

		parts := strings.Split(headerLine, ";")
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid chunk header: %s", headerLine)
		}

		// Parse hex size
		sizeStr := parts[0]
		size, err := strconv.ParseInt(sizeStr, 16, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse chunk size '%s': %w", sizeStr, err)
		}

		// If size is 0, we've reached the end
		if size == 0 {
			break
		}

		// Read the chunk data
		chunkData := make([]byte, size)
		n, err := io.ReadFull(reader, chunkData)
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk data of size %d: %w", size, err)
		}
		if int64(n) != size {
			return nil, fmt.Errorf("chunk size mismatch: expected %d, got %d", size, n)
		}

		decoded.Write(chunkData)

		// Skip the trailing \r\n after chunk data
		trailer := make([]byte, 2)
		_, err = reader.Read(trailer)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read chunk trailer: %w", err)
		}
	}

	return decoded.Bytes(), nil
}

// decodeSimpleChunkedBody decodes simple HTTP chunked transfer encoding (without AWS signatures)
func decodeSimpleChunkedBody(chunkedData []byte) ([]byte, error) {
	var decoded bytes.Buffer
	reader := bytes.NewReader(chunkedData)

	for {
		// Read the chunk size line
		line, err := readLine(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk size: %w", err)
		}

		// Parse hex size
		size, err := strconv.ParseInt(line, 16, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse chunk size '%s': %w", line, err)
		}

		// If size is 0, we've reached the end
		if size == 0 {
			break
		}

		// Read the chunk data
		chunkData := make([]byte, size)
		n, err := io.ReadFull(reader, chunkData)
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk data of size %d: %w", size, err)
		}
		if int64(n) != size {
			return nil, fmt.Errorf("chunk size mismatch: expected %d, got %d", size, n)
		}

		decoded.Write(chunkData)

		// Skip the trailing \r\n after chunk data
		trailer := make([]byte, 2)
		_, err = reader.Read(trailer)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read chunk trailer: %w", err)
		}
	}

	return decoded.Bytes(), nil
}

// readLine reads a line terminated by \r\n from the reader
func readLine(reader *bytes.Reader) (string, error) {
	var line bytes.Buffer
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return "", err
		}
		if b == '\r' {
			// Peek at next byte
			next, err := reader.ReadByte()
			if err == nil && next == '\n' {
				// Found \r\n
				return line.String(), nil
			}
			// Not \r\n, put back the byte
			if err == nil {
				reader.UnreadByte()
			}
		}
		line.WriteByte(b)
	}
}

// getRemoteSignature sends a request to the remote signer and returns signed headers
func (r *RemoteSigningTransport) getRemoteSignature(ctx context.Context, method, uri string, headers map[string][]string) (map[string]string, error) {
	reqBody := RemoteSigningRequest{
		Method:  method,
		URI:     uri,
		Headers: headers,
		Region:  r.region,
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal signing request: %w", err)
	}

	// Combine base URI with endpoint path
	signerURL := r.signerURI
	if r.signerEndpoint != "" {
		// Ensure proper URL joining - handle trailing/leading slashes
		baseURL := strings.TrimRight(r.signerURI, "/")
		endpoint := strings.TrimLeft(r.signerEndpoint, "/")
		signerURL = baseURL + "/" + endpoint
	}

	req, err := http.NewRequestWithContext(ctx, "POST", signerURL, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create signer request to %s: %w", signerURL, err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Add authentication token if configured
	if r.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+r.authToken)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to contact remote signer at %s: %w", signerURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		// Read the response body for better error diagnostics
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("remote signer at %s returned status %d (failed to read response body: %v)", signerURL, resp.StatusCode, readErr)
		}
		// Log error for debugging if needed

		// Provide detailed error information based on status code
		switch resp.StatusCode {
		case 401:
			return nil, fmt.Errorf("remote signer authentication failed (401) at %s: %s", signerURL, string(body))
		case 403:
			return nil, fmt.Errorf("remote signer authorization denied (403) at %s: %s. Check that the signer service has proper AWS credentials and permissions for the target resource. Request was: %s", signerURL, string(body), string(payload))
		case 404:
			return nil, fmt.Errorf("remote signer endpoint not found (404) at %s: %s. Check the signer URI configuration", signerURL, string(body))
		case 500:
			return nil, fmt.Errorf("remote signer internal error (500) at %s: %s", signerURL, string(body))
		default:
			return nil, fmt.Errorf("remote signer at %s returned status %d: %s", signerURL, resp.StatusCode, string(body))
		}
	}

	var signingResponse RemoteSigningResponse
	if err := json.NewDecoder(resp.Body).Decode(&signingResponse); err != nil {
		return nil, fmt.Errorf("failed to decode signer response from %s: %w", signerURL, err)
	}

	// Convert headers from []string to string (take the first value for each header)
	resultHeaders := make(map[string]string)
	for key, values := range signingResponse.Headers {
		if len(values) > 0 {
			resultHeaders[key] = values[0]
		}
	}

	// Handle x-amz-content-sha256 header based on signer response
	signerSha256 := ""
	if signerSha256Values, ok := signingResponse.Headers["x-amz-content-sha256"]; ok && len(signerSha256Values) > 0 {
		signerSha256 = signerSha256Values[0]
	}

	// Check if this is a chunked upload (has Content-Encoding: aws-chunked)
	isChunkedUpload := false
	if contentEncoding, ok := headers["Content-Encoding"]; ok && len(contentEncoding) > 0 {
		isChunkedUpload = contentEncoding[0] == "aws-chunked"
	}

	if isChunkedUpload {
		// For chunked uploads, we should have pre-processed the headers to avoid conflicts
		// Use the signer's x-amz-content-sha256 if available
		if signerSha256 != "" {
			resultHeaders["X-Amz-Content-Sha256"] = signerSha256
			// Use signer's x-amz-content-sha256 for pre-processed chunked upload
		}
	} else {
		// For non-chunked requests, use the signer's x-amz-content-sha256 if available
		if signerSha256 != "" {
			resultHeaders["X-Amz-Content-Sha256"] = signerSha256
			// Use signer's x-amz-content-sha256
		}
	}

	// The signer might return 'authorization' (lowercase). We need to ensure
	// this is used for the canonical 'Authorization' header.
	if auth, ok := signingResponse.Headers["authorization"]; ok && len(auth) > 0 {
		resultHeaders["Authorization"] = auth[0]
		// Use signer's authorization header
	} else if auth, ok := signingResponse.Headers["Authorization"]; ok && len(auth) > 0 {
		resultHeaders["Authorization"] = auth[0]
		// Use signer's Authorization header
	}

	// Preserve the original date header from the signer if available
	if signerDate, ok := signingResponse.Headers["x-amz-date"]; ok && len(signerDate) > 0 {
		resultHeaders["X-Amz-Date"] = signerDate[0]
		// Use signer's x-amz-date
	} else if signerDate, ok := signingResponse.Headers["X-Amz-Date"]; ok && len(signerDate) > 0 {
		resultHeaders["X-Amz-Date"] = signerDate[0]
		// Use signer's X-Amz-Date
	}

	// Return the signed headers

	// Validate header consistency for chunked uploads
	if isChunkedUpload {
		contentSha256 := resultHeaders["X-Amz-Content-Sha256"]
		if contentSha256 == "UNSIGNED-PAYLOAD" {
			// Successfully using UNSIGNED-PAYLOAD with pre-processed headers
		} else {
			// Using custom content sha256 for pre-processed chunked upload
		}
	}

	return resultHeaders, nil
}
