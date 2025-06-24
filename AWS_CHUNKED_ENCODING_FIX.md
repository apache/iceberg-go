# AWS Chunked Transfer Encoding Fix for AVRO Files

## Problem

When reading Iceberg manifest files (AVRO format) from AWS S3, the following error can occur:

```
decoder: unexpected error: Meta: reading map[string][]uint8: avro: ReadSTRING: invalid string length
```

This happens because AWS S3 sometimes returns responses with chunked transfer encoding, especially for AVRO files. The AVRO decoder expects raw bytes but receives chunked-encoded data with hex size prefixes, causing parsing errors.

## Solution

The fix adds a transparent chunked encoding handler that:

1. **Automatically detects** if the content is using AWS chunked encoding
2. **Transparently decodes** the chunked data before passing it to the AVRO decoder
3. **Passes through** raw content unchanged if no chunking is detected

### Implementation Details

#### New Files Added:

1. **`io/chunked_reader.go`** - Core implementation of the chunked reader
   - Detects chunked encoding by looking for hex size prefixes
   - Handles AWS chunk extensions (e.g., `chunk-signature`)
   - Supports both small and large chunks
   - Implements standard `io.Reader` interface

2. **`io/chunked_reader_test.go`** - Comprehensive tests
   - Tests raw AVRO content passthrough
   - Tests simple chunked content
   - Tests AWS-style chunked content with extensions
   - Tests partial reads

#### Modified Files:

1. **`io/blob.go`** - Integration with the blob file system
   - Automatically wraps AVRO files with `ChunkedReader`
   - Maintains compatibility with existing `ReadAt` operations
   - Adds logging for debugging

### How It Works

When an AVRO file is opened through the blob file system:

```go
// Before: Raw reader that fails on chunked content
reader, _ := fs.Open("s3://bucket/manifest.avro")

// After: Automatically wrapped with chunked decoder
// This happens transparently inside Open() for .avro files
```

The chunked reader detects the encoding format:

1. **Raw AVRO**: Starts with magic bytes `Obj\x01` - passed through unchanged
2. **Chunked**: Starts with hex size like `1a3;chunk-signature=...` - decoded transparently

### AWS Chunked Format

AWS uses HTTP chunked transfer encoding with extensions:

```
400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n
[1024 bytes of data]\r\n
0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n
\r\n
```

The implementation handles:
- Hex chunk sizes
- Chunk extensions (semicolon-separated metadata)
- Trailing headers after the final chunk
- Proper CRLF handling

## Testing

Run the tests to verify the implementation:

```bash
cd /Users/fritz/__/Git/iceberg-go
go test -v ./io -run TestChunkedReader
```

## Debugging

The implementation includes debug logging. When an AVRO file is opened, you'll see:

```
blobFileIO.Open: Opening AVRO file s3://bucket/manifest.avro with chunked reader wrapper
ChunkedReader: Detected AWS chunked encoding (hex size: 1a3)
```

Or for raw content:

```
ChunkedReader: Detected raw AVRO content (magic bytes), not using chunked decoding
```

## Future Improvements

1. The fix currently only applies to files with `.avro` extension. It could be extended to detect AVRO content by magic bytes.
2. Performance could be optimized by buffering larger chunks.
3. The detection logic could be made more sophisticated to handle edge cases.

## References

- [AWS S3 Chunked Transfer Encoding](https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html)
- [Apache Avro Specification](https://avro.apache.org/docs/current/spec.html)
- [Iceberg Manifest Files](https://iceberg.apache.org/spec/#manifest-files)