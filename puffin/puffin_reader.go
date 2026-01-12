package puffin

import "io"

type PuffinReader struct {
	r           io.ReaderAt
	size        int64
	footerStart int64
	footerRead  bool
}

func NewPuffinReader(r io.ReaderAt, size int64) *PuffinReader {
	return &PuffinReader{
		r:           r,
		size:        size,
		footerStart: size - 4,
	}
}

func (r *PuffinReader) ReadFooter() {}

func (r *PuffinReader) ReadAllBlobs() {}

// ReadBlob reads the content of one specific blob.
func (r *PuffinReader) ReadBlob() {}

func validateMetadata() {}
