// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
)

type manifestClusterKey struct {
	specID int
	value  any
}

type manifestClusterWriter struct {
	writer     *iceberg.ManifestWriter
	path       string
	counter    *internal.CountingWriter
	fileCloser io.Closer
	hasEntries bool
	manifests  []iceberg.ManifestFile
}

func (w *manifestClusterWriter) close() (mf iceberg.ManifestFile, err error) {
	if w.writer == nil {
		return nil, nil
	}

	writer := w.writer
	w.writer = nil
	defer func() {
		if w.fileCloser != nil {
			err = errors.Join(err, w.fileCloser.Close())
			w.fileCloser = nil
		}
	}()

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return writer.ToManifestFile(w.path, w.counter.Count)
}

func (w *manifestClusterWriter) abort() {
	if w.writer != nil {
		_ = w.writer.Close()
		w.writer = nil
	}
	if w.fileCloser != nil {
		_ = w.fileCloser.Close()
		w.fileCloser = nil
	}
}

func (m *manifestMergeManager) newClusterWriter(specID int) (*manifestClusterWriter, error) {
	spec, err := m.snap.spec(specID)
	if err != nil {
		return nil, err
	}

	writer, path, counter, fileCloser, err := m.snap.newManifestWriter(spec)
	if err != nil {
		return nil, err
	}

	return &manifestClusterWriter{
		writer:     writer,
		path:       path,
		counter:    counter,
		fileCloser: fileCloser,
	}, nil
}

func validateManifestClusterKey(key any) error {
	if key == nil {
		return errors.New("manifest cluster key must be non-nil")
	}

	typ := reflect.TypeOf(key)
	if !typ.Comparable() {
		return fmt.Errorf("manifest cluster key type %s is not comparable", typ)
	}

	value := reflect.ValueOf(key)
	switch value.Kind() {
	case reflect.Chan, reflect.Pointer, reflect.UnsafePointer:
		if value.IsNil() {
			return errors.New("manifest cluster key must be non-nil")
		}
	}

	return nil
}

// clusterManifests rewrites entries into one rolling writer per cluster key and
// partition spec. Writers stay open while entries for other keys are read so a
// later file with the same key is still written beside the earlier files.
func (m *manifestMergeManager) clusterManifests(manifests []iceberg.ManifestFile) ([]iceberg.ManifestFile, error) {
	writers := make(map[manifestClusterKey]*manifestClusterWriter)
	order := make([]manifestClusterKey, 0)
	paths := make([]string, 0)
	completed := false

	defer func() {
		if completed {
			return
		}

		for _, writer := range writers {
			writer.abort()
		}
		for _, path := range paths {
			if removeErr := m.snap.io.Remove(path); removeErr != nil {
				log.Printf("Warning: failed to delete orphaned clustered manifest %s: %v", path, removeErr)
			}
		}
	}()

	closeWriter := func(writer *manifestClusterWriter) error {
		manifest, closeErr := writer.close()
		if closeErr != nil {
			return closeErr
		}
		if manifest != nil {
			writer.manifests = append(writer.manifests, manifest)
		}

		return nil
	}

	for _, manifest := range manifests {
		specID := int(manifest.PartitionSpecID())
		for entry, entryErr := range m.snap.iterManifestEntries(manifest, true) {
			if entryErr != nil {
				return nil, entryErr
			}

			clusterValue := m.clusterBy(entry.DataFile())
			if clusterErr := validateManifestClusterKey(clusterValue); clusterErr != nil {
				return nil, fmt.Errorf("cluster data file %q: %w", entry.DataFile().FilePath(), clusterErr)
			}
			key := manifestClusterKey{specID: specID, value: clusterValue}
			writer, ok := writers[key]
			if !ok {
				writer, entryErr = m.newClusterWriter(specID)
				if entryErr != nil {
					return nil, entryErr
				}
				writers[key] = writer
				order = append(order, key)
				paths = append(paths, writer.path)
			}

			if writer.writer != nil && writer.hasEntries && m.targetSizeBytes > 0 && writer.counter.Count >= m.targetSizeBytes {
				if entryErr := closeWriter(writer); entryErr != nil {
					return nil, entryErr
				}
			}
			if writer.writer == nil {
				next, openErr := m.newClusterWriter(specID)
				if openErr != nil {
					return nil, openErr
				}
				writer.writer = next.writer
				writer.path = next.path
				writer.counter = next.counter
				writer.fileCloser = next.fileCloser
				writer.hasEntries = false
				paths = append(paths, writer.path)
			}

			if err := writer.writer.Existing(entry); err != nil {
				return nil, err
			}
			writer.hasEntries = true
		}
	}

	for _, key := range order {
		writer := writers[key]
		if writer.writer == nil || !writer.hasEntries {
			continue
		}
		if err := closeWriter(writer); err != nil {
			return nil, err
		}
	}

	result := make([]iceberg.ManifestFile, 0)
	for _, key := range order {
		result = append(result, writers[key].manifests...)
	}

	completed = true

	return result, nil
}
