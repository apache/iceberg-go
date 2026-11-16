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

package view

import (
	"context"
	"errors"
	"io"
	"maps"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func metadataForWrite(t *testing.T, location string, props iceberg.Properties) Metadata {
	t.Helper()
	base, err := ParseMetadataString(exampleViewJSON)
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(base)
	require.NoError(t, err)
	result, err := builder.SetLoc(location).SetProperties(props).Build()
	require.NoError(t, err)

	return result.Metadata
}

func TestWriteMetadataLocation(t *testing.T) {
	for _, test := range []struct {
		name      string
		location  string
		directory string
		want      string
	}{
		{"default", "mem://view-write/default", "", "mem://view-write/default/metadata"},
		{"trailing slash", "mem://view-write/trailing/", "", "mem://view-write/trailing/metadata"},
		{"custom directory", "mem://view-write/custom", "mem://other-bucket/custom", "mem://other-bucket/custom"},
		{"custom trailing slash", "mem://view-write/custom", "mem://other-bucket/custom/", "mem://other-bucket/custom"},
		{"literal cloud path", "mem://view-write/my view%20#?", "", "mem://view-write/my view%20#?/metadata"},
		{"literal custom path", "mem://view-write/custom", "mem://other-bucket/my metadata%20#?", "mem://other-bucket/my metadata%20#?"},
		{"literal dot segments", "mem://view-write/a//./b/../v/", "", "mem://view-write/a//./b/../v/metadata"},
	} {
		t.Run(test.name, func(t *testing.T) {
			metadata := metadataForWrite(t, test.location, iceberg.Properties{table.WriteMetadataPathKey: test.directory})
			location, fs, err := WriteMetadata(t.Context(), metadata, nil)
			require.NoError(t, err)
			require.Equal(t, test.want, location[:strings.LastIndex(location, "/")])
			name := path.Base(location)
			require.True(t, strings.HasPrefix(name, "view-"))
			require.True(t, strings.HasSuffix(name, ".metadata.json"))
			_, err = uuid.Parse(strings.TrimSuffix(strings.TrimPrefix(name, "view-"), ".metadata.json"))
			require.NoError(t, err)

			loaded, err := NewFromLocation(t.Context(), table.Identifier{"ns", "v"}, location,
				func(context.Context) (icebergio.IO, error) { return fs, nil })
			require.NoError(t, err)
			require.True(t, metadata.Equals(loaded.Metadata()))
			secondLocation, _, err := WriteMetadata(t.Context(), metadata, nil)
			require.NoError(t, err)
			require.NotEqual(t, location, secondLocation)
		})
	}
}

func TestWriteMetadataLocalPaths(t *testing.T) {
	for _, name := range []string{"view metadata", "literal%20view", "view#fragment", "view?query"} {
		t.Run(name, func(t *testing.T) {
			if strings.Contains(name, "?") && os.PathSeparator == '\\' {
				t.Skip("question marks are not valid in Windows filenames")
			}
			t.Chdir(t.TempDir())
			root := name
			for _, custom := range []bool{false, true} {
				props := iceberg.Properties{}
				directory := root + "/metadata"
				if custom {
					directory = root + "/custom metadata"
					props[table.WriteMetadataPathKey] = directory
				}
				location, fs, err := WriteMetadata(t.Context(), metadataForWrite(t, root, props), nil)
				require.NoError(t, err)
				require.Equal(t, directory, location[:strings.LastIndex(location, "/")])
				_, err = os.Stat(location)
				require.NoError(t, err)
				require.NoError(t, fs.Remove(t.Context(), location))
			}
		})
	}
}

func TestCreateViewUsesMetadataPathAndSeparateIOProperties(t *testing.T) {
	const scheme = "view-metadata-path"
	fs := icebergio.NewMemFS()
	ioProps := iceberg.Properties{"credential": "private", table.WriteMetadataPathKey: "unused://io-property"}
	metadataProps := iceberg.Properties{table.WriteMetadataPathKey: scheme + "://metadata-bucket/custom/", "comment": "kept"}
	wantIOProps, wantMetadataProps := maps.Clone(ioProps), maps.Clone(metadataProps)
	icebergio.Register(scheme, func(_ context.Context, parsed *url.URL, props map[string]string) (icebergio.IO, error) {
		require.Equal(t, "metadata-bucket", parsed.Host)
		require.Equal(t, wantIOProps, iceberg.Properties(props))

		return fs, nil
	})
	t.Cleanup(func() { icebergio.Unregister(scheme) })

	created, err := CreateViewWithIOProperties(t.Context(), "catalog", table.Identifier{"ns", "v"}, newTestSchema(0),
		Representation{SQL: "select 1", Dialect: "spark"}, table.Identifier{"ns"},
		"unused://view-bucket/v", ioProps, metadataProps)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(created.MetadataLocation(), scheme+"://metadata-bucket/custom/view-"))
	loaded, err := NewFromLocation(t.Context(), created.Identifier(), created.MetadataLocation(),
		func(context.Context) (icebergio.IO, error) { return fs, nil })
	require.NoError(t, err)
	require.Equal(t, wantMetadataProps, loaded.Properties())
	require.NotContains(t, loaded.Properties(), "credential")
	require.Equal(t, wantIOProps, ioProps)
	require.Equal(t, wantMetadataProps, metadataProps)
}

type unmarshalableViewMetadata struct {
	Metadata
	err error
}

func (m unmarshalableViewMetadata) MarshalJSON() ([]byte, error) { return nil, m.err }

func TestWriteMetadataRejectsInvalidInputs(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	marshalErr := errors.New("marshal failed")
	for _, test := range []struct {
		name     string
		ctx      context.Context
		metadata Metadata
		wantErr  error
	}{
		{"nil metadata", t.Context(), nil, ErrInvalidViewMetadata},
		{"canceled", ctx, metadataForWrite(t, "unused://view/v", nil), context.Canceled},
		{"invalid location", t.Context(), metadataForWrite(t, "://invalid", nil), nil},
		{"invalid metadata directory", t.Context(), metadataForWrite(t, "unused://view/v", iceberg.Properties{table.WriteMetadataPathKey: "://invalid"}), nil},
		{"marshal failure", t.Context(), unmarshalableViewMetadata{metadataForWrite(t, "unused://view/v", nil), marshalErr}, marshalErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			location, fs, err := WriteMetadata(test.ctx, test.metadata, nil)
			require.Error(t, err)
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
			}
			require.Empty(t, location)
			require.Nil(t, fs)
		})
	}
}

type readOnlyViewFS struct{ icebergio.IO }

func TestWriteMetadataFilesystemErrors(t *testing.T) {
	loadErr := errors.New("load failed")
	for _, test := range []struct {
		name string
		fs   icebergio.IO
		err  error
	}{
		{"load failure", nil, loadErr},
		{"read only", readOnlyViewFS{icebergio.NewMemFS()}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			const scheme = "view-metadata-fs-error"
			icebergio.Register(scheme, func(context.Context, *url.URL, map[string]string) (icebergio.IO, error) {
				return test.fs, test.err
			})
			t.Cleanup(func() { icebergio.Unregister(scheme) })
			location, fs, err := WriteMetadata(t.Context(), metadataForWrite(t, scheme+"://bucket/v", nil), nil)
			require.Error(t, err)
			if test.err != nil {
				require.ErrorIs(t, err, test.err)
			} else {
				require.ErrorContains(t, err, "does not support writing")
			}
			require.Empty(t, location)
			require.Nil(t, fs)
		})
	}
}

type viewMetadataWriter struct {
	icebergio.FileWriter
	cancel   context.CancelFunc
	writeErr error
	closeErr error
	short    bool
	closed   bool
}

func (w *viewMetadataWriter) Write(data []byte) (int, error) {
	if w.cancel != nil {
		w.cancel()
	}
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	if w.short {
		return 0, nil
	}

	return w.FileWriter.Write(data)
}

func (w *viewMetadataWriter) Close() error {
	w.closed = true

	return errors.Join(w.FileWriter.Close(), w.closeErr)
}

type viewMetadataFS struct {
	icebergio.LocalFS
	writer         *viewMetadataWriter
	createErr      error
	removeErr      error
	removeCtx      context.Context
	closedAtRemove bool
}

func (fs *viewMetadataFS) Create(ctx context.Context, location string) (icebergio.FileWriter, error) {
	if fs.createErr != nil {
		return nil, fs.createErr
	}
	writer, err := fs.LocalFS.Create(ctx, location)
	if err != nil {
		return nil, err
	}
	fs.writer.FileWriter = writer

	return fs.writer, nil
}

func (fs *viewMetadataFS) Remove(ctx context.Context, location string) error {
	fs.removeCtx = ctx
	fs.closedAtRemove = fs.writer.closed

	return errors.Join(fs.LocalFS.Remove(ctx, location), fs.removeErr)
}

func TestWriteViewMetadataCleansFailedWrites(t *testing.T) {
	writeErr, closeErr, removeErr := errors.New("write failed"), errors.New("close failed"), errors.New("remove failed")
	for _, test := range []struct {
		name      string
		writeErr  error
		closeErr  error
		removeErr error
		short     bool
		wantErr   error
	}{
		{"write", writeErr, nil, nil, false, writeErr},
		{"close", nil, closeErr, nil, false, closeErr},
		{"short write", nil, nil, nil, true, io.ErrShortWrite},
		{"write and close", writeErr, closeErr, nil, false, writeErr},
		{"cleanup error", writeErr, nil, removeErr, false, writeErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			fs := &viewMetadataFS{
				writer:    &viewMetadataWriter{cancel: cancel, writeErr: test.writeErr, closeErr: test.closeErr, short: test.short},
				removeErr: test.removeErr,
			}
			location := filepath.Join(t.TempDir(), "metadata.json")
			err := writeViewMetadata(ctx, fs, location, []byte("metadata"))
			require.ErrorIs(t, err, test.wantErr)
			if test.closeErr != nil {
				require.ErrorIs(t, err, test.closeErr)
			}
			if test.removeErr != nil {
				require.ErrorIs(t, err, test.removeErr)
			}
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			require.NotNil(t, fs.removeCtx)
			require.NoError(t, fs.removeCtx.Err())
			require.True(t, fs.closedAtRemove, "the writer must close before cleanup")
			_, err = os.Stat(location)
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestWriteViewMetadataCreateError(t *testing.T) {
	createErr := errors.New("create failed")
	fs := &viewMetadataFS{createErr: createErr}
	err := writeViewMetadata(t.Context(), fs, filepath.Join(t.TempDir(), "metadata.json"), []byte("metadata"))
	require.ErrorIs(t, err, createErr)
	require.Nil(t, fs.removeCtx, "cleanup starts only after a writer is created")
}
