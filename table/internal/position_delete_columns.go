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

package internal

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
)

func PositionDeleteColumnIndices(schema *arrow.Schema) (int, int, error) {
	// Position-delete columns are resolved by their spec-defined names because
	// Arrow schemas read from external files do not always retain Iceberg IDs.
	requiredColumn := func(name string) (int, error) {
		indices := schema.FieldIndices(name)
		if len(indices) != 1 {
			return 0, fmt.Errorf("%w: position delete file must contain exactly one %q column, found %d",
				iceberg.ErrInvalidSchema, name, len(indices))
		}

		return indices[0], nil
	}

	filePathIndex, err := requiredColumn("file_path")
	if err != nil {
		return 0, 0, err
	}
	posIndex, err := requiredColumn("pos")
	if err != nil {
		return 0, 0, err
	}

	return filePathIndex, posIndex, nil
}

func FilePathValues(values arrow.Array) (arrow.TypedArray[string], error) {
	switch arr := values.(type) {
	case *array.Dictionary:
		wrapped, err := array.NewDictWrapper[string](arr)
		if err != nil {
			return nil, fmt.Errorf("%w: file_path column is not string: %w",
				iceberg.ErrInvalidSchema, err)
		}

		return wrapped, nil
	case arrow.TypedArray[string]:
		return arr, nil
	default:
		return nil, fmt.Errorf("%w: unsupported file_path column type %s in position delete file",
			iceberg.ErrInvalidSchema, values.DataType())
	}
}

func ValidatePositionDeletePositions(positions *array.Int64) error {
	for row := range positions.Len() {
		if position := positions.Value(row); position < 0 {
			return fmt.Errorf("%w: negative pos %d in position delete file",
				iceberg.ErrInvalidSchema, position)
		}
	}

	return nil
}
