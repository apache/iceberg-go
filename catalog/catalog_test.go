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

package catalog

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetLocationForTable(t *testing.T) {
	type args struct {
		location        string
		defaultLocation string
		database        string
		tableName       string
	}

	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "should return location if location is provided",
			args: args{
				location:        "s3://new-bucket/test-table",
				defaultLocation: "s3://test-bucket",
				database:        "test-database",
				tableName:       "test-table",
			},
			want:    "s3://new-bucket/test-table",
			wantErr: false,
		},
		{
			name: "should return default location with generated path if location is not provided",
			args: args{
				location:        "",
				defaultLocation: "s3://test-bucket/test-prefix/",
				database:        "test-database",
				tableName:       "test-table",
			},
			want:    "s3://test-bucket/test-prefix/test-database.db/test-table",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getLocationForTable(tt.args.location, tt.args.defaultLocation, tt.args.database, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("tableS3Location() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got.String() != tt.want {
				t.Errorf("tableS3Location() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetMetadataPath(t *testing.T) {
	type args struct {
		locationPath string
		version      int
	}

	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "should return metadata location with version 0",
			args: args{
				locationPath: "/test-table/",
				version:      1,
			},
			want:    "^test-table/metadata/00001-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}.metadata.json$",
			wantErr: false,
		},
		{
			name: "should return metadata location with version 1",
			args: args{
				locationPath: "/test-table/",
				version:      0,
			},
			want:    "^test-table/metadata/00000-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}.metadata.json$",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getMetadataPath(tt.args.locationPath, tt.args.version)
			if (err != nil) != tt.wantErr {
				t.Errorf("getMetadataPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			require.Regexp(t, regexp.MustCompile(tt.want), got)
		})
	}
}
