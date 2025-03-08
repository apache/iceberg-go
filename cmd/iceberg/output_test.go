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

package main

import (
	"bytes"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/pterm/pterm"
	"github.com/stretchr/testify/assert"
)

var testArgs = []struct {
	meta     string
	expected string
}{
	{
		`{
    "format-version": 2,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    "location": "s3://bucket/test/location",
    "last-sequence-number": 0,
    "last-updated-ms": 1602638573590,
    "last-column-id": 3,
    "current-schema-id": 0,
    "schemas": [
        {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]},
        {
            "type": "struct",
            "fields": [
                {"id": 1, "name": "x", "required": true, "type": "long"}
            ]
        }
    ],
    "default-spec-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "last-partition-id": 1000,
    "default-sort-order-id": 0,
    "sort-orders": [
        {
            "order-id": 0,
            "fields": [ ]
        }
    ],
    "properties": {"read.split.target.size": "134217728"},
    "current-snapshot-id": -1,
    "snapshots": [ ],
    "snapshot-log": [ ],
    "metadata-log": [ ],
    "refs": { }
}`,
		`Table format version | 2                                   
Metadata location    |                                     
Table UUID           | 9c12d441-03fe-4693-9a96-a0705ddf69c1
Last updated         | 1602638573590                       
Sort Order           | 0: []                               
Partition Spec       | []                                  

Current Schema, id=0
└──1: x: required long

Current Snapshot | 

Snapshots

Properties
key                    | value    
----------------------------------
read.split.target.size | 134217728

`,
	},
	{
		`{
    "format-version": 2,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    "location": "s3://bucket/test/location",
    "last-sequence-number": 34,
    "last-updated-ms": 1602638573590,
    "last-column-id": 3,
    "current-schema-id": 1,
    "schemas": [
        {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]},
        {
            "type": "struct",
            "schema-id": 1,
            "identifier-field-ids": [1, 2],
            "fields": [
                {"id": 1, "name": "x", "required": true, "type": "long"},
                {"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"},
                {"id": 3, "name": "z", "required": true, "type": "long"}
            ]
        }
    ],
    "default-spec-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
    "last-partition-id": 1000,
    "default-sort-order-id": 3,
    "sort-orders": [
        {
            "order-id": 3,
            "fields": [
                {"transform": "identity", "source-id": 2, "direction": "asc", "null-order": "nulls-first"},
                {"transform": "bucket[4]", "source-id": 3, "direction": "desc", "null-order": "nulls-last"}
            ]
        }
    ],
    "properties": {"read.split.target.size": "134217728"},
    "current-snapshot-id": 3055729675574597004,
    "snapshots": [
        {
            "snapshot-id": 3051729675574597004,
            "timestamp-ms": 1515100955770,
            "sequence-number": 0,
            "summary": {"operation": "append"},
            "manifest-list": "s3://a/b/1.avro",
	    "schema-id": 1
        },
        {
            "snapshot-id": 3055729675574597004,
            "parent-snapshot-id": 3051729675574597004,
            "timestamp-ms": 1555100955770,
            "sequence-number": 1,
            "summary": {"operation": "append"},
            "manifest-list": "s3://a/b/2.avro",
            "schema-id": 1
        }
    ],
    "snapshot-log": [
        {"snapshot-id": 3051729675574597004, "timestamp-ms": 1515100955770},
        {"snapshot-id": 3055729675574597004, "timestamp-ms": 1555100955770}
    ],
    "metadata-log": [{"metadata-file": "s3://bucket/.../v1.json", "timestamp-ms": 1515100}],
    "refs": {"test": {"snapshot-id": 3051729675574597004, "type": "tag", "max-ref-age-ms": 10000000}}
}`,
		`Table format version | 2                                   
Metadata location    |                                     
Table UUID           | 9c12d441-03fe-4693-9a96-a0705ddf69c1
Last updated         | 1602638573590                       
Sort Order           | 3: [                                
                     | 2 asc nulls-first                   
                     | bucket[4](3) desc nulls-last        
                     | ]                                   
Partition Spec       | [                                   
                     | 	1000: x: identity(1)                
                     | ]                                   

Current Schema, id=1
├──1: x: required long
├──2: y: required long (comment)
└──3: z: required long

Current Snapshot | append, {}: id=3055729675574597004, parent_id=3051729675574597004, schema_id=1, sequence_number=1, timestamp_ms=1555100955770, manifest_list=s3://a/b/2.avro

Snapshots
├──Snapshot 3051729675574597004, schema 1: s3://a/b/1.avro
└──Snapshot 3055729675574597004, schema 1: s3://a/b/2.avro

Properties
key                    | value    
----------------------------------
read.split.target.size | 134217728

`,
	},
}

func TestDescribeTable(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()

	for _, tt := range testArgs {
		meta, _ := table.ParseMetadataBytes([]byte(tt.meta))
		table := table.New([]string{"t"}, meta, "", nil, nil)
		buf.Reset()

		text{}.DescribeTable(table)

		assert.Equal(t, tt.expected, buf.String())
	}
}
