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
	"errors"

	"github.com/apache/iceberg-go"
)

type MetadataBuilder struct{}

func (b *MetadataBuilder) AssignUUID(_ string) error {
	return errors.New("not implemented")
}

func (b *MetadataBuilder) UpgradeFormatVersion(_ int) error {
	return errors.New("not implemented")
}

func (b *MetadataBuilder) AddSchema(_ *iceberg.Schema) error {
	return errors.New("not implemented")
}

func (b *MetadataBuilder) SetLocation(_ string) error {
	return errors.New("not implemented")
}

func (b *MetadataBuilder) SetProperties(_ map[string]string) error {
	return errors.New("not implemented")
}

func (b *MetadataBuilder) RemoveProperties(_ []string) error {
	return errors.New("not implemented")
}

func (b *MetadataBuilder) AddVersion(_ *Version) error {
	return errors.New("not implemented")
}

func (b *MetadataBuilder) SetCurrentVersionID(_ int64) error {
	return errors.New("not implemented")
}
