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
	"fmt"
	"github.com/apache/iceberg-go"
)

type UpdateSpec struct {
	txn                   *Transaction
	nameToField           map[string]iceberg.PartitionField
	nameToAddedField      map[string]iceberg.PartitionField
	transformToField      map[transformKey]iceberg.PartitionField
	transformToAddedField map[transformKey]iceberg.PartitionField
	renames               map[string]string
	addedTimeFields       map[int]iceberg.PartitionField
	caseSensitive         bool
	adds                  []iceberg.PartitionField
	deletes               map[int]bool
	lastAssignedFieldId   int
}

func NewUpdateSpec(t *Transaction, caseSensitive bool) *UpdateSpec {
	transformToField := make(map[transformKey]iceberg.PartitionField)
	nameToField := make(map[string]iceberg.PartitionField)
	partitionSpec := t.tbl.Metadata().PartitionSpec()
	for partitionField := range partitionSpec.Fields() {
		transformToField[transformKey{
			FieldID:   partitionField.SourceID,
			Transform: partitionField.Transform.String(),
		}] = partitionField
		nameToField[partitionField.Name] = partitionField
	}

	return &UpdateSpec{
		txn:                   t,
		nameToField:           nameToField,
		nameToAddedField:      make(map[string]iceberg.PartitionField),
		transformToField:      transformToField,
		transformToAddedField: make(map[transformKey]iceberg.PartitionField),
		renames:               make(map[string]string),
		addedTimeFields:       make(map[int]iceberg.PartitionField),
		caseSensitive:         caseSensitive,
		adds:                  make([]iceberg.PartitionField, 0),
		deletes:               make(map[int]bool),
		lastAssignedFieldId:   partitionSpec.LastAssignedFieldID(),
	}
}

type transformKey struct {
	FieldID   int
	Transform string
}

func (us *UpdateSpec) AddField(sourceColName string, transform iceberg.Transform, partitionFieldName string) (*UpdateSpec, error) {
	// Finds the column in the schema and binds it with case sensitivity.
	ref := iceberg.Reference(sourceColName)
	boundTerm, err := ref.Bind(us.txn.tbl.Schema(), us.caseSensitive)
	if err != nil {
		return nil, err
	}

	// Validate the transform
	outputType := boundTerm.Type()
	if !transform.CanTransform(outputType) {
		return nil, fmt.Errorf("%s cannot transform %s values from %s", transform.String(), outputType.String(), boundTerm.Ref().Field().Name)
	}

	// Check for duplicate transform on same column
	key := transformKey{
		FieldID:   boundTerm.Ref().Field().ID,
		Transform: transform.String(),
	}
	existingPartitionField, exists := us.transformToField[key]
	if exists && us.isDuplicatePartition(transform, existingPartitionField) {
		return nil, fmt.Errorf("duplicate partition field for %s=%v, %v already exists", ref.String(), ref, existingPartitionField)
	}

	// Check if this transform was already added
	added, exists := us.transformToAddedField[key]
	if exists {
		return nil, fmt.Errorf("already added partition: %s ", added.Name)
	}

	// Create the new partition field and Check for name collisions
	// with existing fields
	newField := us.partitionField(key, partitionFieldName)
	if _, exists = us.nameToAddedField[newField.Name]; exists {
		return nil, fmt.Errorf("already added partition field with name: %s", newField.Name)
	}

	// Handle special case for time transforms
	if _, isTimeTransform := newField.Transform.(iceberg.TimeTransform); isTimeTransform {
		if existingTimeField, exists := us.addedTimeFields[newField.SourceID]; exists {
			return nil, fmt.Errorf("cannot add time partition field: %s conflicts with %s", newField.Name, existingTimeField.Name)
		}
		us.addedTimeFields[newField.SourceID] = newField
	}
	us.transformToAddedField[key] = newField

	// If name matches an existing field, rename it (if VOID)
	existingPartitionField, exists = us.nameToField[newField.Name]
	if _, inDelete := us.deletes[existingPartitionField.FieldID]; exists && !inDelete {
		if _, isVoidTransform := existingPartitionField.Transform.(iceberg.VoidTransform); isVoidTransform {
			_, err = us.RenameField(existingPartitionField.Name, fmt.Sprintf("%s_%d", existingPartitionField.Name, existingPartitionField.FieldID))
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("cannot add duplicate partition field name: %s", existingPartitionField.Name)
		}
	}

	// Register the new field
	us.nameToAddedField[newField.Name] = newField
	us.adds = append(us.adds, newField)
	return us, nil
}

func (us *UpdateSpec) RenameField(name string, newName string) (*UpdateSpec, error) {
	return nil, nil
}

func (us *UpdateSpec) Apply() *iceberg.PartitionSpec {
	partitionFields := make([]iceberg.PartitionField, 0)
	partitionNames := make(map[string]bool)
	spec := us.txn.tbl.Metadata().PartitionSpec()
	for field := range spec.Fields() {
		var newField iceberg.PartitionField
		var err error
		if _, deleted := us.deletes[field.FieldID]; !deleted {
			if rename, renamed := us.renames[field.Name]; renamed {
				newField, err = us.addNewField(us.txn.tbl.Schema(), field.SourceID, field.FieldID, rename, field.Transform, partitionNames)
			} else {
				newField, err = us.addNewField(us.txn.tbl.Schema(), field.SourceID, field.FieldID, field.Name, field.Transform, partitionNames)
			}
		} else if us.txn.tbl.Metadata().Version() == 1 {
			if rename, renamed := us.renames[field.Name]; renamed {
				newField, err = us.addNewField(us.txn.tbl.Schema(), field.SourceID, field.FieldID, rename, iceberg.VoidTransform{}, partitionNames)
			} else {
				newField, err = us.addNewField(us.txn.tbl.Schema(), field.SourceID, field.FieldID, field.Name, iceberg.VoidTransform{}, partitionNames)
			}
		}
		if err != nil {
			return nil
		}
		partitionFields = append(partitionFields, newField)
	}

	for _, field := range us.adds {
		newField := iceberg.PartitionField{
			SourceID:  field.SourceID,
			FieldID:   field.FieldID,
			Name:      field.Name,
			Transform: field.Transform,
		}
		partitionFields = append(partitionFields, newField)
	}

	newSpec := iceberg.NewPartitionSpec(partitionFields...)
	newSpecId := iceberg.InitialPartitionSpecID
	for _, spec = range us.txn.tbl.Metadata().PartitionSpecs() {
		if newSpec.CompatibleWith(&spec) {
			newSpecId = spec.ID()
			break
		} else if newSpecId <= spec.ID() {
			newSpecId = spec.ID() + 1
		}
	}
	newSpec = iceberg.NewPartitionSpecID(newSpecId, partitionFields...)
	return &newSpec
}

func (us *UpdateSpec) Commit() ([]Update, []Requirement, error) {
	return nil, nil, nil
}

func (us *UpdateSpec) partitionField(key transformKey, name string) iceberg.PartitionField {
	if us.txn.tbl.Metadata().Version() == 2 {
		fmt.Println("table metadata version is 2")
	}
	newFieldId := us.newFieldId()
	transform, _ := iceberg.ParseTransform(key.Transform)

	return iceberg.PartitionField{
		SourceID:  key.FieldID,
		FieldID:   newFieldId,
		Name:      name,
		Transform: transform,
	}
}

func (us *UpdateSpec) newFieldId() int {
	us.lastAssignedFieldId += 1
	return us.lastAssignedFieldId
}

func (us *UpdateSpec) isDuplicatePartition(transform iceberg.Transform, partitionField iceberg.PartitionField) bool {
	_, exists := us.deletes[partitionField.FieldID]
	return !exists && transform == partitionField.Transform
}

func (us *UpdateSpec) checkAndAddPartitionName(schema *iceberg.Schema, name string, sourceId int, partitionNames map[string]bool) error {
	field, found := schema.FindFieldByName(name)
	if found && field.ID != sourceId {
		return fmt.Errorf("cannot create partition from name that exists in schema %s", name)
	}
	if _, exists := partitionNames[name]; exists {
		return fmt.Errorf("partition name has to be unique: %s", name)
	}
	partitionNames[name] = true
	return nil
}

func (us *UpdateSpec) addNewField(schema *iceberg.Schema, sourceId int, fieldId int, name string, transform iceberg.Transform, partitionNames map[string]bool) (iceberg.PartitionField, error) {
	err := us.checkAndAddPartitionName(schema, name, sourceId, partitionNames)
	if err != nil {
		return iceberg.PartitionField{}, err
	}
	return iceberg.PartitionField{
		SourceID:  sourceId,
		FieldID:   fieldId,
		Name:      name,
		Transform: transform,
	}, nil
}
