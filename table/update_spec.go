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
	"slices"

	"github.com/apache/iceberg-go"
)

// UpdateSpec implements a builder for evolving a table's partition specification.
//
// It accumulates a sequence of partition spec update operations (e.g., AddField, RemoveField, RenameField)
// which are applied during BuildUpdates.
//
// Use the builder methods to chain operations, and call BuildUpdates to apply them and produce the
// final set of partition fields and update requirements, or call Commit to apply the updates in the transaction.
type UpdateSpec struct {
	operations []updateSpecOp

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
type updateSpecOp func() error

type transformKey struct {
	SourceId  int
	Transform string
}

func NewUpdateSpec(t *Transaction, caseSensitive bool) *UpdateSpec {
	transformToField := make(map[transformKey]iceberg.PartitionField)
	nameToField := make(map[string]iceberg.PartitionField)
	partitionSpec := t.tbl.Metadata().PartitionSpec()
	for partitionField := range partitionSpec.Fields() {
		transformToField[transformKey{
			SourceId:  partitionField.SourceID,
			Transform: partitionField.Transform.String(),
		}] = partitionField
		nameToField[partitionField.Name] = partitionField
	}
	lastAssignedFieldId := t.tbl.Metadata().LastPartitionSpecID()
	if lastAssignedFieldId == nil {
		v := iceberg.PartitionDataIDStart - 1
		lastAssignedFieldId = &v
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
		lastAssignedFieldId:   *lastAssignedFieldId,
	}
}

func (us *UpdateSpec) AddField(sourceColName string, transform iceberg.Transform, partitionFieldName string) *UpdateSpec {
	us.operations = append(us.operations, us.addField(sourceColName, transform, partitionFieldName))

	return us
}

func (us *UpdateSpec) AddIdentity(sourceColName string) *UpdateSpec {
	return us.AddField(sourceColName, iceberg.IdentityTransform{}, "")
}

func (us *UpdateSpec) RemoveField(name string) *UpdateSpec {
	us.operations = append(us.operations, us.removeField(name))

	return us
}

func (us *UpdateSpec) RenameField(name string, newName string) *UpdateSpec {
	us.operations = append(us.operations, us.renameField(name, newName))

	return us
}

func (us *UpdateSpec) BuildUpdates() ([]Update, []Requirement, error) {
	for _, op := range us.operations {
		if err := op(); err != nil {
			return nil, nil, err
		}
	}

	newSpec, err := us.Apply()
	if err != nil {
		return nil, nil, err
	}
	updates := make([]Update, 0)
	requirements := make([]Requirement, 0)

	if us.txn.tbl.Metadata().DefaultPartitionSpec() != newSpec.ID() {
		if us.isNewPartitionSpec(newSpec.ID()) {
			updates = append(updates, NewAddPartitionSpecUpdate(&newSpec, false))
			updates = append(updates, NewSetDefaultSpecUpdate(-1))
		} else {
			updates = append(updates, NewSetDefaultSpecUpdate(newSpec.ID()))
		}
		requiredLastAssignedPartitionId := us.txn.tbl.Metadata().LastPartitionSpecID()
		requirements = append(requirements, AssertLastAssignedPartitionID(*requiredLastAssignedPartitionId))
	}

	return updates, requirements, nil
}

func (us *UpdateSpec) Apply() (iceberg.PartitionSpec, error) {
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
			if err != nil {
				return iceberg.PartitionSpec{}, err
			}
			partitionFields = append(partitionFields, newField)
		} else if us.txn.tbl.Metadata().Version() == 1 {
			if rename, renamed := us.renames[field.Name]; renamed {
				newField, err = us.addNewField(us.txn.tbl.Schema(), field.SourceID, field.FieldID, rename, iceberg.VoidTransform{}, partitionNames)
			} else {
				newField, err = us.addNewField(us.txn.tbl.Schema(), field.SourceID, field.FieldID, field.Name, iceberg.VoidTransform{}, partitionNames)
			}
			if err != nil {
				return iceberg.PartitionSpec{}, err
			}
			partitionFields = append(partitionFields, newField)
		}
	}

	partitionFields = append(partitionFields, us.adds...)
	opts := make([]iceberg.PartitionOption, len(partitionFields))
	for i, field := range partitionFields {
		opts[i] = iceberg.AddPartitionFieldBySourceID(field.SourceID, field.Name, field.Transform, us.txn.tbl.Schema(), &field.FieldID)
	}

	newSpec, err := iceberg.NewPartitionSpecOpts(opts...)
	if err != nil {
		return iceberg.PartitionSpec{}, err
	}
	newSpecId := iceberg.InitialPartitionSpecID
	for _, spec = range us.txn.tbl.Metadata().PartitionSpecs() {
		if newSpec.CompatibleWith(&spec) {
			newSpecId = spec.ID()

			break
		} else if newSpecId <= spec.ID() {
			newSpecId = spec.ID() + 1
		}
	}

	return iceberg.NewPartitionSpecID(newSpecId, partitionFields...), nil
}

func (us *UpdateSpec) Commit() error {
	updates, requirements, err := us.BuildUpdates()
	if err != nil {
		return err
	}

	if len(updates) == 0 {
		return nil
	}

	return us.txn.apply(updates, requirements)
}

func (us *UpdateSpec) addField(sourceColName string, transform iceberg.Transform, partitionFieldName string) updateSpecOp {
	return func() error {
		// Finds the column in the schema and binds it with case sensitivity.
		ref := iceberg.Reference(sourceColName)
		boundTerm, err := ref.Bind(us.txn.tbl.Schema(), us.caseSensitive)
		if err != nil {
			return err
		}

		// Validate the transform
		outputType := boundTerm.Type()
		if !transform.CanTransform(outputType) {
			return fmt.Errorf("%s cannot transform %s values from %s", transform.String(), outputType.String(), boundTerm.Ref().Field().Name)
		}

		// Check for duplicate partition on same source
		key := transformKey{
			SourceId:  boundTerm.Ref().Field().ID,
			Transform: transform.String(),
		}
		existingPartitionField, exists := us.transformToField[key]
		if exists && us.isDuplicatePartition(transform, existingPartitionField) {
			return fmt.Errorf("duplicate partition field for %s=%v, %v already exists", ref.String(), ref, existingPartitionField)
		}

		// Check if this transform was already added
		added, exists := us.transformToAddedField[key]
		if exists {
			return fmt.Errorf("already added partition: %s ", added.Name)
		}

		// Create the new partition field and Check for name collisions
		// with existing fields
		newField, err := us.partitionField(key, partitionFieldName)
		if err != nil {
			return err
		}
		if _, exists = us.nameToAddedField[newField.Name]; exists {
			return fmt.Errorf("already added partition field with name: %s", newField.Name)
		}

		// Handle special case for time transforms
		if _, isTimeTransform := newField.Transform.(iceberg.TimeTransform); isTimeTransform {
			if existingTimeField, exists := us.addedTimeFields[newField.SourceID]; exists {
				return fmt.Errorf("cannot add time partition field: %s conflicts with %s", newField.Name, existingTimeField.Name)
			}
			us.addedTimeFields[newField.SourceID] = newField
		}
		us.transformToAddedField[key] = newField

		// If name matches an existing field, rename it if it's VOID transform
		existingPartitionField, exists = us.nameToField[newField.Name]
		if _, inDelete := us.deletes[existingPartitionField.FieldID]; exists && !inDelete {
			if _, isVoidTransform := existingPartitionField.Transform.(iceberg.VoidTransform); isVoidTransform {
				if err := us.renameField(
					existingPartitionField.Name,
					fmt.Sprintf("%s_%d", existingPartitionField.Name, existingPartitionField.FieldID),
				)(); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("cannot add duplicate partition field name: %s", existingPartitionField.Name)
			}
		}

		// Register the new field
		us.nameToAddedField[newField.Name] = newField
		us.adds = append(us.adds, newField)

		return nil
	}
}

func (us *UpdateSpec) removeField(name string) updateSpecOp {
	return func() error {
		if _, added := us.nameToAddedField[name]; added {
			return fmt.Errorf("cannot remove newly added field %s", name)
		}
		if _, renamed := us.renames[name]; renamed {
			return fmt.Errorf("cannot rename and delete field %s", name)
		}
		field, exists := us.nameToField[name]
		if !exists {
			return fmt.Errorf("cannot find partition field %s", name)
		}
		us.deletes[field.FieldID] = true

		return nil
	}
}

func (us *UpdateSpec) renameField(name string, newName string) updateSpecOp {
	return func() error {
		existingField, exists := us.nameToField[newName]
		_, isVoidTransform := existingField.Transform.(iceberg.VoidTransform)
		if exists && isVoidTransform {
			return us.renameField(
				name,
				fmt.Sprintf("%s_%d", name, existingField.FieldID),
			)()
		}
		if _, added := us.nameToAddedField[name]; added {
			return errors.New("cannot rename recently added partitions")
		}

		field, exists := us.nameToField[name]
		if !exists {
			return fmt.Errorf("cannot find partition field: %s", name)
		}
		if _, deleted := us.deletes[field.FieldID]; deleted {
			return fmt.Errorf("cannot delete and rename partition field: %s", name)
		}
		us.renames[name] = newName

		return nil
	}
}

func (us *UpdateSpec) partitionField(key transformKey, name string) (iceberg.PartitionField, error) {
	if us.txn.tbl.Metadata().Version() == 2 {
		sourceId, transform := key.SourceId, key.Transform
		historicalFields := make([]iceberg.PartitionField, 0)
		for _, spec := range us.txn.tbl.Metadata().PartitionSpecs() {
			historicalFields = slices.AppendSeq(historicalFields, spec.Fields())
		}
		for _, field := range historicalFields {
			if field.SourceID == sourceId && field.Transform.String() == transform {
				if len(name) > 0 && field.Name == name {
					return iceberg.PartitionField{
						SourceID:  sourceId,
						FieldID:   field.FieldID,
						Name:      name,
						Transform: field.Transform,
					}, nil
				}
			}
		}
	}
	newFieldId := us.newFieldId()
	transform, _ := iceberg.ParseTransform(key.Transform)
	if name == "" {
		tmpField := iceberg.PartitionField{
			SourceID:  key.SourceId,
			FieldID:   newFieldId,
			Name:      "",
			Transform: transform,
		}
		var err error
		name, err = iceberg.GeneratePartitionFieldName(us.txn.tbl.Schema(), tmpField)
		if err != nil {
			return iceberg.PartitionField{}, err
		}
	}

	return iceberg.PartitionField{
		SourceID:  key.SourceId,
		FieldID:   newFieldId,
		Name:      name,
		Transform: transform,
	}, nil
}

func (us *UpdateSpec) newFieldId() int {
	us.lastAssignedFieldId += 1

	return us.lastAssignedFieldId
}

func (us *UpdateSpec) isDuplicatePartition(transform iceberg.Transform, partitionField iceberg.PartitionField) bool {
	_, deleted := us.deletes[partitionField.FieldID]

	return !deleted && transform.Equals(partitionField.Transform)
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

func (us *UpdateSpec) isNewPartitionSpec(newSpecId int) bool {
	spec := us.txn.tbl.Metadata().PartitionSpecByID(newSpecId)

	return spec == nil
}
