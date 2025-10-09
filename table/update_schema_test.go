package table

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
)

var originalSchema = iceberg.NewSchema(1,
	iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
	iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
	iceberg.NestedField{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
	iceberg.NestedField{ID: 4, Name: "address", Type: &iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
		},
	}, Required: false, Doc: ""},
	iceberg.NestedField{ID: 7, Name: "tags", Type: &iceberg.ListType{
		ElementID:       8,
		Element:         iceberg.PrimitiveTypes.String,
		ElementRequired: false,
	}, Required: false, Doc: ""},
	iceberg.NestedField{ID: 9, Name: "properties", Type: &iceberg.MapType{
		KeyID:         10,
		KeyType:       iceberg.PrimitiveTypes.String,
		ValueID:       11,
		ValueType:     iceberg.PrimitiveTypes.String,
		ValueRequired: false,
	}, Required: false, Doc: ""},
)

var testMetadata, _ = NewMetadata(originalSchema, nil, UnsortedSortOrder, "", nil)

func TestNewUpdateSchema(t *testing.T) {
	t.Run("test update schema with add primitive type on top level", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).AddColumn([]string{"gender"}, iceberg.PrimitiveTypes.String, "", false, iceberg.StringLiteral("male")).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
			{ID: 4, Name: "address", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				},
			}, Required: false, Doc: ""},
			{ID: 7, Name: "tags", Type: &iceberg.ListType{
				ElementID:       8,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
			{ID: 9, Name: "properties", Type: &iceberg.MapType{
				KeyID:         10,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       11,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
			{ID: 12, Name: "gender", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
		}, newSchema.Fields())
	})

	t.Run("test update schema with add list of primitive type on top level", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).AddColumn([]string{"files"}, &iceberg.ListType{
			Element:         iceberg.PrimitiveTypes.String,
			ElementRequired: false,
		}, "", false, nil).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
			{ID: 4, Name: "address", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				},
			}, Required: false, Doc: ""},
			{ID: 7, Name: "tags", Type: &iceberg.ListType{
				ElementID:       8,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
			{ID: 9, Name: "properties", Type: &iceberg.MapType{
				KeyID:         10,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       11,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
			{ID: 12, Name: "files", Type: &iceberg.ListType{
				ElementID:       13,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
		}, newSchema.Fields())
	})

	t.Run("test update schema with add map of primitive type on top level", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).AddColumn([]string{"files"}, &iceberg.MapType{
			KeyType:       iceberg.PrimitiveTypes.String,
			ValueType:     iceberg.PrimitiveTypes.String,
			ValueRequired: false,
		}, "", false, nil).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
			{ID: 4, Name: "address", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				},
			}, Required: false, Doc: ""},
			{ID: 7, Name: "tags", Type: &iceberg.ListType{
				ElementID:       8,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
			{ID: 9, Name: "properties", Type: &iceberg.MapType{
				KeyID:         10,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       11,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
			{ID: 12, Name: "files", Type: &iceberg.MapType{
				KeyID:         13,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       14,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
		}, newSchema.Fields())
	})

	t.Run("test update schema with add struct type on top level", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).AddColumn([]string{"files"}, &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 5, Name: "id", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				{ID: 6, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			},
		}, "", false, nil).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
			{ID: 4, Name: "address", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				},
			}, Required: false, Doc: ""},
			{ID: 7, Name: "tags", Type: &iceberg.ListType{
				ElementID:       8,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
			{ID: 9, Name: "properties", Type: &iceberg.MapType{
				KeyID:         10,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       11,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
			{ID: 12, Name: "files", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 13, Name: "id", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 14, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				},
			}, Required: false, Doc: ""},
		}, newSchema.Fields())
	})

	t.Run("test update schema with add primitive in struct", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).AddColumn([]string{"address", "code"}, iceberg.PrimitiveTypes.String, "", false, nil).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
			{ID: 4, Name: "address", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 12, Name: "code", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				},
			}, Required: false, Doc: ""},
			{ID: 7, Name: "tags", Type: &iceberg.ListType{
				ElementID:       8,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
			{ID: 9, Name: "properties", Type: &iceberg.MapType{
				KeyID:         10,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       11,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
		}, newSchema.Fields())
	})

	t.Run("test update schema with add struct in struct", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).AddColumn([]string{"address", "code"}, &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 5, Name: "code-1", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				{ID: 6, Name: "code-2", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			},
		}, "", false, nil).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
			{ID: 4, Name: "address", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 12, Name: "code", Type: &iceberg.StructType{
						FieldList: []iceberg.NestedField{
							{ID: 13, Name: "code-1", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
							{ID: 14, Name: "code-2", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
						},
					}, Required: false, Doc: ""},
				},
			}},
			{ID: 7, Name: "tags", Type: &iceberg.ListType{
				ElementID:       8,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
			{ID: 9, Name: "properties", Type: &iceberg.MapType{
				KeyID:         10,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       11,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
		}, newSchema.Fields())
	})

	t.Run("test update schema with multiple adds", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).AddColumn([]string{"address", "code"}, &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 5, Name: "code-1", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				{ID: 6, Name: "code-2", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			},
		}, "", false, nil).AddColumn([]string{"gender"}, iceberg.PrimitiveTypes.String, "", false, nil).AddColumn([]string{"files"}, &iceberg.ListType{
			Element:         iceberg.PrimitiveTypes.String,
			ElementRequired: false,
		}, "", false, nil).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
			{ID: 4, Name: "address", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 12, Name: "code", Type: &iceberg.StructType{
						FieldList: []iceberg.NestedField{
							{ID: 13, Name: "code-1", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
							{ID: 14, Name: "code-2", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
						},
					}, Required: false, Doc: ""},
				},
			}, Required: false, Doc: ""},
			{ID: 7, Name: "tags", Type: &iceberg.ListType{
				ElementID:       8,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
			{ID: 9, Name: "properties", Type: &iceberg.MapType{
				KeyID:         10,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       11,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
			{ID: 15, Name: "gender", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 16, Name: "files", Type: &iceberg.ListType{
				ElementID:       17,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}},
		}, newSchema.Fields())
	})
}

func TestApplyChanges(t *testing.T) {
	t.Run("test apply changes on schema", func(t *testing.T) {
		deletes := map[int]struct{}{
			2: {},
		}
		updates := map[int]iceberg.NestedField{
			3: {Name: "age", Type: iceberg.PrimitiveTypes.Int64, Required: true, Doc: ""},
		}
		adds := map[int][]iceberg.NestedField{
			-1: {
				{ID: 12, Name: "gender", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			},
		}
		moves := map[int][]move{
			4: {
				{FieldID: 6, RelativeTo: 5, Op: MoveOpBefore},
			},
		}

		st, err := iceberg.Visit(originalSchema, &applyChanges{
			deletes: deletes,
			updates: updates,
			adds:    adds,
			moves:   moves,
		})
		assert.NoError(t, err)
		assert.NotNil(t, st)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int64, Required: true, Doc: ""},
			{ID: 4, Name: "address", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				},
			}, Required: false, Doc: ""},
			{ID: 7, Name: "tags", Type: &iceberg.ListType{
				ElementID:       8,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
			{ID: 9, Name: "properties", Type: &iceberg.MapType{
				KeyID:         10,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       11,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
			{ID: 12, Name: "gender", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
		}, st.(*iceberg.StructType).Fields())
	})

	t.Run("test apply changes on add field that delete in same time", func(t *testing.T) {
		originalSchema := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			iceberg.NestedField{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
		)
		deletes := map[int]struct{}{
			2: {},
		}
		adds := map[int][]iceberg.NestedField{
			-1: {
				{ID: 4, Name: "name", Type: iceberg.PrimitiveTypes.UUID, Required: false, Doc: ""},
			},
		}

		st, err := iceberg.Visit(originalSchema, &applyChanges{
			deletes: deletes,
			adds:    adds,
		})
		assert.NoError(t, err)
		assert.NotNil(t, st)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
			{ID: 4, Name: "name", Type: iceberg.PrimitiveTypes.UUID, Required: false, Doc: ""},
		}, st.(*iceberg.StructType).Fields())
	})
}
