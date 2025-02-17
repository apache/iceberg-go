package internal

import (
	"github.com/hamba/avro/v2"
)

func MustNewUnionSchema(types []avro.Schema, opts ...avro.SchemaOption) *avro.UnionSchema {
	s, err := avro.NewUnionSchema(types, opts...)

	if err != nil {
		panic(err)
	}

	return s
}

func MustNewRecordSchema(name, namespace string, fields []*avro.Field, opts ...avro.SchemaOption) *avro.RecordSchema {
	s, err := avro.NewRecordSchema(name, namespace, fields, opts...)

	if err != nil {
		panic(err)
	}

	return s
}

func MustNewField(name string, typ avro.Schema, opts ...avro.SchemaOption) *avro.Field {
	f, err := avro.NewField(name, typ, opts...)

	if err != nil {
		panic(err)
	}

	return f
}
