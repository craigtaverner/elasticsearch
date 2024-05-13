/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.Objects;

/**
 * Attribute for an ES field within ES|QL.
 * This class extends the FieldAttribute class from the ql plugin, and serves as a first step towards completely
 * replacing that class with this one. The initial purpose of this class is just to modify the hashCode and equals
 * methods to include the EsField in the comparison. This is required for the 'union types' feature where
 * re-writing the query with the union type requires that the FieldAttribute appear ot have changed when the underlying field changes.
 * This happens if the underlying field changes from an InvalidMappedField to a MultiTypeEsField during union types resolution.
 */
public class FieldAttribute extends org.elasticsearch.xpack.ql.expression.FieldAttribute {

    public FieldAttribute(Source source, String name, EsField field) {
        super(source, null, name, field);
    }

    public FieldAttribute(Source source, org.elasticsearch.xpack.ql.expression.FieldAttribute parent, String name, EsField field) {
        super(source, parent, name, field, null, Nullability.TRUE, null, false);
    }

    public FieldAttribute(
        Source source,
        org.elasticsearch.xpack.ql.expression.FieldAttribute parent,
        String name,
        EsField field,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        super(source, parent, name, field.getDataType(), field, qualifier, nullability, id, synthetic);
    }

    public FieldAttribute(
        Source source,
        org.elasticsearch.xpack.ql.expression.FieldAttribute parent,
        String name,
        DataType type,
        EsField field,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        super(source, parent, name, type, field, qualifier, nullability, id, synthetic);
    }

    @Override
    protected Attribute clone(
        Source source,
        String name,
        DataType type,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        org.elasticsearch.xpack.ql.expression.FieldAttribute qualifiedParent = parent() != null
            ? (org.elasticsearch.xpack.ql.expression.FieldAttribute) parent().withQualifier(qualifier)
            : null;
        return new FieldAttribute(source, qualifiedParent, name, field(), qualifier, nullability, id, synthetic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), field());
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(field(), ((org.elasticsearch.xpack.ql.expression.FieldAttribute) obj).field());
    }
}
