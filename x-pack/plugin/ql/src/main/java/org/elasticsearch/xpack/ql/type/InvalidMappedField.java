/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Representation of field mapped differently across indices.
 * Used during mapping discovery only.
 */
public class InvalidMappedField extends EsField {

    private final String errorMessage;
    private final Map<String, Set<String>> typesToIndices;

    public InvalidMappedField(String name, String errorMessage, Map<String, EsField> properties) {
        this(name, errorMessage, properties, Map.of());
    }

    public InvalidMappedField(String name, String errorMessage) {
        this(name, errorMessage, new TreeMap<>());
    }

    public InvalidMappedField(String name) {
        this(name, StringUtils.EMPTY, new TreeMap<>());
    }

    /**
     * Constructor supporting union types, used in ES|QL.
     */
    public InvalidMappedField(String name, Map<String, Set<String>> typesToIndices) {
        this(name, makeErrorMessage(typesToIndices), new TreeMap<>(), typesToIndices);
    }

    private InvalidMappedField(String name, String errorMessage, Map<String, EsField> properties, Map<String, Set<String>> typesToIndices) {
        super(name, DataTypes.UNSUPPORTED, properties, false);
        this.errorMessage = errorMessage;
        this.typesToIndices = typesToIndices;
    }

    private static String makeErrorMessage(Map<String, Set<String>> typesToIndices) {
        StringBuilder errorMessage = new StringBuilder();
        errorMessage.append("mapped as [");
        errorMessage.append(typesToIndices.size());
        errorMessage.append("] incompatible types: ");
        boolean first = true;
        for (Map.Entry<String, Set<String>> e : typesToIndices.entrySet()) {
            if (first) {
                first = false;
            } else {
                errorMessage.append(", ");
            }
            errorMessage.append("[");
            errorMessage.append(e.getKey());
            errorMessage.append("] in ");
            errorMessage.append(e.getValue());
        }
        return errorMessage.toString();
    }

    public Map<String, Set<String>> getTypesToIndices() {
        return typesToIndices;
    }

    public String errorMessage() {
        return errorMessage;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), errorMessage);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            InvalidMappedField other = (InvalidMappedField) obj;
            return Objects.equals(errorMessage, other.errorMessage);
        }

        return false;
    }

    @Override
    public EsField getExactField() {
        throw new QlIllegalArgumentException("Field [" + getName() + "] is invalid, cannot access it");

    }

    @Override
    public Exact getExactInfo() {
        return new Exact(false, "Field [" + getName() + "] is invalid, cannot access it");
    }
}
