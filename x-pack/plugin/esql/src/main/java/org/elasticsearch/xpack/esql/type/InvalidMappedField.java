/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Representation of field mapped differently across indices.
 * Used during mapping discovery only.
 * Note that, for backwards compatibility purposes, this class extends the QL class InvalidMappedField,
 * and uses the same simple name and constructor signature. This allows serialization of query plans to
 * work across upgrades involving older servers that use the QL version of this class.
 * The additional field <code>typesToIndices</code> is not serialized in any version of this class,
 * because that information is not required through the cluster, only surviving as long as the Analyser
 * phase of query planning.
 * TODO: Once QL and ESQL code bases are split, we should incorporate the error message from the QL version into this class.
 * Breaking the inheritance requires that we port IndexResolver to ESQL, so it cannot be done yet.
 */
public class InvalidMappedField extends org.elasticsearch.xpack.esql.core.type.InvalidMappedField {
    private final Map<String, Set<String>> typesToIndices;

    public InvalidMappedField(String name, String errorMessage, Map<String, EsField> properties) {
        this(name, errorMessage, properties, Map.of());
    }

    public InvalidMappedField(String name, String errorMessage) {
        this(name, errorMessage, new TreeMap<>());
    }

    /**
     * Constructor supporting union types, used in ES|QL.
     */
    public InvalidMappedField(String name, Map<String, Set<String>> typesToIndices) {
        this(name, makeErrorMessage(typesToIndices), new TreeMap<>(), typesToIndices);
    }

    private InvalidMappedField(String name, String errorMessage, Map<String, EsField> properties, Map<String, Set<String>> typesToIndices) {
        super(name, errorMessage, properties);
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
}
