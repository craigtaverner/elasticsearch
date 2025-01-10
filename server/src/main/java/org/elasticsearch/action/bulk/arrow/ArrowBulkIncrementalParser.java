/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk.arrow;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VariableWidthFieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestParser;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.arrow.Arrow;
import org.elasticsearch.arrow.ArrowToXContent;
import org.elasticsearch.arrow.XContentBuffer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;


class ArrowBulkIncrementalParser extends BulkRequestParser.XContentIncrementalParser {

    /** XContent format used to encode source documents */
    private static final XContent SOURCE_XCONTENT = XContentType.CBOR.xContent();

    private static final String ID = "_id";
    private static final String ACTION = "_bulk_action";

    private DocWriteRequest.OpType defaultOpType;

    private ArrowIncrementalParser arrowParser;
    private BufferAllocator allocator;
    private VectorSchemaRoot schemaRoot;

    private Integer idField = null;
    private Integer actionField = null;
    private BitSet valueFields;

    ArrowBulkIncrementalParser(
        DocWriteRequest.OpType defaultOpType,
        @Nullable String defaultIndex,
        @Nullable String defaultRouting,
        @Nullable FetchSourceContext defaultFetchSourceContext,
        @Nullable String defaultPipeline,
        @Nullable Boolean defaultRequireAlias,
        @Nullable Boolean defaultRequireDataStream,
        @Nullable Boolean defaultListExecutedPipelines,
        boolean allowExplicitIndex,
        XContentType xContentType,
        BiConsumer<IndexRequest, String> indexRequestConsumer,
        Consumer<UpdateRequest> updateRequestConsumer,
        Consumer<DeleteRequest> deleteRequestConsumer
    ) {
        super(
            defaultIndex,
            defaultRouting,
            defaultFetchSourceContext,
            defaultPipeline,
            defaultRequireAlias,
            defaultRequireDataStream,
            defaultListExecutedPipelines,
            allowExplicitIndex,
            true, // deprecateOrErrorOnType
            xContentType,
            null, // contentType
            indexRequestConsumer,
            updateRequestConsumer,
            deleteRequestConsumer
        );

        this.defaultOpType = defaultOpType;

        // FIXME: limit size of child allocator. Add an AllocationListener that calls a CircuitBreaker?
        this.allocator = Arrow.rootAllocator().newChildAllocator("bulk-ingestion", 0, Integer.MAX_VALUE);

        this.arrowParser = new ArrowIncrementalParser(
            new RootAllocator(),
            new ArrowIncrementalParser.Listener() {
                @Override
                public void startStream(VectorSchemaRoot schemaRoot) throws IOException {
                    startArrowStream(schemaRoot);
                }

                @Override
                public void nextBatch() throws IOException {
                    nextArrowBatch();
                }

                @Override
                public void endStream() throws IOException {
                    endArrowStream();
                }
            }
        );
    }

    @Override
    public int parse(BytesReference data, boolean lastData) throws IOException {
        return arrowParser.parse(data, lastData);
    }

    @Override
    public void close() {
        super.close();
        if (schemaRoot != null) {
            schemaRoot.close();
            schemaRoot = null;
        }
    }

    private void startArrowStream(VectorSchemaRoot root) {

        this.schemaRoot = root;

        var schemaFields = root.getFieldVectors();
        var valueFields = new BitSet(schemaFields.size());

        for (int i = 0; i < schemaFields.size(); i++) {
            var field = schemaFields.get(i);

            switch (field.getName()) {
                case ID -> idField = i;
                case ACTION -> {
                    var type = field.getMinorType();
                    if (type != Types.MinorType.MAP && type != Types.MinorType.STRUCT) {
                        throw new IllegalArgumentException("Field '" + ACTION + "' should be a map or a struct");
                    }
                    actionField = i;
                }
                default -> valueFields.set(i);
            }
        }

        this.valueFields = valueFields;
    }

    private void nextArrowBatch() throws IOException {
        int rowCount = schemaRoot.getRowCount();
        FieldVector idVector = idField == null ? null : schemaRoot.getVector(idField);
        FieldVector actionVector = actionField == null ? null : schemaRoot.getVector(actionField);

        for (int i = 0; i < rowCount; i++) {
            String id = idVector == null ? null : getString(idVector, i);

            var action = parseAction(actionVector, i, id);
            switch (action) {
                case IndexRequest ir -> {
                    ir.source(generateSource(i), SOURCE_XCONTENT.type());
                    indexRequestConsumer.accept(ir, null);
                }
                case UpdateRequest ur -> {
                    ur.doc(generateSource(i), SOURCE_XCONTENT.type());
                    updateRequestConsumer.accept(ur);
                }
                case DeleteRequest dr -> {
                    deleteRequestConsumer.accept(dr);
                }
                default -> {}
            }
        }
    }

    protected BytesReference generateSource(int position) throws IOException {
        var output = new BytesReferenceOutputStream();
        try(var generator = SOURCE_XCONTENT.createGenerator(output)) {
            generator.writeStartObject();
            int rowCount = schemaRoot.getRowCount();
            for (int i = 0; i < rowCount; i++) {
                if (valueFields.get(i)) {
                    ArrowToXContent.writeField(schemaRoot.getVector(i), position, generator);
                }
            }
            generator.writeEndObject();
        }

        return output.asBytesReference();
    }

    private void endArrowStream() {
        close();
    }

    private DocWriteRequest<?> parseAction(@Nullable FieldVector actionVector, int position, String id) throws IOException {

        DocWriteRequest<?> request;

        try (var generator = new XContentBuffer()) {

            if (actionVector != null) {
                String opType = getNamedString(actionVector, "op_type", position);
                if (opType != null) {
                    opType = defaultOpType.getLowercase();
                }

                // Create { opType: { properties } }. The "op_type" property may also exist, but the action parser accepts it.
                generator.writeStartObject();
                generator.writeFieldName(opType);
                generator.writeStartObject();
                ArrowToXContent.writeValue(actionVector, position, generator);
                generator.writeEndObject();
                generator.writeEndObject();

            } else {
                // Create a `{ defaultOpType: {} }` action
                generator.writeStartObject();
                generator.writeFieldName(defaultOpType.getLowercase());
                generator.writeStartObject();
                generator.writeEndObject();
                generator.writeEndObject();
            }

            request = parseActionLine(generator.asParser());
        }

        if (id != null) {
            if (request.id() != null) {
                throw new IllegalArgumentException(
                    "'" + ID + "' found both as top-level field and in '" + ACTION + "' at position [" + position + "]"
                );
            }

            switch (request) {
                case IndexRequest ir -> ir.id(id);
                case UpdateRequest ur -> ur.id(id);
                case DeleteRequest ur -> ur.id(id);
                default -> throw new IllegalArgumentException("Unknown request type [" + request.opType() + "]");
            }
        }

        return request;
    }

    private String getNamedString(FieldVector vector, String name, int position) {
        for (var child: vector.getChildrenFromFields()) {
            if (child instanceof ValueVector valueVector && valueVector.getName().equals(name)) {
                return getString(valueVector, position);
            }
        }
        return null;
    }

    private String getString(ValueVector vector, int position) {
        if (vector.isNull(position)) {
            return null;
        }

        return switch (vector.getMinorType()) {
            case TINYINT, SMALLINT, INT, BIGINT, UINT1, UINT2, UINT4, UINT8 ->
                String.valueOf(((BaseIntVector)vector).getValueAsLong(position));

            case VARCHAR, LARGEVARCHAR, VIEWVARCHAR -> {
                var bytesVector = (VariableWidthFieldVector)vector;
                yield new String(bytesVector.get(position), StandardCharsets.UTF_8);
            }

            default -> {
                throw new IllegalArgumentException(
                    "Arrow type [" + vector.getMinorType() + "] cannot be converted to string"
                );
            }
        };
    }

    /**
     * A byte array stream that can be converted to {@code BytesReference} with zero copy.
     */
    private static class BytesReferenceOutputStream extends ByteArrayOutputStream {
        BytesArray asBytesReference() {
            return new BytesArray(buf, 0, count);
        }
    }
}
