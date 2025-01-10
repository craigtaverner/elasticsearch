/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ArrowToXContentTests extends ESTestCase {
    private BufferAllocator allocator = Arrow.rootAllocator().newChildAllocator(this.getClass().getName(), 0, 1_000_000);

    @Before
    public void staticSetup() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @After
    public void staticTearDown() {
        allocator.close();
    }

    private static void checkPosition(VectorSchemaRoot root, int position, String json) throws IOException {
        var out = new ByteArrayOutputStream();
        try (var generator = XContentType.JSON.xContent().createGenerator(out)) {
            generator.writeStartObject();
            for (var vector: root.getFieldVectors()) {
                ArrowToXContent.writeField(vector, position, generator);
            }
            generator.writeEndObject();
        }

        assertEquals(json, out.toString(StandardCharsets.UTF_8));
    }

    private static void checkPosition(ValueVector vector, int position, String json) throws IOException {
        var out = new ByteArrayOutputStream();
        try (var generator = XContentType.JSON.xContent().createGenerator(out)) {
            generator.writeStartObject();
            ArrowToXContent.writeField(vector, position, generator);
            generator.writeEndObject();
        }

        assertEquals(json, out.toString(StandardCharsets.UTF_8));
    }

    public void testWriteField() throws IOException {

        try (IntVector vector = new IntVector("intField", allocator)) {
            vector.allocateNew(1);
            vector.set(0, 123);
            vector.setValueCount(1);

            checkPosition(vector, 0, "{\"intField\":123}");

        }
    }

//    public void testWriteValue() throws IOException {
//        try (VarCharVector vector = new VarCharVector("stringField", allocator)) {
//            vector.allocateNew(1);
//            vector.setSafe(0, "test".getBytes(StandardCharsets.UTF_8));
//            vector.setValueCount(1);
//
//            StringWriter writer = new StringWriter();
//            JsonGenerator jsonGenerator = new JsonFactory().createGenerator(writer);
//            XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, jsonGenerator);
//
//            ArrowToXContent.writeValue(vector, 0, builder);
//
//            builder.close();
//            assertEquals("\"test\"", writer.toString());
//        }
//    }
//
//    public void testWriteMap() throws IOException {
//        try (MapVector mapVector = MapVector.empty("mapField", allocator, false)) {
//            mapVector.allocateNew();
//            StructVector structVector = mapVector.addOrGetStruct("mapField");
//            VarCharVector keyVector = structVector.addOrGet("key", FieldType.nullable(Types.MinorType.VARCHAR.getType()), VarCharVector.class);
//            IntVector valueVector = structVector.addOrGet("value", FieldType.nullable(Types.MinorType.INT.getType()), IntVector.class);
//
//            mapVector.setValueCount(1);
//            structVector.setValueCount(1);
//            keyVector.setSafe(0, "key1".getBytes(StandardCharsets.UTF_8));
//            valueVector.setSafe(0, 42);
//
//            StringWriter writer = new StringWriter();
//            JsonGenerator jsonGenerator = new JsonFactory().createGenerator(writer);
//            XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, jsonGenerator);
//
//            ArrowToXContent.writeValue(mapVector, 0, builder);
//
//            builder.close();
//            assertEquals("{\"key1\":42}", writer.toString());
//        }
//    }
//
//    public void testWriteNullValue() throws IOException {
//        try (IntVector vector = new IntVector("intField", allocator)) {
//            vector.allocateNew(1);
//            vector.setNull(0);
//            vector.setValueCount(1);
//
//            StringWriter writer = new StringWriter();
//            JsonGenerator jsonGenerator = new JsonFactory().createGenerator(writer);
//            XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, jsonGenerator);
//
//            ArrowToXContent.writeField(vector, 0, builder);
//
//            builder.close();
//            assertEquals("{\"intField\":null}", writer.toString());
//        }
//    }
//
//    public void testWriteEmptyVector() throws IOException {
//        try (IntVector vector = new IntVector("intField", allocator)) {
//            vector.allocateNew(0);
//            vector.setValueCount(0);
//
//            StringWriter writer = new StringWriter();
//            JsonGenerator jsonGenerator = new JsonFactory().createGenerator(writer);
//            XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, jsonGenerator);
//
//            ArrowToXContent.writeField(vector, 0, builder);
//
//            builder.close();
//            assertEquals("{\"intField\":null}", writer.toString());
//        }
//    }
//
//    public void testWriteUnsupportedType() {
//        try (NullVector vector = new NullVector("nullField", FieldType.nullable(Types.MinorType.NULL.getType()), allocator)) {
//            vector.allocateNew(1);
//            vector.setValueCount(1);
//
//            StringWriter writer = new StringWriter();
//            JsonGenerator jsonGenerator = new JsonFactory().createGenerator(writer);
//            XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, jsonGenerator);
//
//            assertThrows(JsonParseException.class, () -> ArrowToXContent.writeField(vector, 0, builder));
//        }
//    }
}
