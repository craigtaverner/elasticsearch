/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow;

import com.fasterxml.jackson.core.JsonParseException;

import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VariableWidthFieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.BaseListVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.Types;
import org.elasticsearch.xcontent.XContentGenerator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;

/**
 * Utility methods to serialize Arrow dataframes to XContent events.
 * <p>
 * Limitations:
 * <ul>
 * <li>time and timestamps are converted to milliseconds (no support for nanoseconds)
 * </li>
 * <li>some types aren't implemented
 * </li>
 * </ul>
 *
 * @see <a href="https://arrow.apache.org/docs/format/Columnar.html#data-types">Arrow data types</a>
 */
public class ArrowToXContent {

    private static final EnumSet<Types.MinorType> STRING_TYPES = EnumSet.of(
        Types.MinorType.VARCHAR,
        Types.MinorType.LARGEVARCHAR,
        Types.MinorType.VIEWVARCHAR
    );

    public static void writeField(ValueVector vector, int position, XContentGenerator generator) throws IOException {
        generator.writeFieldName(vector.getName());
        writeValue(vector, position, generator);
    }

    public static void writeValue(ValueVector vector, int position, XContentGenerator generator) throws IOException {
        if (vector.isNull(position)) {
            generator.writeNull();
            return;
        }

        switch (vector.getMinorType()) {

            //----- Primitive values

            case BIT -> {
                generator.writeBoolean(((BitVector)vector).get(position) != 0);
            }

            case TINYINT, SMALLINT, INT, BIGINT, UINT1, UINT2, UINT4, UINT8 -> {
                generator.writeNumber(((BaseIntVector)vector).getValueAsLong(position));
            }

            case FLOAT2, FLOAT4, FLOAT8 -> {
                generator.writeNumber(((FloatingPointVector)vector).getValueAsDouble(position));
            }

            //----- strings and bytes

            case VARCHAR, LARGEVARCHAR, VIEWVARCHAR -> {
                var bytesVector = (VariableWidthFieldVector)vector;
                generator.writeString(new String(bytesVector.get(position), StandardCharsets.UTF_8));
            }

            case VARBINARY, LARGEVARBINARY, VIEWVARBINARY -> {
                var bytesVector = (VariableWidthFieldVector)vector;
                generator.writeBinary(bytesVector.get(position));
            }

            case FIXEDSIZEBINARY -> {
                var bytesVector = (FixedSizeBinaryVector)vector;
                generator.writeBinary(bytesVector.get(position));
            }

            //----- lists

            case LIST, FIXED_SIZE_LIST, LISTVIEW -> {
                var listVector = (BaseListVector)vector;
                var valueVector = listVector.getChildrenFromFields().get(0);
                int start = listVector.getElementStartIndex(position);
                int end = listVector.getElementEndIndex(position);

                generator.writeStartArray();
                for (int i = start; i < end; i++) {
                    writeValue(valueVector, i, generator);
                }
                generator.writeEndArray();
            }

            //----- Time & Timestamp (time + timezone)

            // Timestamps are the elapsed time since the Epoch, with an optional timezone that
            // can be used for timezome-aware operations or display. Since ES date fields
            // don't support timezones, we ignore it.
            // See https://github.com/apache/arrow/blob/main/format/Schema.fbs
            // and https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html

            case TIMESEC, TIMESTAMPSEC -> {
                var tsVector = (TimeStampVector)vector;
                generator.writeNumber(tsVector.get(position)*1000);
            }

            case TIMEMILLI, TIMESTAMPMILLI -> {
                var tsVector = (TimeStampVector)vector;
                generator.writeNumber(tsVector.get(position));
            }

            case TIMEMICRO, TIMESTAMPMICRO -> {
                var tsVector = (TimeStampVector)vector;
                generator.writeNumber(tsVector.get(position)/1000);
            }

            case TIMENANO, TIMESTAMPNANO -> {
                var tsVector = (TimeStampVector)vector;
                generator.writeNumber(tsVector.get(position)/1_000_000);
            }

            //----- Composite types

            case MAP -> {
                // A map is a container vector that is composed of a list of struct values with "key" and "value" fields. The MapVector
                // is nullable, but if a map is set at a given index, there must be an entry. In other words, the StructVector data is
                // non-nullable. Also for a given entry, the "key" is non-nullable, however the "value" can be null.

                var mapVector = (MapVector)vector;
                var structVector = (StructVector)mapVector.getChildrenFromFields().get(0);
                var kVector = structVector.getChildrenFromFields().get(0);
                if (STRING_TYPES.contains(kVector.getMinorType()) == false) {
                    throw new IllegalArgumentException("Maps must have string keys");
                }

                var keyVector = (VarBinaryVector)kVector;
                var valueVector = structVector.getChildrenFromFields().get(1);

                int start = mapVector.getElementStartIndex(position);
                int end = mapVector.getElementEndIndex(position);

                generator.writeStartObject();
                for (int i = start; i < end; i++) {
                    var key = new String(keyVector.get(i), StandardCharsets.UTF_8);
                    generator.writeFieldName(key);
                    writeValue(valueVector, i, generator);
                }
                generator.writeEndObject();
            }

            case STRUCT -> {
                var structVector = (StructVector)vector;
                generator.writeStartObject();
                for (var field: structVector.getChildrenFromFields()) {
                    generator.writeFieldName(field.getName());
                    writeValue(field, position, generator);
                }
                generator.writeEndObject();
            }

            case DENSEUNION -> {
                var unionVector = (DenseUnionVector)vector;
                var typeId = unionVector.getTypeId(position);
                var valueVector = unionVector.getVectorByType(typeId);
                var valuePosition = unionVector.getOffset(position);

                writeValue(valueVector, valuePosition, generator);
            }

            case UNION -> { // sparse union
                var unionVector = (UnionVector)vector;
                var typeId = unionVector.getTypeValue(position);
                var valueVector = unionVector.getVectorByType(typeId);

                writeValue(valueVector, position, generator);
            }

            default -> throw new JsonParseException(
                "Arrow type [" + vector.getMinorType() + "] not supported for field [" + vector.getName() + "]"
            );
        };
    }
}
