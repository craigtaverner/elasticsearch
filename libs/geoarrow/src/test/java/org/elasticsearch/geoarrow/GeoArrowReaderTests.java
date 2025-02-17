/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.geoarrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;

import static org.elasticsearch.geoarrow.GeoArrowReader.parseLineString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

public class GeoArrowReaderTests {

    @Test
    public void testDummy() {
        assertThat("Dummy", true, is(true));
    }

    @Test
    public void testLoadArrowFile() {
        try (
            BufferAllocator allocator = new RootAllocator();
            FileInputStream fileInputStream = fromResources("test.arrow");
            FileChannel fileChannel = fileInputStream.getChannel();
            ArrowFileReader arrowReader = new ArrowFileReader(new SeekableReadChannel(fileChannel), allocator)
        ) {

            VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
            arrowReader.loadNextBatch();
            Schema schema = root.getSchema();
            schema.getCustomMetadata().forEach((k, v) -> System.out.println("Custom Metadata: " + k + ": " + v));
            var geo = schema.getCustomMetadata().get("geo");
            geo.lines().forEach(line -> System.out.println("Line: " + line));
            var geoMetadata = schema.findField("geometry").getMetadata();
            var geoType = geoMetadata.get("ARROW:extension:name");
            System.out.println("geometry metadata: " + geoMetadata);
            var geometryField = root.getVector("geometry");

            // Example processing (adapt depending on schema)
            for (int i = 0; i < root.getRowCount(); i++) {
                // Parse geometry
                var geometry = switch (geoType) {
                    case "geoarrow.linestring" -> parseLineString(geometryField.getObject(i));
                    default -> throw new IllegalArgumentException("Unsupported geometry type: " + geoType);
                };

                System.out.println("Feature " + i + ":");
                System.out.println("\tGeometry:\t" + geometry);
                printAttributes(root, i);
            }
        } catch (FileNotFoundException e) {
            fail(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void printAttributes(VectorSchemaRoot root, final int index) {
        System.out.println("\tAttributes:");
        Schema schema = root.getSchema();
        schema.getFields().forEach(f -> {
            if (f.getName().equals("geometry") == false) {
                var field = root.getVector(f.getName()).getObject(index);
                System.out.println("\t\t" + f.getName() + ":\t" + field);
            }
        });
    }

    FileInputStream fromResources(String filename) throws FileNotFoundException {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(filename).getFile());
        return new FileInputStream(file);
    }
}
