/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.geoarrow;

import org.elasticsearch.geometry.Coordinate;
import org.elasticsearch.geometry.GeometryFactory;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.utils.StandardGeometryFactory;

import java.util.List;
import java.util.Map;

public class GeoArrowReader {
    public static final GeometryFactory geometryFactory = new StandardGeometryFactory();

    public static Line parseLineString(Object rawGeometry) {
        return geometryFactory.createLineString(parseCoordinates(rawGeometry));
    }

    public static Coordinate[] parseCoordinates(Object rawGeometry) {
        if (rawGeometry instanceof List<?> list) {
            var coordinates = new Coordinate[list.size()];
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i) instanceof Map<?, ?> obj) {
                    coordinates[i] = new Coordinate((Double) obj.get("x"), (Double) obj.get("y"));
                } else {
                    throw new IllegalArgumentException(
                        "Unsupported geometry array object class: " + list.get(i).getClass().getSimpleName()
                    );
                }
            }
            return coordinates;
        }
        throw new IllegalArgumentException("Unsupported geometry object class: " + rawGeometry.getClass().getSimpleName());
    }
}
