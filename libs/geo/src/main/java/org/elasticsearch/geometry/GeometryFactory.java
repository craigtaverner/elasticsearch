/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.geometry;

public interface GeometryFactory {
    Point createPoint(double x, double y);

    Point createPoint(double x, double y, double z);

    Line createLine(double[] x, double[] y);

    Line createLine(double[] x, double[] y, double[] z);
    Line createLineString(Coordinate[] coordinates);

    Polygon createPolygon(double[] x, double[] y);

    Polygon createPolygon(double[] x, double[] y, double[] z);

    MultiPoint createMultiPoint(double[] x, double[] y);

    MultiPoint createMultiPoint(double[] x, double[] y, double[] z);

    MultiLine createMultiLine(double[][] x, double[][] y);

    MultiLine createMultiLine(double[][] x, double[][] y, double[][] z);

    MultiPolygon createMultiPolygon(double[][] x, double[][] y);

    MultiPolygon createMultiPolygon(double[][] x, double[][] y, double[][] z);

}
