/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.geometry.utils;

import org.elasticsearch.geometry.Coordinate;
import org.elasticsearch.geometry.GeometryFactory;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;

import java.util.ArrayList;
import java.util.List;

public class StandardGeometryFactory implements GeometryFactory {
    @Override
    public Point createPoint(double x, double y) {
        return new Point(x, y);
    }

    @Override
    public Point createPoint(double x, double y, double z) {
        return new Point(x, y, z);
    }

    @Override
    public Line createLine(double[] x, double[] y) {
        return new Line(x, y);
    }

    @Override
    public Line createLine(double[] x, double[] y, double[] z) {
        return new Line(x, y, z);
    }

    @Override
    public Line createLineString(Coordinate[] coordinates) {
        if (coordinates.length == 0) {
            throw new IllegalArgumentException("LineString must have at least one coordinate");
        }
        if (Double.isNaN(coordinates[0].getZ())) {
            return create2DLineString(coordinates);
        }
        return create3DLineString(coordinates);
    }

    private Line create2DLineString(Coordinate[] coordinates) {
        double x[] = new double[coordinates.length];
        double y[] = new double[coordinates.length];
        for (int i = 0; i < coordinates.length; i++) {
            x[i] = coordinates[i].getX();
            y[i] = coordinates[i].getY();
        }
        return new Line(x, y);
    }

    private Line create3DLineString(Coordinate[] coordinates) {
        double x[] = new double[coordinates.length];
        double y[] = new double[coordinates.length];
        double z[] = new double[coordinates.length];
        for (int i = 0; i < coordinates.length; i++) {
            x[i] = coordinates[i].getX();
            y[i] = coordinates[i].getY();
            z[i] = coordinates[i].getZ();
        }
        return new Line(x, y, z);
    }

    @Override
    public Polygon createPolygon(double[] x, double[] y) {
        return new Polygon(new LinearRing(x, y));
    }

    @Override
    public Polygon createPolygon(double[] x, double[] y, double[] z) {
        return new Polygon(new LinearRing(x, y, z));
    }

    @Override
    public MultiPoint createMultiPoint(double[] x, double[] y) {
        List<Point> points = new ArrayList<>(x.length);
        for (int i = 0; i < x.length; i++) {
            points.add(new Point(x[i], y[i]));
        }
        // TODO: Consider refactoring MultiPoint to take arrays of doubles directly, might be more arrow friendly
        return new MultiPoint(points);
    }

    @Override
    public MultiPoint createMultiPoint(double[] x, double[] y, double[] z) {
        List<Point> points = new ArrayList<>(x.length);
        for (int i = 0; i < x.length; i++) {
            points.add(new Point(x[i], y[i], z[i]));
        }
        // TODO: Consider refactoring MultiPoint to take arrays of doubles directly, might be more arrow friendly
        return new MultiPoint(points);
    }

    @Override
    public MultiLine createMultiLine(double[][] x, double[][] y) {
        return null;
    }

    @Override
    public MultiLine createMultiLine(double[][] x, double[][] y, double[][] z) {
        return null;
    }

    @Override
    public MultiPolygon createMultiPolygon(double[][] x, double[][] y) {
        return null;
    }

    @Override
    public MultiPolygon createMultiPolygon(double[][] x, double[][] y, double[][] z) {
        return null;
    }
}
