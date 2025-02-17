/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.geometry;

/**
 * Represents a Point on the earth's surface in decimal degrees and optional altitude in meters.
 */
public class Coordinate {
    protected final double y;
    protected final double x;
    protected final double z;

    public Coordinate(double x, double y) {
        this(x, y, Double.NaN);
    }

    public Coordinate(double x, double y, double z) {
        this.y = y;
        this.x = x;
        this.z = z;
    }

    public double getY() {
        return y;
    }

    public double getX() {
        return x;
    }

    public double getZ() {
        return z;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Coordinate point = (Coordinate) o;
        if (Double.compare(point.y, y) != 0) return false;
        if (Double.compare(point.x, x) != 0) return false;
        return Double.compare(point.z, z) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(y);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(x);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(z);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return Double.isNaN(z) == false ? "Coordinate(" + x + ", " + y + ", " + z + ")" : "Coordinate(" + x + ", " + y + ")";
    }
}
