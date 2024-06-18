// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StDWithin}.
 * This class is generated. Do not edit it.
 */
public final class StDWithinGeoPointDocValuesAndFieldAndConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator leftValue;

  private final EvalOperator.ExpressionEvaluator rightValue;

  private final double distance;

  private final DriverContext driverContext;

  public StDWithinGeoPointDocValuesAndFieldAndConstantEvaluator(Source source,
      EvalOperator.ExpressionEvaluator leftValue, EvalOperator.ExpressionEvaluator rightValue,
      double distance, DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.leftValue = leftValue;
    this.rightValue = rightValue;
    this.distance = distance;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock leftValueBlock = (LongBlock) leftValue.eval(page)) {
      try (BytesRefBlock rightValueBlock = (BytesRefBlock) rightValue.eval(page)) {
        LongVector leftValueVector = leftValueBlock.asVector();
        if (leftValueVector == null) {
          return eval(page.getPositionCount(), leftValueBlock, rightValueBlock);
        }
        BytesRefVector rightValueVector = rightValueBlock.asVector();
        if (rightValueVector == null) {
          return eval(page.getPositionCount(), leftValueBlock, rightValueBlock);
        }
        return eval(page.getPositionCount(), leftValueVector, rightValueVector);
      }
    }
  }

  public BooleanBlock eval(int positionCount, LongBlock leftValueBlock,
      BytesRefBlock rightValueBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef rightValueScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (leftValueBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (leftValueBlock.getValueCount(p) != 1) {
          if (leftValueBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (rightValueBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (rightValueBlock.getValueCount(p) != 1) {
          if (rightValueBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendBoolean(StDWithin.processGeoPointDocValuesAndField(leftValueBlock.getLong(leftValueBlock.getFirstValueIndex(p)), rightValueBlock.getBytesRef(rightValueBlock.getFirstValueIndex(p), rightValueScratch), distance));
        } catch (IllegalArgumentException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BooleanBlock eval(int positionCount, LongVector leftValueVector,
      BytesRefVector rightValueVector) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef rightValueScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendBoolean(StDWithin.processGeoPointDocValuesAndField(leftValueVector.getLong(p), rightValueVector.getBytesRef(p, rightValueScratch), distance));
        } catch (IllegalArgumentException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "StDWithinGeoPointDocValuesAndFieldAndConstantEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + ", distance=" + distance + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(leftValue, rightValue);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory leftValue;

    private final EvalOperator.ExpressionEvaluator.Factory rightValue;

    private final double distance;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory leftValue,
        EvalOperator.ExpressionEvaluator.Factory rightValue, double distance) {
      this.source = source;
      this.leftValue = leftValue;
      this.rightValue = rightValue;
      this.distance = distance;
    }

    @Override
    public StDWithinGeoPointDocValuesAndFieldAndConstantEvaluator get(DriverContext context) {
      return new StDWithinGeoPointDocValuesAndFieldAndConstantEvaluator(source, leftValue.get(context), rightValue.get(context), distance, context);
    }

    @Override
    public String toString() {
      return "StDWithinGeoPointDocValuesAndFieldAndConstantEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + ", distance=" + distance + "]";
    }
  }
}
