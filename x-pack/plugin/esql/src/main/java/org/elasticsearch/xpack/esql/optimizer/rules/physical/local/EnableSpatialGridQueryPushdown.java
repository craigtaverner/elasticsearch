/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialGridFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.capabilities.TranslationAware.translatable;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.splitAnd;
import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushFiltersToSource.getAliasReplacedBy;

/**
 * Rewrites {@code WHERE ST_GEOHASH(field, precision) == cell_id} (and the geotile/geohex variants)
 * into {@code WHERE ST_INTERSECTS(field, cell_boundary_geometry)}, which can subsequently be pushed
 * down to Lucene by {@link PushFiltersToSource}.
 *
 * <p>The three spatial grid functions produce a cell ID (encoded as a {@code long}) for each
 * document. Comparing a cell ID to a known constant is equivalent to asking whether the document's
 * spatial field intersects the corresponding grid cell boundary — which Lucene's geo_shape query
 * can answer directly, without enumerating intersecting cells per document at all.
 *
 * <p>The rewrite also handles the pattern where the grid function is computed in a preceding
 * {@code EVAL} command:
 * <pre>
 *     FROM index
 *     | EVAL cell = ST_GEOHEX(field, 3)
 *     | WHERE cell == "8a1fb46622dffff"::geohex
 * </pre>
 * This is rewritten to:
 * <pre>
 *     FROM index
 *     | WHERE ST_INTERSECTS(field, cell_boundary)
 *     | EVAL cell = ST_GEOHEX(field, 3)
 * </pre>
 * so that {@link PushFiltersToSource} subsequently pushes the intersects predicate to Lucene.
 */
public class EnableSpatialGridQueryPushdown extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    FilterExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(FilterExec filterExec, LocalPhysicalOptimizerContext ctx) {
        PhysicalPlan plan = filterExec;
        LucenePushdownPredicates lucenePushdownPredicates = LucenePushdownPredicates.from(ctx.searchStats(), ctx.flags());

        if (filterExec.child() instanceof EsQueryExec esQueryExec) {
            plan = rewrite(ctx.foldCtx(), filterExec, esQueryExec, lucenePushdownPredicates);
        } else if (filterExec.child() instanceof EvalExec evalExec && evalExec.child() instanceof EsQueryExec esQueryExec) {
            plan = rewriteBySplittingFilter(ctx.foldCtx(), filterExec, evalExec, esQueryExec, lucenePushdownPredicates);
        }

        return plan;
    }

    private FilterExec rewrite(
        FoldContext ctx,
        FilterExec filterExec,
        EsQueryExec esQueryExec,
        LucenePushdownPredicates lucenePushdownPredicates
    ) {
        Expression rewritten = filterExec.condition()
            .transformDown(Equals.class, equality -> rewriteGridEquality(ctx, equality, lucenePushdownPredicates));
        if (rewritten.equals(filterExec.condition()) == false
            && translatable(rewritten, lucenePushdownPredicates).finish() == TranslationAware.FinishedTranslatable.YES) {
            return new FilterExec(filterExec.source(), esQueryExec, rewritten);
        }
        return filterExec;
    }

    /**
     * Handles the pattern:
     * <pre>
     *     FROM index
     *     | EVAL cell = ST_GEOHEX(field, 3), other = …
     *     | WHERE cell == cell_id AND other > 10
     * </pre>
     * Rewritten to:
     * <pre>
     *     FROM index
     *     | WHERE ST_INTERSECTS(field, cell_boundary)
     *     | EVAL cell = ST_GEOHEX(field, 3), other = …
     *     | WHERE other > 10
     * </pre>
     */
    private PhysicalPlan rewriteBySplittingFilter(
        FoldContext ctx,
        FilterExec filterExec,
        EvalExec evalExec,
        EsQueryExec esQueryExec,
        LucenePushdownPredicates lucenePushdownPredicates
    ) {
        // Collect all aliases that refer to a spatial grid function in the EVAL
        Map<NameId, SpatialGridFunction> gridAliases = getPushableGridAliases(evalExec.fields(), lucenePushdownPredicates);
        if (gridAliases.isEmpty()) {
            return filterExec;
        }

        AttributeMap<Attribute> aliasReplacedBy = getAliasReplacedBy(evalExec);

        List<Expression> pushable = new ArrayList<>();
        List<Expression> nonPushable = new ArrayList<>();

        for (Expression exp : splitAnd(filterExec.condition())) {
            Expression resExp = exp.transformUp(ReferenceAttribute.class, r -> aliasReplacedBy.resolve(r, r));
            Expression rewritten = rewriteGridFilters(ctx, resExp, gridAliases);
            if (rewritten.equals(resExp) == false
                && translatable(rewritten, lucenePushdownPredicates).finish() == TranslationAware.FinishedTranslatable.YES) {
                pushable.add(rewritten);
            } else {
                nonPushable.add(exp);
            }
        }

        if (pushable.isEmpty()) {
            return filterExec;
        }

        var gridFilter = new FilterExec(filterExec.source(), esQueryExec, Predicates.combineAnd(pushable));
        var newEval = new EvalExec(evalExec.source(), gridFilter, evalExec.fields());
        if (nonPushable.isEmpty()) {
            return newEval;
        } else {
            return new FilterExec(filterExec.source(), newEval, Predicates.combineAnd(nonPushable));
        }
    }

    private Map<NameId, SpatialGridFunction> getPushableGridAliases(
        List<Alias> aliases,
        LucenePushdownPredicates lucenePushdownPredicates
    ) {
        Map<NameId, SpatialGridFunction> gridAliases = new LinkedHashMap<>();
        aliases.forEach(alias -> {
            if (alias.child() instanceof SpatialGridFunction gridFn
                && isPushableSpatialField(gridFn.spatialField(), lucenePushdownPredicates)) {
                gridAliases.put(alias.id(), gridFn);
            } else if (alias.child() instanceof ReferenceAttribute ref && gridAliases.containsKey(ref.id())) {
                gridAliases.put(alias.id(), gridAliases.get(ref.id()));
            }
        });
        return gridAliases;
    }

    private Expression rewriteGridFilters(FoldContext ctx, Expression expr, Map<NameId, SpatialGridFunction> gridAliases) {
        return expr.transformDown(Equals.class, equality -> {
            if (equality.left() instanceof ReferenceAttribute r && gridAliases.containsKey(r.id()) && equality.right().foldable()) {
                return rewriteGridEquality(ctx, equality, gridAliases.get(r.id()), equality.right());
            } else if (equality.right() instanceof ReferenceAttribute r && gridAliases.containsKey(r.id()) && equality.left().foldable()) {
                return rewriteGridEquality(ctx, equality, gridAliases.get(r.id()), equality.left());
            }
            return equality;
        });
    }

    /**
     * Rewrites {@code ST_GEOHEX(field, prec) == cell_id_literal} into
     * {@code ST_INTERSECTS(field, cell_boundary_wkb)} by converting the grid cell ID to its boundary
     * geometry.  Returns the original {@code equality} unchanged when:
     * <ul>
     *   <li>the spatial field is not indexed / pushable,</li>
     *   <li>the cell-ID literal cannot be folded or is not a {@code Long}, or</li>
     *   <li>neither side is a grid function.</li>
     * </ul>
     */
    private Expression rewriteGridEquality(FoldContext ctx, Equals equality, LucenePushdownPredicates pushdownPredicates) {
        SpatialGridFunction gridFn = null;
        Expression cellIdExpr = null;

        if (equality.left() instanceof SpatialGridFunction gf && equality.right().foldable()) {
            gridFn = gf;
            cellIdExpr = equality.right();
        } else if (equality.right() instanceof SpatialGridFunction gf && equality.left().foldable()) {
            gridFn = gf;
            cellIdExpr = equality.left();
        }

        if (gridFn == null || isPushableSpatialField(gridFn.spatialField(), pushdownPredicates) == false) {
            return equality;
        }

        return rewriteGridEquality(ctx, equality, gridFn, cellIdExpr);
    }

    private Expression rewriteGridEquality(FoldContext ctx, Equals equality, SpatialGridFunction gridFn, Expression cellIdExpr) {
        DataType gridType = gridFn.dataType(); // GEOHASH, GEOTILE, or GEOHEX
        if (gridType == DataType.GEOHEX) {
            // H3 cell boundaries are mathematical approximations; H3's own containment
            // algorithm can disagree with polygon intersection for geo_point fields near
            // cell boundaries. Skip pushdown to preserve exact H3 semantics.
            // TODO: use GeoGridQueryBuilder (from the spatial plugin) for GEOHEX.
            return equality;
        }
        Object cellIdValue = cellIdExpr.fold(ctx);
        if (cellIdValue instanceof Long cellId) {
            // GEOHASH and GEOTILE cells are exact rectangles; converting the cell ID to its
            // WKB boundary and creating ST_INTERSECTS gives identical results to H3/geohash
            // in-memory evaluation. The resulting SpatialIntersects is then pushed to Lucene
            // by PushFiltersToSource.
            BytesRef wkb = SpatialRelatesUtils.gridCellToWkb(cellId, gridType);
            Literal cellBoundaryLiteral = new Literal(cellIdExpr.source(), wkb, DataType.GEO_SHAPE);
            return new SpatialIntersects(equality.source(), gridFn.spatialField(), cellBoundaryLiteral);
        }
        return equality;
    }

    /**
     * Returns {@code true} when {@code exp} is a spatial {@link FieldAttribute} that can be pushed
     * down to Lucene (indexed, has an exact sub-field, and is not a script-only field).
     * Mirrors the check in {@code BinarySpatialFunction.isPushableSpatialAttribute}.
     */
    private static boolean isPushableSpatialField(Expression exp, LucenePushdownPredicates pushdownPredicates) {
        // A FunctionEsField is synthesized by the block loader and has no indexed Lucene field behind it,
        // so it must not be pushed, even though it reports itself as exact (see FunctionEsField).
        return exp instanceof FieldAttribute fa
            && fa.field() instanceof FunctionEsField == false
            && DataType.isSpatial(fa.dataType())
            && fa.getExactInfo().hasExact()
            && pushdownPredicates.isIndexed(fa);
    }
}
