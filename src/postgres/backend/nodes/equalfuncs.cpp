/*-------------------------------------------------------------------------
 *
 * equalfuncs.c
 *	  Equality functions to compare node trees.
 *
 * NOTE: we currently support comparing all node types found in parse
 * trees.  We do not support comparing executor state trees; there
 * is no need for that, and no point in maintaining all the code that
 * would be needed.  We also do not support comparing Path trees, mainly
 * because the circular linkages between RelOptInfo and Path nodes can't
 * be handled easily in a simple depth-first traversal.
 *
 * Currently, in fact, equal() doesn't know how to compare Plan trees
 * either.  This might need to be fixed someday.
 *
 * NOTE: it is intentional that parse location fields (in nodes that have
 * one) are not compared.  This is because we want, for example, a variable
 * "x" to be considered equal() to another reference to "x" in the query.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/nodes/equalfuncs.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/relation.h"
#include "utils/datum.h"

/*
 * Macros to simplify comparison of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire the convention that the local variables in an Equal routine are
 * named 'a' and 'b'.
 */

/* Compare a simple scalar field (int, float, bool, enum, etc) */
#define COMPARE_SCALAR_FIELD(fldname)           \
  do {                                          \
    if (a->fldname != b->fldname) return false; \
  } while (0)

/* Compare a field that is a pointer to some kind of Node or Node tree */
#define COMPARE_NODE_FIELD(fldname)                   \
  do {                                                \
    if (!equal(a->fldname, b->fldname)) return false; \
  } while (0)

/* Compare a field that is a pointer to a Bitmapset */
#define COMPARE_BITMAPSET_FIELD(fldname)                  \
  do {                                                    \
    if (!bms_equal(a->fldname, b->fldname)) return false; \
  } while (0)

/* Compare a field that is a pointer to a C string, or perhaps NULL */
#define COMPARE_STRING_FIELD(fldname)                    \
  do {                                                   \
    if (!equalstr(a->fldname, b->fldname)) return false; \
  } while (0)

/* Macro for comparing string fields that might be NULL */
#define equalstr(a, b) \
  (((a) != NULL && (b) != NULL) ? (strcmp(a, b) == 0) : (a) == (b))

/* Compare a field that is a pointer to a simple palloc'd object of size sz */
#define COMPARE_POINTER_FIELD(fldname, sz)                       \
  do {                                                           \
    if (memcmp(a->fldname, b->fldname, (sz)) != 0) return false; \
  } while (0)

/* Compare a parse location field (this is a no-op, per note above) */
#define COMPARE_LOCATION_FIELD(fldname) ((void)0)

/* Compare a CoercionForm field (also a no-op, per comment in primnodes.h) */
#define COMPARE_COERCIONFORM_FIELD(fldname) ((void)0)

/*
 *	Stuff from primnodes.h
 */

static bool _equalAlias(const Alias *a, const Alias *b) {
  COMPARE_STRING_FIELD(aliasname);
  COMPARE_NODE_FIELD(colnames);

  return true;
}

static bool _equalRangeVar(const RangeVar *a, const RangeVar *b) {
  COMPARE_STRING_FIELD(catalogname);
  COMPARE_STRING_FIELD(schemaname);
  COMPARE_STRING_FIELD(relname);
  COMPARE_SCALAR_FIELD(inhOpt);
  COMPARE_SCALAR_FIELD(relpersistence);
  COMPARE_NODE_FIELD(alias);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalIntoClause(const IntoClause *a, const IntoClause *b) {
  COMPARE_NODE_FIELD(rel);
  COMPARE_NODE_FIELD(colNames);
  COMPARE_NODE_FIELD(options);
  COMPARE_SCALAR_FIELD(onCommit);
  COMPARE_STRING_FIELD(tableSpaceName);
  COMPARE_NODE_FIELD(viewQuery);
  COMPARE_SCALAR_FIELD(skipData);

  return true;
}

/*
 * We don't need an _equalExpr because Expr is an abstract supertype which
 * should never actually get instantiated.  Also, since it has no common
 * fields except NodeTag, there's no need for a helper routine to factor
 * out comparing the common fields...
 */

static bool _equalVar(const Var *a, const Var *b) {
  COMPARE_SCALAR_FIELD(varno);
  COMPARE_SCALAR_FIELD(varattno);
  COMPARE_SCALAR_FIELD(vartype);
  COMPARE_SCALAR_FIELD(vartypmod);
  COMPARE_SCALAR_FIELD(varcollid);
  COMPARE_SCALAR_FIELD(varlevelsup);
  COMPARE_SCALAR_FIELD(varnoold);
  COMPARE_SCALAR_FIELD(varoattno);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalConst(const Const *a, const Const *b) {
  COMPARE_SCALAR_FIELD(consttype);
  COMPARE_SCALAR_FIELD(consttypmod);
  COMPARE_SCALAR_FIELD(constcollid);
  COMPARE_SCALAR_FIELD(constlen);
  COMPARE_SCALAR_FIELD(constisnull);
  COMPARE_SCALAR_FIELD(constbyval);
  COMPARE_LOCATION_FIELD(location);

  /*
   * We treat all NULL constants of the same type as equal. Someday this
   * might need to change?  But datumIsEqual doesn't work on nulls, so...
   */
  if (a->constisnull) return true;
  return datumIsEqual(a->constvalue, b->constvalue, a->constbyval, a->constlen);
}

static bool _equalParam(const Param *a, const Param *b) {
  COMPARE_SCALAR_FIELD(paramkind);
  COMPARE_SCALAR_FIELD(paramid);
  COMPARE_SCALAR_FIELD(paramtype);
  COMPARE_SCALAR_FIELD(paramtypmod);
  COMPARE_SCALAR_FIELD(paramcollid);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalAggref(const Aggref *a, const Aggref *b) {
  COMPARE_SCALAR_FIELD(aggfnoid);
  COMPARE_SCALAR_FIELD(aggtype);
  COMPARE_SCALAR_FIELD(aggcollid);
  COMPARE_SCALAR_FIELD(inputcollid);
  COMPARE_NODE_FIELD(aggdirectargs);
  COMPARE_NODE_FIELD(args);
  COMPARE_NODE_FIELD(aggorder);
  COMPARE_NODE_FIELD(aggdistinct);
  COMPARE_NODE_FIELD(aggfilter);
  COMPARE_SCALAR_FIELD(aggstar);
  COMPARE_SCALAR_FIELD(aggvariadic);
  COMPARE_SCALAR_FIELD(aggkind);
  COMPARE_SCALAR_FIELD(agglevelsup);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalGroupingFunc(const GroupingFunc *a, const GroupingFunc *b) {
  COMPARE_NODE_FIELD(args);

  /*
   * We must not compare the refs or cols field
   */

  COMPARE_SCALAR_FIELD(agglevelsup);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalWindowFunc(const WindowFunc *a, const WindowFunc *b) {
  COMPARE_SCALAR_FIELD(winfnoid);
  COMPARE_SCALAR_FIELD(wintype);
  COMPARE_SCALAR_FIELD(wincollid);
  COMPARE_SCALAR_FIELD(inputcollid);
  COMPARE_NODE_FIELD(args);
  COMPARE_NODE_FIELD(aggfilter);
  COMPARE_SCALAR_FIELD(winref);
  COMPARE_SCALAR_FIELD(winstar);
  COMPARE_SCALAR_FIELD(winagg);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalArrayRef(const ArrayRef *a, const ArrayRef *b) {
  COMPARE_SCALAR_FIELD(refarraytype);
  COMPARE_SCALAR_FIELD(refelemtype);
  COMPARE_SCALAR_FIELD(reftypmod);
  COMPARE_SCALAR_FIELD(refcollid);
  COMPARE_NODE_FIELD(refupperindexpr);
  COMPARE_NODE_FIELD(reflowerindexpr);
  COMPARE_NODE_FIELD(refexpr);
  COMPARE_NODE_FIELD(refassgnexpr);

  return true;
}

static bool _equalFuncExpr(const FuncExpr *a, const FuncExpr *b) {
  COMPARE_SCALAR_FIELD(funcid);
  COMPARE_SCALAR_FIELD(funcresulttype);
  COMPARE_SCALAR_FIELD(funcretset);
  COMPARE_SCALAR_FIELD(funcvariadic);
  COMPARE_COERCIONFORM_FIELD(funcformat);
  COMPARE_SCALAR_FIELD(funccollid);
  COMPARE_SCALAR_FIELD(inputcollid);
  COMPARE_NODE_FIELD(args);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalNamedArgExpr(const NamedArgExpr *a, const NamedArgExpr *b) {
  COMPARE_NODE_FIELD(arg);
  COMPARE_STRING_FIELD(name);
  COMPARE_SCALAR_FIELD(argnumber);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalOpExpr(const OpExpr *a, const OpExpr *b) {
  COMPARE_SCALAR_FIELD(opno);

  /*
   * Special-case opfuncid: it is allowable for it to differ if one node
   * contains zero and the other doesn't.  This just means that the one node
   * isn't as far along in the parse/plan pipeline and hasn't had the
   * opfuncid cache filled yet.
   */
  if (a->opfuncid != b->opfuncid && a->opfuncid != 0 && b->opfuncid != 0)
    return false;

  COMPARE_SCALAR_FIELD(opresulttype);
  COMPARE_SCALAR_FIELD(opretset);
  COMPARE_SCALAR_FIELD(opcollid);
  COMPARE_SCALAR_FIELD(inputcollid);
  COMPARE_NODE_FIELD(args);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalDistinctExpr(const DistinctExpr *a, const DistinctExpr *b) {
  COMPARE_SCALAR_FIELD(opno);

  /*
   * Special-case opfuncid: it is allowable for it to differ if one node
   * contains zero and the other doesn't.  This just means that the one node
   * isn't as far along in the parse/plan pipeline and hasn't had the
   * opfuncid cache filled yet.
   */
  if (a->opfuncid != b->opfuncid && a->opfuncid != 0 && b->opfuncid != 0)
    return false;

  COMPARE_SCALAR_FIELD(opresulttype);
  COMPARE_SCALAR_FIELD(opretset);
  COMPARE_SCALAR_FIELD(opcollid);
  COMPARE_SCALAR_FIELD(inputcollid);
  COMPARE_NODE_FIELD(args);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalNullIfExpr(const NullIfExpr *a, const NullIfExpr *b) {
  COMPARE_SCALAR_FIELD(opno);

  /*
   * Special-case opfuncid: it is allowable for it to differ if one node
   * contains zero and the other doesn't.  This just means that the one node
   * isn't as far along in the parse/plan pipeline and hasn't had the
   * opfuncid cache filled yet.
   */
  if (a->opfuncid != b->opfuncid && a->opfuncid != 0 && b->opfuncid != 0)
    return false;

  COMPARE_SCALAR_FIELD(opresulttype);
  COMPARE_SCALAR_FIELD(opretset);
  COMPARE_SCALAR_FIELD(opcollid);
  COMPARE_SCALAR_FIELD(inputcollid);
  COMPARE_NODE_FIELD(args);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalScalarArrayOpExpr(const ScalarArrayOpExpr *a,
                                    const ScalarArrayOpExpr *b) {
  COMPARE_SCALAR_FIELD(opno);

  /*
   * Special-case opfuncid: it is allowable for it to differ if one node
   * contains zero and the other doesn't.  This just means that the one node
   * isn't as far along in the parse/plan pipeline and hasn't had the
   * opfuncid cache filled yet.
   */
  if (a->opfuncid != b->opfuncid && a->opfuncid != 0 && b->opfuncid != 0)
    return false;

  COMPARE_SCALAR_FIELD(useOr);
  COMPARE_SCALAR_FIELD(inputcollid);
  COMPARE_NODE_FIELD(args);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalBoolExpr(const BoolExpr *a, const BoolExpr *b) {
  COMPARE_SCALAR_FIELD(boolop);
  COMPARE_NODE_FIELD(args);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalSubLink(const SubLink *a, const SubLink *b) {
  COMPARE_SCALAR_FIELD(subLinkType);
  COMPARE_SCALAR_FIELD(subLinkId);
  COMPARE_NODE_FIELD(testexpr);
  COMPARE_NODE_FIELD(operName);
  COMPARE_NODE_FIELD(subselect);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalSubPlan(const SubPlan *a, const SubPlan *b) {
  COMPARE_SCALAR_FIELD(subLinkType);
  COMPARE_NODE_FIELD(testexpr);
  COMPARE_NODE_FIELD(paramIds);
  COMPARE_SCALAR_FIELD(plan_id);
  COMPARE_STRING_FIELD(plan_name);
  COMPARE_SCALAR_FIELD(firstColType);
  COMPARE_SCALAR_FIELD(firstColTypmod);
  COMPARE_SCALAR_FIELD(firstColCollation);
  COMPARE_SCALAR_FIELD(useHashTable);
  COMPARE_SCALAR_FIELD(unknownEqFalse);
  COMPARE_NODE_FIELD(setParam);
  COMPARE_NODE_FIELD(parParam);
  COMPARE_NODE_FIELD(args);
  COMPARE_SCALAR_FIELD(startup_cost);
  COMPARE_SCALAR_FIELD(per_call_cost);

  return true;
}

static bool _equalAlternativeSubPlan(const AlternativeSubPlan *a,
                                     const AlternativeSubPlan *b) {
  COMPARE_NODE_FIELD(subplans);

  return true;
}

static bool _equalFieldSelect(const FieldSelect *a, const FieldSelect *b) {
  COMPARE_NODE_FIELD(arg);
  COMPARE_SCALAR_FIELD(fieldnum);
  COMPARE_SCALAR_FIELD(resulttype);
  COMPARE_SCALAR_FIELD(resulttypmod);
  COMPARE_SCALAR_FIELD(resultcollid);

  return true;
}

static bool _equalFieldStore(const FieldStore *a, const FieldStore *b) {
  COMPARE_NODE_FIELD(arg);
  COMPARE_NODE_FIELD(newvals);
  COMPARE_NODE_FIELD(fieldnums);
  COMPARE_SCALAR_FIELD(resulttype);

  return true;
}

static bool _equalRelabelType(const RelabelType *a, const RelabelType *b) {
  COMPARE_NODE_FIELD(arg);
  COMPARE_SCALAR_FIELD(resulttype);
  COMPARE_SCALAR_FIELD(resulttypmod);
  COMPARE_SCALAR_FIELD(resultcollid);
  COMPARE_COERCIONFORM_FIELD(relabelformat);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalCoerceViaIO(const CoerceViaIO *a, const CoerceViaIO *b) {
  COMPARE_NODE_FIELD(arg);
  COMPARE_SCALAR_FIELD(resulttype);
  COMPARE_SCALAR_FIELD(resultcollid);
  COMPARE_COERCIONFORM_FIELD(coerceformat);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalArrayCoerceExpr(const ArrayCoerceExpr *a,
                                  const ArrayCoerceExpr *b) {
  COMPARE_NODE_FIELD(arg);
  COMPARE_SCALAR_FIELD(elemfuncid);
  COMPARE_SCALAR_FIELD(resulttype);
  COMPARE_SCALAR_FIELD(resulttypmod);
  COMPARE_SCALAR_FIELD(resultcollid);
  COMPARE_SCALAR_FIELD(isExplicit);
  COMPARE_COERCIONFORM_FIELD(coerceformat);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalConvertRowtypeExpr(const ConvertRowtypeExpr *a,
                                     const ConvertRowtypeExpr *b) {
  COMPARE_NODE_FIELD(arg);
  COMPARE_SCALAR_FIELD(resulttype);
  COMPARE_COERCIONFORM_FIELD(convertformat);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalCollateExpr(const CollateExpr *a, const CollateExpr *b) {
  COMPARE_NODE_FIELD(arg);
  COMPARE_SCALAR_FIELD(collOid);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalCaseExpr(const CaseExpr *a, const CaseExpr *b) {
  COMPARE_SCALAR_FIELD(casetype);
  COMPARE_SCALAR_FIELD(casecollid);
  COMPARE_NODE_FIELD(arg);
  COMPARE_NODE_FIELD(args);
  COMPARE_NODE_FIELD(defresult);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalCaseWhen(const CaseWhen *a, const CaseWhen *b) {
  COMPARE_NODE_FIELD(expr);
  COMPARE_NODE_FIELD(result);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalCaseTestExpr(const CaseTestExpr *a, const CaseTestExpr *b) {
  COMPARE_SCALAR_FIELD(typeId);
  COMPARE_SCALAR_FIELD(typeMod);
  COMPARE_SCALAR_FIELD(collation);

  return true;
}

static bool _equalArrayExpr(const ArrayExpr *a, const ArrayExpr *b) {
  COMPARE_SCALAR_FIELD(array_typeid);
  COMPARE_SCALAR_FIELD(array_collid);
  COMPARE_SCALAR_FIELD(element_typeid);
  COMPARE_NODE_FIELD(elements);
  COMPARE_SCALAR_FIELD(multidims);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalRowExpr(const RowExpr *a, const RowExpr *b) {
  COMPARE_NODE_FIELD(args);
  COMPARE_SCALAR_FIELD(row_typeid);
  COMPARE_COERCIONFORM_FIELD(row_format);
  COMPARE_NODE_FIELD(colnames);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalRowCompareExpr(const RowCompareExpr *a,
                                 const RowCompareExpr *b) {
  COMPARE_SCALAR_FIELD(rctype);
  COMPARE_NODE_FIELD(opnos);
  COMPARE_NODE_FIELD(opfamilies);
  COMPARE_NODE_FIELD(inputcollids);
  COMPARE_NODE_FIELD(largs);
  COMPARE_NODE_FIELD(rargs);

  return true;
}

static bool _equalCoalesceExpr(const CoalesceExpr *a, const CoalesceExpr *b) {
  COMPARE_SCALAR_FIELD(coalescetype);
  COMPARE_SCALAR_FIELD(coalescecollid);
  COMPARE_NODE_FIELD(args);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalMinMaxExpr(const MinMaxExpr *a, const MinMaxExpr *b) {
  COMPARE_SCALAR_FIELD(minmaxtype);
  COMPARE_SCALAR_FIELD(minmaxcollid);
  COMPARE_SCALAR_FIELD(inputcollid);
  COMPARE_SCALAR_FIELD(op);
  COMPARE_NODE_FIELD(args);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalXmlExpr(const XmlExpr *a, const XmlExpr *b) {
  COMPARE_SCALAR_FIELD(op);
  COMPARE_STRING_FIELD(name);
  COMPARE_NODE_FIELD(named_args);
  COMPARE_NODE_FIELD(arg_names);
  COMPARE_NODE_FIELD(args);
  COMPARE_SCALAR_FIELD(xmloption);
  COMPARE_SCALAR_FIELD(type);
  COMPARE_SCALAR_FIELD(typmod);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalNullTest(const NullTest *a, const NullTest *b) {
  COMPARE_NODE_FIELD(arg);
  COMPARE_SCALAR_FIELD(nulltesttype);
  COMPARE_SCALAR_FIELD(argisrow);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalBooleanTest(const BooleanTest *a, const BooleanTest *b) {
  COMPARE_NODE_FIELD(arg);
  COMPARE_SCALAR_FIELD(booltesttype);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalCoerceToDomain(const CoerceToDomain *a,
                                 const CoerceToDomain *b) {
  COMPARE_NODE_FIELD(arg);
  COMPARE_SCALAR_FIELD(resulttype);
  COMPARE_SCALAR_FIELD(resulttypmod);
  COMPARE_SCALAR_FIELD(resultcollid);
  COMPARE_COERCIONFORM_FIELD(coercionformat);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalCoerceToDomainValue(const CoerceToDomainValue *a,
                                      const CoerceToDomainValue *b) {
  COMPARE_SCALAR_FIELD(typeId);
  COMPARE_SCALAR_FIELD(typeMod);
  COMPARE_SCALAR_FIELD(collation);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalSetToDefault(const SetToDefault *a, const SetToDefault *b) {
  COMPARE_SCALAR_FIELD(typeId);
  COMPARE_SCALAR_FIELD(typeMod);
  COMPARE_SCALAR_FIELD(collation);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalCurrentOfExpr(const CurrentOfExpr *a,
                                const CurrentOfExpr *b) {
  COMPARE_SCALAR_FIELD(cvarno);
  COMPARE_STRING_FIELD(cursor_name);
  COMPARE_SCALAR_FIELD(cursor_param);

  return true;
}

static bool _equalInferenceElem(const InferenceElem *a,
                                const InferenceElem *b) {
  COMPARE_NODE_FIELD(expr);
  COMPARE_SCALAR_FIELD(infercollid);
  COMPARE_SCALAR_FIELD(inferopclass);

  return true;
}

static bool _equalTargetEntry(const TargetEntry *a, const TargetEntry *b) {
  COMPARE_NODE_FIELD(expr);
  COMPARE_SCALAR_FIELD(resno);
  COMPARE_STRING_FIELD(resname);
  COMPARE_SCALAR_FIELD(ressortgroupref);
  COMPARE_SCALAR_FIELD(resorigtbl);
  COMPARE_SCALAR_FIELD(resorigcol);
  COMPARE_SCALAR_FIELD(resjunk);

  return true;
}

static bool _equalRangeTblRef(const RangeTblRef *a, const RangeTblRef *b) {
  COMPARE_SCALAR_FIELD(rtindex);

  return true;
}

static bool _equalJoinExpr(const JoinExpr *a, const JoinExpr *b) {
  COMPARE_SCALAR_FIELD(jointype);
  COMPARE_SCALAR_FIELD(isNatural);
  COMPARE_NODE_FIELD(larg);
  COMPARE_NODE_FIELD(rarg);
  COMPARE_NODE_FIELD(usingClause);
  COMPARE_NODE_FIELD(quals);
  COMPARE_NODE_FIELD(alias);
  COMPARE_SCALAR_FIELD(rtindex);

  return true;
}

static bool _equalFromExpr(const FromExpr *a, const FromExpr *b) {
  COMPARE_NODE_FIELD(fromlist);
  COMPARE_NODE_FIELD(quals);

  return true;
}

static bool _equalOnConflictExpr(const OnConflictExpr *a,
                                 const OnConflictExpr *b) {
  COMPARE_SCALAR_FIELD(action);
  COMPARE_NODE_FIELD(arbiterElems);
  COMPARE_NODE_FIELD(arbiterWhere);
  COMPARE_NODE_FIELD(onConflictSet);
  COMPARE_NODE_FIELD(onConflictWhere);
  COMPARE_SCALAR_FIELD(constraint);
  COMPARE_SCALAR_FIELD(exclRelIndex);
  COMPARE_NODE_FIELD(exclRelTlist);

  return true;
}

/*
 * Stuff from relation.h
 */

static bool _equalPathKey(const PathKey *a, const PathKey *b) {
  /* We assume pointer equality is sufficient to compare the eclasses */
  COMPARE_SCALAR_FIELD(pk_eclass);
  COMPARE_SCALAR_FIELD(pk_opfamily);
  COMPARE_SCALAR_FIELD(pk_strategy);
  COMPARE_SCALAR_FIELD(pk_nulls_first);

  return true;
}

static bool _equalRestrictInfo(const RestrictInfo *a, const RestrictInfo *b) {
  COMPARE_NODE_FIELD(clause);
  COMPARE_SCALAR_FIELD(is_pushed_down);
  COMPARE_SCALAR_FIELD(outerjoin_delayed);
  COMPARE_BITMAPSET_FIELD(required_relids);
  COMPARE_BITMAPSET_FIELD(outer_relids);
  COMPARE_BITMAPSET_FIELD(nullable_relids);

  /*
   * We ignore all the remaining fields, since they may not be set yet, and
   * should be derivable from the clause anyway.
   */

  return true;
}

static bool _equalPlaceHolderVar(const PlaceHolderVar *a,
                                 const PlaceHolderVar *b) {
  /*
   * We intentionally do not compare phexpr.  Two PlaceHolderVars with the
   * same ID and levelsup should be considered equal even if the contained
   * expressions have managed to mutate to different states.  This will
   * happen during final plan construction when there are nested PHVs, since
   * the inner PHV will get replaced by a Param in some copies of the outer
   * PHV.  Another way in which it can happen is that initplan sublinks
   * could get replaced by differently-numbered Params when sublink folding
   * is done.  (The end result of such a situation would be some
   * unreferenced initplans, which is annoying but not really a problem.) On
   * the same reasoning, there is no need to examine phrels.
   *
   * COMPARE_NODE_FIELD(phexpr);
   *
   * COMPARE_BITMAPSET_FIELD(phrels);
   */
  COMPARE_SCALAR_FIELD(phid);
  COMPARE_SCALAR_FIELD(phlevelsup);

  return true;
}

static bool _equalSpecialJoinInfo(const SpecialJoinInfo *a,
                                  const SpecialJoinInfo *b) {
  COMPARE_BITMAPSET_FIELD(min_lefthand);
  COMPARE_BITMAPSET_FIELD(min_righthand);
  COMPARE_BITMAPSET_FIELD(syn_lefthand);
  COMPARE_BITMAPSET_FIELD(syn_righthand);
  COMPARE_SCALAR_FIELD(jointype);
  COMPARE_SCALAR_FIELD(lhs_strict);
  COMPARE_SCALAR_FIELD(delay_upper_joins);
  COMPARE_SCALAR_FIELD(semi_can_btree);
  COMPARE_SCALAR_FIELD(semi_can_hash);
  COMPARE_NODE_FIELD(semi_operators);
  COMPARE_NODE_FIELD(semi_rhs_exprs);

  return true;
}

static bool _equalLateralJoinInfo(const LateralJoinInfo *a,
                                  const LateralJoinInfo *b) {
  COMPARE_BITMAPSET_FIELD(lateral_lhs);
  COMPARE_BITMAPSET_FIELD(lateral_rhs);

  return true;
}

static bool _equalAppendRelInfo(const AppendRelInfo *a,
                                const AppendRelInfo *b) {
  COMPARE_SCALAR_FIELD(parent_relid);
  COMPARE_SCALAR_FIELD(child_relid);
  COMPARE_SCALAR_FIELD(parent_reltype);
  COMPARE_SCALAR_FIELD(child_reltype);
  COMPARE_NODE_FIELD(translated_vars);
  COMPARE_SCALAR_FIELD(parent_reloid);

  return true;
}

static bool _equalPlaceHolderInfo(const PlaceHolderInfo *a,
                                  const PlaceHolderInfo *b) {
  COMPARE_SCALAR_FIELD(phid);
  COMPARE_NODE_FIELD(ph_var); /* should be redundant */
  COMPARE_BITMAPSET_FIELD(ph_eval_at);
  COMPARE_BITMAPSET_FIELD(ph_lateral);
  COMPARE_BITMAPSET_FIELD(ph_needed);
  COMPARE_SCALAR_FIELD(ph_width);

  return true;
}

/*
 * Stuff from parsenodes.h
 */

static bool _equalQuery(const Query *a, const Query *b) {
  COMPARE_SCALAR_FIELD(commandType);
  COMPARE_SCALAR_FIELD(querySource);
  /* we intentionally ignore queryId, since it might not be set */
  COMPARE_SCALAR_FIELD(canSetTag);
  COMPARE_NODE_FIELD(utilityStmt);
  COMPARE_SCALAR_FIELD(resultRelation);
  COMPARE_SCALAR_FIELD(hasAggs);
  COMPARE_SCALAR_FIELD(hasWindowFuncs);
  COMPARE_SCALAR_FIELD(hasSubLinks);
  COMPARE_SCALAR_FIELD(hasDistinctOn);
  COMPARE_SCALAR_FIELD(hasRecursive);
  COMPARE_SCALAR_FIELD(hasModifyingCTE);
  COMPARE_SCALAR_FIELD(hasForUpdate);
  COMPARE_SCALAR_FIELD(hasRowSecurity);
  COMPARE_NODE_FIELD(cteList);
  COMPARE_NODE_FIELD(rtable);
  COMPARE_NODE_FIELD(jointree);
  COMPARE_NODE_FIELD(targetList);
  COMPARE_NODE_FIELD(withCheckOptions);
  COMPARE_NODE_FIELD(onConflict);
  COMPARE_NODE_FIELD(returningList);
  COMPARE_NODE_FIELD(groupClause);
  COMPARE_NODE_FIELD(groupingSets);
  COMPARE_NODE_FIELD(havingQual);
  COMPARE_NODE_FIELD(windowClause);
  COMPARE_NODE_FIELD(distinctClause);
  COMPARE_NODE_FIELD(sortClause);
  COMPARE_NODE_FIELD(limitOffset);
  COMPARE_NODE_FIELD(limitCount);
  COMPARE_NODE_FIELD(rowMarks);
  COMPARE_NODE_FIELD(setOperations);
  COMPARE_NODE_FIELD(constraintDeps);

  return true;
}

static bool _equalInsertStmt(const InsertStmt *a, const InsertStmt *b) {
  COMPARE_NODE_FIELD(relation);
  COMPARE_NODE_FIELD(cols);
  COMPARE_NODE_FIELD(selectStmt);
  COMPARE_NODE_FIELD(onConflictClause);
  COMPARE_NODE_FIELD(returningList);
  COMPARE_NODE_FIELD(withClause);

  return true;
}

static bool _equalDeleteStmt(const DeleteStmt *a, const DeleteStmt *b) {
  COMPARE_NODE_FIELD(relation);
  COMPARE_NODE_FIELD(usingClause);
  COMPARE_NODE_FIELD(whereClause);
  COMPARE_NODE_FIELD(returningList);
  COMPARE_NODE_FIELD(withClause);

  return true;
}

static bool _equalUpdateStmt(const UpdateStmt *a, const UpdateStmt *b) {
  COMPARE_NODE_FIELD(relation);
  COMPARE_NODE_FIELD(targetList);
  COMPARE_NODE_FIELD(whereClause);
  COMPARE_NODE_FIELD(fromClause);
  COMPARE_NODE_FIELD(returningList);
  COMPARE_NODE_FIELD(withClause);

  return true;
}

static bool _equalSelectStmt(const SelectStmt *a, const SelectStmt *b) {
  COMPARE_NODE_FIELD(distinctClause);
  COMPARE_NODE_FIELD(intoClause);
  COMPARE_NODE_FIELD(targetList);
  COMPARE_NODE_FIELD(fromClause);
  COMPARE_NODE_FIELD(whereClause);
  COMPARE_NODE_FIELD(groupClause);
  COMPARE_NODE_FIELD(havingClause);
  COMPARE_NODE_FIELD(windowClause);
  COMPARE_NODE_FIELD(valuesLists);
  COMPARE_NODE_FIELD(sortClause);
  COMPARE_NODE_FIELD(limitOffset);
  COMPARE_NODE_FIELD(limitCount);
  COMPARE_NODE_FIELD(lockingClause);
  COMPARE_NODE_FIELD(withClause);
  COMPARE_SCALAR_FIELD(op);
  COMPARE_SCALAR_FIELD(all);
  COMPARE_NODE_FIELD(larg);
  COMPARE_NODE_FIELD(rarg);

  return true;
}

static bool _equalSetOperationStmt(const SetOperationStmt *a,
                                   const SetOperationStmt *b) {
  COMPARE_SCALAR_FIELD(op);
  COMPARE_SCALAR_FIELD(all);
  COMPARE_NODE_FIELD(larg);
  COMPARE_NODE_FIELD(rarg);
  COMPARE_NODE_FIELD(colTypes);
  COMPARE_NODE_FIELD(colTypmods);
  COMPARE_NODE_FIELD(colCollations);
  COMPARE_NODE_FIELD(groupClauses);

  return true;
}

static bool _equalAlterTableStmt(const AlterTableStmt *a,
                                 const AlterTableStmt *b) {
  COMPARE_NODE_FIELD(relation);
  COMPARE_NODE_FIELD(cmds);
  COMPARE_SCALAR_FIELD(relkind);
  COMPARE_SCALAR_FIELD(missing_ok);

  return true;
}

static bool _equalAlterTableCmd(const AlterTableCmd *a,
                                const AlterTableCmd *b) {
  COMPARE_SCALAR_FIELD(subtype);
  COMPARE_STRING_FIELD(name);
  COMPARE_NODE_FIELD(newowner);
  COMPARE_NODE_FIELD(def);
  COMPARE_SCALAR_FIELD(behavior);
  COMPARE_SCALAR_FIELD(missing_ok);

  return true;
}

static bool _equalAlterDomainStmt(const AlterDomainStmt *a,
                                  const AlterDomainStmt *b) {
  COMPARE_SCALAR_FIELD(subtype);
  COMPARE_NODE_FIELD(typeName);
  COMPARE_STRING_FIELD(name);
  COMPARE_NODE_FIELD(def);
  COMPARE_SCALAR_FIELD(behavior);
  COMPARE_SCALAR_FIELD(missing_ok);

  return true;
}

static bool _equalGrantStmt(const GrantStmt *a, const GrantStmt *b) {
  COMPARE_SCALAR_FIELD(is_grant);
  COMPARE_SCALAR_FIELD(targtype);
  COMPARE_SCALAR_FIELD(objtype);
  COMPARE_NODE_FIELD(objects);
  COMPARE_NODE_FIELD(privileges);
  COMPARE_NODE_FIELD(grantees);
  COMPARE_SCALAR_FIELD(grant_option);
  COMPARE_SCALAR_FIELD(behavior);

  return true;
}

static bool _equalFuncWithArgs(const FuncWithArgs *a, const FuncWithArgs *b) {
  COMPARE_NODE_FIELD(funcname);
  COMPARE_NODE_FIELD(funcargs);

  return true;
}

static bool _equalAccessPriv(const AccessPriv *a, const AccessPriv *b) {
  COMPARE_STRING_FIELD(priv_name);
  COMPARE_NODE_FIELD(cols);

  return true;
}

static bool _equalGrantRoleStmt(const GrantRoleStmt *a,
                                const GrantRoleStmt *b) {
  COMPARE_NODE_FIELD(granted_roles);
  COMPARE_NODE_FIELD(grantee_roles);
  COMPARE_SCALAR_FIELD(is_grant);
  COMPARE_SCALAR_FIELD(admin_opt);
  COMPARE_NODE_FIELD(grantor);
  COMPARE_SCALAR_FIELD(behavior);

  return true;
}

static bool _equalAlterDefaultPrivilegesStmt(
    const AlterDefaultPrivilegesStmt *a, const AlterDefaultPrivilegesStmt *b) {
  COMPARE_NODE_FIELD(options);
  COMPARE_NODE_FIELD(action);

  return true;
}

static bool _equalDeclareCursorStmt(const DeclareCursorStmt *a,
                                    const DeclareCursorStmt *b) {
  COMPARE_STRING_FIELD(portalname);
  COMPARE_SCALAR_FIELD(options);
  COMPARE_NODE_FIELD(query);

  return true;
}

static bool _equalClosePortalStmt(const ClosePortalStmt *a,
                                  const ClosePortalStmt *b) {
  COMPARE_STRING_FIELD(portalname);

  return true;
}

static bool _equalClusterStmt(const ClusterStmt *a, const ClusterStmt *b) {
  COMPARE_NODE_FIELD(relation);
  COMPARE_STRING_FIELD(indexname);
  COMPARE_SCALAR_FIELD(verbose);

  return true;
}

static bool _equalCopyStmt(const CopyStmt *a, const CopyStmt *b) {
  COMPARE_NODE_FIELD(relation);
  COMPARE_NODE_FIELD(query);
  COMPARE_NODE_FIELD(attlist);
  COMPARE_SCALAR_FIELD(is_from);
  COMPARE_SCALAR_FIELD(is_program);
  COMPARE_STRING_FIELD(filename);
  COMPARE_NODE_FIELD(options);

  return true;
}

static bool _equalCreateStmt(const CreateStmt *a, const CreateStmt *b) {
  COMPARE_NODE_FIELD(relation);
  COMPARE_NODE_FIELD(tableElts);
  COMPARE_NODE_FIELD(inhRelations);
  COMPARE_NODE_FIELD(ofTypename);
  COMPARE_NODE_FIELD(constraints);
  COMPARE_NODE_FIELD(options);
  COMPARE_SCALAR_FIELD(oncommit);
  COMPARE_STRING_FIELD(tablespacename);
  COMPARE_SCALAR_FIELD(if_not_exists);

  return true;
}

static bool _equalTableLikeClause(const TableLikeClause *a,
                                  const TableLikeClause *b) {
  COMPARE_NODE_FIELD(relation);
  COMPARE_SCALAR_FIELD(options);

  return true;
}

static bool _equalDefineStmt(const DefineStmt *a, const DefineStmt *b) {
  COMPARE_SCALAR_FIELD(kind);
  COMPARE_SCALAR_FIELD(oldstyle);
  COMPARE_NODE_FIELD(defnames);
  COMPARE_NODE_FIELD(args);
  COMPARE_NODE_FIELD(definition);

  return true;
}

static bool _equalDropStmt(const DropStmt *a, const DropStmt *b) {
  COMPARE_NODE_FIELD(objects);
  COMPARE_NODE_FIELD(arguments);
  COMPARE_SCALAR_FIELD(removeType);
  COMPARE_SCALAR_FIELD(behavior);
  COMPARE_SCALAR_FIELD(missing_ok);
  COMPARE_SCALAR_FIELD(concurrent);

  return true;
}

static bool _equalTruncateStmt(const TruncateStmt *a, const TruncateStmt *b) {
  COMPARE_NODE_FIELD(relations);
  COMPARE_SCALAR_FIELD(restart_seqs);
  COMPARE_SCALAR_FIELD(behavior);

  return true;
}

static bool _equalCommentStmt(const CommentStmt *a, const CommentStmt *b) {
  COMPARE_SCALAR_FIELD(objtype);
  COMPARE_NODE_FIELD(objname);
  COMPARE_NODE_FIELD(objargs);
  COMPARE_STRING_FIELD(comment);

  return true;
}

static bool _equalSecLabelStmt(const SecLabelStmt *a, const SecLabelStmt *b) {
  COMPARE_SCALAR_FIELD(objtype);
  COMPARE_NODE_FIELD(objname);
  COMPARE_NODE_FIELD(objargs);
  COMPARE_STRING_FIELD(provider);
  COMPARE_STRING_FIELD(label);

  return true;
}

static bool _equalFetchStmt(const FetchStmt *a, const FetchStmt *b) {
  COMPARE_SCALAR_FIELD(direction);
  COMPARE_SCALAR_FIELD(howMany);
  COMPARE_STRING_FIELD(portalname);
  COMPARE_SCALAR_FIELD(ismove);

  return true;
}

static bool _equalIndexStmt(const IndexStmt *a, const IndexStmt *b) {
  COMPARE_STRING_FIELD(idxname);
  COMPARE_NODE_FIELD(relation);
  COMPARE_STRING_FIELD(accessMethod);
  COMPARE_STRING_FIELD(tableSpace);
  COMPARE_NODE_FIELD(indexParams);
  COMPARE_NODE_FIELD(options);
  COMPARE_NODE_FIELD(whereClause);
  COMPARE_NODE_FIELD(excludeOpNames);
  COMPARE_STRING_FIELD(idxcomment);
  COMPARE_SCALAR_FIELD(indexOid);
  COMPARE_SCALAR_FIELD(oldNode);
  COMPARE_SCALAR_FIELD(unique);
  COMPARE_SCALAR_FIELD(primary);
  COMPARE_SCALAR_FIELD(isconstraint);
  COMPARE_SCALAR_FIELD(deferrable);
  COMPARE_SCALAR_FIELD(initdeferred);
  COMPARE_SCALAR_FIELD(transformed);
  COMPARE_SCALAR_FIELD(concurrent);
  COMPARE_SCALAR_FIELD(if_not_exists);

  return true;
}

static bool _equalCreateFunctionStmt(const CreateFunctionStmt *a,
                                     const CreateFunctionStmt *b) {
  COMPARE_SCALAR_FIELD(replace);
  COMPARE_NODE_FIELD(funcname);
  COMPARE_NODE_FIELD(parameters);
  COMPARE_NODE_FIELD(returnType);
  COMPARE_NODE_FIELD(options);
  COMPARE_NODE_FIELD(withClause);

  return true;
}

static bool _equalFunctionParameter(const FunctionParameter *a,
                                    const FunctionParameter *b) {
  COMPARE_STRING_FIELD(name);
  COMPARE_NODE_FIELD(argType);
  COMPARE_SCALAR_FIELD(mode);
  COMPARE_NODE_FIELD(defexpr);

  return true;
}

static bool _equalAlterFunctionStmt(const AlterFunctionStmt *a,
                                    const AlterFunctionStmt *b) {
  COMPARE_NODE_FIELD(func);
  COMPARE_NODE_FIELD(actions);

  return true;
}

static bool _equalDoStmt(const DoStmt *a, const DoStmt *b) {
  COMPARE_NODE_FIELD(args);

  return true;
}

static bool _equalRenameStmt(const RenameStmt *a, const RenameStmt *b) {
  COMPARE_SCALAR_FIELD(renameType);
  COMPARE_SCALAR_FIELD(relationType);
  COMPARE_NODE_FIELD(relation);
  COMPARE_NODE_FIELD(object);
  COMPARE_NODE_FIELD(objarg);
  COMPARE_STRING_FIELD(subname);
  COMPARE_STRING_FIELD(newname);
  COMPARE_SCALAR_FIELD(behavior);
  COMPARE_SCALAR_FIELD(missing_ok);

  return true;
}

static bool _equalAlterObjectSchemaStmt(const AlterObjectSchemaStmt *a,
                                        const AlterObjectSchemaStmt *b) {
  COMPARE_SCALAR_FIELD(objectType);
  COMPARE_NODE_FIELD(relation);
  COMPARE_NODE_FIELD(object);
  COMPARE_NODE_FIELD(objarg);
  COMPARE_STRING_FIELD(newschema);
  COMPARE_SCALAR_FIELD(missing_ok);

  return true;
}

static bool _equalAlterOwnerStmt(const AlterOwnerStmt *a,
                                 const AlterOwnerStmt *b) {
  COMPARE_SCALAR_FIELD(objectType);
  COMPARE_NODE_FIELD(relation);
  COMPARE_NODE_FIELD(object);
  COMPARE_NODE_FIELD(objarg);
  COMPARE_NODE_FIELD(newowner);

  return true;
}

static bool _equalRuleStmt(const RuleStmt *a, const RuleStmt *b) {
  COMPARE_NODE_FIELD(relation);
  COMPARE_STRING_FIELD(rulename);
  COMPARE_NODE_FIELD(whereClause);
  COMPARE_SCALAR_FIELD(event);
  COMPARE_SCALAR_FIELD(instead);
  COMPARE_NODE_FIELD(actions);
  COMPARE_SCALAR_FIELD(replace);

  return true;
}

static bool _equalNotifyStmt(const NotifyStmt *a, const NotifyStmt *b) {
  COMPARE_STRING_FIELD(conditionname);
  COMPARE_STRING_FIELD(payload);

  return true;
}

static bool _equalListenStmt(const ListenStmt *a, const ListenStmt *b) {
  COMPARE_STRING_FIELD(conditionname);

  return true;
}

static bool _equalUnlistenStmt(const UnlistenStmt *a, const UnlistenStmt *b) {
  COMPARE_STRING_FIELD(conditionname);

  return true;
}

static bool _equalTransactionStmt(const TransactionStmt *a,
                                  const TransactionStmt *b) {
  COMPARE_SCALAR_FIELD(kind);
  COMPARE_NODE_FIELD(options);
  COMPARE_STRING_FIELD(gid);

  return true;
}

static bool _equalCompositeTypeStmt(const CompositeTypeStmt *a,
                                    const CompositeTypeStmt *b) {
  COMPARE_NODE_FIELD(typevar);
  COMPARE_NODE_FIELD(coldeflist);

  return true;
}

static bool _equalCreateEnumStmt(const CreateEnumStmt *a,
                                 const CreateEnumStmt *b) {
  COMPARE_NODE_FIELD(typeName);
  COMPARE_NODE_FIELD(vals);

  return true;
}

static bool _equalCreateRangeStmt(const CreateRangeStmt *a,
                                  const CreateRangeStmt *b) {
  COMPARE_NODE_FIELD(typeName);
  COMPARE_NODE_FIELD(params);

  return true;
}

static bool _equalAlterEnumStmt(const AlterEnumStmt *a,
                                const AlterEnumStmt *b) {
  COMPARE_NODE_FIELD(typeName);
  COMPARE_STRING_FIELD(newVal);
  COMPARE_STRING_FIELD(newValNeighbor);
  COMPARE_SCALAR_FIELD(newValIsAfter);
  COMPARE_SCALAR_FIELD(skipIfExists);

  return true;
}

static bool _equalViewStmt(const ViewStmt *a, const ViewStmt *b) {
  COMPARE_NODE_FIELD(view);
  COMPARE_NODE_FIELD(aliases);
  COMPARE_NODE_FIELD(query);
  COMPARE_SCALAR_FIELD(replace);
  COMPARE_NODE_FIELD(options);
  COMPARE_SCALAR_FIELD(withCheckOption);

  return true;
}

static bool _equalLoadStmt(const LoadStmt *a, const LoadStmt *b) {
  COMPARE_STRING_FIELD(filename);

  return true;
}

static bool _equalCreateDomainStmt(const CreateDomainStmt *a,
                                   const CreateDomainStmt *b) {
  COMPARE_NODE_FIELD(domainname);
  COMPARE_NODE_FIELD(typeName);
  COMPARE_NODE_FIELD(collClause);
  COMPARE_NODE_FIELD(constraints);

  return true;
}

static bool _equalCreateOpClassStmt(const CreateOpClassStmt *a,
                                    const CreateOpClassStmt *b) {
  COMPARE_NODE_FIELD(opclassname);
  COMPARE_NODE_FIELD(opfamilyname);
  COMPARE_STRING_FIELD(amname);
  COMPARE_NODE_FIELD(datatype);
  COMPARE_NODE_FIELD(items);
  COMPARE_SCALAR_FIELD(isDefault);

  return true;
}

static bool _equalCreateOpClassItem(const CreateOpClassItem *a,
                                    const CreateOpClassItem *b) {
  COMPARE_SCALAR_FIELD(itemtype);
  COMPARE_NODE_FIELD(name);
  COMPARE_NODE_FIELD(args);
  COMPARE_SCALAR_FIELD(number);
  COMPARE_NODE_FIELD(order_family);
  COMPARE_NODE_FIELD(class_args);
  COMPARE_NODE_FIELD(storedtype);

  return true;
}

static bool _equalCreateOpFamilyStmt(const CreateOpFamilyStmt *a,
                                     const CreateOpFamilyStmt *b) {
  COMPARE_NODE_FIELD(opfamilyname);
  COMPARE_STRING_FIELD(amname);

  return true;
}

static bool _equalAlterOpFamilyStmt(const AlterOpFamilyStmt *a,
                                    const AlterOpFamilyStmt *b) {
  COMPARE_NODE_FIELD(opfamilyname);
  COMPARE_STRING_FIELD(amname);
  COMPARE_SCALAR_FIELD(isDrop);
  COMPARE_NODE_FIELD(items);

  return true;
}

static bool _equalCreatedbStmt(const CreatedbStmt *a, const CreatedbStmt *b) {
  COMPARE_STRING_FIELD(dbname);
  COMPARE_NODE_FIELD(options);

  return true;
}

static bool _equalAlterDatabaseStmt(const AlterDatabaseStmt *a,
                                    const AlterDatabaseStmt *b) {
  COMPARE_STRING_FIELD(dbname);
  COMPARE_NODE_FIELD(options);

  return true;
}

static bool _equalAlterDatabaseSetStmt(const AlterDatabaseSetStmt *a,
                                       const AlterDatabaseSetStmt *b) {
  COMPARE_STRING_FIELD(dbname);
  COMPARE_NODE_FIELD(setstmt);

  return true;
}

static bool _equalDropdbStmt(const DropdbStmt *a, const DropdbStmt *b) {
  COMPARE_STRING_FIELD(dbname);
  COMPARE_SCALAR_FIELD(missing_ok);

  return true;
}

static bool _equalVacuumStmt(const VacuumStmt *a, const VacuumStmt *b) {
  COMPARE_SCALAR_FIELD(options);
  COMPARE_NODE_FIELD(relation);
  COMPARE_NODE_FIELD(va_cols);

  return true;
}

static bool _equalExplainStmt(const ExplainStmt *a, const ExplainStmt *b) {
  COMPARE_NODE_FIELD(query);
  COMPARE_NODE_FIELD(options);

  return true;
}

static bool _equalCreateTableAsStmt(const CreateTableAsStmt *a,
                                    const CreateTableAsStmt *b) {
  COMPARE_NODE_FIELD(query);
  COMPARE_NODE_FIELD(into);
  COMPARE_SCALAR_FIELD(relkind);
  COMPARE_SCALAR_FIELD(is_select_into);
  COMPARE_SCALAR_FIELD(if_not_exists);

  return true;
}

static bool _equalRefreshMatViewStmt(const RefreshMatViewStmt *a,
                                     const RefreshMatViewStmt *b) {
  COMPARE_SCALAR_FIELD(concurrent);
  COMPARE_SCALAR_FIELD(skipData);
  COMPARE_NODE_FIELD(relation);

  return true;
}

static bool _equalReplicaIdentityStmt(const ReplicaIdentityStmt *a,
                                      const ReplicaIdentityStmt *b) {
  COMPARE_SCALAR_FIELD(identity_type);
  COMPARE_STRING_FIELD(name);

  return true;
}

static bool _equalAlterSystemStmt(const AlterSystemStmt *a,
                                  const AlterSystemStmt *b) {
  COMPARE_NODE_FIELD(setstmt);

  return true;
}

static bool _equalCreateSeqStmt(const CreateSeqStmt *a,
                                const CreateSeqStmt *b) {
  COMPARE_NODE_FIELD(sequence);
  COMPARE_NODE_FIELD(options);
  COMPARE_SCALAR_FIELD(ownerId);
  COMPARE_SCALAR_FIELD(if_not_exists);

  return true;
}

static bool _equalAlterSeqStmt(const AlterSeqStmt *a, const AlterSeqStmt *b) {
  COMPARE_NODE_FIELD(sequence);
  COMPARE_NODE_FIELD(options);
  COMPARE_SCALAR_FIELD(missing_ok);

  return true;
}

static bool _equalVariableSetStmt(const VariableSetStmt *a,
                                  const VariableSetStmt *b) {
  COMPARE_SCALAR_FIELD(kind);
  COMPARE_STRING_FIELD(name);
  COMPARE_NODE_FIELD(args);
  COMPARE_SCALAR_FIELD(is_local);

  return true;
}

static bool _equalVariableShowStmt(const VariableShowStmt *a,
                                   const VariableShowStmt *b) {
  COMPARE_STRING_FIELD(name);

  return true;
}

static bool _equalDiscardStmt(const DiscardStmt *a, const DiscardStmt *b) {
  COMPARE_SCALAR_FIELD(target);

  return true;
}

static bool _equalCreateTableSpaceStmt(const CreateTableSpaceStmt *a,
                                       const CreateTableSpaceStmt *b) {
  COMPARE_STRING_FIELD(tablespacename);
  COMPARE_NODE_FIELD(owner);
  COMPARE_STRING_FIELD(location);
  COMPARE_NODE_FIELD(options);

  return true;
}

static bool _equalDropTableSpaceStmt(const DropTableSpaceStmt *a,
                                     const DropTableSpaceStmt *b) {
  COMPARE_STRING_FIELD(tablespacename);
  COMPARE_SCALAR_FIELD(missing_ok);

  return true;
}

static bool _equalAlterTableSpaceOptionsStmt(
    const AlterTableSpaceOptionsStmt *a, const AlterTableSpaceOptionsStmt *b) {
  COMPARE_STRING_FIELD(tablespacename);
  COMPARE_NODE_FIELD(options);
  COMPARE_SCALAR_FIELD(isReset);

  return true;
}

static bool _equalAlterTableMoveAllStmt(const AlterTableMoveAllStmt *a,
                                        const AlterTableMoveAllStmt *b) {
  COMPARE_STRING_FIELD(orig_tablespacename);
  COMPARE_SCALAR_FIELD(objtype);
  COMPARE_NODE_FIELD(roles);
  COMPARE_STRING_FIELD(new_tablespacename);
  COMPARE_SCALAR_FIELD(nowait);

  return true;
}

static bool _equalCreateExtensionStmt(const CreateExtensionStmt *a,
                                      const CreateExtensionStmt *b) {
  COMPARE_STRING_FIELD(extname);
  COMPARE_SCALAR_FIELD(if_not_exists);
  COMPARE_NODE_FIELD(options);

  return true;
}

static bool _equalAlterExtensionStmt(const AlterExtensionStmt *a,
                                     const AlterExtensionStmt *b) {
  COMPARE_STRING_FIELD(extname);
  COMPARE_NODE_FIELD(options);

  return true;
}

static bool _equalAlterExtensionContentsStmt(
    const AlterExtensionContentsStmt *a, const AlterExtensionContentsStmt *b) {
  COMPARE_STRING_FIELD(extname);
  COMPARE_SCALAR_FIELD(action);
  COMPARE_SCALAR_FIELD(objtype);
  COMPARE_NODE_FIELD(objname);
  COMPARE_NODE_FIELD(objargs);

  return true;
}

static bool _equalCreateFdwStmt(const CreateFdwStmt *a,
                                const CreateFdwStmt *b) {
  COMPARE_STRING_FIELD(fdwname);
  COMPARE_NODE_FIELD(func_options);
  COMPARE_NODE_FIELD(options);

  return true;
}

static bool _equalAlterFdwStmt(const AlterFdwStmt *a, const AlterFdwStmt *b) {
  COMPARE_STRING_FIELD(fdwname);
  COMPARE_NODE_FIELD(func_options);
  COMPARE_NODE_FIELD(options);

  return true;
}

static bool _equalCreateForeignServerStmt(const CreateForeignServerStmt *a,
                                          const CreateForeignServerStmt *b) {
  COMPARE_STRING_FIELD(servername);
  COMPARE_STRING_FIELD(servertype);
  COMPARE_STRING_FIELD(version);
  COMPARE_STRING_FIELD(fdwname);
  COMPARE_NODE_FIELD(options);

  return true;
}

static bool _equalAlterForeignServerStmt(const AlterForeignServerStmt *a,
                                         const AlterForeignServerStmt *b) {
  COMPARE_STRING_FIELD(servername);
  COMPARE_STRING_FIELD(version);
  COMPARE_NODE_FIELD(options);
  COMPARE_SCALAR_FIELD(has_version);

  return true;
}

static bool _equalCreateUserMappingStmt(const CreateUserMappingStmt *a,
                                        const CreateUserMappingStmt *b) {
  COMPARE_NODE_FIELD(user);
  COMPARE_STRING_FIELD(servername);
  COMPARE_NODE_FIELD(options);

  return true;
}

static bool _equalAlterUserMappingStmt(const AlterUserMappingStmt *a,
                                       const AlterUserMappingStmt *b) {
  COMPARE_NODE_FIELD(user);
  COMPARE_STRING_FIELD(servername);
  COMPARE_NODE_FIELD(options);

  return true;
}

static bool _equalDropUserMappingStmt(const DropUserMappingStmt *a,
                                      const DropUserMappingStmt *b) {
  COMPARE_NODE_FIELD(user);
  COMPARE_STRING_FIELD(servername);
  COMPARE_SCALAR_FIELD(missing_ok);

  return true;
}

static bool _equalCreateForeignTableStmt(const CreateForeignTableStmt *a,
                                         const CreateForeignTableStmt *b) {
  if (!_equalCreateStmt(&a->base, &b->base)) return false;

  COMPARE_STRING_FIELD(servername);
  COMPARE_NODE_FIELD(options);

  return true;
}

static bool _equalImportForeignSchemaStmt(const ImportForeignSchemaStmt *a,
                                          const ImportForeignSchemaStmt *b) {
  COMPARE_STRING_FIELD(server_name);
  COMPARE_STRING_FIELD(remote_schema);
  COMPARE_STRING_FIELD(local_schema);
  COMPARE_SCALAR_FIELD(list_type);
  COMPARE_NODE_FIELD(table_list);
  COMPARE_NODE_FIELD(options);

  return true;
}

static bool _equalCreateTransformStmt(const CreateTransformStmt *a,
                                      const CreateTransformStmt *b) {
  COMPARE_SCALAR_FIELD(replace);
  COMPARE_NODE_FIELD(type_name);
  COMPARE_STRING_FIELD(lang);
  COMPARE_NODE_FIELD(fromsql);
  COMPARE_NODE_FIELD(tosql);

  return true;
}

static bool _equalCreateTrigStmt(const CreateTrigStmt *a,
                                 const CreateTrigStmt *b) {
  COMPARE_STRING_FIELD(trigname);
  COMPARE_NODE_FIELD(relation);
  COMPARE_NODE_FIELD(funcname);
  COMPARE_NODE_FIELD(args);
  COMPARE_SCALAR_FIELD(row);
  COMPARE_SCALAR_FIELD(timing);
  COMPARE_SCALAR_FIELD(events);
  COMPARE_NODE_FIELD(columns);
  COMPARE_NODE_FIELD(whenClause);
  COMPARE_SCALAR_FIELD(isconstraint);
  COMPARE_SCALAR_FIELD(deferrable);
  COMPARE_SCALAR_FIELD(initdeferred);
  COMPARE_NODE_FIELD(constrrel);

  return true;
}

static bool _equalCreateEventTrigStmt(const CreateEventTrigStmt *a,
                                      const CreateEventTrigStmt *b) {
  COMPARE_STRING_FIELD(trigname);
  COMPARE_STRING_FIELD(eventname);
  COMPARE_NODE_FIELD(funcname);
  COMPARE_NODE_FIELD(whenclause);

  return true;
}

static bool _equalAlterEventTrigStmt(const AlterEventTrigStmt *a,
                                     const AlterEventTrigStmt *b) {
  COMPARE_STRING_FIELD(trigname);
  COMPARE_SCALAR_FIELD(tgenabled);

  return true;
}

static bool _equalCreatePLangStmt(const CreatePLangStmt *a,
                                  const CreatePLangStmt *b) {
  COMPARE_SCALAR_FIELD(replace);
  COMPARE_STRING_FIELD(plname);
  COMPARE_NODE_FIELD(plhandler);
  COMPARE_NODE_FIELD(plinline);
  COMPARE_NODE_FIELD(plvalidator);
  COMPARE_SCALAR_FIELD(pltrusted);

  return true;
}

static bool _equalCreateRoleStmt(const CreateRoleStmt *a,
                                 const CreateRoleStmt *b) {
  COMPARE_SCALAR_FIELD(stmt_type);
  COMPARE_STRING_FIELD(role);
  COMPARE_NODE_FIELD(options);

  return true;
}

static bool _equalAlterRoleStmt(const AlterRoleStmt *a,
                                const AlterRoleStmt *b) {
  COMPARE_NODE_FIELD(role);
  COMPARE_NODE_FIELD(options);
  COMPARE_SCALAR_FIELD(action);

  return true;
}

static bool _equalAlterRoleSetStmt(const AlterRoleSetStmt *a,
                                   const AlterRoleSetStmt *b) {
  COMPARE_NODE_FIELD(role);
  COMPARE_STRING_FIELD(database);
  COMPARE_NODE_FIELD(setstmt);

  return true;
}

static bool _equalDropRoleStmt(const DropRoleStmt *a, const DropRoleStmt *b) {
  COMPARE_NODE_FIELD(roles);
  COMPARE_SCALAR_FIELD(missing_ok);

  return true;
}

static bool _equalLockStmt(const LockStmt *a, const LockStmt *b) {
  COMPARE_NODE_FIELD(relations);
  COMPARE_SCALAR_FIELD(mode);
  COMPARE_SCALAR_FIELD(nowait);

  return true;
}

static bool _equalConstraintsSetStmt(const ConstraintsSetStmt *a,
                                     const ConstraintsSetStmt *b) {
  COMPARE_NODE_FIELD(constraints);
  COMPARE_SCALAR_FIELD(deferred);

  return true;
}

static bool _equalReindexStmt(const ReindexStmt *a, const ReindexStmt *b) {
  COMPARE_SCALAR_FIELD(kind);
  COMPARE_NODE_FIELD(relation);
  COMPARE_STRING_FIELD(name);
  COMPARE_SCALAR_FIELD(options);

  return true;
}

static bool _equalCreateSchemaStmt(const CreateSchemaStmt *a,
                                   const CreateSchemaStmt *b) {
  COMPARE_STRING_FIELD(schemaname);
  COMPARE_NODE_FIELD(authrole);
  COMPARE_NODE_FIELD(schemaElts);
  COMPARE_SCALAR_FIELD(if_not_exists);

  return true;
}

static bool _equalCreateConversionStmt(const CreateConversionStmt *a,
                                       const CreateConversionStmt *b) {
  COMPARE_NODE_FIELD(conversion_name);
  COMPARE_STRING_FIELD(for_encoding_name);
  COMPARE_STRING_FIELD(to_encoding_name);
  COMPARE_NODE_FIELD(func_name);
  COMPARE_SCALAR_FIELD(def);

  return true;
}

static bool _equalCreateCastStmt(const CreateCastStmt *a,
                                 const CreateCastStmt *b) {
  COMPARE_NODE_FIELD(sourcetype);
  COMPARE_NODE_FIELD(targettype);
  COMPARE_NODE_FIELD(func);
  COMPARE_SCALAR_FIELD(context);
  COMPARE_SCALAR_FIELD(inout);

  return true;
}

static bool _equalPrepareStmt(const PrepareStmt *a, const PrepareStmt *b) {
  COMPARE_STRING_FIELD(name);
  COMPARE_NODE_FIELD(argtypes);
  COMPARE_NODE_FIELD(query);

  return true;
}

static bool _equalExecuteStmt(const ExecuteStmt *a, const ExecuteStmt *b) {
  COMPARE_STRING_FIELD(name);
  COMPARE_NODE_FIELD(params);

  return true;
}

static bool _equalDeallocateStmt(const DeallocateStmt *a,
                                 const DeallocateStmt *b) {
  COMPARE_STRING_FIELD(name);

  return true;
}

static bool _equalDropOwnedStmt(const DropOwnedStmt *a,
                                const DropOwnedStmt *b) {
  COMPARE_NODE_FIELD(roles);
  COMPARE_SCALAR_FIELD(behavior);

  return true;
}

static bool _equalReassignOwnedStmt(const ReassignOwnedStmt *a,
                                    const ReassignOwnedStmt *b) {
  COMPARE_NODE_FIELD(roles);
  COMPARE_NODE_FIELD(newrole);

  return true;
}

static bool _equalAlterTSDictionaryStmt(const AlterTSDictionaryStmt *a,
                                        const AlterTSDictionaryStmt *b) {
  COMPARE_NODE_FIELD(dictname);
  COMPARE_NODE_FIELD(options);

  return true;
}

static bool _equalAlterTSConfigurationStmt(const AlterTSConfigurationStmt *a,
                                           const AlterTSConfigurationStmt *b) {
  COMPARE_SCALAR_FIELD(kind);
  COMPARE_NODE_FIELD(cfgname);
  COMPARE_NODE_FIELD(tokentype);
  COMPARE_NODE_FIELD(dicts);
  COMPARE_SCALAR_FIELD(override);
  COMPARE_SCALAR_FIELD(replace);
  COMPARE_SCALAR_FIELD(missing_ok);

  return true;
}

static bool _equalCreatePolicyStmt(const CreatePolicyStmt *a,
                                   const CreatePolicyStmt *b) {
  COMPARE_STRING_FIELD(policy_name);
  COMPARE_NODE_FIELD(table);
  COMPARE_SCALAR_FIELD(cmd);
  COMPARE_NODE_FIELD(roles);
  COMPARE_NODE_FIELD(qual);
  COMPARE_NODE_FIELD(with_check);

  return true;
}

static bool _equalAlterPolicyStmt(const AlterPolicyStmt *a,
                                  const AlterPolicyStmt *b) {
  COMPARE_STRING_FIELD(policy_name);
  COMPARE_NODE_FIELD(table);
  COMPARE_NODE_FIELD(roles);
  COMPARE_NODE_FIELD(qual);
  COMPARE_NODE_FIELD(with_check);

  return true;
}

static bool _equalAExpr(const A_Expr *a, const A_Expr *b) {
  COMPARE_SCALAR_FIELD(kind);
  COMPARE_NODE_FIELD(name);
  COMPARE_NODE_FIELD(lexpr);
  COMPARE_NODE_FIELD(rexpr);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalColumnRef(const ColumnRef *a, const ColumnRef *b) {
  COMPARE_NODE_FIELD(fields);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalParamRef(const ParamRef *a, const ParamRef *b) {
  COMPARE_SCALAR_FIELD(number);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalAConst(const A_Const *a, const A_Const *b) {
  if (!equal(&a->val, &b->val)) /* hack for in-line Value field */
    return false;
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalFuncCall(const FuncCall *a, const FuncCall *b) {
  COMPARE_NODE_FIELD(funcname);
  COMPARE_NODE_FIELD(args);
  COMPARE_NODE_FIELD(agg_order);
  COMPARE_NODE_FIELD(agg_filter);
  COMPARE_SCALAR_FIELD(agg_within_group);
  COMPARE_SCALAR_FIELD(agg_star);
  COMPARE_SCALAR_FIELD(agg_distinct);
  COMPARE_SCALAR_FIELD(func_variadic);
  COMPARE_NODE_FIELD(over);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalAStar(const A_Star *a, const A_Star *b) { return true; }

static bool _equalAIndices(const A_Indices *a, const A_Indices *b) {
  COMPARE_NODE_FIELD(lidx);
  COMPARE_NODE_FIELD(uidx);

  return true;
}

static bool _equalA_Indirection(const A_Indirection *a,
                                const A_Indirection *b) {
  COMPARE_NODE_FIELD(arg);
  COMPARE_NODE_FIELD(indirection);

  return true;
}

static bool _equalA_ArrayExpr(const A_ArrayExpr *a, const A_ArrayExpr *b) {
  COMPARE_NODE_FIELD(elements);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalResTarget(const ResTarget *a, const ResTarget *b) {
  COMPARE_STRING_FIELD(name);
  COMPARE_NODE_FIELD(indirection);
  COMPARE_NODE_FIELD(val);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalMultiAssignRef(const MultiAssignRef *a,
                                 const MultiAssignRef *b) {
  COMPARE_NODE_FIELD(source);
  COMPARE_SCALAR_FIELD(colno);
  COMPARE_SCALAR_FIELD(ncolumns);

  return true;
}

static bool _equalTypeName(const TypeName *a, const TypeName *b) {
  COMPARE_NODE_FIELD(names);
  COMPARE_SCALAR_FIELD(typeOid);
  COMPARE_SCALAR_FIELD(setof);
  COMPARE_SCALAR_FIELD(pct_type);
  COMPARE_NODE_FIELD(typmods);
  COMPARE_SCALAR_FIELD(typemod);
  COMPARE_NODE_FIELD(arrayBounds);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalTypeCast(const TypeCast *a, const TypeCast *b) {
  COMPARE_NODE_FIELD(arg);
  COMPARE_NODE_FIELD(typeName);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalCollateClause(const CollateClause *a,
                                const CollateClause *b) {
  COMPARE_NODE_FIELD(arg);
  COMPARE_NODE_FIELD(collname);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalSortBy(const SortBy *a, const SortBy *b) {
  COMPARE_NODE_FIELD(node);
  COMPARE_SCALAR_FIELD(sortby_dir);
  COMPARE_SCALAR_FIELD(sortby_nulls);
  COMPARE_NODE_FIELD(useOp);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalWindowDef(const WindowDef *a, const WindowDef *b) {
  COMPARE_STRING_FIELD(name);
  COMPARE_STRING_FIELD(refname);
  COMPARE_NODE_FIELD(partitionClause);
  COMPARE_NODE_FIELD(orderClause);
  COMPARE_SCALAR_FIELD(frameOptions);
  COMPARE_NODE_FIELD(startOffset);
  COMPARE_NODE_FIELD(endOffset);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalRangeSubselect(const RangeSubselect *a,
                                 const RangeSubselect *b) {
  COMPARE_SCALAR_FIELD(lateral);
  COMPARE_NODE_FIELD(subquery);
  COMPARE_NODE_FIELD(alias);

  return true;
}

static bool _equalRangeFunction(const RangeFunction *a,
                                const RangeFunction *b) {
  COMPARE_SCALAR_FIELD(lateral);
  COMPARE_SCALAR_FIELD(ordinality);
  COMPARE_SCALAR_FIELD(is_rowsfrom);
  COMPARE_NODE_FIELD(functions);
  COMPARE_NODE_FIELD(alias);
  COMPARE_NODE_FIELD(coldeflist);

  return true;
}

static bool _equalIndexElem(const IndexElem *a, const IndexElem *b) {
  COMPARE_STRING_FIELD(name);
  COMPARE_NODE_FIELD(expr);
  COMPARE_STRING_FIELD(indexcolname);
  COMPARE_NODE_FIELD(collation);
  COMPARE_NODE_FIELD(opclass);
  COMPARE_SCALAR_FIELD(ordering);
  COMPARE_SCALAR_FIELD(nulls_ordering);

  return true;
}

static bool _equalColumnDef(const ColumnDef *a, const ColumnDef *b) {
  COMPARE_STRING_FIELD(colname);
  COMPARE_NODE_FIELD(typeName);
  COMPARE_SCALAR_FIELD(inhcount);
  COMPARE_SCALAR_FIELD(is_local);
  COMPARE_SCALAR_FIELD(is_not_null);
  COMPARE_SCALAR_FIELD(is_from_type);
  COMPARE_SCALAR_FIELD(storage);
  COMPARE_NODE_FIELD(raw_default);
  COMPARE_NODE_FIELD(cooked_default);
  COMPARE_NODE_FIELD(collClause);
  COMPARE_SCALAR_FIELD(collOid);
  COMPARE_NODE_FIELD(constraints);
  COMPARE_NODE_FIELD(fdwoptions);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalConstraint(const Constraint *a, const Constraint *b) {
  COMPARE_SCALAR_FIELD(contype);
  COMPARE_STRING_FIELD(conname);
  COMPARE_SCALAR_FIELD(deferrable);
  COMPARE_SCALAR_FIELD(initdeferred);
  COMPARE_LOCATION_FIELD(location);
  COMPARE_SCALAR_FIELD(is_no_inherit);
  COMPARE_NODE_FIELD(raw_expr);
  COMPARE_STRING_FIELD(cooked_expr);
  COMPARE_NODE_FIELD(keys);
  COMPARE_NODE_FIELD(exclusions);
  COMPARE_NODE_FIELD(options);
  COMPARE_STRING_FIELD(indexname);
  COMPARE_STRING_FIELD(indexspace);
  COMPARE_STRING_FIELD(access_method);
  COMPARE_NODE_FIELD(where_clause);
  COMPARE_NODE_FIELD(pktable);
  COMPARE_NODE_FIELD(fk_attrs);
  COMPARE_NODE_FIELD(pk_attrs);
  COMPARE_SCALAR_FIELD(fk_matchtype);
  COMPARE_SCALAR_FIELD(fk_upd_action);
  COMPARE_SCALAR_FIELD(fk_del_action);
  COMPARE_NODE_FIELD(old_conpfeqop);
  COMPARE_SCALAR_FIELD(old_pktable_oid);
  COMPARE_SCALAR_FIELD(skip_validation);
  COMPARE_SCALAR_FIELD(initially_valid);

  return true;
}

static bool _equalDefElem(const DefElem *a, const DefElem *b) {
  COMPARE_STRING_FIELD(defnamespace);
  COMPARE_STRING_FIELD(defname);
  COMPARE_NODE_FIELD(arg);
  COMPARE_SCALAR_FIELD(defaction);

  return true;
}

static bool _equalLockingClause(const LockingClause *a,
                                const LockingClause *b) {
  COMPARE_NODE_FIELD(lockedRels);
  COMPARE_SCALAR_FIELD(strength);
  COMPARE_SCALAR_FIELD(waitPolicy);

  return true;
}

static bool _equalRangeTblEntry(const RangeTblEntry *a,
                                const RangeTblEntry *b) {
  COMPARE_SCALAR_FIELD(rtekind);
  COMPARE_SCALAR_FIELD(relid);
  COMPARE_SCALAR_FIELD(relkind);
  COMPARE_NODE_FIELD(tablesample);
  COMPARE_NODE_FIELD(subquery);
  COMPARE_SCALAR_FIELD(security_barrier);
  COMPARE_SCALAR_FIELD(jointype);
  COMPARE_NODE_FIELD(joinaliasvars);
  COMPARE_NODE_FIELD(functions);
  COMPARE_SCALAR_FIELD(funcordinality);
  COMPARE_NODE_FIELD(values_lists);
  COMPARE_NODE_FIELD(values_collations);
  COMPARE_STRING_FIELD(ctename);
  COMPARE_SCALAR_FIELD(ctelevelsup);
  COMPARE_SCALAR_FIELD(self_reference);
  COMPARE_NODE_FIELD(ctecoltypes);
  COMPARE_NODE_FIELD(ctecoltypmods);
  COMPARE_NODE_FIELD(ctecolcollations);
  COMPARE_NODE_FIELD(alias);
  COMPARE_NODE_FIELD(eref);
  COMPARE_SCALAR_FIELD(lateral);
  COMPARE_SCALAR_FIELD(inh);
  COMPARE_SCALAR_FIELD(inFromCl);
  COMPARE_SCALAR_FIELD(requiredPerms);
  COMPARE_SCALAR_FIELD(checkAsUser);
  COMPARE_BITMAPSET_FIELD(selectedCols);
  COMPARE_BITMAPSET_FIELD(insertedCols);
  COMPARE_BITMAPSET_FIELD(updatedCols);
  COMPARE_NODE_FIELD(securityQuals);

  return true;
}

static bool _equalRangeTblFunction(const RangeTblFunction *a,
                                   const RangeTblFunction *b) {
  COMPARE_NODE_FIELD(funcexpr);
  COMPARE_SCALAR_FIELD(funccolcount);
  COMPARE_NODE_FIELD(funccolnames);
  COMPARE_NODE_FIELD(funccoltypes);
  COMPARE_NODE_FIELD(funccoltypmods);
  COMPARE_NODE_FIELD(funccolcollations);
  COMPARE_BITMAPSET_FIELD(funcparams);

  return true;
}

static bool _equalWithCheckOption(const WithCheckOption *a,
                                  const WithCheckOption *b) {
  COMPARE_SCALAR_FIELD(kind);
  COMPARE_STRING_FIELD(relname);
  COMPARE_NODE_FIELD(qual);
  COMPARE_SCALAR_FIELD(cascaded);

  return true;
}

static bool _equalSortGroupClause(const SortGroupClause *a,
                                  const SortGroupClause *b) {
  COMPARE_SCALAR_FIELD(tleSortGroupRef);
  COMPARE_SCALAR_FIELD(eqop);
  COMPARE_SCALAR_FIELD(sortop);
  COMPARE_SCALAR_FIELD(nulls_first);
  COMPARE_SCALAR_FIELD(hashable);

  return true;
}

static bool _equalGroupingSet(const GroupingSet *a, const GroupingSet *b) {
  COMPARE_SCALAR_FIELD(kind);
  COMPARE_NODE_FIELD(content);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalWindowClause(const WindowClause *a, const WindowClause *b) {
  COMPARE_STRING_FIELD(name);
  COMPARE_STRING_FIELD(refname);
  COMPARE_NODE_FIELD(partitionClause);
  COMPARE_NODE_FIELD(orderClause);
  COMPARE_SCALAR_FIELD(frameOptions);
  COMPARE_NODE_FIELD(startOffset);
  COMPARE_NODE_FIELD(endOffset);
  COMPARE_SCALAR_FIELD(winref);
  COMPARE_SCALAR_FIELD(copiedOrder);

  return true;
}

static bool _equalRowMarkClause(const RowMarkClause *a,
                                const RowMarkClause *b) {
  COMPARE_SCALAR_FIELD(rti);
  COMPARE_SCALAR_FIELD(strength);
  COMPARE_SCALAR_FIELD(waitPolicy);
  COMPARE_SCALAR_FIELD(pushedDown);

  return true;
}

static bool _equalWithClause(const WithClause *a, const WithClause *b) {
  COMPARE_NODE_FIELD(ctes);
  COMPARE_SCALAR_FIELD(recursive);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalInferClause(const InferClause *a, const InferClause *b) {
  COMPARE_NODE_FIELD(indexElems);
  COMPARE_NODE_FIELD(whereClause);
  COMPARE_STRING_FIELD(conname);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalOnConflictClause(const OnConflictClause *a,
                                   const OnConflictClause *b) {
  COMPARE_SCALAR_FIELD(action);
  COMPARE_NODE_FIELD(infer);
  COMPARE_NODE_FIELD(targetList);
  COMPARE_NODE_FIELD(whereClause);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalCommonTableExpr(const CommonTableExpr *a,
                                  const CommonTableExpr *b) {
  COMPARE_STRING_FIELD(ctename);
  COMPARE_NODE_FIELD(aliascolnames);
  COMPARE_NODE_FIELD(ctequery);
  COMPARE_LOCATION_FIELD(location);
  COMPARE_SCALAR_FIELD(cterecursive);
  COMPARE_SCALAR_FIELD(cterefcount);
  COMPARE_NODE_FIELD(ctecolnames);
  COMPARE_NODE_FIELD(ctecoltypes);
  COMPARE_NODE_FIELD(ctecoltypmods);
  COMPARE_NODE_FIELD(ctecolcollations);

  return true;
}

static bool _equalRangeTableSample(const RangeTableSample *a,
                                   const RangeTableSample *b) {
  COMPARE_NODE_FIELD(relation);
  COMPARE_STRING_FIELD(method);
  COMPARE_NODE_FIELD(repeatable);
  COMPARE_NODE_FIELD(args);

  return true;
}

static bool _equalTableSampleClause(const TableSampleClause *a,
                                    const TableSampleClause *b) {
  COMPARE_SCALAR_FIELD(tsmid);
  COMPARE_SCALAR_FIELD(tsmseqscan);
  COMPARE_SCALAR_FIELD(tsmpagemode);
  COMPARE_SCALAR_FIELD(tsminit);
  COMPARE_SCALAR_FIELD(tsmnextblock);
  COMPARE_SCALAR_FIELD(tsmnexttuple);
  COMPARE_SCALAR_FIELD(tsmexaminetuple);
  COMPARE_SCALAR_FIELD(tsmend);
  COMPARE_SCALAR_FIELD(tsmreset);
  COMPARE_SCALAR_FIELD(tsmcost);
  COMPARE_NODE_FIELD(repeatable);
  COMPARE_NODE_FIELD(args);

  return true;
}

static bool _equalXmlSerialize(const XmlSerialize *a, const XmlSerialize *b) {
  COMPARE_SCALAR_FIELD(xmloption);
  COMPARE_NODE_FIELD(expr);
  COMPARE_NODE_FIELD(typeName);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

static bool _equalRoleSpec(const RoleSpec *a, const RoleSpec *b) {
  COMPARE_SCALAR_FIELD(roletype);
  COMPARE_STRING_FIELD(rolename);
  COMPARE_LOCATION_FIELD(location);

  return true;
}

/*
 * Stuff from pg_list.h
 */

static bool _equalList(const List *a, const List *b) {
  const ListCell *item_a;
  const ListCell *item_b;

  /*
   * Try to reject by simple scalar checks before grovelling through all the
   * list elements...
   */
  COMPARE_SCALAR_FIELD(type);
  COMPARE_SCALAR_FIELD(length);

  /*
   * We place the switch outside the loop for the sake of efficiency; this
   * may not be worth doing...
   */
  switch (a->type) {
    case T_List:
      forboth(item_a, a, item_b, b) {
        if (!equal(lfirst(item_a), lfirst(item_b))) return false;
      }
      break;
    case T_IntList:
      forboth(item_a, a, item_b, b) {
        if (lfirst_int(item_a) != lfirst_int(item_b)) return false;
      }
      break;
    case T_OidList:
      forboth(item_a, a, item_b, b) {
        if (lfirst_oid(item_a) != lfirst_oid(item_b)) return false;
      }
      break;
    default:
      elog(ERROR, "unrecognized list node type: %d", (int)a->type);
      return false; /* keep compiler quiet */
  }

  /*
   * If we got here, we should have run out of elements of both lists
   */
  Assert(item_a == NULL);
  Assert(item_b == NULL);

  return true;
}

/*
 * Stuff from value.h
 */

static bool _equalValue(const Value *a, const Value *b) {
  COMPARE_SCALAR_FIELD(type);

  switch (a->type) {
    case T_Integer:
      COMPARE_SCALAR_FIELD(val.ival);
      break;
    case T_Float:
    case T_String:
    case T_BitString:
      COMPARE_STRING_FIELD(val.str);
      break;
    case T_Null:
      /* nothing to do */
      break;
    default:
      elog(ERROR, "unrecognized node type: %d", (int)a->type);
      break;
  }

  return true;
}

/*
 * equal
 *	  returns whether two nodes are equal
 */
bool equal(const void *a, const void *b) {
  bool retval;

  if (a == b) return true;

  /*
   * note that a!=b, so only one of them can be NULL
   */
  if (a == NULL || b == NULL) return false;

  /*
   * are they the same type of nodes?
   */
  if (nodeTag(a) != nodeTag(b)) return false;

  switch (nodeTag(a)) {
    /*
     * PRIMITIVE NODES
     */
    case T_Alias:
      retval = _equalAlias(static_cast<const Alias *>(a),
                           static_cast<const Alias *>(b));
      break;
    case T_RangeVar:
      retval = _equalRangeVar(static_cast<const RangeVar *>(a),
                              static_cast<const RangeVar *>(b));
      break;
    case T_IntoClause:
      retval = _equalIntoClause(static_cast<const IntoClause *>(a),
                                static_cast<const IntoClause *>(b));
      break;
    case T_Var:
      retval =
          _equalVar(static_cast<const Var *>(a), static_cast<const Var *>(b));
      break;
    case T_Const:
      retval = _equalConst(static_cast<const Const *>(a),
                           static_cast<const Const *>(b));
      break;
    case T_Param:
      retval = _equalParam(static_cast<const Param *>(a),
                           static_cast<const Param *>(b));
      break;
    case T_Aggref:
      retval = _equalAggref(static_cast<const Aggref *>(a),
                            static_cast<const Aggref *>(b));
      break;
    case T_GroupingFunc:
      retval = _equalGroupingFunc(static_cast<const GroupingFunc *>(a),
                                  static_cast<const GroupingFunc *>(b));
      break;
    case T_WindowFunc:
      retval = _equalWindowFunc(static_cast<const WindowFunc *>(a),
                                static_cast<const WindowFunc *>(b));
      break;
    case T_ArrayRef:
      retval = _equalArrayRef(static_cast<const ArrayRef *>(a),
                              static_cast<const ArrayRef *>(b));
      break;
    case T_FuncExpr:
      retval = _equalFuncExpr(static_cast<const FuncExpr *>(a),
                              static_cast<const FuncExpr *>(b));
      break;
    case T_NamedArgExpr:
      retval = _equalNamedArgExpr(static_cast<const NamedArgExpr *>(a),
                                  static_cast<const NamedArgExpr *>(b));
      break;
    case T_OpExpr:
      retval = _equalOpExpr(static_cast<const OpExpr *>(a),
                            static_cast<const OpExpr *>(b));
      break;
    case T_DistinctExpr:
      retval = _equalDistinctExpr(static_cast<const OpExpr *>(a),
                                  static_cast<const OpExpr *>(b));
      break;
    case T_NullIfExpr:
      retval = _equalNullIfExpr(static_cast<const OpExpr *>(a),
                                static_cast<const OpExpr *>(b));
      break;
    case T_ScalarArrayOpExpr:
      retval =
          _equalScalarArrayOpExpr(static_cast<const ScalarArrayOpExpr *>(a),
                                  static_cast<const ScalarArrayOpExpr *>(b));
      break;
    case T_BoolExpr:
      retval = _equalBoolExpr(static_cast<const BoolExpr *>(a),
                              static_cast<const BoolExpr *>(b));
      break;
    case T_SubLink:
      retval = _equalSubLink(static_cast<const SubLink *>(a),
                             static_cast<const SubLink *>(b));
      break;
    case T_SubPlan:
      retval = _equalSubPlan(static_cast<const SubPlan *>(a),
                             static_cast<const SubPlan *>(b));
      break;
    case T_AlternativeSubPlan:
      retval =
          _equalAlternativeSubPlan(static_cast<const AlternativeSubPlan *>(a),
                                   static_cast<const AlternativeSubPlan *>(b));
      break;
    case T_FieldSelect:
      retval = _equalFieldSelect(static_cast<const FieldSelect *>(a),
                                 static_cast<const FieldSelect *>(b));
      break;
    case T_FieldStore:
      retval = _equalFieldStore(static_cast<const FieldStore *>(a),
                                static_cast<const FieldStore *>(b));
      break;
    case T_RelabelType:
      retval = _equalRelabelType(static_cast<const RelabelType *>(a),
                                 static_cast<const RelabelType *>(b));
      break;
    case T_CoerceViaIO:
      retval = _equalCoerceViaIO(static_cast<const CoerceViaIO *>(a),
                                 static_cast<const CoerceViaIO *>(b));
      break;
    case T_ArrayCoerceExpr:
      retval = _equalArrayCoerceExpr(static_cast<const ArrayCoerceExpr *>(a),
                                     static_cast<const ArrayCoerceExpr *>(b));
      break;
    case T_ConvertRowtypeExpr:
      retval =
          _equalConvertRowtypeExpr(static_cast<const ConvertRowtypeExpr *>(a),
                                   static_cast<const ConvertRowtypeExpr *>(b));
      break;
    case T_CollateExpr:
      retval = _equalCollateExpr(static_cast<const CollateExpr *>(a),
                                 static_cast<const CollateExpr *>(b));
      break;
    case T_CaseExpr:
      retval = _equalCaseExpr(static_cast<const CaseExpr *>(a),
                              static_cast<const CaseExpr *>(b));
      break;
    case T_CaseWhen:
      retval = _equalCaseWhen(static_cast<const CaseWhen *>(a),
                              static_cast<const CaseWhen *>(b));
      break;
    case T_CaseTestExpr:
      retval = _equalCaseTestExpr(static_cast<const CaseTestExpr *>(a),
                                  static_cast<const CaseTestExpr *>(b));
      break;
    case T_ArrayExpr:
      retval = _equalArrayExpr(static_cast<const ArrayExpr *>(a),
                               static_cast<const ArrayExpr *>(b));
      break;
    case T_RowExpr:
      retval = _equalRowExpr(static_cast<const RowExpr *>(a),
                             static_cast<const RowExpr *>(b));
      break;
    case T_RowCompareExpr:
      retval = _equalRowCompareExpr(static_cast<const RowCompareExpr *>(a),
                                    static_cast<const RowCompareExpr *>(b));
      break;
    case T_CoalesceExpr:
      retval = _equalCoalesceExpr(static_cast<const CoalesceExpr *>(a),
                                  static_cast<const CoalesceExpr *>(b));
      break;
    case T_MinMaxExpr:
      retval = _equalMinMaxExpr(static_cast<const MinMaxExpr *>(a),
                                static_cast<const MinMaxExpr *>(b));
      break;
    case T_XmlExpr:
      retval = _equalXmlExpr(static_cast<const XmlExpr *>(a),
                             static_cast<const XmlExpr *>(b));
      break;
    case T_NullTest:
      retval = _equalNullTest(static_cast<const NullTest *>(a),
                              static_cast<const NullTest *>(b));
      break;
    case T_BooleanTest:
      retval = _equalBooleanTest(static_cast<const BooleanTest *>(a),
                                 static_cast<const BooleanTest *>(b));
      break;
    case T_CoerceToDomain:
      retval = _equalCoerceToDomain(static_cast<const CoerceToDomain *>(a),
                                    static_cast<const CoerceToDomain *>(b));
      break;
    case T_CoerceToDomainValue:
      retval = _equalCoerceToDomainValue(
          static_cast<const CoerceToDomainValue *>(a),
          static_cast<const CoerceToDomainValue *>(b));
      break;
    case T_SetToDefault:
      retval = _equalSetToDefault(static_cast<const SetToDefault *>(a),
                                  static_cast<const SetToDefault *>(b));
      break;
    case T_CurrentOfExpr:
      retval = _equalCurrentOfExpr(static_cast<const CurrentOfExpr *>(a),
                                   static_cast<const CurrentOfExpr *>(b));
      break;
    case T_InferenceElem:
      retval = _equalInferenceElem(static_cast<const InferenceElem *>(a),
                                   static_cast<const InferenceElem *>(b));
      break;
    case T_TargetEntry:
      retval = _equalTargetEntry(static_cast<const TargetEntry *>(a),
                                 static_cast<const TargetEntry *>(b));
      break;
    case T_RangeTblRef:
      retval = _equalRangeTblRef(static_cast<const RangeTblRef *>(a),
                                 static_cast<const RangeTblRef *>(b));
      break;
    case T_FromExpr:
      retval = _equalFromExpr(static_cast<const FromExpr *>(a),
                              static_cast<const FromExpr *>(b));
      break;
    case T_OnConflictExpr:
      retval = _equalOnConflictExpr(static_cast<const OnConflictExpr *>(a),
                                    static_cast<const OnConflictExpr *>(b));
      break;
    case T_JoinExpr:
      retval = _equalJoinExpr(static_cast<const JoinExpr *>(a),
                              static_cast<const JoinExpr *>(b));
      break;

    /*
     * RELATION NODES
     */
    case T_PathKey:
      retval = _equalPathKey(static_cast<const PathKey *>(a),
                             static_cast<const PathKey *>(b));
      break;
    case T_RestrictInfo:
      retval = _equalRestrictInfo(static_cast<const RestrictInfo *>(a),
                                  static_cast<const RestrictInfo *>(b));
      break;
    case T_PlaceHolderVar:
      retval = _equalPlaceHolderVar(static_cast<const PlaceHolderVar *>(a),
                                    static_cast<const PlaceHolderVar *>(b));
      break;
    case T_SpecialJoinInfo:
      retval = _equalSpecialJoinInfo(static_cast<const SpecialJoinInfo *>(a),
                                     static_cast<const SpecialJoinInfo *>(b));
      break;
    case T_LateralJoinInfo:
      retval = _equalLateralJoinInfo(static_cast<const LateralJoinInfo *>(a),
                                     static_cast<const LateralJoinInfo *>(b));
      break;
    case T_AppendRelInfo:
      retval = _equalAppendRelInfo(static_cast<const AppendRelInfo *>(a),
                                   static_cast<const AppendRelInfo *>(b));
      break;
    case T_PlaceHolderInfo:
      retval = _equalPlaceHolderInfo(static_cast<const PlaceHolderInfo *>(a),
                                     static_cast<const PlaceHolderInfo *>(b));
      break;

    case T_List:
    case T_IntList:
    case T_OidList:
      retval = _equalList(static_cast<const List *>(a),
                          static_cast<const List *>(b));
      break;

    case T_Integer:
    case T_Float:
    case T_String:
    case T_BitString:
    case T_Null:
      retval = _equalValue(static_cast<const Value *>(a),
                           static_cast<const Value *>(b));
      break;

    /*
     * PARSE NODES
     */
    case T_Query:
      retval = _equalQuery(static_cast<const Query *>(a),
                           static_cast<const Query *>(b));
      break;
    case T_InsertStmt:
      retval = _equalInsertStmt(static_cast<const InsertStmt *>(a),
                                static_cast<const InsertStmt *>(b));
      break;
    case T_DeleteStmt:
      retval = _equalDeleteStmt(static_cast<const DeleteStmt *>(a),
                                static_cast<const DeleteStmt *>(b));
      break;
    case T_UpdateStmt:
      retval = _equalUpdateStmt(static_cast<const UpdateStmt *>(a),
                                static_cast<const UpdateStmt *>(b));
      break;
    case T_SelectStmt:
      retval = _equalSelectStmt(static_cast<const SelectStmt *>(a),
                                static_cast<const SelectStmt *>(b));
      break;
    case T_SetOperationStmt:
      retval = _equalSetOperationStmt(static_cast<const SetOperationStmt *>(a),
                                      static_cast<const SetOperationStmt *>(b));
      break;
    case T_AlterTableStmt:
      retval = _equalAlterTableStmt(static_cast<const AlterTableStmt *>(a),
                                    static_cast<const AlterTableStmt *>(b));
      break;
    case T_AlterTableCmd:
      retval = _equalAlterTableCmd(static_cast<const AlterTableCmd *>(a),
                                   static_cast<const AlterTableCmd *>(b));
      break;
    case T_AlterDomainStmt:
      retval = _equalAlterDomainStmt(static_cast<const AlterDomainStmt *>(a),
                                     static_cast<const AlterDomainStmt *>(b));
      break;
    case T_GrantStmt:
      retval = _equalGrantStmt(static_cast<const GrantStmt *>(a),
                               static_cast<const GrantStmt *>(b));
      break;
    case T_GrantRoleStmt:
      retval = _equalGrantRoleStmt(static_cast<const GrantRoleStmt *>(a),
                                   static_cast<const GrantRoleStmt *>(b));
      break;
    case T_AlterDefaultPrivilegesStmt:
      retval = _equalAlterDefaultPrivilegesStmt(
          static_cast<const AlterDefaultPrivilegesStmt *>(a),
          static_cast<const AlterDefaultPrivilegesStmt *>(b));
      break;
    case T_DeclareCursorStmt:
      retval =
          _equalDeclareCursorStmt(static_cast<const DeclareCursorStmt *>(a),
                                  static_cast<const DeclareCursorStmt *>(b));
      break;
    case T_ClosePortalStmt:
      retval = _equalClosePortalStmt(static_cast<const ClosePortalStmt *>(a),
                                     static_cast<const ClosePortalStmt *>(b));
      break;
    case T_ClusterStmt:
      retval = _equalClusterStmt(static_cast<const ClusterStmt *>(a),
                                 static_cast<const ClusterStmt *>(b));
      break;
    case T_CopyStmt:
      retval = _equalCopyStmt(static_cast<const CopyStmt *>(a),
                              static_cast<const CopyStmt *>(b));
      break;
    case T_CreateStmt:
      retval = _equalCreateStmt(static_cast<const CreateStmt *>(a),
                                static_cast<const CreateStmt *>(b));
      break;
    case T_TableLikeClause:
      retval = _equalTableLikeClause(static_cast<const TableLikeClause *>(a),
                                     static_cast<const TableLikeClause *>(b));
      break;
    case T_DefineStmt:
      retval = _equalDefineStmt(static_cast<const DefineStmt *>(a),
                                static_cast<const DefineStmt *>(b));
      break;
    case T_DropStmt:
      retval = _equalDropStmt(static_cast<const DropStmt *>(a),
                              static_cast<const DropStmt *>(b));
      break;
    case T_TruncateStmt:
      retval = _equalTruncateStmt(static_cast<const TruncateStmt *>(a),
                                  static_cast<const TruncateStmt *>(b));
      break;
    case T_CommentStmt:
      retval = _equalCommentStmt(static_cast<const CommentStmt *>(a),
                                 static_cast<const CommentStmt *>(b));
      break;
    case T_SecLabelStmt:
      retval = _equalSecLabelStmt(static_cast<const SecLabelStmt *>(a),
                                  static_cast<const SecLabelStmt *>(b));
      break;
    case T_FetchStmt:
      retval = _equalFetchStmt(static_cast<const FetchStmt *>(a),
                               static_cast<const FetchStmt *>(b));
      break;
    case T_IndexStmt:
      retval = _equalIndexStmt(static_cast<const IndexStmt *>(a),
                               static_cast<const IndexStmt *>(b));
      break;
    case T_CreateFunctionStmt:
      retval =
          _equalCreateFunctionStmt(static_cast<const CreateFunctionStmt *>(a),
                                   static_cast<const CreateFunctionStmt *>(b));
      break;
    case T_FunctionParameter:
      retval =
          _equalFunctionParameter(static_cast<const FunctionParameter *>(a),
                                  static_cast<const FunctionParameter *>(b));
      break;
    case T_AlterFunctionStmt:
      retval =
          _equalAlterFunctionStmt(static_cast<const AlterFunctionStmt *>(a),
                                  static_cast<const AlterFunctionStmt *>(b));
      break;
    case T_DoStmt:
      retval = _equalDoStmt(static_cast<const DoStmt *>(a),
                            static_cast<const DoStmt *>(b));
      break;
    case T_RenameStmt:
      retval = _equalRenameStmt(static_cast<const RenameStmt *>(a),
                                static_cast<const RenameStmt *>(b));
      break;
    case T_AlterObjectSchemaStmt:
      retval = _equalAlterObjectSchemaStmt(
          static_cast<const AlterObjectSchemaStmt *>(a),
          static_cast<const AlterObjectSchemaStmt *>(b));
      break;
    case T_AlterOwnerStmt:
      retval = _equalAlterOwnerStmt(static_cast<const AlterOwnerStmt *>(a),
                                    static_cast<const AlterOwnerStmt *>(b));
      break;
    case T_RuleStmt:
      retval = _equalRuleStmt(static_cast<const RuleStmt *>(a),
                              static_cast<const RuleStmt *>(b));
      break;
    case T_NotifyStmt:
      retval = _equalNotifyStmt(static_cast<const NotifyStmt *>(a),
                                static_cast<const NotifyStmt *>(b));
      break;
    case T_ListenStmt:
      retval = _equalListenStmt(static_cast<const ListenStmt *>(a),
                                static_cast<const ListenStmt *>(b));
      break;
    case T_UnlistenStmt:
      retval = _equalUnlistenStmt(static_cast<const UnlistenStmt *>(a),
                                  static_cast<const UnlistenStmt *>(b));
      break;
    case T_TransactionStmt:
      retval = _equalTransactionStmt(static_cast<const TransactionStmt *>(a),
                                     static_cast<const TransactionStmt *>(b));
      break;
    case T_CompositeTypeStmt:
      retval =
          _equalCompositeTypeStmt(static_cast<const CompositeTypeStmt *>(a),
                                  static_cast<const CompositeTypeStmt *>(b));
      break;
    case T_CreateEnumStmt:
      retval = _equalCreateEnumStmt(static_cast<const CreateEnumStmt *>(a),
                                    static_cast<const CreateEnumStmt *>(b));
      break;
    case T_CreateRangeStmt:
      retval = _equalCreateRangeStmt(static_cast<const CreateRangeStmt *>(a),
                                     static_cast<const CreateRangeStmt *>(b));
      break;
    case T_AlterEnumStmt:
      retval = _equalAlterEnumStmt(static_cast<const AlterEnumStmt *>(a),
                                   static_cast<const AlterEnumStmt *>(b));
      break;
    case T_ViewStmt:
      retval = _equalViewStmt(static_cast<const ViewStmt *>(a),
                              static_cast<const ViewStmt *>(b));
      break;
    case T_LoadStmt:
      retval = _equalLoadStmt(static_cast<const LoadStmt *>(a),
                              static_cast<const LoadStmt *>(b));
      break;
    case T_CreateDomainStmt:
      retval = _equalCreateDomainStmt(static_cast<const CreateDomainStmt *>(a),
                                      static_cast<const CreateDomainStmt *>(b));
      break;
    case T_CreateOpClassStmt:
      retval =
          _equalCreateOpClassStmt(static_cast<const CreateOpClassStmt *>(a),
                                  static_cast<const CreateOpClassStmt *>(b));
      break;
    case T_CreateOpClassItem:
      retval =
          _equalCreateOpClassItem(static_cast<const CreateOpClassItem *>(a),
                                  static_cast<const CreateOpClassItem *>(b));
      break;
    case T_CreateOpFamilyStmt:
      retval =
          _equalCreateOpFamilyStmt(static_cast<const CreateOpFamilyStmt *>(a),
                                   static_cast<const CreateOpFamilyStmt *>(b));
      break;
    case T_AlterOpFamilyStmt:
      retval =
          _equalAlterOpFamilyStmt(static_cast<const AlterOpFamilyStmt *>(a),
                                  static_cast<const AlterOpFamilyStmt *>(b));
      break;
    case T_CreatedbStmt:
      retval = _equalCreatedbStmt(static_cast<const CreatedbStmt *>(a),
                                  static_cast<const CreatedbStmt *>(b));
      break;
    case T_AlterDatabaseStmt:
      retval =
          _equalAlterDatabaseStmt(static_cast<const AlterDatabaseStmt *>(a),
                                  static_cast<const AlterDatabaseStmt *>(b));
      break;
    case T_AlterDatabaseSetStmt:
      retval = _equalAlterDatabaseSetStmt(
          static_cast<const AlterDatabaseSetStmt *>(a),
          static_cast<const AlterDatabaseSetStmt *>(b));
      break;
    case T_DropdbStmt:
      retval = _equalDropdbStmt(static_cast<const DropdbStmt *>(a),
                                static_cast<const DropdbStmt *>(b));
      break;
    case T_VacuumStmt:
      retval = _equalVacuumStmt(static_cast<const VacuumStmt *>(a),
                                static_cast<const VacuumStmt *>(b));
      break;
    case T_ExplainStmt:
      retval = _equalExplainStmt(static_cast<const ExplainStmt *>(a),
                                 static_cast<const ExplainStmt *>(b));
      break;
    case T_CreateTableAsStmt:
      retval =
          _equalCreateTableAsStmt(static_cast<const CreateTableAsStmt *>(a),
                                  static_cast<const CreateTableAsStmt *>(b));
      break;
    case T_RefreshMatViewStmt:
      retval =
          _equalRefreshMatViewStmt(static_cast<const RefreshMatViewStmt *>(a),
                                   static_cast<const RefreshMatViewStmt *>(b));
      break;
    case T_ReplicaIdentityStmt:
      retval = _equalReplicaIdentityStmt(
          static_cast<const ReplicaIdentityStmt *>(a),
          static_cast<const ReplicaIdentityStmt *>(b));
      break;
    case T_AlterSystemStmt:
      retval = _equalAlterSystemStmt(static_cast<const AlterSystemStmt *>(a),
                                     static_cast<const AlterSystemStmt *>(b));
      break;
    case T_CreateSeqStmt:
      retval = _equalCreateSeqStmt(static_cast<const CreateSeqStmt *>(a),
                                   static_cast<const CreateSeqStmt *>(b));
      break;
    case T_AlterSeqStmt:
      retval = _equalAlterSeqStmt(static_cast<const AlterSeqStmt *>(a),
                                  static_cast<const AlterSeqStmt *>(b));
      break;
    case T_VariableSetStmt:
      retval = _equalVariableSetStmt(static_cast<const VariableSetStmt *>(a),
                                     static_cast<const VariableSetStmt *>(b));
      break;
    case T_VariableShowStmt:
      retval = _equalVariableShowStmt(static_cast<const VariableShowStmt *>(a),
                                      static_cast<const VariableShowStmt *>(b));
      break;
    case T_DiscardStmt:
      retval = _equalDiscardStmt(static_cast<const DiscardStmt *>(a),
                                 static_cast<const DiscardStmt *>(b));
      break;
    case T_CreateTableSpaceStmt:
      retval = _equalCreateTableSpaceStmt(
          static_cast<const CreateTableSpaceStmt *>(a),
          static_cast<const CreateTableSpaceStmt *>(b));
      break;
    case T_DropTableSpaceStmt:
      retval =
          _equalDropTableSpaceStmt(static_cast<const DropTableSpaceStmt *>(a),
                                   static_cast<const DropTableSpaceStmt *>(b));
      break;
    case T_AlterTableSpaceOptionsStmt:
      retval = _equalAlterTableSpaceOptionsStmt(
          static_cast<const AlterTableSpaceOptionsStmt *>(a),
          static_cast<const AlterTableSpaceOptionsStmt *>(b));
      break;
    case T_AlterTableMoveAllStmt:
      retval = _equalAlterTableMoveAllStmt(
          static_cast<const AlterTableMoveAllStmt *>(a),
          static_cast<const AlterTableMoveAllStmt *>(b));
      break;
    case T_CreateExtensionStmt:
      retval = _equalCreateExtensionStmt(
          static_cast<const CreateExtensionStmt *>(a),
          static_cast<const CreateExtensionStmt *>(b));
      break;
    case T_AlterExtensionStmt:
      retval =
          _equalAlterExtensionStmt(static_cast<const AlterExtensionStmt *>(a),
                                   static_cast<const AlterExtensionStmt *>(b));
      break;
    case T_AlterExtensionContentsStmt:
      retval = _equalAlterExtensionContentsStmt(
          static_cast<const AlterExtensionContentsStmt *>(a),
          static_cast<const AlterExtensionContentsStmt *>(b));
      break;
    case T_CreateFdwStmt:
      retval = _equalCreateFdwStmt(static_cast<const CreateFdwStmt *>(a),
                                   static_cast<const CreateFdwStmt *>(b));
      break;
    case T_AlterFdwStmt:
      retval = _equalAlterFdwStmt(static_cast<const AlterFdwStmt *>(a),
                                  static_cast<const AlterFdwStmt *>(b));
      break;
    case T_CreateForeignServerStmt:
      retval = _equalCreateForeignServerStmt(
          static_cast<const CreateForeignServerStmt *>(a),
          static_cast<const CreateForeignServerStmt *>(b));
      break;
    case T_AlterForeignServerStmt:
      retval = _equalAlterForeignServerStmt(
          static_cast<const AlterForeignServerStmt *>(a),
          static_cast<const AlterForeignServerStmt *>(b));
      break;
    case T_CreateUserMappingStmt:
      retval = _equalCreateUserMappingStmt(
          static_cast<const CreateUserMappingStmt *>(a),
          static_cast<const CreateUserMappingStmt *>(b));
      break;
    case T_AlterUserMappingStmt:
      retval = _equalAlterUserMappingStmt(
          static_cast<const AlterUserMappingStmt *>(a),
          static_cast<const AlterUserMappingStmt *>(b));
      break;
    case T_DropUserMappingStmt:
      retval = _equalDropUserMappingStmt(
          static_cast<const DropUserMappingStmt *>(a),
          static_cast<const DropUserMappingStmt *>(b));
      break;
    case T_CreateForeignTableStmt:
      retval = _equalCreateForeignTableStmt(
          static_cast<const CreateForeignTableStmt *>(a),
          static_cast<const CreateForeignTableStmt *>(b));
      break;
    case T_ImportForeignSchemaStmt:
      retval = _equalImportForeignSchemaStmt(
          static_cast<const ImportForeignSchemaStmt *>(a),
          static_cast<const ImportForeignSchemaStmt *>(b));
      break;
    case T_CreateTransformStmt:
      retval = _equalCreateTransformStmt(
          static_cast<const CreateTransformStmt *>(a),
          static_cast<const CreateTransformStmt *>(b));
      break;
    case T_CreateTrigStmt:
      retval = _equalCreateTrigStmt(static_cast<const CreateTrigStmt *>(a),
                                    static_cast<const CreateTrigStmt *>(b));
      break;
    case T_CreateEventTrigStmt:
      retval = _equalCreateEventTrigStmt(
          static_cast<const CreateEventTrigStmt *>(a),
          static_cast<const CreateEventTrigStmt *>(b));
      break;
    case T_AlterEventTrigStmt:
      retval =
          _equalAlterEventTrigStmt(static_cast<const AlterEventTrigStmt *>(a),
                                   static_cast<const AlterEventTrigStmt *>(b));
      break;
    case T_CreatePLangStmt:
      retval = _equalCreatePLangStmt(static_cast<const CreatePLangStmt *>(a),
                                     static_cast<const CreatePLangStmt *>(b));
      break;
    case T_CreateRoleStmt:
      retval = _equalCreateRoleStmt(static_cast<const CreateRoleStmt *>(a),
                                    static_cast<const CreateRoleStmt *>(b));
      break;
    case T_AlterRoleStmt:
      retval = _equalAlterRoleStmt(static_cast<const AlterRoleStmt *>(a),
                                   static_cast<const AlterRoleStmt *>(b));
      break;
    case T_AlterRoleSetStmt:
      retval = _equalAlterRoleSetStmt(static_cast<const AlterRoleSetStmt *>(a),
                                      static_cast<const AlterRoleSetStmt *>(b));
      break;
    case T_DropRoleStmt:
      retval = _equalDropRoleStmt(static_cast<const DropRoleStmt *>(a),
                                  static_cast<const DropRoleStmt *>(b));
      break;
    case T_LockStmt:
      retval = _equalLockStmt(static_cast<const LockStmt *>(a),
                              static_cast<const LockStmt *>(b));
      break;
    case T_ConstraintsSetStmt:
      retval =
          _equalConstraintsSetStmt(static_cast<const ConstraintsSetStmt *>(a),
                                   static_cast<const ConstraintsSetStmt *>(b));
      break;
    case T_ReindexStmt:
      retval = _equalReindexStmt(static_cast<const ReindexStmt *>(a),
                                 static_cast<const ReindexStmt *>(b));
      break;
    case T_CheckPointStmt:
      retval = true;
      break;
    case T_CreateSchemaStmt:
      retval = _equalCreateSchemaStmt(static_cast<const CreateSchemaStmt *>(a),
                                      static_cast<const CreateSchemaStmt *>(b));
      break;
    case T_CreateConversionStmt:
      retval = _equalCreateConversionStmt(
          static_cast<const CreateConversionStmt *>(a),
          static_cast<const CreateConversionStmt *>(b));
      break;
    case T_CreateCastStmt:
      retval = _equalCreateCastStmt(static_cast<const CreateCastStmt *>(a),
                                    static_cast<const CreateCastStmt *>(b));
      break;
    case T_PrepareStmt:
      retval = _equalPrepareStmt(static_cast<const PrepareStmt *>(a),
                                 static_cast<const PrepareStmt *>(b));
      break;
    case T_ExecuteStmt:
      retval = _equalExecuteStmt(static_cast<const ExecuteStmt *>(a),
                                 static_cast<const ExecuteStmt *>(b));
      break;
    case T_DeallocateStmt:
      retval = _equalDeallocateStmt(static_cast<const DeallocateStmt *>(a),
                                    static_cast<const DeallocateStmt *>(b));
      break;
    case T_DropOwnedStmt:
      retval = _equalDropOwnedStmt(static_cast<const DropOwnedStmt *>(a),
                                   static_cast<const DropOwnedStmt *>(b));
      break;
    case T_ReassignOwnedStmt:
      retval =
          _equalReassignOwnedStmt(static_cast<const ReassignOwnedStmt *>(a),
                                  static_cast<const ReassignOwnedStmt *>(b));
      break;
    case T_AlterTSDictionaryStmt:
      retval = _equalAlterTSDictionaryStmt(
          static_cast<const AlterTSDictionaryStmt *>(a),
          static_cast<const AlterTSDictionaryStmt *>(b));
      break;
    case T_AlterTSConfigurationStmt:
      retval = _equalAlterTSConfigurationStmt(
          static_cast<const AlterTSConfigurationStmt *>(a),
          static_cast<const AlterTSConfigurationStmt *>(b));
      break;
    case T_CreatePolicyStmt:
      retval = _equalCreatePolicyStmt(static_cast<const CreatePolicyStmt *>(a),
                                      static_cast<const CreatePolicyStmt *>(b));
      break;
    case T_AlterPolicyStmt:
      retval = _equalAlterPolicyStmt(static_cast<const AlterPolicyStmt *>(a),
                                     static_cast<const AlterPolicyStmt *>(b));
      break;
    case T_A_Expr:
      retval = _equalAExpr(static_cast<const A_Expr *>(a),
                           static_cast<const A_Expr *>(b));
      break;
    case T_ColumnRef:
      retval = _equalColumnRef(static_cast<const ColumnRef *>(a),
                               static_cast<const ColumnRef *>(b));
      break;
    case T_ParamRef:
      retval = _equalParamRef(static_cast<const ParamRef *>(a),
                              static_cast<const ParamRef *>(b));
      break;
    case T_A_Const:
      retval = _equalAConst(static_cast<const A_Const *>(a),
                            static_cast<const A_Const *>(b));
      break;
    case T_FuncCall:
      retval = _equalFuncCall(static_cast<const FuncCall *>(a),
                              static_cast<const FuncCall *>(b));
      break;
    case T_A_Star:
      retval = _equalAStar(static_cast<const A_Star *>(a),
                           static_cast<const A_Star *>(b));
      break;
    case T_A_Indices:
      retval = _equalAIndices(static_cast<const A_Indices *>(a),
                              static_cast<const A_Indices *>(b));
      break;
    case T_A_Indirection:
      retval = _equalA_Indirection(static_cast<const A_Indirection *>(a),
                                   static_cast<const A_Indirection *>(b));
      break;
    case T_A_ArrayExpr:
      retval = _equalA_ArrayExpr(static_cast<const A_ArrayExpr *>(a),
                                 static_cast<const A_ArrayExpr *>(b));
      break;
    case T_ResTarget:
      retval = _equalResTarget(static_cast<const ResTarget *>(a),
                               static_cast<const ResTarget *>(b));
      break;
    case T_MultiAssignRef:
      retval = _equalMultiAssignRef(static_cast<const MultiAssignRef *>(a),
                                    static_cast<const MultiAssignRef *>(b));
      break;
    case T_TypeCast:
      retval = _equalTypeCast(static_cast<const TypeCast *>(a),
                              static_cast<const TypeCast *>(b));
      break;
    case T_CollateClause:
      retval = _equalCollateClause(static_cast<const CollateClause *>(a),
                                   static_cast<const CollateClause *>(b));
      break;
    case T_SortBy:
      retval = _equalSortBy(static_cast<const SortBy *>(a),
                            static_cast<const SortBy *>(b));
      break;
    case T_WindowDef:
      retval = _equalWindowDef(static_cast<const WindowDef *>(a),
                               static_cast<const WindowDef *>(b));
      break;
    case T_RangeSubselect:
      retval = _equalRangeSubselect(static_cast<const RangeSubselect *>(a),
                                    static_cast<const RangeSubselect *>(b));
      break;
    case T_RangeFunction:
      retval = _equalRangeFunction(static_cast<const RangeFunction *>(a),
                                   static_cast<const RangeFunction *>(b));
      break;
    case T_TypeName:
      retval = _equalTypeName(static_cast<const TypeName *>(a),
                              static_cast<const TypeName *>(b));
      break;
    case T_IndexElem:
      retval = _equalIndexElem(static_cast<const IndexElem *>(a),
                               static_cast<const IndexElem *>(b));
      break;
    case T_ColumnDef:
      retval = _equalColumnDef(static_cast<const ColumnDef *>(a),
                               static_cast<const ColumnDef *>(b));
      break;
    case T_Constraint:
      retval = _equalConstraint(static_cast<const Constraint *>(a),
                                static_cast<const Constraint *>(b));
      break;
    case T_DefElem:
      retval = _equalDefElem(static_cast<const DefElem *>(a),
                             static_cast<const DefElem *>(b));
      break;
    case T_LockingClause:
      retval = _equalLockingClause(static_cast<const LockingClause *>(a),
                                   static_cast<const LockingClause *>(b));
      break;
    case T_RangeTblEntry:
      retval = _equalRangeTblEntry(static_cast<const RangeTblEntry *>(a),
                                   static_cast<const RangeTblEntry *>(b));
      break;
    case T_RangeTblFunction:
      retval = _equalRangeTblFunction(static_cast<const RangeTblFunction *>(a),
                                      static_cast<const RangeTblFunction *>(b));
      break;
    case T_WithCheckOption:
      retval = _equalWithCheckOption(static_cast<const WithCheckOption *>(a),
                                     static_cast<const WithCheckOption *>(b));
      break;
    case T_SortGroupClause:
      retval = _equalSortGroupClause(static_cast<const SortGroupClause *>(a),
                                     static_cast<const SortGroupClause *>(b));
      break;
    case T_GroupingSet:
      retval = _equalGroupingSet(static_cast<const GroupingSet *>(a),
                                 static_cast<const GroupingSet *>(b));
      break;
    case T_WindowClause:
      retval = _equalWindowClause(static_cast<const WindowClause *>(a),
                                  static_cast<const WindowClause *>(b));
      break;
    case T_RowMarkClause:
      retval = _equalRowMarkClause(static_cast<const RowMarkClause *>(a),
                                   static_cast<const RowMarkClause *>(b));
      break;
    case T_WithClause:
      retval = _equalWithClause(static_cast<const WithClause *>(a),
                                static_cast<const WithClause *>(b));
      break;
    case T_InferClause:
      retval = _equalInferClause(static_cast<const InferClause *>(a),
                                 static_cast<const InferClause *>(b));
      break;
    case T_OnConflictClause:
      retval = _equalOnConflictClause(static_cast<const OnConflictClause *>(a),
                                      static_cast<const OnConflictClause *>(b));
      break;
    case T_CommonTableExpr:
      retval = _equalCommonTableExpr(static_cast<const CommonTableExpr *>(a),
                                     static_cast<const CommonTableExpr *>(b));
      break;
    case T_RangeTableSample:
      retval = _equalRangeTableSample(static_cast<const RangeTableSample *>(a),
                                      static_cast<const RangeTableSample *>(b));
      break;
    case T_TableSampleClause:
      retval =
          _equalTableSampleClause(static_cast<const TableSampleClause *>(a),
                                  static_cast<const TableSampleClause *>(b));
      break;
    case T_FuncWithArgs:
      retval = _equalFuncWithArgs(static_cast<const FuncWithArgs *>(a),
                                  static_cast<const FuncWithArgs *>(b));
      break;
    case T_AccessPriv:
      retval = _equalAccessPriv(static_cast<const AccessPriv *>(a),
                                static_cast<const AccessPriv *>(b));
      break;
    case T_XmlSerialize:
      retval = _equalXmlSerialize(static_cast<const XmlSerialize *>(a),
                                  static_cast<const XmlSerialize *>(b));
      break;
    case T_RoleSpec:
      retval = _equalRoleSpec(static_cast<const RoleSpec *>(a),
                              static_cast<const RoleSpec *>(b));
      break;

    default:
      elog(ERROR, "unrecognized node type: %d", (int)nodeTag(a));
      retval = false; /* keep compiler quiet */
      break;
  }

  return retval;
}
