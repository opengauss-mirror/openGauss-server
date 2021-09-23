/* -------------------------------------------------------------------------
 *
 * makefuncs.h
 *	  prototypes for the creator functions (for primitive nodes)
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/makefuncs.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef MAKEFUNC_H
#define MAKEFUNC_H

#ifndef FRONTEND_PARSER
#include "nodes/parsenodes.h"
#include "nodes/execnodes.h"
#else
#include "datatypes.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "catalog/pg_attribute.h"
#include "access/tupdesc.h"
#include "nodes/parsenodes_common.h"
#endif /* FRONTEND_PARSER */

extern Const *makeConst(Oid consttype, int32 consttypmod, Oid constcollid, int constlen, Datum constvalue,
    bool constisnull, bool constbyval, Cursor_Data *cur = NULL);

#ifndef FRONTEND_PARSER

extern Var* makeVar(Index varno, AttrNumber varattno, Oid vartype, int32 vartypmod, Oid varcollid, Index varlevelsup);

extern Var* makeVarFromTargetEntry(Index varno, TargetEntry* tle);

extern Var* makeWholeRowVar(RangeTblEntry* rte, Index varno, Index varlevelsup, bool allowScalar);

extern TargetEntry* makeTargetEntry(Expr* expr, AttrNumber resno, char* resname, bool resjunk);

extern TargetEntry* flatCopyTargetEntry(TargetEntry* src_tle);

extern FromExpr* makeFromExpr(List* fromlist, Node* quals);

extern ColumnRef* makeColumnRef(char* relname, char* colname, int location);

extern Const* makeMaxConst(Oid consttype, int32 consttypmod, Oid constcollid);

extern Node* makeNullAConst(int location);

extern Const* makeNullConst(Oid consttype, int32 consttypmod, Oid constcollid);

extern Node* makeBoolConst(bool value, bool isnull);

extern Expr* makeBoolExpr(BoolExprType boolop, List* args, int location);

extern Alias* makeAlias(const char* aliasname, List* colnames);

extern RelabelType* makeRelabelType(Expr* arg, Oid rtype, int32 rtypmod, Oid rcollid, CoercionForm rformat);

extern TypeName* makeTypeNameFromOid(Oid typeOid, int32 typmod);

extern FuncExpr* makeFuncExpr(
    Oid funcid, Oid rettype, List* args, Oid funccollid, Oid inputcollid, CoercionForm fformat);

extern NullTest* makeNullTest(NullTestType type, Expr* expr);
extern Expr* makeBoolExprTreeNode(BoolExprType boolop, List* args);
extern HashFilter* makeHashFilter(List* arg, List* typeOidlist, List* nodelist);
extern Node* makeTidConst(ItemPointer item);
extern FuncCall* makeFuncCall(List* funcname, List* args, int location);
extern Param* makeParam(
    ParamKind paramkind, int paramid, Oid paramtype, int32 paramtypmod, Oid paramcollid, int location);
extern IndexInfo* makeIndexInfo(int numattrs, List *expressions, List *predicates,
    bool unique, bool isready, bool concurrent);

#endif /* !FRONTEND_PARSER */
extern A_Expr *makeA_Expr(A_Expr_Kind kind, List *name, Node *lexpr, Node *rexpr, int location);
extern A_Expr *makeSimpleA_Expr(A_Expr_Kind kind, char *name, Node *lexpr, Node *rexpr, int location);
extern RangeVar *makeRangeVar(char *schemaname, char *relname, int location);
extern TypeName *makeTypeName(char *typnam);
extern TypeName *makeTypeNameFromNameList(List *names);
extern DefElem *makeDefElem(char *name, Node *arg);
extern DefElem *MakeDefElemWithLoc(char *name, Node *arg, int begin_loc, int end_loc);
extern DefElem *makeDefElemExtended(char *nameSpace, char *name, Node *arg, DefElemAction defaction);
extern GroupingSet *makeGroupingSet(GroupingSetKind kind, List *content, int location);

#endif /* MAKEFUNC_H */
