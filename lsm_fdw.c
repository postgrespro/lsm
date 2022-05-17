#include "postgres.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "access/attnum.h"
#include "utils/relcache.h"
#include "access/reloptions.h"
#include "foreign/fdwapi.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "funcapi.h"
#include "utils/rel.h"
#include "nodes/makefuncs.h"
#if PG_VERSION_NUM < 120000
#include "access/tuptoaster.h"
#else
#include "access/heapam.h"
#include "access/heaptoast.h"
#endif
#include "catalog/pg_operator.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "utils/builtins.h"
#include "miscadmin.h"

#include "lsm_fdw.h"
#include "lsm_api.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(lsm_fdw_handler);


/*
root是规划器的关于该查询的全局信息
baserel是规划器的关于该表的信息
foreigntableid是外部表在pg_class中的 OID （foreigntableid可以从规划器的数据结构中获得，但是为了减少工作量，这里直接显式地将它传递给函数）。
*/

// 这些hook函数的参数都是系统定义好的
// 获取外部表格的size
// baserel 是planner中关于外部表格的信息
static void
GetForeignRelSize(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreignTableId)
{
    /*
     * Obtain relation size estimates for a foreign table. This is called at
     * the beginning of planning for a query that scans a foreign table. root
     * is the planner's global information about the query; baserel is the
     * planner's information about this table; and foreignTableId is the
     * pg_class OID of the foreign table. (foreignTableId could be obtained
     * from the planner data structures, but it's passed explicitly to save
     * effort.)
     *
     * This function should update baserel->rows to be the expected number of
     * rows returned by the table scan, after accounting for the filtering
     * done by the restriction quals. The initial value of baserel->rows is
     * just a constant default estimate, which should be replaced if at all
     * possible. The function may also choose to update baserel->width if it
     * can compute a better estimate of the average result row width.
     */

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));

    /*
     * min & max will call GetForeignRelSize & GetForeignPaths multiple times,
     * we should open & close db multiple times.
     */
    /* TODO: better estimation */
    // baserel is the planer's informatino about this table
    baserel->rows = LsmCount(MyBackendId, foreignTableId);
}


// 创建一个扫描外部表的访问路径
static void
GetForeignPaths(PlannerInfo *root,
				RelOptInfo *baserel,
				Oid foreignTableId)
{
    /*
     * Create possible access paths for a scan on a foreign table. This is
     * called during query planning. The parameters are the same as for
     * GetForeignRelSize, which has already been called.
     *
     * This function must generate at least one access path (ForeignPath node)
     * for a scan on the foreign table and must call add_path to add each such
     * path to baserel->pathlist. It's recommended to use
     * create_foreignscan_path to build the ForeignPath nodes. The function
     * can generate multiple access paths, e.g., a path which has valid
     * pathkeys to represent a pre-sorted result. Each access path must
     * contain cost estimates, and can contain any FDW-private information
     * that is needed to identify the specific scan method intended.
     */

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));

    Cost startupCost = 0;
    Cost totalCost = startupCost + baserel->rows;

    /* Create a ForeignPath node and add it as only possible path */
    // https://doxygen.postgresql.org/pathnode_8c.html#a20b2c8a564bb57ed4187825dec56f707
    add_path(baserel,
             (Path *) create_foreignscan_path(root,
                                              baserel,
                                              NULL,  /* default pathtarget */
                                              baserel->rows,
                                              startupCost,
                                              totalCost,
                                              NIL,   /* no pathkeys */
                                              NULL,  /* no outer rel either */
                                              NULL,  /* no extra plan */
                                              NIL)); /* no fdw_private data */
}


// 创建一个ForeignScan 计划的节点，从选择的外部acess path中创建
static ForeignScan*
GetForeignPlan(PlannerInfo *root,
			   RelOptInfo *baserel,
			   Oid foreignTableId,
			   ForeignPath *bestPath,
			   List *targetList,
			   List *scanClauses,    //
			   Plan *outerPlan)
{
    /*
     * Create a ForeignScan plan node from the selected foreign access path.
     * This is called at the end of query planning. The parameters are as for
     * GetForeignRelSize, plus the selected ForeignPath (previously produced
     * by GetForeignPaths), the target list to be emitted by the plan node,
     * and the restriction clauses to be enforced by the plan node.
     *
     * This function must create and return a ForeignScan plan node; it's
     * recommended to use make_foreignscan to build the ForeignScan node.
     *
     */

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));

    /*
     * We have no native ability to evaluate restriction clauses, so we just
     * put all the scan_clauses into the plan node's qual list for the
     * executor to check. So all we have to do here is strip RestrictInfo
     * nodes from the clauses and ignore pseudoconstants (which will be
     * handled elsewhere).
     */

    // clause 从句
    scanClauses = extract_actual_clauses(scanClauses, false);

    /* Create the ForeignScan node */
    return make_foreignscan(targetList,
                            scanClauses,
                            baserel->relid,
                            NIL, /* no expressions to evaluate */
                            NIL,
                            NIL, /* no custom tlist */
                            NIL, /* no remote quals */
                            NULL);
}

static void
GetKeyBasedQual(ForeignScanState *scanState,
				Node *node,
				Relation relation,
				TableReadState *readState)
{
    if (!node || !IsA(node, OpExpr)) {
        return;
    }

    OpExpr *op = (OpExpr *) node;
    if (list_length(op->args) != 2) {
        return;
    }

    Node *left = list_nth(op->args, 0);
    if (!IsA(left, Var)) {
        return;
    }

    Node *right = list_nth(op->args, 1);
	if (IsA(right, RelabelType)) {
		right = (Node*) ((RelabelType*)right)->arg;
	}
    if (!IsA(right, Const) && !IsA(right, Param)) {
        return;
    }

    Index varattno = ((Var *) left)->varattno;
    if (varattno != 1) {
        return;
    }

    /* get the name of the operator according to PG_OPERATOR OID */
    HeapTuple opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
    if (!HeapTupleIsValid(opertup)) {
        ereport(ERROR, (errmsg("LSM: cache lookup failed for operator %u",
                               op->opno)));
    }
    Form_pg_operator operform = (Form_pg_operator) GETSTRUCT(opertup);
    char *oprname = NameStr(operform->oprname);
    /* TODO: support more operators */
    if (strncmp(oprname, "=", NAMEDATALEN)) {
        ReleaseSysCache(opertup);
        return;
    }
    ReleaseSysCache(opertup);

    Datum keyDatum;
	Oid   keyType;

	if (IsA(right, Const))
	{
		Const *constNode = (Const *) right;
		keyDatum = constNode->constvalue;
		keyType = constNode->consttype;
	}
	else
	{
		Param *paramNode = (Param *) right;
		keyType = paramNode->paramtype;
		keyDatum = scanState->ss.ps.state->es_param_list_info->params[paramNode->paramid-1].value;
	}
    TypeCacheEntry *typeEntry = lookup_type_cache(keyType, 0);

    /* constant gets varlena with 4B header, same with copy uility */
    keyDatum = ShortVarlena(keyDatum, typeEntry->typlen, typeEntry->typstorage);

    /*
     * We can push down this qual if:
     * - The operatory is =
     * - The qual is on the key column
     */
    readState->isKeyBased = true;
    readState->key = makeStringInfo();
    TupleDesc tupleDescriptor = relation->rd_att;

    SerializeAttribute(tupleDescriptor, varattno-1, keyDatum, readState->key);

    return;
}


// 开始执行外部表格的扫描
static void
BeginForeignScan(ForeignScanState *scanState, 
                int executorFlags)
{
	static LsmCursorId operationId = 0;  /* a SQL might cause multiple scans */

    /*
     * Begin executing a foreign scan. This is called during executor startup.
     * It should perform any initialization needed before the scan can start,
     * but not start executing the actual scan (that should be done upon the
     * first call to IterateForeignScan). The ForeignScanState node has
     * already been created, but its fdw_state field is still NULL.
     * Information about the table to scan is accessible through the
     * ForeignScanState node (in particular, from the underlying ForeignScan
     * plan node, which contains any FDW-private information provided by
     * GetForeignPlan). executorFlags contains flag bits describing the
     * executor's operating mode for this plan node.
     *
     * Note that when (executorFlags & EXEC_FLAG_EXPLAIN_ONLY) is true, this
     * function should not perform any externally-visible actions; it should
     * only do the minimum required to make the node state valid for
     * ExplainForeignScan and EndForeignScan.
     *
     */

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));

    TableReadState *readState = palloc0(sizeof(TableReadState));
    readState->isKeyBased = false;
    readState->operationId = 0;
    readState->done = false;
    readState->key = NULL;
    readState->bufLen = 0;

    scanState->fdw_state = (void *) readState;

    /* must after readState is recorded, otherwise explain won't close db */
    if (executorFlags & EXEC_FLAG_EXPLAIN_ONLY) {
        return;
    }

    ListCell *lc;
    foreach (lc, scanState->ss.ps.plan->qual) {
        Expr *state = lfirst(lc);
        GetKeyBasedQual(scanState,
						(Node *) state,
                        scanState->ss.ss_currentRelation,
                        readState);
        if (readState->isKeyBased) {
            break;
        }
    }

    if (!readState->isKeyBased)
	{
        Oid relationId = RelationGetRelid(scanState->ss.ss_currentRelation);

        readState->hasNext = LsmReadNext(MyBackendId,
										 relationId,
										 ++operationId,
										 readState->buf,
										 &readState->bufLen);

        readState->next = readState->buf;
        readState->operationId = operationId;
    }
}

static void
DeserializeTuple(StringInfo key,
				 StringInfo val,
				 TupleTableSlot *tupleSlot)
{

    Datum *values = tupleSlot->tts_values;
    bool *nulls = tupleSlot->tts_isnull;

    TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
    int count = tupleDescriptor->natts;

    /* initialize all values for this row to null */
    memset(values, 0, count * sizeof(Datum));
    memset(nulls, false, count * sizeof(bool));

    int offset = 0;
    char *current = key->data;
    for (int index = 0; index < count; index++)
	{
        if (index > 0)
		{
            uint64 dataLen = 0;
            uint8 headerLen = DecodeVarintLength(current,
                                                 val->data + val->len,
                                                 &dataLen);
            offset += headerLen;
            current = val->data + offset;
            if (dataLen == 0)
			{
                nulls[index] = true;
                continue;
            }
        }

        Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, index);
        bool byValue = attributeForm->attbyval;
        int typeLength = attributeForm->attlen;

        values[index] = fetch_att(current, byValue, typeLength);
        offset = att_addlength_datum(offset, typeLength, current);

        if (index == 0)
            offset = 0;

        current = val->data + offset;
    }
}

static bool
GetNextFromBatch(Oid relationId,
				 TableReadState *readState,
				 char **key,
				 size_t *keyLen,
				 char **val,
				 size_t *valLen)
{
    bool found = false;
    if (readState->next < readState->buf + readState->bufLen)
	{
        found = true;
    }
	else if (readState->hasNext)
	{
        readState->hasNext = LsmReadNext(MyBackendId,
										 relationId,
										 readState->operationId,
										 readState->buf,
										 &readState->bufLen);

        readState->next  = readState->buf;
        if (readState->bufLen > 0) {
            found = true;
        }
    }

    if (found) {
		int len;
        memcpy(&len, readState->next, sizeof(len));
		*keyLen = len;
        readState->next += sizeof(len);
        *key = readState->next;
        readState->next += len;

        memcpy(&len, readState->next, sizeof(len));
		*valLen = len;
        readState->next += sizeof(len);
        *val = readState->next;
        readState->next += len;
    }
    return found;
}

// 从外部表格数据中，返回一行数据从外部表的slot中
static TupleTableSlot*
IterateForeignScan(ForeignScanState *scanState)
{
    /*
     * Fetch one row from the foreign source, returning it in a tuple table
     * slot (the node's ScanTupleSlot should be used for this purpose). Return
     * NULL if no more rows are available. The tuple table slot infrastructure
     * allows either a physical or virtual tuple to be returned; in most cases
     * the latter choice is preferable from a performance standpoint. Note
     * that this is called in a short-lived memory context that will be reset
     * between invocations. Create a memory context in BeginForeignScan if you
     * need longer-lived storage, or use the es_query_cxt of the node's
     * EState.
     *
     * The rows returned must match the column signature of the foreign table
     * being scanned. If you choose to optimize away fetching columns that are
     * not needed, you should insert nulls in those column positions.
     *
     * Note that PostgreSQL's executor doesn't care whether the rows returned
     * violate any NOT NULL constraints that were defined on the foreign table
     * columns — but the planner does care, and may optimize queries
     * incorrectly if NULL values are present in a column declared not to
     * contain them. If a NULL value is encountered when the user has declared
     * that none should be present, it may be appropriate to raise an error
     * (just as you would need to do in the case of a data type mismatch).
     */

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));

    TupleTableSlot *tupleSlot = scanState->ss.ss_ScanTupleSlot;
    ExecClearTuple(tupleSlot);

    TableReadState *readState = (TableReadState *) scanState->fdw_state;
    char *k = NULL, *v = NULL;
    size_t kLen = 0, vLen = 0;
    Oid relationId = RelationGetRelid(scanState->ss.ss_currentRelation);
    bool found = false;
    if (readState->isKeyBased) {
        if (!readState->done) {
            k = readState->key->data;
            kLen = readState->key->len;
            found = LsmLookup(MyBackendId, relationId, k, kLen, readState->buf, &vLen);
			v = readState->buf;
            readState->done = true;
        }
    }
	else
	{
        found = GetNextFromBatch(relationId,
                                 readState,
                                 &k,
                                 &kLen,
                                 &v,
                                 &vLen);
    }

    if (found)
	{
        StringInfoData key;
        StringInfoData val;
		initStringInfo(&key);
		initStringInfo(&val);
        appendBinaryStringInfo(&key, k, kLen);
        appendBinaryStringInfo(&val, v, vLen);

        DeserializeTuple(&key, &val, tupleSlot);
        ExecStoreVirtualTuple(tupleSlot);
    }

    return tupleSlot;
}

static void
ReScanForeignScan(ForeignScanState *scanState)
{
    /*
     * Restart the scan from the beginning. Note that any parameters the scan
     * depends on may have changed value, so the new scan does not necessarily
     * return exactly the same rows.
     */

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));
}

static void
EndForeignScan(ForeignScanState *scanState)
{
    /*
     * End the scan and release resources. It is normally not important to
     * release palloc'd memory, but for example open files and connections to
     * remote servers should be cleaned up.
     */

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));

    TableReadState *readState = (TableReadState *) scanState->fdw_state;
    Assert(readState);

    Oid relationId = RelationGetRelid(scanState->ss.ss_currentRelation);
    if (!readState->isKeyBased)
	{
        LsmCloseCursor(MyBackendId, relationId, readState->operationId);
    }

    pfree(readState);
}

static void
AddForeignUpdateTargets(Query *parsetree,
						RangeTblEntry *tableEntry,
						Relation targetRelation)
{
    /*
     * UPDATE and DELETE operations are performed against rows previously
     * fetched by the table-scanning functions. The FDW may need extra
     * information, such as a row ID or the values of primary-key columns, to
     * ensure that it can identify the exact row to update or delete. To
     * support that, this function can add extra hidden, or "junk", target
     * columns to the list of columns that are to be retrieved from the
     * foreign table during an UPDATE or DELETE.
     *
     * To do that, add TargetEntry items to parsetree->targetList, containing
     * expressions for the extra values to be fetched. Each such entry must be
     * marked resjunk = true, and must have a distinct resname that will
     * identify it at execution time. Avoid using names matching ctidN or
     * wholerowN, as the core system can generate junk columns of these names.
     *
     * This function is called in the rewriter, not the planner, so the
     * information available is a bit different from that available to the
     * planning routines. parsetree is the parse tree for the UPDATE or DELETE
     * command, while target_rte and target_relation describe the target
     * foreign table.
     *
     * If the AddForeignUpdateTargets pointer is set to NULL, no extra target
     * expressions are added. (This will make it impossible to implement
     * DELETE operations, though UPDATE may still be feasible if the FDW
     * relies on an unchanging primary key to identify rows.)
     */

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));

    /*
     * We are using first column as row identification column, so we are adding
     * that into target list.
     */
    Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(targetRelation), 0);

    Var *var = makeVar(parsetree->resultRelation,
                       1,
                       attr->atttypid,
                       attr->atttypmod,
                       InvalidOid,
                       0);

    /* Wrap it in a TLE with the right name ... */
    const char *attrname = NameStr(attr->attname);
    TargetEntry *entry = makeTargetEntry((Expr *) var,
                                         list_length(parsetree->targetList) + 1,
                                         pstrdup(attrname),
                                         true);

    /* ... and add it to the query's targetlist */
    parsetree->targetList = lappend(parsetree->targetList, entry);
}



static List*
PlanForeignModify(PlannerInfo *root,
				  ModifyTable *plan,
				  Index resultRelation,
				  int subplanIndex)
{
    /*
     * Perform any additional planning actions needed for an insert, update,
     * or delete on a foreign table. This function generates the FDW-private
     * information that will be attached to the ModifyTable plan node that
     * performs the update action. This private information must have the form
     * of a List, and will be delivered to BeginForeignModify during the
     * execution stage.
     *
     * root is the planner's global information about the query. plan is the
     * ModifyTable plan node, which is complete except for the fdwPrivLists
     * field. resultRelation identifies the target foreign table by its
     * rangetable index. subplanIndex identifies which target of the
     * ModifyTable plan node this is, counting from zero; use this if you want
     * to index into plan->plans or other substructure of the plan node.
     *
     * If the PlanForeignModify pointer is set to NULL, no additional
     * plan-time actions are taken, and the fdw_private list delivered to
     * BeginForeignModify will be NIL.
     */

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));

    return NULL;
}

// 开始执行一个外部表的修改，比如curd
static void
BeginForeignModify(ModifyTableState *modifyTableState,
				   ResultRelInfo *resultRelInfo,
				   List *fdwPrivate,
				   int subplanIndex,
				   int executorFlags)
{
    /*
     * Begin executing a foreign table modification operation. This routine is
     * called during executor startup. It should perform any initialization
     * needed prior to the actual table modifications. Subsequently,
     * ExecForeignInsert, ExecForeignUpdate or ExecForeignDelete will be
     * called for each tuple to be inserted, updated, or deleted.
     *
     * modifyTableState is the overall state of the ModifyTable plan node being
     * executed; global data about the plan and execution state is available
     * via this structure. resultRelInfo is the ResultRelInfo struct describing
     * the target foreign table. (The ri_FdwState field of ResultRelInfo is
     * available for the FDW to store any private state it needs for this
     * operation.) fdw_private contains the private data generated by
     * PlanForeignModify, if any. subplanIndex identifies which target of the
     * ModifyTable plan node this is. executorFlags contains flag bits
     * describing the executor's operating mode for this plan node.
     *
     * Note that when (executorFlags & EXEC_FLAG_EXPLAIN_ONLY) is true, this
     * function should not perform any externally-visible actions; it should
     * only do the minimum required to make the node state valid for
     * ExplainForeignModify and EndForeignModify.
     *
     * If the BeginForeignModify pointer is set to NULL, no action is taken
     * during executor startup.
     */

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));

    if (executorFlags & EXEC_FLAG_EXPLAIN_ONLY) {
        return;
    }

    TableWriteState *writeState = palloc0(sizeof(TableWriteState));

    CmdType operation = modifyTableState->operation;
    writeState->operation = operation;

    Relation relation = resultRelInfo->ri_RelationDesc;

    Oid foreignTableId = RelationGetRelid(relation);
    heap_open(foreignTableId, RowExclusiveLock);

    resultRelInfo->ri_FdwState = (void *) writeState;
}


// 将slot序列化为key和val进行插入操作
static void
SerializeTuple(StringInfo key,
			   StringInfo val,
			   TupleTableSlot *tupleSlot)
{
    TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
    int count = tupleDescriptor->natts;

    for (int index = 0; index < count; index++) {

        Datum datum = tupleSlot->tts_values[index];
        if (tupleSlot->tts_isnull[index]) {
            if (index == 0) {
                // 元组的第一个属性为空
                ereport(ERROR, (errmsg("LSM: first column cannot be null!")));
            }

            SerializeNullAttribute(tupleDescriptor, index, val);
        } else {
            // 序列化非空的字段
            SerializeAttribute(tupleDescriptor, //元组
                               index,   // 当前属性的下标
                               datum,   // 当前属性所对应的值
                               index == 0 ? key : val); //key,val初始化为空
        }
    }
}


// 插入一个tuple到外部表中
// slot : 槽
// resultRelInfo 用于描述外部表
// slot 
// planSlot 
static TupleTableSlot*
ExecForeignInsert(EState *executorState,
				  ResultRelInfo *resultRelInfo,
				  TupleTableSlot *slot,
				  TupleTableSlot *planSlot)
{

    /**
     * 
     * 执行器机制被用于四种基本SQL查询类型：SELECT、INSERT、 UPDATE以及DELETE。对于SELECT，
     * 顶层执行器代码只需要发送查询计划树返回的每个行给客户端。
     * 对于INSERT，每一个被返回的行被插入到INSERT中指定的目标表中。
     * 这通过一个被称为ModifyTable的特殊顶层计划节点完成
     * （一个简单的INSERT ... VALUES命令会创建一个由一个Result节点组成的简单计划树，
     *  该节点只计算一个结果行，在它之上的ModifyTable节点会执行插入。但是INSERT ... SELECT可以用到执行器机制的全部功能）。
     * 对于UPDATE，规划器会安排每一个计算行包含所有被更新的列值加上原始目标行的TID（元组ID或行ID），
     * 这些数据也会被输入到一个ModifyTable节点，
     * 该节点将利用这些信息创建一个新的被更新行并标记旧行为被删除。
     * 对于DELETE，唯一被计划返回的列是TID，ModifyTable节点简单地使用TID访问每一个目标行并将其标记为被删除。
     * ModifyTable以及CRUD操作的底层原理：https://www.cnblogs.com/flying-tiger/p/8418293.html
     * /


    /*
     * Insert one tuple into the foreign table. executorState is global
     * execution state for the query. resultRelInfo is the ResultRelInfo struct
     * describing the target foreign table. slot contains the tuple to be
     * inserted; it will match the rowtype definition of the foreign table.
     * planSlot contains the tuple that was generated by the ModifyTable plan
     * node's subplan; it differs from slot in possibly containing additional
     * "junk" columns. (The planSlot is typically of little interest for INSERT
     * cases, but is provided for completeness.)
     *
     * The return value is either a slot containing the data that was actually
     * inserted (this might differ from the data supplied, for example as a
     * result of trigger actions), or NULL if no row was actually inserted
     * (again, typically as a result of triggers). The passed-in slot can be
     * re-used for this purpose.
     *
     * The data in the returned slot is used only if the INSERT query has a
     * RETURNING clause. Hence, the FDW could choose to optimize away returning
     * some or all columns depending on the contents of the RETURNING clause.
     * However, some slot must be returned to indicate success, or the query's
     * reported rowcount will be wrong.
     *
     * If the ExecForeignInsert pointer is set to NULL, attempts to insert into
     * the foreign table will fail with an error message.
     */

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));

    TupleDesc tupleDescriptor = slot->tts_tupleDescriptor;
#if PG_VERSION_NUM>=130000
	bool shouldFree;
	HeapTuple heapTuple = ExecFetchSlotHeapTuple(slot, false, &shouldFree);
    if (HeapTupleHasExternal(heapTuple))
	{
        /* detoast any toasted attributes */
		HeapTuple newTuple = toast_flatten_tuple(heapTuple, tupleDescriptor);
		ExecForceStoreHeapTuple(newTuple, slot, shouldFree);
		shouldFree = false;
    }
#else
    if (HeapTupleHasExternal(slot->tts_tuple)) {
        /* detoast any toasted attributes */
        slot->tts_tuple = toast_flatten_tuple(slot->tts_tuple, tupleDescriptor);
    }
#endif
    slot_getallattrs(slot);

    StringInfoData key;
    StringInfoData val;

    Relation relation = resultRelInfo->ri_RelationDesc;
    Oid foreignTableId = RelationGetRelid(relation);

	initStringInfo(&key);
	initStringInfo(&val);
    SerializeTuple(&key, &val, slot);

    // 调用lsm_client中的接口进行插入
    if (!LsmInsert(MyBackendId, foreignTableId, key.data, key.len, val.data, val.len))
		elog(ERROR, "LSM: Failed to insert tuple");

#if PG_VERSION_NUM>=130000
	if (shouldFree)
		pfree(heapTuple);
#endif
	pfree(key.data);
	pfree(val.data);

    return slot;
}


// 执行外部数据更新
static TupleTableSlot*
ExecForeignUpdate(EState *executorState,
				  ResultRelInfo *resultRelInfo,
				  TupleTableSlot *slot,
				  TupleTableSlot *planSlot)
{
    /*
     * Update one tuple in the foreign table. executorState is global execution
     * state for the query. resultRelInfo is the ResultRelInfo struct describing
     * the target foreign table. slot contains the new data for the tuple; it
     * will match the rowtype definition of the foreign table. planSlot contains
     * the tuple that was generated by the ModifyTable plan node's subplan; it
     * differs from slot in possibly containing additional "junk" columns. In  // 重要：in possibly containing additional "junk" columns
     * particular, any junk columns that were requested by
     * AddForeignUpdateTargets will be available from this slot.
     *
     * The return value is either a slot containing the row as it was actually
     * updated (this might differ from the data supplied, for example as a
     * result of trigger actions), or NULL if no row was actually updated
     * (again, typically as a result of triggers). The passed-in slot can be
     * re-used for this purpose.
     *
     * The data in the returned slot is used only if the UPDATE query has a
     * RETURNING clause. Hence, the FDW could choose to optimize away returning
     * some or all columns depending on the contents of the RETURNING clause.
     * However, some slot must be returned to indicate success, or the query's
     * reported rowcount will be wrong.
     *
     * If the ExecForeignUpdate pointer is set to NULL, attempts to update the
     * foreign table will fail with an error message.
     *
     */

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));

    TupleDesc tupleDescriptor = slot->tts_tupleDescriptor;
#if PG_VERSION_NUM>=130000
	bool shouldFree;
	HeapTuple heapTuple = ExecFetchSlotHeapTuple(slot, false, &shouldFree);
    if (HeapTupleHasExternal(heapTuple))
	{
        /* detoast any toasted attributes */
		HeapTuple newTuple = toast_flatten_tuple(heapTuple, tupleDescriptor);
		ExecForceStoreHeapTuple(newTuple, slot, shouldFree);
		shouldFree = false;
    }
#else
    if (HeapTupleHasExternal(slot->tts_tuple))
	{
        /* detoast any toasted attributes */
        slot->tts_tuple = toast_flatten_tuple(slot->tts_tuple, tupleDescriptor);
    }
#endif
    slot_getallattrs(slot);

    StringInfoData key;
    StringInfoData val;

    Relation relation = resultRelInfo->ri_RelationDesc;
    Oid foreignTableId = RelationGetRelid(relation);

	initStringInfo(&key);
	initStringInfo(&val);
    // 将slot序列化为key 和 val
    SerializeTuple(&key, &val, slot);
    // 将获取到的key和val进行insert操作
    LsmInsert(MyBackendId, foreignTableId, key.data, key.len, val.data, val.len);

#if PG_VERSION_NUM>=130000
	if (shouldFree)
		pfree(heapTuple);
#endif
	pfree(key.data);
	pfree(val.data);

    return slot;
}

static TupleTableSlot*
ExecForeignDelete(EState *executorState,
				  ResultRelInfo *resultRelInfo,
				  TupleTableSlot *slot,
				  TupleTableSlot *planSlot)
{
    /*
     * Delete one tuple from the foreign table. executorState is global
     * execution state for the query. resultRelInfo is the ResultRelInfo struct
     * describing the target foreign table. slot contains nothing useful upon
     * call, but can be used to hold the returned tuple. planSlot contains the
     * tuple that was generated by the ModifyTable plan node's subplan; in
     * particular, it will carry any junk columns that were requested by
     * AddForeignUpdateTargets. The junk column(s) must be used to identify
     * the tuple to be deleted.
     *
     * The return value is either a slot containing the row that was deleted,
     * or NULL if no row was deleted (typically as a result of triggers). The
     * passed-in slot can be used to hold the tuple to be returned.
     *
     * The data in the returned slot is used only if the DELETE query has a
     * RETURNING clause. Hence, the FDW could choose to optimize away returning
     * some or all columns depending on the contents of the RETURNING clause.
     * However, some slot must be returned to indicate success, or the query's
     * reported rowcount will be wrong.
     *
     * If the ExecForeignDelete pointer is set to NULL, attempts to delete
     * from the foreign table will fail with an error message.
     */

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));

    slot_getallattrs(planSlot);

    StringInfoData key;
    StringInfoData val;

    Relation relation = resultRelInfo->ri_RelationDesc;
    Oid foreignTableId = RelationGetRelid(relation);

	initStringInfo(&key);
	initStringInfo(&val);
    SerializeTuple(&key, &val, planSlot);

    if (!LsmDelete(MyBackendId, foreignTableId, key.data, key.len))
		elog(ERROR, "LSM: Failed to delete tuple");

	pfree(key.data);
	pfree(val.data);

	return slot;
}

static void
EndForeignModify(EState *executorState,
				 ResultRelInfo *resultRelInfo)
{
    /*
     * End the table update and release resources. It is normally not important
     * to release palloc'd memory, but for example open files and connections
     * to remote servers should be cleaned up.
     *
     * If the EndForeignModify pointer is set to NULL, no action is taken during
     * executor shutdown.
     */

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));

    TableWriteState *writeState = (TableWriteState *) resultRelInfo->ri_FdwState;

    if (writeState) {
        Relation relation = resultRelInfo->ri_RelationDesc;

        /* CMD_UPDATE and CMD_DELETE close will be taken care of by endScan */
        heap_close(relation, RowExclusiveLock);

        pfree(writeState);
    }
}

static void
ExplainForeignScan(ForeignScanState *scanState,
				   struct ExplainState * explainState)
{
    /*
     * Print additional EXPLAIN output for a foreign table scan. This function
     * can call ExplainPropertyText and related functions to add fields to the
     * EXPLAIN output. The flag fields in es can be used to determine what to
     * print, and the state of the ForeignScanState node can be inspected to
     * provide run-time statistics in the EXPLAIN ANALYZE case.
     *
     * If the ExplainForeignScan pointer is set to NULL, no additional
     * information is printed during EXPLAIN.
     */

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));
}

static void
ExplainForeignModify(ModifyTableState *modifyTableState,
					 ResultRelInfo *relationInfo,
					 List *fdwPrivate,
					 int subplanIndex,
					 struct ExplainState *explainState)
{
    /*
     * Print additional EXPLAIN output for a foreign table update. This
     * function can call ExplainPropertyText and related functions to add
     * fields to the EXPLAIN output. The flag fields in es can be used to
     * determine what to print, and the state of the ModifyTableState node can
     * be inspected to provide run-time statistics in the EXPLAIN ANALYZE
     * case. The first four arguments are the same as for BeginForeignModify.
     *
     * If the ExplainForeignModify pointer is set to NULL, no additional
     * information is printed during EXPLAIN.
     */

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));
}

static bool
AnalyzeForeignTable(Relation relation,
					AcquireSampleRowsFunc *acquireSampleRowsFunc,
					BlockNumber *totalPageCount)
{
    /* ----
     * This function is called when ANALYZE is executed on a foreign table. If
     * the FDW can collect statistics for this foreign table, it should return
     * true, and provide a pointer to a function that will collect sample rows
     * from the table in func, plus the estimated size of the table in pages
     * in totalpages. Otherwise, return false.
     *
     * If the FDW does not support collecting statistics for any tables, the
     * AnalyzeForeignTable pointer can be set to NULL.
     *
     * If provided, the sample collection function must have the signature:
     *
     *	  int
     *	  AcquireSampleRowsFunc (Relation relation, int elevel,
     *							 HeapTuple *rows, int targrows,
     *							 double *totalrows,
     *							 double *totaldeadrows);
     *
     * A random sample of up to targrows rows should be collected from the
     * table and stored into the caller-provided rows array. The actual number
     * of rows collected must be returned. In addition, store estimates of the
     * total numbers of live and dead rows in the table into the output
     * parameters totalrows and totaldeadrows. (Set totaldeadrows to zero if
     * the FDW does not have any concept of dead rows.)
     * ----
     */

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));

    return false;
}

// 文档：https://www.postgresql.org/docs/9.6/fdwhandler.html
// 这是整个fdw程序的入口
Datum lsm_fdw_handler(PG_FUNCTION_ARGS)
{
    //FDW 处理函数返回一个 palloc 的FdwRoutine结构，其中包含指向下面描述的回调函数的指针。
    //FdwRoutine结构类型在src/include/foreign/fdwapi.h中声明
    //https://doxygen.postgresql.org/fdwapi_8h_source.html#l00204
    FdwRoutine *routine = makeNode(FdwRoutine);

    ereport(DEBUG1, (errmsg("LSM: entering function %s", __func__)));

    /*
     * assign the handlers for the FDW
     *
     * This function might be called a number of times. In particular, it is
     * likely to be called for each INSERT statement. For an explanation, see
     * core postgres file src/optimizer/plan/createplan.c where it calls
     * GetFdwRoutineByRelId(().
     */

    /* these are required */
    // http://www.postgres.cn/docs/12/fdw-callbacks.html
    /*
    在对一个扫描外部表的查询进行规划的开头将调用该函数
    */
    routine->GetForeignRelSize = GetForeignRelSize;
    routine->GetForeignPaths = GetForeignPaths;
    routine->GetForeignPlan = GetForeignPlan;
    routine->BeginForeignScan = BeginForeignScan;
    routine->IterateForeignScan = IterateForeignScan;
    routine->ReScanForeignScan = ReScanForeignScan;
    routine->EndForeignScan = EndForeignScan;

    // remainder-余下的，余下的函数是可选的，如果不需要的话，可以为NULL
    /* remainder are optional - use NULL if not required */
    /* support for insert / update / delete */
    routine->AddForeignUpdateTargets = AddForeignUpdateTargets;
    routine->PlanForeignModify = PlanForeignModify;
    routine->BeginForeignModify = BeginForeignModify;
    routine->ExecForeignInsert = ExecForeignInsert;
    routine->ExecForeignUpdate = ExecForeignUpdate;
    routine->ExecForeignDelete = ExecForeignDelete;
    routine->EndForeignModify = EndForeignModify;

    /* support for EXPLAIN */
    routine->ExplainForeignScan = ExplainForeignScan;
    routine->ExplainForeignModify = ExplainForeignModify;

    /* support for ANALYSE */
    routine->AnalyzeForeignTable = AnalyzeForeignTable;

    PG_RETURN_POINTER(routine);
}



