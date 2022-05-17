#include <sys/stat.h>
#include <sys/types.h>

#include "postgres.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "commands/event_trigger.h"
#include "tcop/utility.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "commands/defrem.h"
#include "utils/rel.h"
#include "storage/ipc.h"
#include "commands/copy.h"
#include "utils/memutils.h"
#include "parser/parser.h"
#include "utils/builtins.h"
#include "parser/parse_coerce.h"
#include "parser/parse_type.h"
#include "postmaster/bgworker.h"
#include "executor/executor.h"

#include "lsm_api.h"
#include "lsm_fdw.h"

#define HEADERBUFFSIZE 10

#define PREVIOUS_UTILITY (PreviousProcessUtilityHook != NULL ? \
                          PreviousProcessUtilityHook : standard_ProcessUtility)

#define CALL_PREVIOUS_UTILITY(parseTree, queryString, context, paramListInfo, \
                              destReceiver, completionTag) \
        PREVIOUS_UTILITY(plannedStmt, queryString, context, paramListInfo, \
                         queryEnvironment, destReceiver, completionTag)


/* saved hook value in case of unload */
static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;
static shmem_startup_hook_type PreviousShmemStartupHook = NULL;


/* local functions forward declarations */
static void LsmProcessUtility(PlannedStmt *plannedStmt,
							  const char *queryString,
							  ProcessUtilityContext context,
							  ParamListInfo paramListInfo,
							  QueryEnvironment *queryEnvironment,
							  DestReceiver *destReceiver,
#if PG_VERSION_NUM>=130000
							  QueryCompletion *completionTag);
#else
                              char *completionTag);
#endif


static uint8
EncodeVarintLength(uint64 v, char* buf)
{
    char* dst = buf;
    while (v >= 0x80) {
        *dst++ = (char)((v & 0x7F) | 0x80);
        v >>= 7;
    }
    *dst++ = (char)v;
	return (uint8)(dst - buf);
}

static const char*
GetVarint64Ptr(const char* p, const char* limit,
			   uint64_t* value)
{
    uint64_t result = 0;
    for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
        uint64_t byte = (unsigned char)*p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return (const char*)p;
        }
    }
    return NULL;
}

uint8
DecodeVarintLength(char* start, char* limit, uint64* len)
{
    const char* ret = GetVarint64Ptr(start, limit, len);
    return ret ? (ret - start) : 0;
}

/*
 * Checks if the given foreign server belongs to kv_fdw. If it
 * does, the function returns true. Otherwise, it returns false.
 */
//http://www.postgres.cn/docs/9.4/fdw-helpers.html
static bool LsmServer(ForeignServer *server) {
    char *fdwName = GetForeignDataWrapper(server->fdwid)->fdwname;
    return strncmp(fdwName, LSM_FDW_NAME "_fdw", NAMEDATALEN) == 0;
}





// 判断给定了表是否是属于外部表格
/*
 * Checks if the given table name belongs to a foreign Lsm table.
 * If it does, the function returns true. Otherwise, it returns false.
 */
static bool LsmTable(Oid relationId) {
    if (relationId == InvalidOid) {
        return false;
    }

    char relationKind = get_rel_relkind(relationId);
    if (relationKind == RELKIND_FOREIGN_TABLE) {
        ForeignTable *foreignTable = GetForeignTable(relationId);
        ForeignServer *server = GetForeignServer(foreignTable->serverid);

        if (LsmServer(server)) {
            return true;
        }
    }

    return false;
}


static char*
LsmFilePath(Oid relid)
{
	return psprintf("%s/%d", LSM_FDW_NAME, relid);
}

/*
 * Extracts and returns the list of kv file (directory) names from DROP table
 * statement.
 */
static List*
LsmDroppedFilenameList(DropStmt *dropStmt)
{
    List *droppedFileList = NIL;
    if (dropStmt->removeType == OBJECT_FOREIGN_TABLE) {

        ListCell *dropObjectCell = NULL;
        foreach(dropObjectCell, dropStmt->objects) {

            List *tableNameList = (List *) lfirst(dropObjectCell);
            RangeVar *rangeVar = makeRangeVarFromNameList(tableNameList);
            Oid relationId = RangeVarGetRelid(rangeVar, AccessShareLock, true);

            if (LsmTable(relationId)) {
                char *defaultFilename = LsmFilePath(relationId);
                droppedFileList = lappend(droppedFileList, defaultFilename);
            }
        }
    }
    return droppedFileList;
}

/*
 * Checks whether the COPY statement is a "COPY kv_table FROM ..." or
 * "COPY kv_table TO ...." statement. If it is then the function returns
 * true. The function returns false otherwise.
 */
static bool LsmCopyTableStatement(CopyStmt* copyStmt) {
    if (!copyStmt->relation) {
        return false;
    }

    Oid relationId = RangeVarGetRelid(copyStmt->relation,
                                      AccessShareLock,
                                      true);
    return LsmTable(relationId);
}

/*
 * Checks if superuser privilege is required by copy operation and reports
 * error if user does not have superuser rights.
 */
static void LsmCheckSuperuserPrivilegesForCopy(const CopyStmt* copyStmt) {
    /*
     * We disallow copy from file or program except to superusers. These checks
     * are based on the checks in DoCopy() function of copy.c.
     */
    if (copyStmt->filename != NULL && !superuser()) {
        if (copyStmt->is_program) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                     errmsg("must be superuser to COPY to or from a program"),
                     errhint("Anyone can COPY to stdout or from stdin. "
                             "psql's \\copy command also works for anyone.")));
        } else {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                     errmsg("must be superuser to COPY to or from a file"),
                     errhint("Anyone can COPY to stdout or from stdin. "
                             "psql's \\copy command also works for anyone.")));
        }
    }
}

Datum ShortVarlena(Datum datum, int typeLength, char storage) {
    /* Make sure item to be inserted is not toasted */
    if (typeLength == -1) {
        datum = PointerGetDatum(PG_DETOAST_DATUM_PACKED(datum));
    }

    if (typeLength == -1 && storage != 'p' && VARATT_CAN_MAKE_SHORT(datum)) {
        /* convert to short varlena -- no alignment */
        Pointer val = DatumGetPointer(datum);
        uint32 shortSize = VARATT_CONVERTED_SHORT_SIZE(val);
        Pointer temp = palloc0(shortSize);
        SET_VARSIZE_SHORT(temp, shortSize);
        memcpy(temp + 1, VARDATA(val), shortSize - 1);
        datum = PointerGetDatum(temp);
    }

    PG_RETURN_DATUM(datum);
}

void SerializeNullAttribute(TupleDesc tupleDescriptor,
                            Index index,
                            StringInfo buffer) {
    enlargeStringInfo(buffer, buffer->len + HEADERBUFFSIZE);
    char *current = buffer->data + buffer->len;
    memset(current, 0, HEADERBUFFSIZE);
    uint8 headerLen = EncodeVarintLength(0, current);
    buffer->len += headerLen;
}


// 序列化属性
void SerializeAttribute(TupleDesc tupleDescriptor,  //元组
                        Index index,    // 当前属性的下标
                        Datum datum,    // 当前属性的值
                        StringInfo buffer) {    //以当前这一行的第一个属性的值作为key，其他的属性为val
    Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, index);
    bool byValue = attributeForm->attbyval;
    int typeLength = attributeForm->attlen;
    char storage = attributeForm->attstorage;

    /* copy utility gets varlena with 4B header, same with constant */
    datum = ShortVarlena(datum, typeLength, storage);

    int offset = buffer->len;
    int datumLength = att_addlength_datum(offset, typeLength, datum);

    /* the key does not have a size header */ 
    enlargeStringInfo(buffer, datumLength + (index == 0 ? 0 : HEADERBUFFSIZE));

    char *current = buffer->data + buffer->len;
    memset(current, 0, datumLength - offset + (index == 0 ? 0 : HEADERBUFFSIZE));

    /* set the size header */
    uint8 headerLen = 0;
    if (index > 0) {
        uint64 dataLen = typeLength > 0 ? typeLength : (datumLength - offset);
        headerLen = EncodeVarintLength(dataLen, current);
        current += headerLen;
    }

    if (typeLength > 0) {
        if (byValue) {
            // 存储指定的值到指定地址中
            store_att_byval(current, datum, typeLength);
        } else {
            memcpy(current, DatumGetPointer(datum), typeLength);
        }
    } else {
        memcpy(current, DatumGetPointer(datum), datumLength - offset);
    }

    buffer->len = datumLength + headerLen;
}

/*
 * Handles a "COPY kv_table FROM" statement. This function uses the COPY
 * command's functions to read and parse rows from the data source specified
 * in the COPY statement. The function then writes each row to the file
 * specified in the foreign table options. Finally, the function returns the
 * number of copied rows.
 */
static uint64 LsmCopyIntoTable(const CopyStmt *copyStmt,
							   const char *queryString)
{
    /* Only superuser can copy from or to local file */
    LsmCheckSuperuserPrivilegesForCopy(copyStmt);

    /*
     * Open and lock the relation. We acquire ShareUpdateExclusiveLock to allow
     * concurrent reads, but block concurrent writes.
     */
    Relation relation = heap_openrv(copyStmt->relation,
                                    RowExclusiveLock);

    /* init state to read from COPY data source */
    ParseState *pstate = make_parsestate(NULL);
    pstate->p_sourcetext = queryString;
    CopyState copyState = BeginCopyFrom(pstate,
                                        relation,
                                        copyStmt->filename,
                                        copyStmt->is_program,
                                        NULL,
                                        copyStmt->attlist,
                                        copyStmt->options);
    free_parsestate(pstate);

    Oid relationId = RelationGetRelid(relation);
    TupleDesc tupleDescriptor = RelationGetDescr(relation);
    int attrCount = tupleDescriptor->natts;


    Datum *values = palloc0(attrCount * sizeof(Datum));
    bool *nulls = palloc0(attrCount * sizeof(bool));

    EState *estate = CreateExecutorState();
    ExprContext *econtext = GetPerTupleExprContext(estate);

    uint64 rowCount = 0;
    bool found = true;
    while (found) {
        /* read the next row in tupleContext */
        MemoryContext oldContext =
            MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

        /*
         * 'econtext' is used to evaluate default expression for each columns
         * not read from the file. It can be NULL when no default values are
         * used, i.e. when all columns are read from the file.
         */
#if PG_VERSION_NUM>=130000
        found = NextCopyFrom(copyState, econtext, values, nulls);
#else
        found = NextCopyFrom(copyState, econtext, values, nulls, NULL);
#endif
        /* write the row to the kv file */
        if (found) {
            StringInfo key = makeStringInfo();
            StringInfo val = makeStringInfo();

            for (int index = 0; index < attrCount; index++) {
                Datum datum = values[index];
                if (nulls[index]) {
                    if (index == 0) {
                        ereport(ERROR, (errmsg("first column cannot be null!")));
                    }
                    SerializeNullAttribute(tupleDescriptor, index, val);
                } else {
                    SerializeAttribute(tupleDescriptor,
                                       index,
                                       datum,
                                       index == 0 ? key : val);
                }
            }

            LsmInsert(MyBackendId, relationId, key->data, key->len, val->data, val->len);
            rowCount++;
        }

        MemoryContextSwitchTo(oldContext);
        /*
         * Reset the per-tuple exprcontext. We do this after every tuple, to
         * clean-up after expression evaluations etc.
         */
        ResetPerTupleExprContext(estate);
        CHECK_FOR_INTERRUPTS();
    }

    /* end read/write sessions and close the relation */
    EndCopyFrom(copyState);
    heap_close(relation, RowExclusiveLock);

    return rowCount;
}

/*
 * Handles a "COPY kv_table TO ..." statement. Statement is converted to
 * "COPY (SELECT * FROM kv_table) TO ..." and forwarded to Postgres native
 * COPY handler. Function returns number of files copied to external stream.
 */
static uint64 LsmCopyOutTable(CopyStmt *copyStmt, const char *queryString) {

    if (copyStmt->attlist != NIL) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("copy column list is not supported"),
                        errhint("use 'copy (select <columns> from <table>) to "
                                "...' instead")));
    }

    RangeVar *relation = copyStmt->relation;
    char *qualifiedName = quote_qualified_identifier(relation->schemaname,
                                                     relation->relname);
    StringInfo newQuerySubstring = makeStringInfo();
    appendStringInfo(newQuerySubstring, "select * from %s", qualifiedName);
    List *queryList = raw_parser(newQuerySubstring->data);

    /* take the first parse tree */
    Node *rawQuery = linitial(queryList);

    /*
     * Set the relation field to NULL so that COPY command works on
     * query field instead.
     */
    copyStmt->relation = NULL;

    /*
     * raw_parser returns list of RawStmt* in PG 10+ we need to
     * extract actual query from it.
     */
    RawStmt *rawStmt = (RawStmt *) rawQuery;

    ParseState *pstate = make_parsestate(NULL);
    pstate->p_sourcetext = newQuerySubstring->data;
    copyStmt->query = rawStmt->stmt;

    uint64 count = 0;
    DoCopy(pstate, copyStmt, -1, -1, &count);
    free_parsestate(pstate);

    return count;
}

/*
 * Checks alter column type compatible. The function errors out if current
 * column type can not be safely converted to requested column type.
 * This check is more restrictive than PostgreSQL's because we can not
 * change existing data. However, it is not strict enough to prevent cast like
 * float <--> integer, which does not deserialize successfully in our case.
 */
static void
LsmCheckAlterTable(AlterTableStmt *alterStmt)
{
    ObjectType objectType = alterStmt->relkind;
    /* we are only interested in foreign table changes */
    if (objectType != OBJECT_TABLE && objectType != OBJECT_FOREIGN_TABLE) {
        return;
    }

    RangeVar *rangeVar = alterStmt->relation;
    Oid relationId = RangeVarGetRelid(rangeVar, AccessShareLock, true);
    if (!LsmTable(relationId)) {
        return;
    }

    ListCell *cmdCell = NULL;
    List *cmdList = alterStmt->cmds;
    foreach (cmdCell, cmdList) {

        AlterTableCmd *alterCmd = (AlterTableCmd *) lfirst(cmdCell);
        if (alterCmd->subtype == AT_AlterColumnType) {

            char *columnName = alterCmd->name;
            AttrNumber attributeNumber = get_attnum(relationId, columnName);
            if (attributeNumber <= 0) {
                /* let standard utility handle this */
                continue;
            }

            Oid currentTypeId = get_atttype(relationId, attributeNumber);

            /*
             * We are only interested in implicit coersion type compatibility.
             * Erroring out here to prevent further processing.
             */
            ColumnDef *columnDef = (ColumnDef *) alterCmd->def;
            Oid targetTypeId = typenameTypeId(NULL, columnDef->typeName);
            if (!can_coerce_type(1,
                                 &currentTypeId,
                                 &targetTypeId,
                                 COERCION_IMPLICIT)) {
                char *typeName = TypeNameToString(columnDef->typeName);
                ereport(ERROR, (errmsg("Column %s cannot be cast automatically "
                                       "to type %s", columnName, typeName)));
            }
        }

        if (alterCmd->subtype == AT_AddColumn) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("No support for adding column currently")));
        }
    }
}

/*
 * Hook for handling utility commands. This function customizes the behavior of
 * "COPY kv_table" and "DROP FOREIGN TABLE " commands. For all other utility
 * statements, the function calls the previous utility hook or the standard
 * utility command via macro CALL_PREVIOUS_UTILITY.
 */
static void LsmProcessUtility(PlannedStmt *plannedStmt,
                             const char *queryString,
                             ProcessUtilityContext context,
                             ParamListInfo paramListInfo,
                             QueryEnvironment *queryEnvironment,
                             DestReceiver *destReceiver,
#if PG_VERSION_NUM>=130000
							 QueryCompletion *completionTag)
#else
							  char *completionTag)
#endif
{
    Node *parseTree = plannedStmt->utilityStmt;
    if (nodeTag(parseTree) == T_CopyStmt) {

        CopyStmt *copyStmt = (CopyStmt *) parseTree;
        if (LsmCopyTableStatement(copyStmt)) {

            uint64 rowCount = 0;
            if (copyStmt->is_from) {
                rowCount = LsmCopyIntoTable(copyStmt, queryString);
            } else {
                rowCount = LsmCopyOutTable(copyStmt, queryString);
            }

            if (completionTag != NULL) {
#if PG_VERSION_NUM>=130000
				SetQueryCompletion(completionTag, CMDTAG_COPY, rowCount);
#else
                snprintf(completionTag,
                         COMPLETION_TAG_BUFSIZE,
                         "COPY " UINT64_FORMAT,
                          rowCount);
#endif
            }
        } else {
            CALL_PREVIOUS_UTILITY(parseTree,
                                  queryString,
                                  context,
                                  paramListInfo,
                                  destReceiver,
                                  completionTag);
        }
    } else if (nodeTag(parseTree) == T_DropStmt) {

        DropStmt *dropStmt = (DropStmt *) parseTree;
        if (dropStmt->removeType == OBJECT_EXTENSION) {
            /* drop extension */
            rmtree(LSM_FDW_NAME, true);
        } else {
            /* drop table & drop server */
            List *droppedTables = LsmDroppedFilenameList((DropStmt *) parseTree);

            /* delete metadata */
            CALL_PREVIOUS_UTILITY(parseTree,
                                  queryString,
                                  context,
                                  paramListInfo,
                                  destReceiver,
                                  completionTag);

            /* delete real data */
            ListCell *fileCell = NULL;
            foreach(fileCell, droppedTables) {
                char *path = lfirst(fileCell);
				rmtree(path, true);
            }
        }
    } else if (nodeTag(parseTree) == T_AlterTableStmt) {
        AlterTableStmt *alterStmt = (AlterTableStmt *) parseTree;
        LsmCheckAlterTable(alterStmt);
        CALL_PREVIOUS_UTILITY(parseTree,
                              queryString,
                              context,
                              paramListInfo,
                              destReceiver,
                              completionTag);
    } else {
        /* handle other utility statements */
        CALL_PREVIOUS_UTILITY(parseTree,
                              queryString,
                              context,
                              paramListInfo,
                              destReceiver,
                              completionTag);
    }
}

static void
LsmShmemStartup(void)
{
	bool found;
	void* ctl;

    if (PreviousShmemStartupHook)
	{
        PreviousShmemStartupHook();
    }

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	ctl = ShmemInitStruct("lsm_control",
						  LsmShmemSize(MaxConnections),
						  &found);
	if (!found)
	{
		LsmInitialize(ctl, MaxConnections);
		if (mkdir(LSM_FDW_NAME, S_IRWXU) != 0 && errno != EEXIST)
			elog(ERROR, "Failed to create lsm directory: %m");
	}
	else
		LsmAttach(ctl);

	LWLockRelease(AddinShmemInitLock);
}

void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
		elog(ERROR, "LSM: this extension should be loaded via shared_preload_libraries");

	DefineCustomIntVariable("lsm.queue_size",
							"Size of LSM queue",
							NULL,
							&LsmQueueSize,
							LSM_MAX_RECORD_SIZE, LSM_MAX_RECORD_SIZE, INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

    DefineCustomBoolVariable("lsm.sync",
							 "Use synchronouse write",
							 NULL,
							 &LsmSync,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

    DefineCustomBoolVariable("lsm.upsert",
							 "Use implicit upsert semantic",
							 "If key of inserted record already exists, then replace old record with new one",
							 &LsmUpsert,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	RequestAddinShmemSpace(LsmShmemSize(MaxConnections));
	elog(DEBUG1, "Request %ld bytes of shared memory",  LsmShmemSize(MaxConnections));

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	strcpy(worker.bgw_library_name, "lsm");
	strcpy(worker.bgw_function_name, "LsmWorkerMain");
	strcpy(worker.bgw_name, "LSM worker");
	strcpy(worker.bgw_type, "LSM worker");

	RegisterBackgroundWorker(&worker);

	PreviousShmemStartupHook = shmem_startup_hook;
	shmem_startup_hook = LsmShmemStartup;
    PreviousProcessUtilityHook = ProcessUtility_hook;
    ProcessUtility_hook = LsmProcessUtility;
}

/*
 * _PG_fini is called when the module is unloaded. This function uninstalls the
 * extension's hooks.
 */
void _PG_fini(void)
{
    ProcessUtility_hook = PreviousProcessUtilityHook;
    shmem_startup_hook = PreviousShmemStartupHook;
}

static void
LsmWorkerSigtermHandler(SIGNAL_ARGS)
{
	LsmStopWorkers();
}

void
LsmWorkerMain(Datum main_arg)
{
	pqsignal(SIGTERM, LsmWorkerSigtermHandler);
	BackgroundWorkerUnblockSignals();
	LsmRunWorkers(MaxConnections);
}

void
LsmError(char const* message)
{
	ereport(ERROR, (errmsg("LSM: %s", message)));
}

void
LsmMemoryBarrier(void)
{
	pg_memory_barrier();
}
