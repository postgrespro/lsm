#ifndef __LSM_FDW_H__
#define __LSM_FDW_H__

#include "lsm_api.h"


#if PG_VERSION_NUM>=130000
#include "access/table.h"
#define heap_open(oid, lock) table_open(oid, lock)
#define heap_close(oid, lock) table_close(oid, lock)
#define heap_openrv(rel, lockmode) table_openrv(rel, lockmode)
#endif

/*
 * The scan state is for maintaining state for a scan, either for a
 * SELECT or UPDATE or DELETE.
 *
 * It is set up in BeginForeignScan and stashed in node->fdw_state and
 * subsequently used in IterateForeignScan, EndForeignScan and ReScanForeignScan.
 */
typedef struct TableReadState {
    uint64 operationId;
    bool   isKeyBased;
    bool   done;
    StringInfo key;
    size_t bufLen; /* shared mem length, no next batch if it is 0 */
    char  *next;    /* pointer to the next data entry for IterateForeignScan */
    bool   hasNext;  /* whether a next batch from RangeQuery or ReadBatch*/
	char   buf[LSM_MAX_RECORD_SIZE];
} TableReadState;

/*
 * The modify state is for maintaining state of modify operations.
 *
 * It is set up in BeginForeignModify and stashed in
 * rinfo->ri_FdwState and subsequently used in ExecForeignInsert,
 * ExecForeignUpdate, ExecForeignDelete and EndForeignModify.
 */
typedef struct TableWriteState {
    CmdType operation;
} TableWriteState;

/* Function declarations for extension loading and unloading */
extern void _PG_init(void);
extern void _PG_fini(void);

/* Functions used across files in lsm_fdw */
extern void SerializeNullAttribute(TupleDesc tupleDescriptor,
                                   Index index,
                                   StringInfo buffer);

extern void SerializeAttribute(TupleDesc tupleDescriptor,
                               Index index,
                               Datum datum,
                               StringInfo buffer);

extern Datum ShortVarlena(Datum datum, int typeLength, char storage);

extern uint8 DecodeVarintLength(char* start, char* limit, uint64* len);

extern void LsmWorkerMain(Datum main_arg);

#endif
