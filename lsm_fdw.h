#ifndef __LSM_FDW_H__
#define __LSM_FDW_H__

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
    char  *buf;     /* shared mem for data returned by RangeQuery or ReadBatch */
    size_t bufLen; /* shared mem length, no next batch if it is 0 */
    char  *next;    /* pointer to the next data entry for IterateForeignScan */
    bool   hasNext;  /* whether a next batch from RangeQuery or ReadBatch*/
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
