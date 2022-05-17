/*
 * This file represents API between client working with Postgres API and C++ server working with RocksDB API.
 * To avoid collision between C/C++ headers they are sharing just this one header file.
 */
#ifndef __LSM_API_H__	// 如果没有定义这个宏
#define __LSM_API_H__	// 就定义下面的宏

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {	//加上extern "C"后，会指示编译器这部分代码按C语言的进行编译，而不是C++的。
#endif

/*
 * Maximal size of record which can be transfered through client-server protocol and read batch size as well
 */
// 定义client 和 server之间传送的数据的最大数据量
#define LSM_MAX_RECORD_SIZE (64*1024)	//64KB

/*
 * Name of the directory in $PGDATA
 */
#define LSM_FDW_NAME        "lsm"	//定义fdw的名称为lsm

extern int  LsmQueueSize;
extern bool LsmSync;
extern bool LsmUpsert;

typedef int     LsmRelationId;
typedef int     LsmQueueId;
typedef int64_t LsmCursorId;

typedef enum
{
	LsmOpTerminate,
	LsmOpInsert,
	LsmOpDelete,
	LsmOpCount,
	LsmOpCloseCursor,
	LsmOpFetch,
	LsmOpLookup
} LsmOperation;

// 定义client和server之间的各种操作
extern void     LsmError(char const* message);
extern size_t   LsmShmemSize(int maxClients);
extern void     LsmInitialize(void* ctl, int maxClients);
extern void     LsmAttach(void* ctl);
extern bool     LsmInsert(LsmQueueId qid, LsmRelationId rid, char *key, size_t keyLen, char *val, size_t valLen);
extern uint64_t LsmCount(LsmQueueId qid, LsmRelationId rid);
extern void     LsmCloseCursor(LsmQueueId qid, LsmRelationId rid, LsmCursorId cid);
extern bool     LsmReadNext(LsmQueueId qid, LsmRelationId rid, LsmCursorId cid, char *buf, size_t *size);
extern bool     LsmLookup(LsmQueueId qid, LsmRelationId rid,  char *key, size_t keyLen, char *val, size_t *valLen);
extern bool     LsmDelete(LsmQueueId qid, LsmRelationId rid,  char *key, size_t keyLen);
extern void     LsmRunWorkers(int maxClients);
extern void     LsmStopWorkers(void);
extern void     LsmMemoryBarrier(void);

#ifdef __cplusplus
}
#endif

#endif

