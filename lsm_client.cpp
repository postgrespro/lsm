//
// Backend part of LSM queue
//
#include "lsm_api.h"
#include "lsm_db.h"

LsmQueue** queues;

size_t
LsmShmemSize(int maxClients)
{
    return (sizeof(LsmQueue) + LsmQueueSize + sizeof(LsmQueue*)) * maxClients;
}

void
LsmInitialize(void* ctl, int maxClients)
{
    queues = (LsmQueue**)ctl;
    char* ptr = (char*)(queues + maxClients);
    for (int i = 0; i < maxClients; i++)
    {
        LsmQueue* queue = (LsmQueue*)ptr;
        ptr += sizeof(LsmQueue) + LsmQueueSize;
        queue->getPos = 0;
        queue->putPos = 0;
        queue->writerBlocked = false;
        SemInit(&queue->empty, 1, 0);
        SemInit(&queue->full,  1, 0);
        SemInit(&queue->ready, 1, 0);
        queues[i] = queue;
    }
}

void
LsmAttach(void* ctl)
{
    queues = (LsmQueue**)ctl;
}


bool
LsmDelete(LsmQueueId qid, LsmRelationId rid, char *key, size_t keyLen)
{
    LsmMessage msg;
    LsmQueue* queue = queues[qid];
    msg.hdr.op = LsmOpDelete;
    msg.hdr.rid = rid;
    msg.hdr.keySize = keyLen;
    msg.hdr.valueSize = 0;
    msg.key = key;
    queue->put(msg);
    if (LsmSync)
    {
        SemWait(&queue->ready);
        return (bool)queue->resp[0];
    }
    return true;
}


// 将传入的数据以LsmMessage进行传输
bool
LsmInsert(LsmQueueId qid, LsmRelationId rid, char *key, size_t keyLen, char *val, size_t valLen)
{
    LsmMessage msg;
    LsmQueue* queue = queues[qid];
    msg.hdr.op = LsmOpInsert;
    msg.hdr.rid = rid;	//外部表oid
    msg.hdr.keySize = keyLen;	//key的长度
    msg.hdr.valueSize = valLen;	//value的长度
    msg.key = key;
    msg.value = val;
    queue->put(msg);
    if (LsmSync)
    {
        SemWait(&queue->ready);
        return (bool)queue->resp[0];
    }
    return true;
}



uint64_t
LsmCount(LsmQueueId qid, LsmRelationId rid)
{
    LsmMessage msg;
    LsmQueue* queue = queues[qid];
    msg.hdr.op = LsmOpCount;
    msg.hdr.rid = rid;
    msg.hdr.keySize = 0;
    msg.hdr.valueSize = 0;
    queue->put(msg);
    SemWait(&queue->ready);
    return *(uint64_t*)queue->resp;
}

void
LsmCloseCursor(LsmQueueId qid, LsmRelationId rid, LsmCursorId cid)
{
    LsmMessage msg;
    LsmQueue* queue = queues[qid];
    msg.hdr.op = LsmOpCloseCursor;
    msg.hdr.rid = rid;
    msg.hdr.cid = cid;
    msg.hdr.keySize = 0;
    msg.hdr.valueSize = 0;
    queue->put(msg);
}

bool
LsmReadNext(LsmQueueId qid, LsmRelationId rid, LsmCursorId cid, char *buf, size_t *size)
{
    LsmMessage msg;
    LsmQueue* queue = queues[qid];
    msg.hdr.op = LsmOpFetch;
    msg.hdr.rid = rid;
    msg.hdr.cid = cid;
    msg.hdr.keySize = 0;
    msg.hdr.valueSize = 0;
    queue->put(msg);
    SemWait(&queue->ready);
    memcpy(buf, queue->resp, queue->respSize);
    *size = queue->respSize;
    return *size != 0;
}


bool
LsmLookup(LsmQueueId qid, LsmRelationId rid, char *key, size_t keyLen, char *val, size_t *valLen)
{
    LsmMessage msg;
    LsmQueue* queue = queues[qid];
    msg.hdr.op = LsmOpLookup;
    msg.hdr.rid = rid;
    msg.hdr.keySize = keyLen;
    msg.hdr.valueSize = 0;
    msg.key = key;
    queue->put(msg);
    SemWait(&queue->ready);
    memcpy(val, queue->resp, queue->respSize);
    *valLen = queue->respSize;
    return *valLen != 0;
}
