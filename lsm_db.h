#ifndef __LSM_DB_H__
#define __LSM_DB_H__

#include <cstdio>
#include <vector>
#include "string.h"

#include "lsm_api.h"
#include "lsm_posix.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

using namespace rocksdb;

// 添加头文件
using ROCKSDB_NAMESPACE;

/*
 * Wrapper for RocksSB
 */
struct LsmConnection
{
	DB* db;

	// @todo hr,wu
	std::string db_path; //数据库路径
    std::vector<ColumnFamilyDescriptor> column_families;    //列族
    std::vector<ColumnFamilyHandle*> handles;   //列族的处理器

	void      open(char const* path);   // RocksDB的打开路径
	uint64_t  count();
	void      close();
	Iterator* getIterator();
	void      releaseIterator(Iterator* iter);
	size_t    lookup(char const* key, size_t keySize, char* buf);
	size_t    next(Iterator* iter, char* buf);
	bool      insert(char* key, size_t keyLen, char* val, size_t valLen);
	bool      remove(char* key, size_t keyLen);

	LsmConnection() : db(NULL) {}
	~LsmConnection() { close(); }
};

/*
 * Client-server protocol message header, followed by optional key and value bodies
 */
struct LsmMessageHeader
{
	LsmOperation  op;
	uint32_t      keySize;
	uint32_t      valueSize;
	LsmRelationId rid;
	LsmCursorId   cid;
};

/*
 * Protocol message
 */
struct LsmMessage
{
	LsmMessageHeader hdr;
	char*            key;
	char*            value;
};

/*
 * Queue for tranferring data between backend and LSM worker thread.
 */
struct LsmQueue
{
	volatile int   getPos;   // get position in ring buffer (updated only by consumer)
	volatile int   putPos;   // put position in ring buffer (updated only by producer)
	volatile int   respSize; // response size
	volatile int   writerBlocked; // producer is blocked because queue is full
	volatile int   terminate;// worker receives termination request
	sem_t          empty;    // semaphore to wait until queue is not empty
	sem_t          full;     // semaphore to wait until queue is not full
	sem_t          ready;    // semaphore to wait response from server
	char           resp[LSM_MAX_RECORD_SIZE]; // response data
	char           req[1];   // ring buffer (LsmQueueSize long)

	void put(LsmMessage const& msg);
	void get(char* buf, LsmMessage& msg);
	void next(LsmMessage const& msg);

	LsmQueue() : getPos(0), putPos(0), respSize(0), writerBlocked(false) {}
};

struct LsmCursor
{
	LsmConnection* con;
	Iterator* iter;

	LsmCursor() : con(NULL), iter(NULL) {}
};

struct LsmServer;

struct LsmWorker
{
	std::map<LsmCursorId, LsmCursor> cursors;
	LsmServer* server;
	LsmQueue*  queue;
	pthread_t  thread;

	LsmWorker(LsmServer* s, LsmQueue* q) : server(s), queue(q) {}

	void start();
	void stop();
	void run();
	void wait();

  private:
	LsmConnection& open(LsmMessage const& msg);

	void insert(LsmMessage const& msg);
	void remove(LsmMessage const& msg);
	void closeCursor(LsmMessage const& msg);
	void fetch(LsmMessage const& msg);
	void count(LsmMessage const& msg);
	void lookup(LsmMessage const& msg);

	static void* main(void* arg);
};

class Mutex
{
	pthread_mutex_t mutex;
  public:
	Mutex()
	{
		PthreadMutexInit(&mutex);
	}

	~Mutex()
	{
		PthreadMutexDestroy(&mutex);
	}

	void lock()
	{
		PthreadMutexLock(&mutex);
	}

	void unlock()
	{
		PthreadMutexUnlock(&mutex);
	}
};

class CriticalSection
{
	Mutex& mutex;
  public:
	CriticalSection(Mutex& m) : mutex(m)
	{
		mutex.lock();
	}
	~CriticalSection()
	{
		mutex.unlock();
	}
};

struct LsmServer
{
	LsmWorker** workers;
	size_t nWorkers;
	Mutex mutex;
	std::map<LsmRelationId, LsmConnection> connections;

	void start();
	void wait();
	void stop();

	LsmConnection& open(LsmMessage const& msg);
	LsmServer(size_t maxClients);
	~LsmServer();
};

extern LsmQueue** queues;

#endif
