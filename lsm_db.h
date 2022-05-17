#ifndef __LSM_DB_H__
#define __LSM_DB_H__

#include "lsm_api.h"
#include "lsm_posix.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

using namespace rocksdb;

/**
 * 对Rocksdb的封装
 * 
 * /

/*
 * Wrapper for RocksDB
 */
struct LsmConnection
{
	DB* db;

	void      open(char const* path);
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
// 用于包装传输的数据
struct LsmMessage
{
	LsmMessageHeader hdr;
	char*            key;
	char*            value;
};

/*
 * Queue for tranferring data between backend and LSM worker thread.
 */
// 用于将数据传送到rocksdb中
struct LsmQueue
{
	// ring buffer 环状缓冲区
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


// 主要封装了对lsm的操作，也就是lsmServer中的一个工作进程
struct LsmWorker
{
	std::map<LsmCursorId, LsmCursor> cursors;
	LsmServer* server;	// 一个server有很多的worker
	LsmQueue*  queue;	//一个worker对应一个queue
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


// 此结构体包含了很多对rocksdb的操作，非常重要，是一个对于rocksdb封装的最大对象，包含了很多的LsmWorker
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
