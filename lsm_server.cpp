//
// Worker's part of LSM queue
//
#include "lsm_api.h"
#include "lsm_db.h"

static LsmServer* server;;

// LSM GUCs
int  LsmQueueSize;
bool LsmSync;
bool LsmUpsert;

/*
 * Enqueue message
 */
void LsmQueue::put(LsmMessage const& msg)
{
	int size = sizeof(LsmMessageHeader) + msg.hdr.keySize + msg.hdr.valueSize;

	if (size > LsmQueueSize)
		LsmError("Message is too long");

	while (true)
	{
		int getPos = this->getPos;
		int putPos = this->putPos;
		int available = putPos >= getPos ? LsmQueueSize - putPos + getPos : getPos - putPos;

		if (size >= available) /* queue overflow? */
		{
			if (!writerBlocked)
			{
				writerBlocked = true;
				LsmMemoryBarrier();
				// Enforce "writeBlocked" flag to be visible by consumer and retry availability check
			}
			else
			{
				SemWait(&full);
			}
			continue;
		}
		size_t tail = LsmQueueSize - putPos;

		// Copy header
		if (tail <= sizeof(LsmMessageHeader))
		{
			memcpy(&req[putPos], &msg, tail);
			memcpy(&req[0], (char*)&msg + tail, sizeof(LsmMessageHeader) - tail);
			putPos = sizeof(LsmMessageHeader) - tail;
		}
		else
		{
			memcpy(&req[putPos], &msg, sizeof(LsmMessageHeader));
			putPos += sizeof(LsmMessageHeader);
		}
		tail = LsmQueueSize - putPos;

		// Copy key
		if (tail <= msg.hdr.keySize)
		{
			memcpy(&req[putPos], msg.key, tail);
			memcpy(&req[0], msg.key + tail, msg.hdr.keySize - tail);
			putPos = msg.hdr.keySize - tail;
		}
		else
		{
			memcpy(&req[putPos], msg.key, msg.hdr.keySize);
			putPos += msg.hdr.keySize;
		}
		tail = LsmQueueSize - putPos;

		// Copy value
		if (tail <= msg.hdr.valueSize)
		{
			memcpy(&req[putPos], msg.value, tail);
			memcpy(&req[0], msg.value + tail, msg.hdr.valueSize - tail);
			putPos = msg.hdr.valueSize - tail;
		}
		else
		{
			memcpy(&req[putPos], msg.value, msg.hdr.valueSize);
			putPos += msg.hdr.valueSize;
		}
		this->putPos = putPos;
		SemPost(&empty); // Enforce write barrier and notify consumer
		return;
	}
}

/*
 * Dequeue message.
 * This method is not advancing getPos to make it possible to point data directly in ring buffer.
 * getPost will be addvance by next() method after th end of message processing
 */
void LsmQueue::get(char* buf, LsmMessage& msg)
{
	// Wait until queue is not empty.
	// We are not comparing getPos with putPos before waiting semaphore to make sure that writer barrier enforced by SemPost
	// makes all data written by producer visible for consumer.
	SemWait(&empty);

	if (terminate)
	{
		msg.hdr.op = LsmOpTerminate;
		return;
	}

	int getPos = this->getPos;
	int putPos = this->putPos;

	if (putPos == getPos)
		LsmError("Queue race condition!");

	size_t tail = LsmQueueSize - getPos;

	//  Copy header
	if (tail <= sizeof(LsmMessageHeader))
	{
		memcpy(&msg, &req[getPos], tail);
		memcpy((char*)&msg + tail, &req[0], sizeof(LsmMessageHeader) - tail);
	    getPos = sizeof(LsmMessageHeader) - tail;
	}
	else
	{
		memcpy(&msg, &req[getPos], sizeof(LsmMessageHeader));
		getPos += sizeof(LsmMessageHeader);
	}
	tail = LsmQueueSize - getPos;

	// Copy key
	if (tail < msg.hdr.keySize)
	{
		memcpy(buf, &req[getPos], tail);
		memcpy(buf + tail, &req[0], msg.hdr.keySize - tail);
	    getPos = msg.hdr.keySize - tail;
		msg.key = buf;
		buf += msg.hdr.keySize;
	}
	else
	{
		msg.key = &req[getPos];
		getPos += msg.hdr.keySize;
		if (getPos == LsmQueueSize)
		{
			getPos = 0;
		}
	}
	tail = LsmQueueSize - getPos;

	// Copy value
	if (tail < msg.hdr.valueSize)
	{
		memcpy(buf, &req[getPos], tail);
		memcpy(buf + tail, &req[0], msg.hdr.valueSize - tail);
		msg.value = buf;
	}
	else
	{
		msg.value = &req[getPos];
	}
}

/*
 * Advance getPos in queue (see comment to get() method
 */
void LsmQueue::next(LsmMessage const& msg)
{
	int getPos = this->getPos;
	bool writerBlocked = this->writerBlocked;
	size_t size = sizeof(LsmMessageHeader) + msg.hdr.keySize + msg.hdr.valueSize;
	size_t tail = LsmQueueSize - getPos;
	this->getPos = (tail <= size) ? size - tail : getPos + size;
	if (writerBlocked)
	{
		// Notify consumer that some more free space is avaialble in ring buffer
		this->writerBlocked = false;
		SemPost(&full);
	}
}

inline LsmConnection&
LsmWorker::open(LsmMessage const& msg)
{
	return server->open(msg);
}

/*
 * Insert or update record in the storage
 */
void
LsmWorker::insert(LsmMessage const& msg)
{
	LsmConnection& con(open(msg));
	queue->resp[0] = (char)con.insert(msg.key, msg.hdr.keySize, msg.value, msg.hdr.valueSize);
	if (LsmSync)
		SemPost(&queue->ready);
}

/*
 * Delete record
 */
void
LsmWorker::remove(LsmMessage const& msg)
{
	LsmConnection& con(open(msg));
	queue->resp[0] = (char)con.remove(msg.key, msg.hdr.keySize);
	if (LsmSync)
		SemPost(&queue->ready);
}

/*
 * Get estimation for number of records in the relation
 */
void
LsmWorker::count(LsmMessage const& msg)
{
	LsmConnection& con(open(msg));
	uint64_t count = con.count();
	memcpy(queue->resp, &count, sizeof(count));
	SemPost(&queue->ready);
}

/*
 * Close cursor implicitly openned by fetch() method
 */
void
LsmWorker::closeCursor(LsmMessage const& msg)
{
	LsmCursor& csr(cursors[msg.hdr.cid]);
	csr.con->releaseIterator(csr.iter);
	cursors.erase(msg.hdr.cid);
}

/*
 * Locate record by key
 */
void
LsmWorker::lookup(LsmMessage const& msg)
{
	LsmConnection& con(open(msg));
    queue->respSize = con.lookup(msg.key, msg.hdr.keySize, queue->resp);
	SemPost(&queue->ready);
}

/*
 * Fetch serveral records from iterator
 */
void
LsmWorker::fetch(LsmMessage const& msg)
{
	LsmCursor& csr(cursors[msg.hdr.cid]);
	if (!csr.con)
	{
		csr.con = &open(msg);
		csr.iter = csr.con->getIterator();
	}
    queue->respSize = csr.con->next(csr.iter, queue->resp);
	SemPost(&queue->ready);
}

/*
 * Worker main loop
 */
void
LsmWorker::run()
{
	while (true)
	{
		LsmMessage msg;
		char buf[LSM_MAX_RECORD_SIZE];
		queue->get(buf, msg);

        switch (msg.hdr.op) {
		  case LsmOpTerminate:
			return;
		  case LsmOpCount:
			count(msg);
			break;
		  case LsmOpCloseCursor:
			closeCursor(msg);
			break;
		  case LsmOpFetch:
			fetch(msg);
			break;
		  case LsmOpLookup:
			lookup(msg);
			break;
		  case LsmOpInsert:
			insert(msg);
			break;
		  case LsmOpDelete:
			remove(msg);
			break;
		  default:
			assert(false);
        }
		queue->next(msg);
	}
}

void
LsmWorker::start()
{
	PthreadCreate(&thread, NULL, LsmWorker::main, this);
}

void
LsmWorker::stop()
{
	queue->terminate = true;
	SemPost(&queue->empty);
}

void
LsmWorker::wait()
{
	void* status;
	PthreadJoin(thread, &status);
}

void*
LsmWorker::main(void* arg)
{
	((LsmWorker*)arg)->run();
	return NULL;
}

/*
 * Start LSM worker threads for all backends and wait their completion.
 * TODO: threads can be started on demand, but it complicates client-server protocol.
 */
void
LsmRunWorkers(int maxClients)
{
	server = new LsmServer(maxClients);
	server->start();
	server->wait();
	delete server;
}

/*
 * Wait terination of LSM threads
 */
void
LsmStopWorkers(void)
{
	server->stop();
}

LsmServer::LsmServer(size_t maxClients) : nWorkers(maxClients)
{
	workers = new LsmWorker*[nWorkers];
	for (size_t i = 0; i < nWorkers; i++)
	{
		workers[i] = new LsmWorker(this, queues[i]);
	}
}

void
LsmServer::start()
{
	for (size_t i = 0; i < nWorkers; i++)
	{
		workers[i]->start();
	}
}

void
LsmServer::wait()
{
	for (size_t i = 0; i < nWorkers; i++)
	{
		workers[i]->wait();
	}
}


LsmServer::~LsmServer()
{
	for (size_t i = 0; i < nWorkers; i++)
	{
		delete workers[i];
	}
	delete[] workers;
}

void
LsmServer::stop()
{
	for (size_t i = 0; i < nWorkers; i++)
	{
		workers[i]->stop();
	}
}

LsmConnection&
LsmServer::open(LsmMessage const& msg)
{
	CriticalSection cs(mutex);
	LsmConnection& con = connections[msg.hdr.rid];
	if (con.db == NULL)
	{
		char path[64];
		sprintf(path, "%s/%d", LSM_FDW_NAME, msg.hdr.rid);

		// @todo 根据msg中的key来打开对应的DB
		char* key = msg.key;
        char* col_family_name = nullptr;    //列族名称
        int i = 0;
        while(key[i] != '_'){   // 而每个列族的名称为key的第一个下划线之前的字符串
            i++;
        }
        col_family_name = (char *) malloc((i+10) * sizeof (char ));
        strncpy(col_family_name, key, i);
        col_family_name[i] = '\0';

        // 判断这个列族是否存在，不存在就创建列族
        std::vector<std::string>* column_families = new std::vector<std::string>;  //表示rksdb中所有的列族
        DBOptions db_options;  //数据库库的配置选项
        DB::ListColumnFamilies(db_options, con.db_path, column_families);
        bool isExist = false;
        for (int j = 0; j < (*column_families).size(); ++j) {
            std::string name = (*column_families)[j];
            if(name == std::string(col_family_name)){
                isExist = true;
                break;
            }
        }
        ColumnFamilyOptions* cf_options;    //列族的配置选项
        ColumnFamilyHandle *cf; //列族的处理器
        if(!isExist){
            // 不存在此列族，就创建一个对应的列族
            Options options;
            options.create_if_missing = true;
            // open db
            Status s = DB::Open(options, con.db_path, &con.db);
            // create column family
            s = con.db->CreateColumnFamily(cf_options, std::string(col_family_name), &cf);
            // close db
            s = con.db->DestroyColumnFamilyHandle(cf);
            delete con.db;
        }

        // 将默认列族和新创建的列族加入其中
        con.column_families.push_back(ColumnFamilyDescriptor(   //打开默认列族
                kDefaultColumnFamilyName, ColumnFamilyOptions()));
        con.column_families.push_back(ColumnFamilyDescriptor(   //打开新的列族
                std::string(col_family_name), ColumnFamilyOptions()));

        // 打开数据库
		con.open(path);
	}
	return con;
}
