//
// RocksDB wrapper implementation
//
#include "lsm_db.h"

// 此函数是LsmConnection 中的成员函数，
// 而LsmConnection中有一个非常重要的成员变量：DB* db;
void
LsmConnection::open(char const* path)
{
    Options options;
    options.create_if_missing = true;

    Status s = DB::Open(options, std::string(path), &db);
    if (!s.ok())
		LsmError(s.getState());
}

void
LsmConnection::close()
{
    delete db;
	db = NULL;
}

uint64_t
LsmConnection::count()
{
	std::string count;
    db->GetProperty("rocksdb.estimate-num-keys", &count);
    return stoull(count);
}

Iterator*
LsmConnection::getIterator()
{
    Iterator* it = db->NewIterator(ReadOptions());
    it->SeekToFirst();
    return it;
}

void
LsmConnection::releaseIterator(Iterator* it)
{
    delete it;
}

size_t
LsmConnection::next(Iterator* it, char* buf)
{
	size_t size;
	// Fetch as much records asfits in response buffer
	for (size = 0; it->Valid(); it->Next())
	{
        int keyLen = it->key().size();
		int valLen = it->value().size();
		int pairSize = sizeof(int)*2 + keyLen + valLen;

		if (size + pairSize > LSM_MAX_RECORD_SIZE)
			break;

		memcpy(&buf[size], &keyLen, sizeof keyLen);
		size += sizeof keyLen;
		memcpy(&buf[size], it->key().data(), keyLen);
		size += keyLen;

		memcpy(&buf[size], &valLen, sizeof valLen);
		size += sizeof valLen;
		memcpy(&buf[size], it->value().data(), valLen);
		size += valLen;
    }
	return size;
}

size_t
LsmConnection::lookup(char const* key, size_t keyLen, char* buf)
{
	std::string sval;
    ReadOptions ro;
    Status s = db->Get(ro, Slice(key, keyLen), &sval);
    if (!s.ok())
		return 0;
	size_t valLen = sval.length();
    memcpy(buf, sval.c_str(), valLen);
	return valLen;
}

bool
LsmConnection::insert(char* key, size_t keyLen, char* val, size_t valLen)
{
	Status s;
	WriteOptions opts;
	if (!LsmUpsert)
	{
		std::string sval;
		ReadOptions ro;
		s = db->Get(ro, Slice(key, keyLen), &sval);
		if (s.ok()) // key already exists 
			return false;
	}
	opts.sync = LsmSync;
	s = db->Put(opts, Slice(key, keyLen), Slice(val, valLen));
    return s.ok();
}

bool
LsmConnection::remove(char* key, size_t keyLen)
{
	WriteOptions opts;
	opts.sync = LsmSync;
    Status s = db->Delete(opts, Slice(key, keyLen));
    return s.ok();
}

