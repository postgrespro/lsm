//
// RocksDB wrapper implementation
//
#include "lsm_db.h"


/**
 *  @param path rocksdb数据库的路径
 * 如果在使用rocksdb时没有显式使用过列族，就会发现，所有的操作都发生在一个列族中，
 * 这个列族名称为default.
 */
void
LsmConnection::open(char const* path)
{
    // https://wanghenshui.github.io/rocksdb-doc-cn/doc/Column-Families.html
    // ColumnFamilyOptions 用于配置列族，DBOptions用于数据库粒度的配置
    // Options 继承了了ColumnFamilyOptions和DBOptions,因此Options可以执行上述两种配置
    Options options;
    options.create_if_missing = true;

    // @todo hr,wu 数据库路径
    std::string p(path);
    db_path = p;   // 给LSMConnection中的属性赋值

    // @todo hr,wu 使用LSMConnection中的关于列族的参数来打开数据库
    Status s = DB::Open(options, std::string(path), column_families, &handles, &db);
    if (!s.ok())
		LsmError(s.getState());
}

void
LsmConnection::close()
{
    // @todo 关闭列族
    for (auto handle : handles) {
        Status s = db->DestroyColumnFamilyHandle(handle);
        assert(s.ok());
    }
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
    Status s = db->Get(ro, handles[1], Slice(key, keyLen), &sval);
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

	// @todo hr,wu---真正向rksdb中插入数据---
	// https://wanghenshui.github.io/rocksdb-doc-cn/doc/Column-Families.html
    // 插入数据的具体操作
	s = db->Put(opts, handles[1], Slice(key, keyLen), Slice(val, valLen));
    return s.ok();
}

bool
LsmConnection::remove(char* key, size_t keyLen)
{
	WriteOptions opts;
	opts.sync = LsmSync;
    Status s = db->Delete(opts, handles[1], Slice(key, keyLen));
    return s.ok();
}

