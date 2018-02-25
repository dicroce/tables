#ifndef __tables_utils_h
#define __tables_utils_h

#include <cstdint>
#include <string>
#include <vector>
#include <stdexcept>
#include "liblmdb/lmdb.h"

namespace tables
{

std::string format(const char* fmt, ...);
std::string format(const char* fmt, va_list& args);
std::vector<std::string> split(const std::string& str, char delim);
std::vector<std::string> split(const std::string& str, const std::string& delim);

uint64_t s_to_uint64(const std::string& s);
std::string uint64_to_s(uint64_t val);

struct trans_state
{
    MDB_txn* txn {NULL};
    MDB_dbi dbi;
    MDB_cursor* cursor {NULL};
};

std::pair<std::string, std::string> _getByKey(MDB_cursor* cursor, const std::string& key);
void _putByKey(MDB_txn* txn, MDB_dbi& dbi, const std::string& key, const std::string& val);
void _putByKey(MDB_txn* txn, MDB_dbi& dbi, const std::string& key, const uint8_t* src, size_t size);
void _removeByKey(MDB_txn* txn, MDB_dbi& dbi, const std::string& key);

template<typename rangeCB>
void _scanByKeyPrefix(const std::string& keyPrefix, MDB_cursor* cursor, rangeCB rcb)
{
    MDB_val shimKey, shimVal;

    shimKey.mv_size = keyPrefix.length();
    shimKey.mv_data = const_cast<char*>(keyPrefix.c_str());

    if(mdb_cursor_get(cursor, &shimKey, &shimVal, MDB_SET_RANGE) != 0)
        throw std::runtime_error(("Unable to locate key in scan."));

    bool endOfData = false;

    do
    {
        // two ways out of this loop: 1) finding a value that doesn't start with "table_name_"
        // 2) hitting the end of data in the db.
        std::string key((char*)shimKey.mv_data, shimKey.mv_size);
        std::string val((char*)shimVal.mv_data, shimVal.mv_size);

        if(key.compare(0, keyPrefix.length(), keyPrefix) == 0)
            rcb(keyPrefix, key, val);
        else break;

        if(mdb_cursor_get(cursor, &shimKey, &shimVal, MDB_NEXT) == MDB_NOTFOUND)
            endOfData = true;
    } while(!endOfData);
}

template<typename bodyCB>
void _transaction(MDB_env* env, bool readOnly, bodyCB f)
{
    trans_state ts;

    try
    {
        if(mdb_txn_begin(env, NULL, (readOnly)?MDB_RDONLY:0, &ts.txn) != 0)
            throw std::runtime_error(("Unable to create transaction."));

        if(mdb_dbi_open(ts.txn, NULL, 0, &ts.dbi) != 0)
            throw std::runtime_error(("Unable to open/create json_database."));

        if(mdb_cursor_open(ts.txn, ts.dbi, &ts.cursor) != 0)
            throw std::runtime_error(("Unable to open cursor."));

        f(ts);

        mdb_cursor_close(ts.cursor);

        mdb_txn_commit(ts.txn);
    }
    catch (...)
    {
        if(ts.cursor)
            mdb_cursor_close(ts.cursor);
        if(ts.txn)
            mdb_txn_abort(ts.txn);
        throw;
    }
}

}

#endif
