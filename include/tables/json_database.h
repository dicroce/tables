
#ifndef __tables_json_database
#define __tables_json_database

//
// TODO
// - TEST: actual thread cases (12 write threads, bursty read threads, all with own db object).
// - TEST: What is the behaviour if a still existing iterator (and hence its txn is still open)
//         primary key is used for a remove? If it works, what happens to iterator after remove()?
// - TEST: remove() row w/ no indexes, row w/ indexes, row w/ indexes and compound indexes.
// - TEST: atomicity of transactions.
// - TEST: No columns
// - TEST: what happens when the map fills up?
// - TEST: Performance when you have 1 million rows?

#include "liblmdb/lmdb.h"
#include "tables/json.h"
#include "tables/utils.h"
#include <string>
#include <vector>
#include <map>
#include <list>
#include <functional>
#include <stdexcept>
#include <mutex>

class json_database_test;

namespace tables
{

struct table_info
{
    std::vector<std::string> regular_columns;
    std::vector<std::string> index_columns;
    std::vector<std::vector<std::string>> compound_indexes;
};

class json_database final
{
    friend class ::json_database_test;

public:
    class iterator final
    {
    public:
        iterator(const json_database* db, const std::string& tableName, const std::string& index = std::string()) :
            _db(db),
            _index(index),
            _tableName(tableName),
            _txn(NULL),
            _dbi(),
            _indexCursor(NULL),
            _validIterator(false),
            _shimKey(),
            _shimVal(),
            _closed(false)
        {
            if(mdb_txn_begin(db->_env, NULL, MDB_RDONLY, &_txn) != 0)
                throw std::runtime_error(("Unable to create transaction."));
            if(mdb_dbi_open(_txn, NULL, 0, &_dbi) != 0)
                throw std::runtime_error(("Unable to open/create json_database."));
            if(mdb_cursor_open(_txn, _dbi, &_indexCursor) != 0)
                throw std::runtime_error(("Unable to create cursor."));

            // Empty search value should cause iterator to go to beginning of index.
            find(std::string());
        }

        iterator(const iterator&) = delete;

        iterator(iterator&& obj) noexcept :
            _db(std::move(obj._db)),
            _index(std::move(obj._index)),
            _tableName(std::move(obj._tableName)),
            _txn(std::move(obj._txn)),
            _dbi(std::move(obj._dbi)),
            _indexCursor(std::move(obj._indexCursor)),
            _validIterator(std::move(obj._validIterator)),
            _shimKey(std::move(obj._shimKey)),
            _shimVal(std::move(obj._shimVal)),
            _closed(std::move(obj._closed))
        {
            obj._db = NULL;
            obj._txn = NULL;
            obj._indexCursor = NULL;
            obj._validIterator = false;
            obj._closed = true;
        }

        ~iterator() noexcept
        {
            _close();
        }

        iterator& operator=(const iterator&) = delete;

        iterator& operator=(iterator&& obj) noexcept
        {
            _close();

            _db = std::move(obj._db);
            obj._db = NULL;
            _index = std::move(obj._index);
            _tableName = std::move(obj._tableName);
            _txn = std::move(obj._txn);
            obj._txn = NULL;
            _dbi = std::move(obj._dbi);
            _indexCursor = std::move(obj._indexCursor);
            obj._indexCursor = NULL;
            _validIterator = std::move(obj._validIterator);
            obj._validIterator = false;
            _shimKey = std::move(obj._shimKey);
            _shimVal = std::move(obj._shimVal);
            _closed = std::move(obj._closed);
            obj._closed = true;

            return *this;
        }

        void find(const std::string& val)
        {
            if(_closed)
                throw std::runtime_error(("Unable to find() on close()d iterators."));

            std::string key, prefix;

            if(_index.empty())
            {
                key = _tableName + "_" + val;
                prefix = _tableName;
            }
            else
            {
                key = "index_" + _tableName + _index + "_" + val;
                prefix = "index_" + _tableName + _index;
            }

            _set_cursor(key, prefix);
        }

        void find(const std::vector<std::string>& vals)
        {
            if(_closed)
                throw std::runtime_error(("Unable to find() on close()d iterators."));

            std::string cv;
            for(auto v : vals)
                cv += "_" + v;

            auto key = "index_" + _tableName + _index + cv;
            auto prefix = "index_" + _tableName + _index;

            _set_cursor(key, prefix);
        }

        void next()
        {
            if(_closed)
                throw std::runtime_error(("Unable to next() on close()d iterators."));

            if(!_validIterator)
                throw std::runtime_error(("Invalid iterator!"));

            auto prefix = (_index.empty())?_tableName:"index_" + _tableName + _index;

            _next_cursor(prefix);
        }

        void prev()
        {
            if(_closed)
                throw std::runtime_error(("Unable to prev() on close()d iterators."));

            if(!_validIterator)
                throw std::runtime_error(("Invalid iterator!"));

            auto prefix = (_index.empty())?_tableName:"index_" + _tableName + _index;

            _prev_cursor(prefix);
        }

        bool valid() const
        {
            if(_closed)
                throw std::runtime_error(("Unable to valid() on close()d iterators."));

            return _validIterator;
        }

        std::string current_pk() const
        {
            if(_closed)
                throw std::runtime_error(("Unable to current_pk() on close()d iterators."));

            if(!_validIterator)
                throw std::runtime_error(("Invalid iterator!"));

            auto key = std::string((char*)_shimVal.mv_data, _shimVal.mv_size);

            auto runder_index = key.rfind('_');

            if(runder_index == std::string::npos)
                throw std::runtime_error(("Malformed primary key."));

            return key.substr(runder_index+1);
        }

        std::string current_data() const
        {
            if(_closed)
                throw std::runtime_error(("Unable to current_data() on close()d iterators."));

            if(!_validIterator)
                throw std::runtime_error(("Invalid iterator!"));

            MDB_val shimKey, shimVal;

            if(_index.empty())
            {
                shimKey.mv_size = _shimKey.mv_size;
                shimKey.mv_data = _shimKey.mv_data;
            }
            else
            {
                shimKey.mv_size = _shimVal.mv_size;
                shimKey.mv_data = _shimVal.mv_data;
            }
    
            if(mdb_get(_txn, _dbi, &shimKey, &shimVal) != 0)
                throw std::runtime_error(("Unable to find data!"));

            return std::string((char*)shimVal.mv_data, shimVal.mv_size);
        }

    private:
        void _close() noexcept
        {
            _validIterator = false;
            _closed = true;

            if(_indexCursor)
            {
                mdb_cursor_close(_indexCursor);
                _indexCursor = NULL;
            }
            if(_txn)
            {
                mdb_txn_commit(_txn);
                _txn = NULL;
            }
        }

        void _set_cursor(const std::string& key, const std::string& prefix)
        {
            _shimKey.mv_size = key.length();
            _shimKey.mv_data = const_cast<char*>(key.c_str());

            if(mdb_cursor_get(_indexCursor, &_shimKey, &_shimVal, MDB_SET_RANGE) == 0)
            {
                auto ck = std::string((char*)_shimKey.mv_data, _shimKey.mv_size);
                if(ck.compare(0, prefix.length(), prefix) == 0)
                    _validIterator = true;
                else _validIterator = false;
            }
            else _validIterator = false;            
        }

        void _next_cursor(const std::string& prefix)
        {
            if(mdb_cursor_get(_indexCursor, &_shimKey, &_shimVal, MDB_NEXT) != MDB_NOTFOUND)
            {
                auto ck = std::string((char*)_shimKey.mv_data, _shimKey.mv_size);
                if(ck.compare(0, prefix.length(), prefix) != 0)
                    _validIterator = false;
            }
            else _validIterator = false;            
        }

        void _prev_cursor(const std::string& prefix)
        {
            if(mdb_cursor_get(_indexCursor, &_shimKey, &_shimVal, MDB_PREV) != MDB_NOTFOUND)
            {
                auto ck = std::string((char*)_shimKey.mv_data, _shimKey.mv_size);
                if(ck.compare(0, prefix.length(), prefix) != 0)
                    _validIterator = false;
            }
            else _validIterator = false;            
        }

        const json_database* _db;
        std::string _index;
        std::string _tableName;
        MDB_txn* _txn;
        MDB_dbi _dbi;
        MDB_cursor* _indexCursor;
        bool _validIterator;
        MDB_val _shimKey;
        MDB_val _shimVal;
        bool _closed;
    };

    json_database(const std::string& fileName) :
        _env(NULL),
        _version(0),
        _schema(),
        _transacting(false),
        _transLok()
    {
        if(mdb_env_create(&_env) != 0)
            throw std::runtime_error(("Unable to create lmdb environment."));

        if(mdb_env_open(_env, fileName.c_str(), MDB_NOSUBDIR | MDB_WRITEMAP | MDB_NOMETASYNC | MDB_NOTLS, 0644))
            throw std::runtime_error(("Unable to open json_database environment."));

        _transaction(_env, true, [this](trans_state& ts) {

            _version = s_to_uint64(tables::_getByKey(ts.cursor, "database_version").second);

            auto tnj = nlohmann::json::parse(_getByKey(ts.cursor, "table_names").second);

            for(auto tn : tnj)
            {
                auto tableName = tn.get<std::string>();

                table_info ti;
                auto rcj = nlohmann::json::parse(_getByKey(ts.cursor, "regular_columns_" + tableName).second);
                for(auto rc : rcj)
                    ti.regular_columns.push_back(rc.get<std::string>());
                auto icj = nlohmann::json::parse(_getByKey(ts.cursor, "index_columns_" + tableName).second);
                for(auto ic : icj)
                    ti.index_columns.push_back(ic.get<std::string>());

                auto cij = nlohmann::json::parse(_getByKey(ts.cursor, "compound_indexes_" + tableName).second);
                for(auto cic : cij)
                {
                    if(!cic.empty())
                    {
                        std::vector<std::string> idx;
                        for(auto col : cic)
                            idx.push_back(col.get<std::string>());
                        ti.compound_indexes.push_back(idx);
                    }
                }

                _schema[tableName] = ti;
            }
        });
    }

    json_database(const json_database&) = delete;
    json_database(json_database&& obj) = delete;

    ~json_database() noexcept
    {
        _close();
    }

    json_database& operator=(const json_database&) = delete;
    json_database& operator=(json_database&& obj) noexcept = delete;

    uint64_t get_version() const { return _version; }

    static void create_database(const std::string& fileName,
                                uint64_t size,
                                const std::string& schema,
                                uint64_t version = 1)
    {
        MDB_env* env = NULL;
        if(mdb_env_create(&env) != 0)
            throw std::runtime_error(("Unable to create lmdb environment."));

        try
        {
            if(mdb_env_set_mapsize(env, size) != 0)
                throw std::runtime_error(("Unable to set mdb environment map size."));

            if(mdb_env_set_maxdbs(env, 10) != 0)
                throw std::runtime_error(("Unable to set max number of json_databases."));

            if(mdb_env_open(env, fileName.c_str(), MDB_NOSUBDIR | MDB_WRITEMAP | MDB_NOMETASYNC, 0644))
                throw std::runtime_error(("Unable to open json_database environment."));

            _transaction(env, false, [schema, version](trans_state& ts) {

                _putByKey(ts.txn, ts.dbi, "database_version", uint64_to_s(version));

                auto j = nlohmann::json::parse(schema);

                // [
                //     {
                //         "table_name": "segment_files",
                //         "regular_columns": [ "sdp" ],
                //         "index_columns": [ "start_time", "end_time", "segment_id" ]
                //         "compound_indexes": [ [ "start_time", "segment_id" ] ]
                //     }
                // ]

                auto tableNames = nlohmann::json::array({});

                for(auto table : j)
                {
                    auto tableName = table["table_name"].get<std::string>();
                    tableNames.push_back(tableName);

                    if(table.find("regular_columns") != table.end())
                        _putByKey(ts.txn, ts.dbi, "regular_columns_" + tableName, table["regular_columns"].dump());
                    else _putByKey(ts.txn, ts.dbi, "regular_columns_" + tableName, "[]");

                    if(table.find("index_columns") != table.end())
                        _putByKey(ts.txn, ts.dbi, "index_columns_" + tableName, table["index_columns"].dump());
                    else _putByKey(ts.txn, ts.dbi, "index_columns_" + tableName, "[]");

                    if(table.find("compound_indexes") != table.end())
                        _putByKey(ts.txn, ts.dbi, "compound_indexes_" + tableName, table["compound_indexes"].dump());
                    else _putByKey(ts.txn, ts.dbi, "compound_indexes_" + tableName, "[]");

                    _putByKey(ts.txn, ts.dbi, "next_pri_key_id_" + tableName, "1");
                    _putByKey(ts.txn, ts.dbi, "last_insert_id_" + tableName, "0");
                }

                _putByKey(ts.txn, ts.dbi, "table_names", tableNames.dump());
            });

            mdb_env_close(env);
        }
        catch (...)
        {
            mdb_env_close(env);
            throw;
        }
    }

    template<typename TRANSCB>
    void transaction(TRANSCB tcb)
    {
        std::unique_lock<std::recursive_mutex> g(_transLok);

        _transaction(_env, false, [&](trans_state& ts){
            _transacting = true;
            tcb(ts);
        });

        _transacting = false;
    }

    // Note: If you're wondering where you get the trans_state from the answer is via the transaction.
    std::string insert_json(trans_state& ts, const std::string& tableName, const std::string& row)
    {
        if(!_transacting)
            throw std::runtime_error(("Unable to insert_json() outside of a transaction."));

        auto newID = _getByKey(ts.cursor, "next_pri_key_id_" + tableName).second;
        _putByKey(ts.txn, ts.dbi, tableName + "_" + newID, row);
        _putByKey(ts.txn, ts.dbi, "last_insert_id_" + tableName, newID);

        _putByKey(ts.txn, ts.dbi, "next_pri_key_id_" + tableName, uint64_to_s(s_to_uint64(newID) + 1));

        auto ti = _schema[tableName];

        auto j = nlohmann::json::parse(row);

        for(auto indexName : ti.index_columns)
            _putByKey(ts.txn, ts.dbi, "index_" + tableName + "_" + indexName + "_" + j[indexName].get<std::string>(), tableName + "_" + newID);

        for(auto ci : ti.compound_indexes)
        {
            std::string key = "index_" + tableName;
            for(auto idx : ci)
                key += "_" + idx;
            for(auto idx : ci)
                key += "_" + j[idx].get<std::string>();
            _putByKey(ts.txn, ts.dbi, key, tableName + "_" + newID);
        }

        return newID;
    }

    void remove(trans_state& ts, const std::string& tableName, const std::string& pk)
    {
        if(!_transacting)
            throw std::runtime_error(("Unable to remove() outside of a transaction."));

        auto rowj = nlohmann::json::parse(_getByKey(ts.cursor, tableName + "_" + pk).second);

        // Remove any rows in any indexes that are pointing at our row...
        for(auto ic : _schema[tableName].index_columns)
        {
            auto key = "index_" + tableName + "_" + ic + "_" + rowj[ic].get<std::string>();
            _removeByKey(ts.txn, ts.dbi, key);
        }

        // Remove any rows in any compound index that is pointing at our row...
        for(auto ci : _schema[tableName].compound_indexes)
        {
            std::string colNames, colVals;
            for(auto cp : ci)
            {
                colNames += "_" + cp;
                colVals += "_" + rowj[cp].get<std::string>();
            }

            auto key = "index_" + tableName + colNames + colVals;

            _removeByKey(ts.txn, ts.dbi, "index_" + tableName + colNames + colVals);
        }

        // Finally, remove our data row...
        _removeByKey(ts.txn, ts.dbi, tableName + "_" + pk);
    }
    
    iterator get_iterator(const std::string& tableName, const std::vector<std::string>& indexes)
    {
        std::string indexKey;
        for(auto idx : indexes)
            indexKey += "_" + idx;
        return iterator(this, tableName, indexKey);
    }

    iterator get_iterator(const std::string& tableName, const std::string& index)
    {
        return iterator(this, tableName, "_" + index);
    }

    iterator get_pk_iterator(const std::string& tableName)
    {
        return iterator(this, tableName);
    }

private:
    void _close() noexcept
    {
        if(_env)
        {
            mdb_env_close(_env);
            _env = NULL;
        }
    }

    MDB_env* _env;
    uint64_t _version;
    std::map<std::string, table_info> _schema;
    bool _transacting;
    std::recursive_mutex _transLok;
};

}

#endif
