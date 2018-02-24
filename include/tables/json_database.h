
#ifndef __tables_database
#define __tables_database

#include "liblmdb/lmdb.h"
#include "tables/json.h"
#include <string>
#include <vector>
#include <map>
#include <list>
#include <functional>
#include <stdexcept>

class database_test;

namespace tables
{

template<typename rangeCB>
void _scanByKeyPrefix( const std::string& keyPrefix, MDB_cursor* cursor, rangeCB rcb )
{
    MDB_val shimKey, shimVal;

    shimKey.mv_size = keyPrefix.length();
    shimKey.mv_data = const_cast<char*>(keyPrefix.c_str());

    if( mdb_cursor_get( cursor, &shimKey, &shimVal, MDB_SET_RANGE ) != 0 )
        throw std::runtime_error(("Unable to locate key in scan."));

    bool endOfData = false;

    do
    {
        // two ways out of this loop: 1) finding a value that doesn't start with "table_name_"
        // 2) hitting the end of data in the db.
        std::string key( (char*)shimKey.mv_data, shimKey.mv_size );
        std::string val( (char*)shimVal.mv_data, shimVal.mv_size );

        if( key.compare(0, keyPrefix.length(), keyPrefix) == 0 )
            rcb( keyPrefix, key, val );
        else break;

        if( mdb_cursor_get( cursor, &shimKey, &shimVal, MDB_NEXT ) == MDB_NOTFOUND )
            endOfData = true;
    } while( !endOfData );
}

std::pair<std::string, std::string> _getByKey( MDB_cursor* cursor, const std::string& key )
{
    MDB_val shimKey, shimVal;

    shimKey.mv_size = key.length();
    shimKey.mv_data = const_cast<char*>(key.c_str());

    if( mdb_cursor_get( cursor, &shimKey, &shimVal, MDB_SET ) != 0 )
        throw std::runtime_error(("Unable to locate key: %s",key.c_str()));

    return std::make_pair( key, std::string( (char*)shimVal.mv_data, shimVal.mv_size ) );
}

void _putByKey( MDB_txn* txn, MDB_dbi& dbi, const std::string& key, const std::string& val )
{
    MDB_val keyShim;
    keyShim.mv_size = key.length();
    keyShim.mv_data = const_cast<char*>(key.c_str());

    MDB_val valShim;
    valShim.mv_size = val.length();
    valShim.mv_data = const_cast<char*>(val.c_str());

    if( mdb_put( txn, dbi, &keyShim, &valShim, 0 ) != 0 )
        throw std::runtime_error(("Unable to mdb_put() " + key));
}

void _putByKey( MDB_txn* txn, MDB_dbi& dbi, const std::string& key, const uint8_t* src, size_t size )
{
    MDB_val keyShim;
    keyShim.mv_size = key.length();
    keyShim.mv_data = const_cast<char*>(key.c_str());

    MDB_val valShim;
    valShim.mv_size = size;
    valShim.mv_data = const_cast<uint8_t*>(src);

    if( mdb_put( txn, dbi, &keyShim, &valShim, 0 ) != 0 )
        throw std::runtime_error(("Unable to mdb_put() " + key));
}

template<typename bodyCB>
void _transaction( MDB_env* env, bool readOnly, bodyCB f )
{
    MDB_txn* txn = NULL;
    MDB_dbi dbi;
    MDB_cursor* cursor = NULL;
    try
    {
        if( mdb_txn_begin( env, NULL, (readOnly)?MDB_RDONLY:0, &txn ) != 0 )
            throw std::runtime_error(("Unable to create transaction."));

        if( mdb_dbi_open( txn, NULL, 0, &dbi ) != 0 )
            throw std::runtime_error(("Unable to open/create database."));

        if( mdb_cursor_open( txn, dbi, &cursor ) != 0 )
            throw std::runtime_error(("Unable to open cursor."));

        f( txn, dbi, cursor );

        mdb_cursor_close( cursor );

        mdb_txn_commit( txn );
    }
    catch (...)
    {
        if( cursor )
            mdb_cursor_close( cursor );
        if( txn )
            mdb_txn_abort( txn );
        throw;
    }
}

struct table_info
{
    std::vector<std::string> regular_columns;
    std::vector<std::string> index_columns;
    std::vector<std::vector<std::string>> compound_indexes;
};

class database final
{
    friend class ::database_test;

public:
    class iterator final
    {
    public:
        iterator( const database* db, const std::string& tableName, const std::string& index = std::string()) :
            _db(db),
            _index(index),
            _tableName(tableName),
            _txn(NULL),
            _dbi(),
            _indexCursor(NULL),
            _validIterator(false),
            _shimKey(),
            _shimVal()
        {
            if( mdb_txn_begin( db->_env, NULL, MDB_RDONLY, &_txn ) != 0 )
                throw std::runtime_error(("Unable to create transaction."));
            if( mdb_dbi_open( _txn, NULL, 0, &_dbi ) != 0 )
                throw std::runtime_error(("Unable to open/create database."));
            if( mdb_cursor_open( _txn, _dbi, &_indexCursor ) != 0 )
                throw std::runtime_error(("Unable to create cursor."));
        }

        iterator( const iterator& ) = delete;

        iterator( iterator&& obj ) noexcept :
            _db( std::move(obj._db) ),
            _index( std::move(obj._index) ),
            _tableName( std::move(obj._tableName) ),
            _txn( std::move(obj._txn) ),
            _dbi( std::move(obj._dbi) ),
            _indexCursor( std::move(obj._indexCursor) ),
            _validIterator( std::move(obj._validIterator) ),
            _shimKey( std::move(obj._shimKey) ),
            _shimVal( std::move(obj._shimVal) )
        {
            obj._db = NULL;
            obj._txn = NULL;
            obj._indexCursor = NULL;
            obj._validIterator = false;
        }

        ~iterator() noexcept
        {
            if( _indexCursor )
                mdb_cursor_close( _indexCursor );
            if( _txn )
                mdb_txn_commit( _txn );
        }

        iterator& operator=( const iterator& ) = delete;

        iterator& operator=( iterator&& obj ) noexcept
        {
            _db = std::move( obj._db );
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
            return *this;
        }

        void find( const std::string& val )
        {
            if(_index.empty())
            {
                auto key = _tableName + "_" + val;
                auto prefix = _tableName;
                _set_cursor(key, prefix);
            }
            else
            {
                auto key = "index_" + _tableName + _index + "_" + val;
                auto prefix = "index_" + _tableName + _index;
                _set_cursor(key, prefix);
            }
        }

        void find(const std::vector<std::string>& vals)
        {
            std::string cv;
            for(auto v : vals)
                cv += "_" + v;

            auto key = "index_" + _tableName + _index + cv;
            auto prefix = "index_" + _tableName + _index;
            _set_cursor(key, prefix);
        }

        void next()
        {
            if( !_validIterator )
                throw std::runtime_error(("Invalid iterator!"));

            auto prefix = (_index.empty())?_tableName:"index_" + _tableName + _index;

            _next_cursor(prefix);
        }

        void prev()
        {
            if( !_validIterator )
                throw std::runtime_error(("Invalid iterator!"));

            auto prefix = (_index.empty())?_tableName:"index_" + _tableName + _index;

            _prev_cursor(prefix);
        }

        bool valid()
        {
            return _validIterator;
        }

        std::string current_key()
        {
            if( !_validIterator )
                throw std::runtime_error(("Invalid iterator!"));

            return std::string( (char*)_shimKey.mv_data, _shimKey.mv_size );
        }

        std::string current_data()
        {
            if( !_validIterator )
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
    
            if( mdb_get( _txn, _dbi, &shimKey, &shimVal ) != 0 )
                throw std::runtime_error(("Unable to find data!"));

            return std::string((char*)shimVal.mv_data, shimVal.mv_size);
        }

    private:
        void _set_cursor(const std::string& key, const std::string& prefix)
        {
            _shimKey.mv_size = key.length();
            _shimKey.mv_data = const_cast<char*>(key.c_str());

            if( mdb_cursor_get( _indexCursor, &_shimKey, &_shimVal, MDB_SET_RANGE ) == 0 )
            {
                auto ck = std::string( (char*)_shimKey.mv_data, _shimKey.mv_size );
                if( ck.compare(0, prefix.length(), prefix) == 0 )
                    _validIterator = true;
                else _validIterator = false;
            }
            else _validIterator = false;            
        }

        void _next_cursor(const std::string& prefix)
        {
            if( mdb_cursor_get( _indexCursor, &_shimKey, &_shimVal, MDB_NEXT ) != MDB_NOTFOUND )
            {
                auto ck = current_key();
                if( ck.compare(0, prefix.length(), prefix) != 0 )
                    _validIterator = false;
            }
            else _validIterator = false;            
        }

        void _prev_cursor(const std::string& prefix)
        {
            if( mdb_cursor_get( _indexCursor, &_shimKey, &_shimVal, MDB_PREV ) != MDB_NOTFOUND )
            {
                auto ck = current_key();
                if( ck.compare(0, prefix.length(), prefix) != 0 )
                    _validIterator = false;
            }
            else _validIterator = false;            
        }

        const database* _db;
        std::string _index;
        std::string _tableName;
        MDB_txn* _txn;
        MDB_dbi _dbi;
        MDB_cursor* _indexCursor;
        bool _validIterator;
        MDB_val _shimKey;
        MDB_val _shimVal;
    };

    database( const std::string& fileName ) :
        _env( NULL ),
        _version( 0 ),
        _schema()
    {
        if( mdb_env_create( &_env ) != 0 )
            throw std::runtime_error(("Unable to create lmdb environment."));

        if( mdb_env_open( _env, fileName.c_str(), MDB_NOSUBDIR | MDB_WRITEMAP | MDB_NOMETASYNC | MDB_NOTLS, 0644 ) )
            throw std::runtime_error(("Unable to open database environment."));

        _transaction( _env, true, [this]( MDB_txn* txn, MDB_dbi dbi, MDB_cursor* cursor ) {

            auto tnj = nlohmann::json::parse( _getByKey(cursor, "table_names").second );

            for(auto tn : tnj)
            {
                auto tableName = tn.get<std::string>();

                table_info ti;
                auto rcj = nlohmann::json::parse(_getByKey(cursor, "regular_columns_" + tableName).second);
                for(auto rc : rcj)
                    ti.regular_columns.push_back(rc.get<std::string>());
                auto icj = nlohmann::json::parse(_getByKey(cursor, "index_columns_" + tableName).second);
                for(auto ic : icj)
                    ti.index_columns.push_back(ic.get<std::string>());

                auto cij = nlohmann::json::parse(_getByKey(cursor, "compound_indexes_" + tableName).second);
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
        } );
    }

    ~database() noexcept
    {
        mdb_env_close( _env );
    }

    static void create_database( const std::string& fileName,
                                 uint64_t size,
                                 const std::string& schema )
    {
        MDB_env* env = NULL;
        if(mdb_env_create(&env) != 0)
            throw std::runtime_error(("Unable to create lmdb environment."));

        try
        {
            if( mdb_env_set_mapsize( env, size ) != 0 )
                throw std::runtime_error(("Unable to set mdb environment map size."));

            if( mdb_env_set_maxdbs( env, 10 ) != 0 )
                throw std::runtime_error(("Unable to set max number of databases."));

            if( mdb_env_open( env, fileName.c_str(), MDB_NOSUBDIR | MDB_WRITEMAP | MDB_NOMETASYNC, 0644 ) )
                throw std::runtime_error(("Unable to open database environment."));

            _transaction( env, false, [schema]( MDB_txn* txn, MDB_dbi dbi, MDB_cursor* cursor ) {
                _putByKey( txn, dbi, "database_version", "1" );

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
                        _putByKey( txn, dbi, "regular_columns_" + tableName, table["regular_columns"].dump() );
                    else _putByKey( txn, dbi, "regular_columns_" + tableName, "[]");

                    if(table.find("index_columns") != table.end())
                        _putByKey( txn, dbi, "index_columns_" + tableName, table["index_columns"].dump() );
                    else _putByKey( txn, dbi, "index_columns_" + tableName, "[]");

                    if(table.find("compound_indexes") != table.end())
                        _putByKey( txn, dbi, "compound_indexes_" + tableName, table["compound_indexes"].dump() );
                    else _putByKey( txn, dbi, "compound_indexes_" + tableName, "[]");

                    _putByKey( txn, dbi, "next_pri_key_id_" + tableName, "1" );
                    _putByKey( txn, dbi, "last_insert_id_" + tableName, "0" );
                }

                _putByKey( txn, dbi, "table_names", tableNames.dump() );
            } );

            mdb_env_close(env);
        }
        catch (...)
        {
            mdb_env_close( env );
            throw;
        }
    }

    std::string insert( const std::string& tableName, const std::string& row)
    {
        std::string newID;
        _transaction( _env, false, [&]( MDB_txn* txn, MDB_dbi dbi, MDB_cursor* cursor ) {
            newID = _getByKey( cursor, "next_pri_key_id_" + tableName ).second;
            _putByKey( txn, dbi, tableName + "_" + newID, row);
            _putByKey( txn, dbi, "last_insert_id_" + tableName, newID );
            _putByKey( txn, dbi, "next_pri_key_id_" + tableName, std::to_string( stoi(newID) + 1) );

            auto ti = _schema[tableName];

            auto j = nlohmann::json::parse(row);

            for(auto indexName : ti.index_columns)
                _putByKey( txn, dbi, "index_" + tableName + "_" + indexName + "_" + j[indexName].get<std::string>(), tableName + "_" + newID );

            for(auto ci : ti.compound_indexes)
            {
                std::string key = "index_" + tableName;
                for(auto idx : ci)
                    key += "_" + idx;
                for(auto idx : ci)
                    key += "_" + j[idx].get<std::string>();
                _putByKey( txn, dbi, key, tableName + "_" + newID );
            }
        } );

        return newID;
    }


    iterator get_iterator( const std::string& tableName, const std::vector<std::string>& indexes )
    {
        std::string indexKey;
        for(auto idx : indexes)
            indexKey += "_" + idx;
        return iterator(this, tableName, indexKey);
    }

    iterator get_iterator( const std::string& tableName, const std::string& index )
    {
        return iterator( this, tableName, "_" + index );
    }

    iterator get_primary_key_iterator(const std::string& tableName)
    {
        return iterator(this, tableName);
    }

private:
    MDB_env* _env;
    uint32_t _version;
    std::map<std::string, table_info> _schema;
};

}

#endif
