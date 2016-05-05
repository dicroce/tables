
#ifndef __tables_database
#define __tables_database

#include "lmdb/lmdb.h"
#include "cppkit/ck_exception.h"
#include <string>
#include <vector>
#include <map>
#include <list>
#include <functional>

class database_test;

namespace tables
{

typedef std::function<std::string(std::string col, const uint8_t* src, size_t size)> indexCB;

class database final
{
    friend class ::database_test;

public:
    class iterator final
    {
    public:
        iterator( const database* db, const std::string& index, const std::string& tableName ) :
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
                CK_THROW(("Unable to create transaction."));
            if( mdb_dbi_open( _txn, NULL, 0, &_dbi ) != 0 )
                CK_THROW(("Unable to open/create database."));
            if( mdb_cursor_open( _txn, _dbi, &_indexCursor ) != 0 )
                CK_THROW(("Unable to create cursor."));
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
            std::string key = "index_" + _tableName + "_" + _index + "_" + val;

            _shimKey.mv_size = key.length();
            _shimKey.mv_data = const_cast<char*>(key.c_str());

            if( mdb_cursor_get( _indexCursor, &_shimKey, &_shimVal, MDB_SET_RANGE ) == 0 )
            {
                std::string prefix = "index_" + _tableName + "_" + _index;
                auto ck = std::string( (char*)_shimKey.mv_data, _shimKey.mv_size );                
                if( ck.compare(0, prefix.length(), prefix) == 0 )
                    _validIterator = true;
                else _validIterator = false;

            }
            else _validIterator = false;
        }

        void next()
        {
            if( !_validIterator )
                CK_THROW(("Invalid iterator!"));

            if( mdb_cursor_get( _indexCursor, &_shimKey, &_shimVal, MDB_NEXT ) != MDB_NOTFOUND )
            {
                std::string prefix = "index_" + _tableName + "_" + _index;
                auto ck = current_key();
                if( ck.compare(0, prefix.length(), prefix) != 0 )
                    _validIterator = false;
            }
            else _validIterator = false;
        }

        void prev()
        {
            if( !_validIterator )
                CK_THROW(("Invalid iterator!"));

            if( mdb_cursor_get( _indexCursor, &_shimKey, &_shimVal, MDB_PREV ) != MDB_NOTFOUND )
            {
                std::string prefix = "index_" + _tableName + "_" + _index;
                auto ck = current_key();
                if( ck.compare(0, prefix.length(), prefix) != 0 )
                    _validIterator = false;
            }
            else _validIterator = false;
        }

        bool valid()
        {
            return _validIterator;
        }

        std::string current_key()
        {
            if( !_validIterator )
                CK_THROW(("Invalid iterator!"));

            return std::string( (char*)_shimKey.mv_data, _shimKey.mv_size );
        }

        std::pair<size_t, const uint8_t*> current_data()
        {
            if( !_validIterator )
                CK_THROW(("Invalid iterator!"));

            MDB_val shimKey, shimVal;
            shimKey.mv_size = _shimVal.mv_size;
            shimKey.mv_data = _shimVal.mv_data;

            if( mdb_get( _txn, _dbi, &shimKey, &shimVal ) != 0 )
                CK_THROW(("Unable to find data!"));

            return std::make_pair( shimVal.mv_size, (uint8_t*)shimVal.mv_data );
        }

    private:
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

    database( const std::string& fileName );
    ~database() noexcept;

    static void create_database( const std::string& fileName,
                                 uint64_t size,
                                 const std::map<std::string, std::list<std::string>>& schema );

    void insert( const std::string& tableName,
                 const uint8_t* src, size_t size,
                 indexCB icb );

    iterator get_iterator( const std::string& tableName, const std::string& index )
    {
        return iterator( this, index, tableName );
    }

private:
    MDB_env* _env;
    uint32_t _version;
    std::map<std::string, std::list<std::string>> _schema;
};

}

#endif
