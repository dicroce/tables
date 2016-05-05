
#include "tables/database.h"

using namespace tables;
using namespace std;

typedef function<void(const string& prefix, const string& key, const string& val)> rangeCB;
typedef function<void(MDB_txn* txn, MDB_dbi& dbi, MDB_cursor* cursor)> bodyCB;

static void _scanByKeyPrefix( const string& keyPrefix, MDB_cursor* cursor, rangeCB rcb )
{
    MDB_val shimKey, shimVal;

    shimKey.mv_size = keyPrefix.length();
    shimKey.mv_data = const_cast<char*>(keyPrefix.c_str());

    if( mdb_cursor_get( cursor, &shimKey, &shimVal, MDB_SET_RANGE ) != 0 )
        CK_THROW(("Unable to locate key in scan."));

    bool endOfData = false;

    do
    {
        // two ways out of this loop: 1) finding a value that doesn't start with "table_name_"
        // 2) hitting the end of data in the db.
        string key( (char*)shimKey.mv_data, shimKey.mv_size );
        string val( (char*)shimVal.mv_data, shimVal.mv_size );

        if( key.compare(0, keyPrefix.length(), keyPrefix) == 0 )
            rcb( keyPrefix, key, val );
        else break;

        if( mdb_cursor_get( cursor, &shimKey, &shimVal, MDB_NEXT ) == MDB_NOTFOUND )
            endOfData = true;
    } while( !endOfData );
}

static pair<string, string> _getByKey( MDB_cursor* cursor, const string& key )
{
    MDB_val shimKey, shimVal;

    shimKey.mv_size = key.length();
    shimKey.mv_data = const_cast<char*>(key.c_str());

    if( mdb_cursor_get( cursor, &shimKey, &shimVal, MDB_SET ) != 0 )
        CK_THROW(("Unable to locate key: %s",key.c_str()));

    return make_pair( key, string( (char*)shimVal.mv_data, shimVal.mv_size ) );
}

static void _putByKey( MDB_txn* txn, MDB_dbi& dbi, const string& key, const string& val )
{
    MDB_val keyShim;
    keyShim.mv_size = key.length();
    keyShim.mv_data = const_cast<char*>(key.c_str());

    MDB_val valShim;
    valShim.mv_size = val.length();
    valShim.mv_data = const_cast<char*>(val.c_str());

    if( mdb_put( txn, dbi, &keyShim, &valShim, 0 ) != 0 )
        CK_THROW(("Unable to mdb_put() " + key));
}

static void _putByKey( MDB_txn* txn, MDB_dbi& dbi, const string& key, const uint8_t* src, size_t size )
{
    MDB_val keyShim;
    keyShim.mv_size = key.length();
    keyShim.mv_data = const_cast<char*>(key.c_str());

    MDB_val valShim;
    valShim.mv_size = size;
    valShim.mv_data = const_cast<uint8_t*>(src);

    if( mdb_put( txn, dbi, &keyShim, &valShim, 0 ) != 0 )
        CK_THROW(("Unable to mdb_put() " + key));
}

static void _transaction( MDB_env* env, bool readOnly, bodyCB f )
{
    MDB_txn* txn = NULL;
    MDB_dbi dbi;
    MDB_cursor* cursor = NULL;
    try
    {
        if( mdb_txn_begin( env, NULL, (readOnly)?MDB_RDONLY:0, &txn ) != 0 )
            CK_THROW(("Unable to create transaction."));

        if( mdb_dbi_open( txn, NULL, 0, &dbi ) != 0 )
            CK_THROW(("Unable to open/create database."));

        if( mdb_cursor_open( txn, dbi, &cursor ) != 0 )
            CK_THROW(("Unable to open cursor."));

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

database::database( const string& fileName ) :
    _env( NULL ),
    _version( 0 ),
    _schema()
{
    if( mdb_env_create( &_env ) != 0 )
        CK_THROW(("Unable to create lmdb environment."));

    if( mdb_env_open( _env, fileName.c_str(), MDB_NOSUBDIR | MDB_WRITEMAP | MDB_NOMETASYNC | MDB_NOTLS, 0644 ) )
        CK_THROW(("Unable to open database environment."));

    _transaction( _env, true, [this]( MDB_txn* txn, MDB_dbi dbi, MDB_cursor* cursor ) {
        this->_version = stoi( _getByKey( cursor, "database_version" ).second );

        _scanByKeyPrefix( "table_name_",
                          cursor,
                          [this](const string& prefix, const string& key, const string& val)->bool {
                              this->_schema.insert( make_pair( key.substr(prefix.length()), list<string>{} ) );
                          } );

        bool endOfData = false;

        for( auto kv : this->_schema )
        {
            _scanByKeyPrefix( "index_name_" + kv.first + "_",
                              cursor,
                              [this, kv](const string& prefix, const string& key, const string& val)->bool {
                                  this->_schema[kv.first].push_back( val.substr( prefix.length() ) );
                              } );
        }
    } );
}

database::~database() noexcept
{
    mdb_env_close( _env );
}

void database::create_database( const string& fileName,
                                uint64_t size,
                                const map<string, list<string>>& schema )
{
    MDB_env* env = NULL;
    if( mdb_env_create( &env ) != 0 )
        CK_THROW(("Unable to create lmdb environment."));

    try
    {
        if( mdb_env_set_mapsize( env, size ) != 0 )
            CK_THROW(("Unable to set mdb environment map size."));

        if( mdb_env_set_maxdbs( env, 10 ) != 0 )
            CK_THROW(("Unable to set max number of databases."));

        if( mdb_env_open( env, fileName.c_str(), MDB_NOSUBDIR | MDB_WRITEMAP | MDB_NOMETASYNC, 0644 ) )
            CK_THROW(("Unable to open database environment."));

        _transaction( env, false, [schema]( MDB_txn* txn, MDB_dbi dbi, MDB_cursor* cursor ) {
            _putByKey( txn, dbi, "database_version", "1" );

            auto tables = schema.begin(), tablesEnd = schema.end();
            for( ; tables != tablesEnd; ++tables )
            {
                auto tableName = (*tables).first;

                _putByKey( txn, dbi, "table_name_" + tableName, "table_name_" + tableName );

                auto indexes = (*tables).second;

                for( auto indexName : indexes )
                {
                    string key = "index_name_" + tableName + "_" + indexName;
                    _putByKey( txn, dbi, key, key );
                }

                _putByKey( txn, dbi, "next_pri_key_id_" + tableName, "1" );
                _putByKey( txn, dbi, "last_insert_id_" + tableName, "0" );
            }
        } );

        mdb_env_close( env );
    }
    catch (...)
    {
        mdb_env_close( env );
        throw;
    }
}

void database::insert( const string& tableName,
                       const uint8_t* src, size_t size,
                       indexCB icb )
{
    string newID;
    _transaction( _env, false, [&]( MDB_txn* txn, MDB_dbi dbi, MDB_cursor* cursor ) {
        newID = _getByKey( cursor, "next_pri_key_id_" + tableName ).second;
        _putByKey( txn, dbi, tableName + "_" + newID, src, size );
        _putByKey( txn, dbi, "last_insert_id_" + tableName, newID );
        _putByKey( txn, dbi, "next_pri_key_id_" + tableName, to_string( stoi(newID) + 1) );

        auto indexes = _schema[tableName];

        for( auto indexName : indexes )
        {
            string val = icb( indexName, src, size );
            _putByKey( txn, dbi, "index_" + tableName + "_" + indexName + "_" + val, tableName + "_" + newID );
        }
    } );
}
