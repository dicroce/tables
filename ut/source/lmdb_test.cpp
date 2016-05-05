
#include "lmdb_test.h"
#include "cppkit/ck_exception.h"
#include "cppkit/os/ck_large_files.h"
#include "lmdb/lmdb.h"

using namespace std;
using namespace cppkit;

REGISTER_TEST_FIXTURE(lmdb_test);

void lmdb_test::setup()
{
}

void lmdb_test::teardown()
{
    ck_unlink( "test.db" );
    ck_unlink( "test.db-lock" );
}

void lmdb_test::test_basic()
{
    MDB_env* env = NULL;
    if( mdb_env_create( &env ) != 0 )
        CK_THROW(("Unable to create lmdb environment."));

    try
    {
        if( mdb_env_set_mapsize( env, 1024*1024*10 ) != 0 )
            CK_THROW(("Unable to set mdb environment map size."));

        if( mdb_env_set_maxdbs( env, 10 ) != 0 )
            CK_THROW(("Unable to set max number of databases."));

        if( mdb_env_open( env, "test.db", MDB_NOSUBDIR | MDB_WRITEMAP | MDB_NOMETASYNC, 0644 ) != 0 )
            CK_THROW(("Unable to open test.db"));

        MDB_txn* txn = NULL;
        if( mdb_txn_begin( env, NULL, 0, &txn ) != 0 )
            CK_THROW(("Unable to create transaction."));

        try
        {
            MDB_dbi dbi;
            if( mdb_dbi_open( txn, NULL, MDB_CREATE | MDB_INTEGERKEY, &dbi ) != 0 )
                CK_THROW(("Unable to open/create database."));

            size_t key = 1;
            MDB_val keyVal;
            keyVal.mv_size = sizeof(key);
            keyVal.mv_data = &key;

            size_t value = 42;
            MDB_val valVal;
            valVal.mv_size = sizeof(value);
            valVal.mv_data = &value;

            if( mdb_put( txn, dbi, &keyVal, &valVal, MDB_NODUPDATA | MDB_NOOVERWRITE ) != 0 )
                CK_THROW(("Unable to mdb_put()!"));

            mdb_txn_commit( txn );
        }
        catch(...)
        {
            mdb_txn_abort( txn );
            throw;
        }


        // read

        txn = NULL;
        if( mdb_txn_begin( env, NULL, MDB_RDONLY, &txn ) != 0 )
            CK_THROW(("Unable to create transaction."));

        try
        {
            MDB_dbi dbi;
            if( mdb_dbi_open( txn, NULL, 0, &dbi ) != 0 )
                CK_THROW(("Unable to open/create database."));

            size_t key = 1;
            MDB_val keyVal;
            keyVal.mv_size = sizeof(key);
            keyVal.mv_data = &key;

            MDB_val valVal;

            if( mdb_get( txn, dbi, &keyVal, &valVal ) != 0 )
                CK_THROW(("Unable to mdb_get()!"));

            mdb_txn_commit( txn );

            size_t outValue;
            outValue = *(size_t*)valVal.mv_data;

            UT_ASSERT( outValue == 42 );
        }
        catch(...)
        {
            mdb_txn_abort( txn );
            throw;
        }

        mdb_env_close( env );
    }
    catch(...)
    {
        mdb_env_close( env );
        throw;
    }
}

void lmdb_test::test_basic_cursor()
{
    MDB_env* env = NULL;
    if( mdb_env_create( &env ) != 0 )
        CK_THROW(("Unable to create lmdb environment."));

    try
    {
        if( mdb_env_set_mapsize( env, 1024*1024*10 ) != 0 )
            CK_THROW(("Unable to set mdb environment map size."));

        if( mdb_env_set_maxdbs( env, 10 ) != 0 )
            CK_THROW(("Unable to set max number of databases."));

        if( mdb_env_open( env, "test.db", MDB_NOSUBDIR | MDB_WRITEMAP | MDB_NOMETASYNC, 0644 ) != 0 )
            CK_THROW(("Unable to open test.db"));

        MDB_txn* txn = NULL;
        if( mdb_txn_begin( env, NULL, 0, &txn ) != 0 )
            CK_THROW(("Unable to create transaction."));

        try
        {
            MDB_dbi dbi;
            if( mdb_dbi_open( txn, NULL, MDB_CREATE | MDB_INTEGERKEY, &dbi ) != 0 )
                CK_THROW(("Unable to open/create database."));

            size_t key = 1;
            MDB_val keyVal;
            keyVal.mv_size = sizeof(key);
            keyVal.mv_data = &key;

            size_t value = 1;
            MDB_val valVal;
            valVal.mv_size = sizeof(value);
            valVal.mv_data = &value;

            if( mdb_put( txn, dbi, &keyVal, &valVal, MDB_NODUPDATA | MDB_NOOVERWRITE ) != 0 )
                CK_THROW(("Unable to mdb_put()!"));

            key = 2;
            keyVal.mv_size = sizeof(key);
            keyVal.mv_data = &key;

            value = 2;
            valVal.mv_size = sizeof(value);
            valVal.mv_data = &value;

            if( mdb_put( txn, dbi, &keyVal, &valVal, MDB_NODUPDATA | MDB_NOOVERWRITE ) != 0 )
                CK_THROW(("Unable to mdb_put()!"));

            key = 3;
            keyVal.mv_size = sizeof(key);
            keyVal.mv_data = &key;

            value = 3;
            valVal.mv_size = sizeof(value);
            valVal.mv_data = &value;

            if( mdb_put( txn, dbi, &keyVal, &valVal, MDB_NODUPDATA | MDB_NOOVERWRITE ) != 0 )
                CK_THROW(("Unable to mdb_put()!"));

            key = 4;
            keyVal.mv_size = sizeof(key);
            keyVal.mv_data = &key;

            value = 4;
            valVal.mv_size = sizeof(value);
            valVal.mv_data = &value;

            if( mdb_put( txn, dbi, &keyVal, &valVal, MDB_NODUPDATA | MDB_NOOVERWRITE ) != 0 )
                CK_THROW(("Unable to mdb_put()!"));

            key = 5;
            keyVal.mv_size = sizeof(key);
            keyVal.mv_data = &key;

            value = 5;
            valVal.mv_size = sizeof(value);
            valVal.mv_data = &value;

            if( mdb_put( txn, dbi, &keyVal, &valVal, MDB_NODUPDATA | MDB_NOOVERWRITE ) != 0 )
                CK_THROW(("Unable to mdb_put()!"));

            mdb_txn_commit( txn );
        }
        catch(...)
        {
            mdb_txn_abort( txn );
            throw;
        }

        // cursor read

        txn = NULL;
        if( mdb_txn_begin( env, NULL, MDB_RDONLY, &txn ) != 0 )
            CK_THROW(("Unable to create transaction."));

        try
        {
            MDB_dbi dbi;
            if( mdb_dbi_open( txn, NULL, 0, &dbi ) != 0 )
                CK_THROW(("Unable to open/create database."));

            MDB_cursor* cursor;
            if( mdb_cursor_open( txn, dbi, &cursor ) != 0 )
                CK_THROW(("Unable to open cursor."));

            size_t key = 1;
            MDB_val keyVal;
            keyVal.mv_size = sizeof(key);
            keyVal.mv_data = &key;

            MDB_val valVal;

            if( mdb_cursor_get( cursor, &keyVal, &valVal, MDB_SET ) != 0 )
                CK_THROW(("Unable to position cursor."));

            size_t value = *(size_t*)valVal.mv_data;
            UT_ASSERT( value == 1 );

            if( mdb_cursor_get( cursor, &keyVal, &valVal, MDB_NEXT ) != 0 )
                CK_THROW(("Unable to next cursor."));

            value = *(size_t*)valVal.mv_data;
            UT_ASSERT( value == 2 );

            if( mdb_cursor_get( cursor, &keyVal, &valVal, MDB_NEXT ) != 0 )
                CK_THROW(("Unable to next cursor."));

            value = *(size_t*)valVal.mv_data;
            UT_ASSERT( value == 3 );

            if( mdb_cursor_get( cursor, &keyVal, &valVal, MDB_NEXT ) != 0 )
                CK_THROW(("Unable to next cursor."));

            value = *(size_t*)valVal.mv_data;
            UT_ASSERT( value == 4 );

            if( mdb_cursor_get( cursor, &keyVal, &valVal, MDB_NEXT ) != 0 )
                CK_THROW(("Unable to next cursor."));

            value = *(size_t*)valVal.mv_data;
            UT_ASSERT( value == 5 );

            UT_ASSERT( mdb_cursor_get( cursor, &keyVal, &valVal, MDB_NEXT ) == MDB_NOTFOUND );

            if( mdb_cursor_get( cursor, &keyVal, &valVal, MDB_PREV ) != 0 )
                CK_THROW(("Unable to prev cursor."));

            // SO, next'ing off the end didn't affect the cursor, but MDB_PREV puts us on previous (4)
            value = *(size_t*)valVal.mv_data;
            UT_ASSERT( value == 4 );

            mdb_cursor_close( cursor );

            mdb_txn_commit( txn );
        }
        catch(...)
        {
            mdb_txn_abort( txn );
            throw;
        }

        mdb_env_close( env );
    }
    catch(...)
    {
        mdb_env_close( env );
        throw;
    }
}

void lmdb_test::test_full_db()
{
    MDB_env* env = NULL;
    if( mdb_env_create( &env ) != 0 )
        CK_THROW(("Unable to create lmdb environment."));

    try
    {
        if( mdb_env_set_mapsize( env, 1024*1024 ) != 0 )
            CK_THROW(("Unable to set mdb environment map size."));

        if( mdb_env_set_maxdbs( env, 10 ) != 0 )
            CK_THROW(("Unable to set max number of databases."));

        if( mdb_env_open( env, "test.db", MDB_NOSUBDIR | MDB_WRITEMAP | MDB_NOMETASYNC, 0644 ) != 0 )
            CK_THROW(("Unable to open test.db"));

        bool done = false;
        bool gotFull = false;

        for( size_t i = 0; i < 1024 && !done; ++i )
        {
            MDB_txn* txn = NULL;
            if( mdb_txn_begin( env, NULL, 0, &txn ) != 0 )
                CK_THROW(("Unable to create transaction."));

            try
            {
                MDB_dbi dbi;
                if( mdb_dbi_open( txn, NULL, MDB_CREATE | MDB_INTEGERKEY, &dbi ) != 0 )
                    CK_THROW(("Unable to open/create database."));

                for( size_t ii = 0; ii < 1024; ++ii )
                {
                    size_t key = i * 1024 + ii;
                    MDB_val keyVal;
                    keyVal.mv_size = sizeof(key);
                    keyVal.mv_data = &key;

                    size_t value = i * 1024 + ii;
                    MDB_val valVal;
                    valVal.mv_size = sizeof(value);
                    valVal.mv_data = &value;

                    int err = mdb_put( txn, dbi, &keyVal, &valVal, MDB_NODUPDATA | MDB_NOOVERWRITE );

                    if( err == MDB_MAP_FULL )
                    {
                        done = true;
                        gotFull = true;
                        throw 42;
                    }
                }

                mdb_txn_commit( txn );
            }
            catch(int ex)
            {
                mdb_txn_abort( txn );
            }
            catch(...)
            {
                mdb_txn_abort( txn );
                throw;
            }
        }

        UT_ASSERT( gotFull == true );

        mdb_env_close( env );
    }
    catch(...)
    {
        mdb_env_close( env );
        throw;
    }
}

void lmdb_test::test_close_cursor()
{
    MDB_env* env = NULL;
    if( mdb_env_create( &env ) != 0 )
        CK_THROW(("Unable to create lmdb environment."));

    try
    {
        if( mdb_env_set_mapsize( env, 1024*1024*10 ) != 0 )
            CK_THROW(("Unable to set mdb environment map size."));

        if( mdb_env_set_maxdbs( env, 10 ) != 0 )
            CK_THROW(("Unable to set max number of databases."));

        if( mdb_env_open( env, "test.db", MDB_NOSUBDIR | MDB_WRITEMAP | MDB_NOMETASYNC, 0644 ) != 0 )
            CK_THROW(("Unable to open test.db"));

        MDB_txn* txn = NULL;
        if( mdb_txn_begin( env, NULL, 0, &txn ) != 0 )
            CK_THROW(("Unable to create transaction."));

        try
        {
            MDB_dbi dbi;
            if( mdb_dbi_open( txn, NULL, MDB_CREATE | MDB_INTEGERKEY, &dbi ) != 0 )
                CK_THROW(("Unable to open/create database."));

            size_t key = 10;
            MDB_val keyVal;
            keyVal.mv_size = sizeof(key);
            keyVal.mv_data = &key;

            size_t value = 10;
            MDB_val valVal;
            valVal.mv_size = sizeof(value);
            valVal.mv_data = &value;

            if( mdb_put( txn, dbi, &keyVal, &valVal, MDB_NODUPDATA | MDB_NOOVERWRITE ) != 0 )
                CK_THROW(("Unable to mdb_put()!"));

            key = 20;
            keyVal.mv_size = sizeof(key);
            keyVal.mv_data = &key;

            value = 20;
            valVal.mv_size = sizeof(value);
            valVal.mv_data = &value;

            if( mdb_put( txn, dbi, &keyVal, &valVal, MDB_NODUPDATA | MDB_NOOVERWRITE ) != 0 )
                CK_THROW(("Unable to mdb_put()!"));

            key = 30;
            keyVal.mv_size = sizeof(key);
            keyVal.mv_data = &key;

            value = 30;
            valVal.mv_size = sizeof(value);
            valVal.mv_data = &value;

            if( mdb_put( txn, dbi, &keyVal, &valVal, MDB_NODUPDATA | MDB_NOOVERWRITE ) != 0 )
                CK_THROW(("Unable to mdb_put()!"));

            key = 40;
            keyVal.mv_size = sizeof(key);
            keyVal.mv_data = &key;

            value = 40;
            valVal.mv_size = sizeof(value);
            valVal.mv_data = &value;

            if( mdb_put( txn, dbi, &keyVal, &valVal, MDB_NODUPDATA | MDB_NOOVERWRITE ) != 0 )
                CK_THROW(("Unable to mdb_put()!"));

            key = 50;
            keyVal.mv_size = sizeof(key);
            keyVal.mv_data = &key;

            value = 50;
            valVal.mv_size = sizeof(value);
            valVal.mv_data = &value;

            if( mdb_put( txn, dbi, &keyVal, &valVal, MDB_NODUPDATA | MDB_NOOVERWRITE ) != 0 )
                CK_THROW(("Unable to mdb_put()!"));

            mdb_txn_commit( txn );
        }
        catch(...)
        {
            mdb_txn_abort( txn );
            throw;
        }

        // Approximate positioning of cursor. Position cursor on first element greater than KEY.

        txn = NULL;
        if( mdb_txn_begin( env, NULL, MDB_RDONLY, &txn ) != 0 )
            CK_THROW(("Unable to create transaction."));

        try
        {
            MDB_dbi dbi;
            if( mdb_dbi_open( txn, NULL, 0, &dbi ) != 0 )
                CK_THROW(("Unable to open/create database."));

            MDB_cursor* cursor;
            if( mdb_cursor_open( txn, dbi, &cursor ) != 0 )
                CK_THROW(("Unable to open cursor."));

            size_t key = 25;
            MDB_val keyVal;
            keyVal.mv_size = sizeof(key);
            keyVal.mv_data = &key;

            MDB_val valVal;

            if( mdb_cursor_get( cursor, &keyVal, &valVal, MDB_SET_RANGE ) != 0 )
                CK_THROW(("Unable to position cursor."));

            size_t value = *(size_t*)valVal.mv_data;
            UT_ASSERT( value == 30 );

            mdb_cursor_close( cursor );

            mdb_txn_commit( txn );
        }
        catch(...)
        {
            mdb_txn_abort( txn );
            throw;
        }

        mdb_env_close( env );
    }
    catch(...)
    {
        mdb_env_close( env );
        throw;
    }
}
