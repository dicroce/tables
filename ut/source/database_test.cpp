
#include "database_test.h"
#include "tables/database.h"
#include <algorithm>
#include <thread>
#include <mutex>

using namespace std;
using namespace tables;

REGISTER_TEST_FIXTURE(database_test);

void database_test::setup()
{
    if( ut_file_exists("test.db") )
        ut_file_unlink( "test.db" );
    if( ut_file_exists("test.db-lock" ) )
        ut_file_unlink( "test.db-lock" );
}

void database_test::teardown()
{
    if( ut_file_exists("test.db") )
        ut_file_unlink( "test.db" );
    if( ut_file_exists("test.db-lock" ) )
        ut_file_unlink( "test.db-lock" );
}

void database_test::test_create()
{
    string schema = "[ { \"table_name\": \"segment_files\", \"regular_columns\": [], \"index_columns\": [ \"start_time\", \"end_time\", \"segment_id\" ] }, "
                      "{ \"table_name\": \"segments\", \"regular_columns\": [ \"sdp\" ], \"index_columns\": [] } ]";

    database::create_database( "test.db", 16 * (1024*1024), schema );

    UT_ASSERT( ut_file_exists( "test.db" ) );

    database db( "test.db" );

    UT_ASSERT( db._schema.size() == 2 );
    UT_ASSERT( db._schema["segment_files"].regular_columns.size() == 0 );
    auto found = std::find( db._schema["segment_files"].index_columns.begin(), db._schema["segment_files"].index_columns.end(), "start_time" );
    UT_ASSERT( found != db._schema["segment_files"].index_columns.end() );
    found = std::find( db._schema["segment_files"].index_columns.begin(), db._schema["segment_files"].index_columns.end(), "end_time" );
    UT_ASSERT( found != db._schema["segment_files"].index_columns.end() );
    found = std::find( db._schema["segment_files"].index_columns.begin(), db._schema["segment_files"].index_columns.end(), "segment_id" );
    UT_ASSERT( found != db._schema["segment_files"].index_columns.end() );

    UT_ASSERT( db._schema["segments"].index_columns.size() == 0 );
    found = std::find( db._schema["segments"].regular_columns.begin(), db._schema["segments"].regular_columns.end(), "sdp" );
    UT_ASSERT( found != db._schema["segments"].regular_columns.end() );
}

void database_test::test_basic_insert()
{
    std::string schema = "[ { \"table_name\": \"segments\", \"index_columns\": [ \"time\" ] } ]";

    database::create_database( "test.db", 16 * (1024*1024), schema );

    database db( "test.db" );
    string val = "{ \"time\": \"1234\" }";
    db.insert( "segments", val );

    auto iter = db.get_iterator( "segments", "time" );
    iter.find( "1234" );
    UT_ASSERT( iter.valid() );
    UT_ASSERT( iter.current_data() == val);
}

void database_test::test_basic_iteration()
{
    std::string schema = "[ { \"table_name\": \"segments\", \"index_columns\": [ \"time\" ] } ]";

    database::create_database( "test.db", 16 * (1024*1024), schema );

    database db( "test.db" );

    string val1 = "{ \"time\": \"100\" }";
    db.insert( "segments", val1);

    string val2 = "{ \"time\": \"200\" }";
    db.insert( "segments", val2);

    string val3 = "{ \"time\": \"300\" }";
    db.insert( "segments", val3);

    string val4 = "{ \"time\": \"400\" }";
    db.insert( "segments", val4);

    string val5 = "{ \"time\": \"500\" }";
    db.insert( "segments", val5);

    string val6 = "{ \"time\": \"600\" }";
    db.insert( "segments", val6);

    string val7 = "{ \"time\": \"700\" }";
    db.insert( "segments", val7);

    auto iter = db.get_iterator( "segments", "time" );

    iter.find( "500" );
    UT_ASSERT( iter.valid() );
    UT_ASSERT(iter.current_data() == val5);

    iter.next();
    UT_ASSERT( iter.valid() );
    UT_ASSERT(iter.current_data() == val6);

    iter.next();
    UT_ASSERT( iter.valid() );
    UT_ASSERT(iter.current_data() == val7);

    iter.next();
    UT_ASSERT(iter.valid() == false);
}

void database_test::test_primary_key_iteration()
{
    std::string schema = "[ { \"table_name\": \"segments\", \"index_columns\": [ \"time\" ] } ]";

    database::create_database( "test.db", 16 * (1024*1024), schema );

    database db( "test.db" );

    string val1 = "{ \"time\": \"100\" }";
    auto pk1 = db.insert( "segments", val1);

    string val2 = "{ \"time\": \"200\" }";
    auto pk2 = db.insert( "segments", val2);

    string val3 = "{ \"time\": \"300\" }";
    auto pk3 = db.insert( "segments", val3);

    string val4 = "{ \"time\": \"400\" }";
    auto pk4 = db.insert( "segments", val4);

    string val5 = "{ \"time\": \"500\" }";
    auto pk5 = db.insert( "segments", val5);

    string val6 = "{ \"time\": \"600\" }";
    auto pk6 = db.insert( "segments", val6);

    string val7 = "{ \"time\": \"700\" }";
    auto pk7 = db.insert( "segments", val7);

    auto iter = db.get_primary_key_iterator( "segments" );

    iter.find( pk1 );
    UT_ASSERT( iter.valid() );
    UT_ASSERT(iter.current_data() == val1);

    iter.next();
    UT_ASSERT( iter.valid() );
    UT_ASSERT(iter.current_data() == val2);

    iter.find(pk7);
    UT_ASSERT( iter.valid() );
    UT_ASSERT(iter.current_data() == val7);

    iter.next();
    UT_ASSERT(iter.valid() == false);
}
void database_test::test_multiple_indexes()
{
    std::string schema = "[ { \"table_name\": \"segments\", \"index_columns\": [ \"time\", \"index\" ] } ]";

    database::create_database( "test.db", 16 * (1024*1024), schema );

    database db( "test.db" );

    string val1 = "{ \"time\": \"100\", \"index\": \"7\" }";
    auto pk1 = db.insert( "segments", val1);

    string val2 = "{ \"time\": \"200\", \"index\": \"6\" }";
    auto pk2 = db.insert( "segments", val2);

    string val3 = "{ \"time\": \"300\", \"index\": \"5\" }";
    auto pk3 = db.insert( "segments", val3);

    string val4 = "{ \"time\": \"400\", \"index\": \"4\" }";
    auto pk4 = db.insert( "segments", val4);

    string val5 = "{ \"time\": \"500\", \"index\": \"3\" }";
    auto pk5 = db.insert( "segments", val5);

    string val6 = "{ \"time\": \"600\", \"index\": \"2\" }";
    auto pk6 = db.insert( "segments", val6);

    string val7 = "{ \"time\": \"700\", \"index\": \"1\" }";
    auto pk7 = db.insert( "segments", val7);

    {
        auto iter = db.get_iterator( "segments", "time" );

        iter.find( "700" );
        UT_ASSERT( iter.valid() );
        UT_ASSERT(iter.current_data() == val7);
    }

    {
        auto iter = db.get_iterator( "segments", "index" );

        iter.find( "7" );
        UT_ASSERT( iter.valid() );
        UT_ASSERT(iter.current_data() == val1);
    }
}

void database_test::test_swmr()
{
    std::string schema = "[ { \"table_name\": \"segments\", \"index_columns\": [ \"foo\" ] } ]";

    database::create_database( "test.db", 16 * (1024*1024), schema );

    database db( "test.db" );

    bool writerRunning = true;
    uint32_t writerIndex = 0;

    thread wt([&](){
        while( writerRunning )
        {
            string val = "{ \"foo\": \"" + to_string(writerIndex) + "\" }";
            db.insert( "segments", val );
            ++writerIndex;
            ut_usleep( 10000 );
        }
    });

    ut_usleep( 1000000 );

    // An interesting observed behaviour:
    // It looks like read transactions only have access to only written data
    // from before the creation of the read transaction. Since our iterator
    // ctor creates a read transaction we must get it INSIDE of the below
    // while loop if we hope to have access to newly written data.

    bool r1Running = true;
    uint32_t r1Reads = 0;
    thread r1([&](){
        while( r1Running )
        {
            if( writerIndex > 0 )
            {
                string target = to_string( random() % writerIndex );
                auto iter = db.get_iterator( "segments", "foo" );
                iter.find( target );
                if( iter.valid() )
                {
                    string key = iter.current_key();
                    UT_ASSERT( key.find(target) != string::npos );
                    ++r1Reads;
                    ut_usleep( 1000 );
                }
            }
        }
    });

    bool r2Running = true;
    uint32_t r2Reads = 0;
    thread r2([&](){
        while( r2Running )
        {
            if( writerIndex > 0 )
            {
                string target = to_string( random() % writerIndex );
                auto iter = db.get_iterator( "segments", "foo" );
                iter.find( target );
                if( iter.valid() )
                {
                    string key = iter.current_key();
                    UT_ASSERT( key.find(target) != string::npos );
                    ++r2Reads;
                    ut_usleep( 1000 );
                }
            }
        }
    });

    ut_usleep( 5000000 );

    writerRunning = false;
    r1Running = false;
    r2Running = false;

    r1.join();
    r2.join();
    wt.join();
}

void database_test::test_mwmr()
{
    std::string schema = "[ { \"table_name\": \"segment_files\", \"index_columns\": [ \"start_time\", \"end_time\" ] } ]";

    database::create_database( "test.db", 16 * (1024*1024), schema );

    database db( "test.db" );

    struct writer_context
    {
        thread wt;
        uint32_t start_time;
        bool writer_running;
    };

    const size_t NUM_THREADS = 12;
    vector<writer_context> writerContexts( NUM_THREADS );

    for( auto& wc : writerContexts )
    {
        wc.start_time = 0;
        wc.writer_running = true;
        wc.wt = thread( [&](){
            while( wc.writer_running )
            {
                string val = "{ \"start_time\": \"" + to_string(wc.start_time) + "\", \"end_time\": \"" + to_string(wc.start_time) + "\" }";
                db.insert( "segment_files", val );
                ++wc.start_time;
                ut_usleep( 1 );
            }
        } );
    }

    ut_usleep( 1000000 );

    struct reader_context
    {
        thread rt;
        size_t writer_index;
        size_t reads;
        bool reader_running;
    };

    vector<reader_context> readerContexts( NUM_THREADS );

    size_t wi = 0;
    for( auto& rc : readerContexts )
    {
        rc.writer_index = wi;
        ++wi;
        rc.reader_running = true;
        rc.rt = thread( [&]() {
            while( rc.reader_running )
            {
                if( writerContexts[rc.writer_index].start_time > 0 )
                {
                    string target = to_string( random() % writerContexts[rc.writer_index].start_time );
                    auto iter = db.get_iterator( "segment_files", "start_time" );
                    iter.find( target );
                    if( iter.valid() )
                    {
                        string key = iter.current_key();
                        UT_ASSERT( key.find(target) != string::npos );
                        ++rc.reads;
                        ut_usleep( 1 );
                    }
                }
            }
        } );
    }

    ut_usleep( 5000000 );

    size_t totalReads = 0;
    for( auto& rc : readerContexts )
    {
        totalReads += rc.reads;
        rc.reader_running = false;
        rc.rt.join();
    }

    size_t totalWrites = 0;
    for( auto& wc : writerContexts )
    {
        totalWrites += wc.start_time;
        wc.writer_running = false;
        wc.wt.join();
    }

    printf("total reads = %lu\n",totalReads);
    printf("total writes = %lu\n",totalWrites);
    fflush(stdout);
}


void database_test::test_scenario()
{
}

void database_test::test_compound_indexes()
{
    std::string schema = "[ { \"table_name\": \"segments\", \"compound_indexes\": [ [ \"index\", \"time\" ] ] } ]";

    database::create_database( "test.db", 16 * (1024*1024), schema );

    database db( "test.db" );

    string val1 = "{ \"time\": \"100\", \"index\": \"7\" }";
    auto pk1 = db.insert( "segments", val1);

    string val2 = "{ \"time\": \"200\", \"index\": \"7\" }";
    auto pk2 = db.insert( "segments", val2);

    string val3 = "{ \"time\": \"300\", \"index\": \"8\" }";
    auto pk3 = db.insert( "segments", val3);

    auto ci = db.get_iterator("segments", vector<string>{ "index", "time" });

    ci.find(vector<string>{"7", "200"});

    auto row = ci.current_data();

    UT_ASSERT(row == val2);
}
