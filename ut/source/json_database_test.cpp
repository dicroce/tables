
#include "json_database_test.h"
#include "tables/json_database.h"
#include <algorithm>
#include <thread>
#include <mutex>

using namespace std;
using namespace tables;

REGISTER_TEST_FIXTURE(json_database_test);

void json_database_test::setup()
{
    if( ut_file_exists("test.db") )
        ut_file_unlink( "test.db" );
    if( ut_file_exists("test.db-lock" ) )
        ut_file_unlink( "test.db-lock" );
}

void json_database_test::teardown()
{
    if( ut_file_exists("test.db") )
        ut_file_unlink( "test.db" );
    if( ut_file_exists("test.db-lock" ) )
        ut_file_unlink( "test.db-lock" );
}

void json_database_test::test_create()
{
    string schema = "[ { \"table_name\": \"segment_files\", \"regular_columns\": [], \"index_columns\": [ \"start_time\", \"end_time\", \"segment_id\" ] }, "
                      "{ \"table_name\": \"segments\", \"regular_columns\": [ \"sdp\" ], \"index_columns\": [] } ]";

    json_database::create_database( "test.db", 16 * (1024*1024), schema );

    UT_ASSERT( ut_file_exists( "test.db" ) );

    json_database db( "test.db" );

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

void json_database_test::test_basic_insert()
{
    std::string schema = "[ { \"table_name\": \"segments\", \"index_columns\": [ \"time\" ] } ]";

    json_database::create_database( "test.db", 16 * (1024*1024), schema );

    json_database db( "test.db" );

    string val;
    db.transaction([&](trans_state& ts){
        val = "{ \"time\": \"1234\" }";
        db.insert_json( ts, "segments", val );
    });

    auto iter = db.get_iterator( "segments", "time" );
    iter.find( "1234" );
    UT_ASSERT( iter.valid() );
    UT_ASSERT( iter.current_data() == val);
}

void json_database_test::test_basic_iteration()
{
    std::string schema = "[ { \"table_name\": \"segments\", \"index_columns\": [ \"time\" ] } ]";

    json_database::create_database( "test.db", 16 * (1024*1024), schema );

    json_database db( "test.db" );

    string val1, val2, val3, val4, val5, val6, val7;

    db.transaction([&](trans_state& ts) {
        val1 = "{ \"time\": \"100\" }";
        db.insert_json( ts, "segments", val1);

        val2 = "{ \"time\": \"200\" }";
        db.insert_json( ts, "segments", val2);

        val3 = "{ \"time\": \"300\" }";
        db.insert_json( ts, "segments", val3);

        val4 = "{ \"time\": \"400\" }";
        db.insert_json( ts, "segments", val4);

        val5 = "{ \"time\": \"500\" }";
        db.insert_json( ts, "segments", val5);

        val6 = "{ \"time\": \"600\" }";
        db.insert_json( ts, "segments", val6);

        val7 = "{ \"time\": \"700\" }";
        db.insert_json( ts, "segments", val7);
    });

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

void json_database_test::test_primary_key_iteration()
{
    std::string schema = "[ { \"table_name\": \"segments\", \"index_columns\": [ \"time\" ] } ]";

    json_database::create_database( "test.db", 16 * (1024*1024), schema );

    json_database db( "test.db" );

    string val1, val2, val3, val4, val5, val6, val7, pk1, pk2, pk3, pk4, pk5, pk6, pk7;

    db.transaction([&](trans_state& ts) {

        val1 = "{ \"time\": \"100\" }";
        pk1 = db.insert_json( ts, "segments", val1);

        val2 = "{ \"time\": \"200\" }";
        pk2 = db.insert_json( ts, "segments", val2);

        val3 = "{ \"time\": \"300\" }";
        pk3 = db.insert_json( ts, "segments", val3);

        val4 = "{ \"time\": \"400\" }";
        pk4 = db.insert_json( ts, "segments", val4);

        val5 = "{ \"time\": \"500\" }";
        pk5 = db.insert_json( ts, "segments", val5);

        val6 = "{ \"time\": \"600\" }";
        pk6 = db.insert_json( ts, "segments", val6);

        val7 = "{ \"time\": \"700\" }";
        pk7 = db.insert_json( ts, "segments", val7);
    });

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
void json_database_test::test_multiple_indexes()
{
    std::string schema = "[ { \"table_name\": \"segments\", \"index_columns\": [ \"time\", \"index\" ] } ]";

    json_database::create_database( "test.db", 16 * (1024*1024), schema );

    json_database db( "test.db" );

    string val1, val2, val3, val4, val5, val6, val7, pk1, pk2, pk3, pk4, pk5, pk6, pk7;

    db.transaction([&](trans_state& ts) {
        val1 = "{ \"time\": \"100\", \"index\": \"7\" }";
        pk1 = db.insert_json( ts, "segments", val1);

        val2 = "{ \"time\": \"200\", \"index\": \"6\" }";
        pk2 = db.insert_json( ts, "segments", val2);

        val3 = "{ \"time\": \"300\", \"index\": \"5\" }";
        pk3 = db.insert_json( ts, "segments", val3);

        val4 = "{ \"time\": \"400\", \"index\": \"4\" }";
        pk4 = db.insert_json( ts, "segments", val4);

        val5 = "{ \"time\": \"500\", \"index\": \"3\" }";
        pk5 = db.insert_json( ts, "segments", val5);

        val6 = "{ \"time\": \"600\", \"index\": \"2\" }";
        pk6 = db.insert_json( ts, "segments", val6);

        val7 = "{ \"time\": \"700\", \"index\": \"1\" }";
        pk7 = db.insert_json( ts, "segments", val7);
    });

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

void json_database_test::test_swmr()
{
    std::string schema = "[ { \"table_name\": \"segments\", \"index_columns\": [ \"foo\" ] } ]";

    json_database::create_database( "test.db", 16 * (1024*1024), schema );

    json_database db( "test.db" );

    bool writerRunning = true;
    uint32_t writerIndex = 0;

    thread wt([&](){
        while( writerRunning )
        {
            db.transaction([&](trans_state& ts) {
                string val = "{ \"foo\": \"" + to_string(writerIndex) + "\" }";
                db.insert_json( ts, "segments", val );
            });
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

void json_database_test::test_mwmr()
{
    std::string schema = "[ { \"table_name\": \"segment_files\", \"index_columns\": [ \"start_time\", \"end_time\" ] } ]";

    json_database::create_database( "test.db", 16 * (1024*1024), schema );

    json_database db( "test.db" );

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
                db.transaction([&](trans_state& ts) {
                    string val = "{ \"start_time\": \"" + to_string(wc.start_time) + "\", \"end_time\": \"" + to_string(wc.start_time) + "\" }";
                    db.insert_json( ts, "segment_files", val );
                });
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

void json_database_test::test_scenario()
{
}

void json_database_test::test_compound_indexes()
{
    std::string schema = "[ { \"table_name\": \"segments\", \"compound_indexes\": [ [ \"index\", \"time\" ] ] } ]";

    json_database::create_database( "test.db", 16 * (1024*1024), schema );

    json_database db( "test.db" );

    string val1, val2, val3, pk1, pk2, pk3;

    db.transaction([&](trans_state& ts) {
        val1 = "{ \"time\": \"100\", \"index\": \"7\" }";
        pk1 = db.insert_json( ts, "segments", val1);

        val2 = "{ \"time\": \"200\", \"index\": \"7\" }";
        pk2 = db.insert_json( ts, "segments", val2);

        val3 = "{ \"time\": \"300\", \"index\": \"8\" }";
        pk3 = db.insert_json( ts, "segments", val3);
    });

    auto ci = db.get_iterator("segments", vector<string>{ "index", "time" });

    ci.find(vector<string>{"7", "200"});

    auto row = ci.current_data();

    UT_ASSERT(row == val2);
}
