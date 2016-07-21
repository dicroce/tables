
#include "database_test.h"
#include "tables2/database.h"
#include <algorithm>
#include <thread>
#include <mutex>

using namespace std;
using namespace tables;

REGISTER_TEST_FIXTURE(database_test);

void database_test::setup()
{
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
    map<string,list<string>> schema = {
        make_pair( "segment_files", list<string>{ "time", "foo" } ),
        make_pair( "segments", list<string>{ "start", "end", "sdp" } )
    };

    database::create_database( "test.db", 16 * (1024*1024), schema );

    UT_ASSERT( ut_file_exists( "test.db" ) );

    database db( "test.db" );
    UT_ASSERT( db._version = 1 );
    UT_ASSERT( db._schema.size() == 2 );
    UT_ASSERT( db._schema["segment_files"].size() == 2 );
    auto found = std::find( db._schema["segment_files"].begin(), db._schema["segment_files"].end(), "time" );
    UT_ASSERT( found != db._schema["segment_files"].end() );
    found = std::find( db._schema["segment_files"].begin(), db._schema["segment_files"].end(), "foo" );
    UT_ASSERT( found != db._schema["segment_files"].end() );

    UT_ASSERT( db._schema["segments"].size() == 3 );
    found = std::find( db._schema["segments"].begin(), db._schema["segments"].end(), "start" );
    UT_ASSERT( found != db._schema["segments"].end() );
    found = std::find( db._schema["segments"].begin(), db._schema["segments"].end(), "end" );
    UT_ASSERT( found != db._schema["segments"].end() );
    found = std::find( db._schema["segments"].begin(), db._schema["segments"].end(), "sdp" );
    UT_ASSERT( found != db._schema["segments"].end() );
}

void database_test::test_basic_insert()
{
    map<string,list<string>> schema = {
        make_pair( "segments", list<string>{ "time" } )
    };

    database::create_database( "test.db", 16 * (1024*1024), schema );

    database db( "test.db" );
    string val = "stick him with the pointy end";
    db.insert( "segments", (uint8_t*)val.c_str(), val.length(), []( string colName, const uint8_t* src, size_t size ) {
        UT_ASSERT( colName == "time" );
        return "1234";
    } );

    auto iter = db.get_iterator( "segments", "time" );
    iter.find( "1234" );
    UT_ASSERT( iter.valid() );
    auto res = iter.current_data();
    string foundVal( (char*)res.second, res.first );
    UT_ASSERT( foundVal == val );
}

void database_test::test_basic_iteration()
{
    map<string,list<string>> schema = {
        make_pair( "segments", list<string>{ "time" } )
    };

    database::create_database( "test.db", 16 * (1024*1024), schema );

    database db( "test.db" );

    string val1 = "Stick him with the pointy end.";
    db.insert( "segments", (uint8_t*)val1.c_str(), val1.length(), []( string colName, const uint8_t* src, size_t size ) {
        UT_ASSERT( colName == "time" );
        return "100";
    } );

    string val2 = "Money buys a mans silence for a time. A bolt in the heart buys it forever.";
    db.insert( "segments", (uint8_t*)val2.c_str(), val2.length(), []( string colName, const uint8_t* src, size_t size ) {
        UT_ASSERT( colName == "time" );
        return "200";
    } );

    string val3 = "Some old wounds never truly heal, and bleed again at the slightest word.";
    db.insert( "segments", (uint8_t*)val3.c_str(), val3.length(), []( string colName, const uint8_t* src, size_t size ) {
        UT_ASSERT( colName == "time" );
        return "300";
    } );

    string val4 = "The lord of light wants his enemies burned. The drowned got wants them drowned. Why are all the Gods such vicious cunts? Where is the god of tits and wine?";
    db.insert( "segments", (uint8_t*)val4.c_str(), val4.length(), []( string colName, const uint8_t* src, size_t size ) {
        UT_ASSERT( colName == "time" );
        return "400";
    } );

    string val5 = "A mind needs books just like a sword needs whetstone.";
    db.insert( "segments", (uint8_t*)val5.c_str(), val5.length(), []( string colName, const uint8_t* src, size_t size ) {
        UT_ASSERT( colName == "time" );
        return "500";
    } );

    auto iter = db.get_iterator( "segments", "time" );
    iter.find( "100" );
    UT_ASSERT( iter.valid() );
    auto res = iter.current_data();
    string foundVal( (char*)res.second, res.first );
    UT_ASSERT( foundVal == val1 );

    iter.next();
    UT_ASSERT( iter.valid() );
    res = iter.current_data();
    foundVal = string( (char*)res.second, res.first );
    UT_ASSERT( foundVal == val2 );

    iter.next();
    UT_ASSERT( iter.valid() );
    res = iter.current_data();
    foundVal = string( (char*)res.second, res.first );
    UT_ASSERT( foundVal == val3 );

    iter.next();
    UT_ASSERT( iter.valid() );
    res = iter.current_data();
    foundVal = string( (char*)res.second, res.first );
    UT_ASSERT( foundVal == val4 );

    iter.next();
    UT_ASSERT( iter.valid() );
    res = iter.current_data();
    foundVal = string( (char*)res.second, res.first );
    UT_ASSERT( foundVal == val5 );

    iter.next();
    UT_ASSERT( iter.valid() == false );

    iter.find( "100" );
    UT_ASSERT( iter.valid() == true );
    res = iter.current_data();
    foundVal = string( (char*)res.second, res.first );
    UT_ASSERT( foundVal == val1 );

    iter.prev();
    UT_ASSERT( iter.valid() == false );
}

void database_test::test_multiple_indexes()
{
    map<string,list<string>> schema = {
        make_pair( "segments", list<string>{ "foo", "bar" } )
    };

    database::create_database( "test.db", 16 * (1024*1024), schema );

    database db( "test.db" );

    string val1 = "Stick him with the pointy end.";
    db.insert( "segments", (uint8_t*)val1.c_str(), val1.length(), []( string colName, const uint8_t* src, size_t size ) {
        if( colName == "foo" )
            return "100";
        else return "A";
    } );

    string val2 = "Money buys a mans silence for a time. A bolt in the heart buys it forever.";
    db.insert( "segments", (uint8_t*)val2.c_str(), val2.length(), []( string colName, const uint8_t* src, size_t size ) {
        if( colName == "foo" )
            return "200";
        else return "B";
    } );

    string val3 = "Some old wounds never truly heal, and bleed again at the slightest word.";
    db.insert( "segments", (uint8_t*)val3.c_str(), val3.length(), []( string colName, const uint8_t* src, size_t size ) {
        if( colName == "foo" )
            return "300";
        else return "C";
    } );

    string val4 = "The lord of light wants his enemies burned. The drowned got wants them drowned. Why are all the Gods such vicious cunts? Where is the god of tits and wine?";
    db.insert( "segments", (uint8_t*)val4.c_str(), val4.length(), []( string colName, const uint8_t* src, size_t size ) {
        if( colName == "foo" )
            return "400";
        else return "D";
    } );

    string val5 = "A mind needs books just like a sword needs whetstone.";
    db.insert( "segments", (uint8_t*)val5.c_str(), val5.length(), []( string colName, const uint8_t* src, size_t size ) {
        if( colName == "foo" )
            return "500";
        else return "E";
    } );

    {
        auto fooIter = db.get_iterator( "segments", "foo" );
        fooIter.find( "100" );
        UT_ASSERT( fooIter.valid() );
        auto res = fooIter.current_data();
        string foundVal( (char*)res.second, res.first );
        UT_ASSERT( foundVal == val1 );
    }

    {
        auto barIter = db.get_iterator( "segments", "bar" );
        barIter.find( "D" );
        UT_ASSERT( barIter.valid() );
        auto res = barIter.current_data();
        string foundVal( (char*)res.second, res.first );
        UT_ASSERT( foundVal == val4 );
    }
}

void database_test::test_swmr()
{
    map<string,list<string>> schema = {
        make_pair( "segments", list<string>{ "foo" } )
    };

    database::create_database( "test.db", 16 * (1024*1024), schema );

    database db( "test.db" );

    bool writerRunning = true;
    uint32_t writerIndex = 0;

    thread wt([&](){
        while( writerRunning )
        {
            string val = ut_uuid_generate();
            db.insert( "segments", (uint8_t*)val.c_str(), val.length(), [writerIndex]( string colName, const uint8_t* src, size_t size ) {
               return to_string(writerIndex);
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

void database_test::test_mwmr()
{
    map<string,list<string>> schema = {
        make_pair( "segment_files", list<string>{ "start_time", "end_time" } )
    };

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
                string val = ut_uuid_generate();
                db.insert( "segment_files", (uint8_t*)val.c_str(), val.length(), [&]( string colName, const uint8_t* src, size_t size ) {
                   return to_string(wc.start_time);
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
