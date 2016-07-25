# Tables

**_Tables is BETA software and I expect to evolve the API a bit more before it stabilizes. Use at your own risk_**

Tables is a C++11 based wrapper for the awesome LMDB embedded database. Tables adds a very thin veneer of the relational database model on top of lmdb:

```c++
map<string,list<string>> schema = { make_pair( "quotes", list<string>{ "page" } ) };

database::create_database( "test.db", 16 * (1024*1024), schema );
```

The above code creates a 16 MB database file called "test.db" that consists of a single table called "quotes" with a single indexable (searchable) column called "page". Since the internal format of the blobs you will be inserting into this table are unknown to tables, at insertion time you must provide a callback that returns to Tables the value of each indexable column for the row being inserted.

```c++
database db( "test.db" );

string val1 = "Stick him with the pointy end.";
db.insert( "quotes", (uint8_t*)val1.c_str(), val1.length(), []( string colName, const uint8_t* src, size_t size ) {
    return "100";
} );

string val2 = "The north remembers.";
db.insert( "quotes", (uint8_t*)val1.c_str(), val1.length(), []( string colName, const uint8_t* src, size_t size ) {
    return "500";
} );

```

The first argument to database::insert() is the name of the table you want to insert your blob into. Then you provide a pointer to you blob and its size. Finally you provide your index callback. The index callback will be called once for every index specified in the schema for this table (in this case once). The callback is called with the index column name and the pointer and size of the blob. It is the responsibility of the callback to return a value (from the row) for the requested index.

Querying data is done by requesting an interator for a particular table and index. The iterator can then be incremented and decremented through the rows.

```c++
    auto iter = db.get_iterator( "quotes", "page" );
    iter.find( "100" );
    auto res = iter.current_data();
    string foundVal( (char*)res.second, res.first );
```

# Building
Don't forget the --recursive option when cloning this repository! Other than that Tables is a standard cmake project and is built in the usual fashion.

```bash
git clone --recursive https://github.com/dicroce/tables
mkdir -p tables/build
cd tables/build
cmake ..
make
make install
```

# TODO
    - Composite Keys
        - Change index to be combined index.
        - Implement custom compare function (and activate via mdb_set_compare()) that nulls out parts of incoming keys
          not included in this iterator.
        - Change iterator find() API to support multiple indexes.
