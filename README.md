# tables

Tables is a C++11 based wrapper for the awesome LMDB embedded database. Tables adds a very thin veneer of the relational database model on top of lmdb:

```c++
map<string,list<string>> schema = { make_pair( "quotes", list<string>{ "page" } ) };

database::create_database( "test.db", 16 * (1024*1024), schema );
```

The above code creates a 16 MB database file called "test.db" that consists of a single table called "quotes" with a single indexable (searchable) column called "page". Since the internal format of the blobs you will be inserting into this table are unknown to tables, at insertion time you must provide a callback that returns to Tables the value of each indexable column for the row being inserted.

```c++
database db( "test.db" );

struct quotes
{
  string words;
  int page;
};

//vector<quotes> q = { }

string val1 = "Stick him with the pointy end.";
db.insert( "segments", (uint8_t*)val1.c_str(), val1.length(), []( string colName, const uint8_t* src, size_t size ) {
    return "100";
} );

string val2 = "The north remembers.";
db.insert( "segments", (uint8_t*)val1.c_str(), val1.length(), []( string colName, const uint8_t* src, size_t size ) {
    return "100";
} );

```
