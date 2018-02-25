
#include "tables/utils.h"
#include <cstdarg>

using namespace tables;
using namespace std;

string tables::format(const char* fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    const string result = format(fmt, args);
    va_end(args);
    return result;
}

string tables::format(const char* fmt, va_list& args)
{
    va_list newargs;
    va_copy(newargs, args);

    int chars_written = vsnprintf(NULL, 0, fmt, newargs);
    int len = chars_written + 1;

    vector<char> str(len);

    va_copy(newargs, args);
    vsnprintf(&str[0], len, fmt, newargs);

    return string(&str[0]);
}

vector<string> tables::split(const string& str, char delim)
{
    return split(str, string(&delim, 1));
}

vector<string> tables::split(const string& str, const string& delim)
{
    vector<string> parts;

    size_t begin = 0;
    size_t end = 0;

    auto delimLen = delim.length();

    while(true)
    {
        end = str.find(delim, begin);

        if(end == string::npos)
        {
            if(str.begin()+begin != str.end())
                parts.emplace_back(str.begin()+begin, str.end());
            break;
        }

        if(end != begin)
            parts.emplace_back(str.begin()+begin, str.begin()+end);

        begin = end + delimLen;
    }

    return parts;
}

uint64_t tables::s_to_uint64(const string& s)
{
    uint64_t val;
    sscanf(s.c_str(), "%lu", &val);
    return val;
}

string tables::uint64_to_s(uint64_t val)
{
    return format("%lu", val);
}

pair<string, string> tables::_getByKey(MDB_cursor* cursor, const string& key)
{
    MDB_val shimKey, shimVal;

    shimKey.mv_size = key.length();
    shimKey.mv_data = const_cast<char*>(key.c_str());

    if(mdb_cursor_get(cursor, &shimKey, &shimVal, MDB_SET) != 0)
        throw runtime_error(("Unable to locate key: %s",key.c_str()));

    return make_pair(key, string((char*)shimVal.mv_data, shimVal.mv_size));
}

void tables::_putByKey(MDB_txn* txn, MDB_dbi& dbi, const string& key, const string& val)
{
    MDB_val keyShim;
    keyShim.mv_size = key.length();
    keyShim.mv_data = const_cast<char*>(key.c_str());

    MDB_val valShim;
    valShim.mv_size = val.length();
    valShim.mv_data = const_cast<char*>(val.c_str());

    if(mdb_put(txn, dbi, &keyShim, &valShim, 0) != 0)
        throw runtime_error(("Unable to mdb_put() " + key));
}

void tables::_putByKey(MDB_txn* txn, MDB_dbi& dbi, const string& key, const uint8_t* src, size_t size)
{
    MDB_val keyShim;
    keyShim.mv_size = key.length();
    keyShim.mv_data = const_cast<char*>(key.c_str());

    MDB_val valShim;
    valShim.mv_size = size;
    valShim.mv_data = const_cast<uint8_t*>(src);

    if(mdb_put(txn, dbi, &keyShim, &valShim, 0) != 0)
        throw runtime_error(("Unable to mdb_put() " + key));
}

void tables::_removeByKey(MDB_txn* txn, MDB_dbi& dbi, const string& key)
{
    MDB_val keyShim;
    keyShim.mv_size = key.length();
    keyShim.mv_data = const_cast<char*>(key.c_str());

    if(mdb_del(txn, dbi, &keyShim, NULL) != 0)
        throw runtime_error(("Unable to mdb_del() " + key));
}
