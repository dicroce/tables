
#include "framework.h"

class lmdb_test : public test_fixture
{
public:
    TEST_SUITE(lmdb_test);
        TEST(lmdb_test::test_basic);
        TEST(lmdb_test::test_basic_cursor);
        TEST(lmdb_test::test_full_db);
        TEST(lmdb_test::test_close_cursor);
    TEST_SUITE_END();

    virtual ~lmdb_test() throw() {}

    void setup();
    void teardown();

    void test_basic();
    void test_basic_cursor();
    void test_full_db();
    void test_close_cursor();
};
