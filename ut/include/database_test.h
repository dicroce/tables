
#include "framework.h"

class database_test : public test_fixture
{
public:
    TEST_SUITE(database_test);
        TEST(database_test::test_create);
        TEST(database_test::test_basic_insert);
        TEST(database_test::test_basic_iteration);
        TEST(database_test::test_multiple_indexes);
        TEST(database_test::test_swmr);
        TEST(database_test::test_mwmr);
        TEST(database_test::test_primary_key_iteration);
    TEST_SUITE_END();

    virtual ~database_test() throw() {}

    void setup();
    void teardown();

    void test_create();
    void test_basic_insert();
    void test_basic_iteration();
    void test_multiple_indexes();
    void test_swmr();
    void test_mwmr();
    void test_primary_key_iteration();
};
