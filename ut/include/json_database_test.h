
#include "framework.h"

class json_database_test : public test_fixture
{
public:
    TEST_SUITE(json_database_test);
        TEST(json_database_test::test_create);
        TEST(json_database_test::test_basic_insert);
        TEST(json_database_test::test_basic_iteration);
        TEST(json_database_test::test_multiple_indexes);
        TEST(json_database_test::test_swmr);
        TEST(json_database_test::test_mwmr);
        TEST(json_database_test::test_primary_key_iteration);
        TEST(json_database_test::test_scenario);
        TEST(json_database_test::test_compound_indexes);
    TEST_SUITE_END();

    virtual ~json_database_test() throw() {}

    void setup();
    void teardown();

    void test_create();
    void test_basic_insert();
    void test_basic_iteration();
    void test_multiple_indexes();
    void test_swmr();
    void test_mwmr();
    void test_primary_key_iteration();
    void test_scenario();
    void test_compound_indexes();
};
