#include "eqgj.hpp"
#include "smallgj.hpp"
#include "uneqgj.hpp"
#include "altgj.hpp"
#include "paragj.hpp"
#include "tests.hpp"

#include "basics.hpp"
#include "aggfuncs.hpp"
#include "util.hpp"

#include <algorithm>

using namespace parajoin;
typedef RowResult<int, int, int> RowRes;

void testEqGJ(uint l_size, uint r_size, uint sel_fac)
{
    // Relation creation
    std::vector<int> val_pool = createValPool(sel_fac);
    IntRel L = createRel(l_size, val_pool);
    IntRel R = createRel(r_size, val_pool);

    auto res = nested(L, R, SumNAgg<int>());
    std::sort(res.begin(), res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });

    // start testing
    auto test_res = hashEq(L, R, SumNAgg<int>());
    std::sort(test_res.begin(), test_res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });
    assert(res == test_res && "Test for hashEq failed");

    test_res = groupLEq(L, R, SumNAgg<int>());
    std::sort(test_res.begin(), test_res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });
    assert(res == test_res && "Test for groupLEq failed");

    test_res = groupREq(L, R, SumNAgg<int>());
    std::sort(test_res.begin(), test_res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });
    assert(res == test_res && "Test for groupREq failed");

    test_res = groupLREq(L, R, SumNAgg<int>());
    std::sort(test_res.begin(), test_res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });
    assert(res == test_res && "Test for groupLREq failed");

    test_res = sortMergeEq(L, R, SumNAgg<int>());
    std::sort(test_res.begin(), test_res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });
    assert(res == test_res && "Test for mergeEq failed");

    test_res = prtLREqSimple(L, R, SumNAgg<int>());
    std::sort(test_res.begin(), test_res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });
    assert(res == test_res && "Test for prtLREqSimple failed");

    test_res = prtLREq(L, R, SumNAgg<int>());
    std::sort(test_res.begin(), test_res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });
    assert(res == test_res && "Test for prtLREq failed");
}

void testUniqueEqGJ(uint l_size, uint r_size, uint sel_fac)
{
    // Relation creation
    std::vector<int> val_pool = createUniqueValPool(sel_fac);
    IntRel L = createUniqueRel(l_size);
    IntRel R = createRel(r_size, val_pool);

    auto res = nested(L, R, SumNAgg<int>());
    std::sort(res.begin(), res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key; });

    // start testing
    auto test_res = hashUniqueEq(L, R, SumNAgg<int>());
    std::sort(test_res.begin(), test_res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key; });
    assert(res == test_res && "Test for hashUniqueEq failed");
}

void testUneqGJ(uint l_size, uint r_size, uint sel_fac)
{
    // Relation creation
    std::vector<int> val_pool = createValPool(sel_fac);
    IntRel L = createRel(l_size, val_pool);
    IntRel R = createRel(r_size, val_pool);

    auto res = nested(L, R, SumNAgg<int>(), std::not_equal_to<int>());
    std::sort(res.begin(), res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });

    // start testing
    auto test_res = groupLUneq(L, R, SumNAgg<int>());
    std::sort(test_res.begin(), test_res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });
    assert(res == test_res && "Test for groupLUneq failed");

    test_res = groupRUneq(L, R, SumNAgg<int>());
    std::sort(test_res.begin(), test_res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });
    assert(res == test_res && "Test for groupRUneq failed");

    test_res = groupLRUneq(L, R, SumNAgg<int>());
    std::sort(test_res.begin(), test_res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });
    assert(res == test_res && "Test for groupLRUneq failed");

    test_res = sortMergeUneq(L, R, SumNAgg<int>());
    std::sort(test_res.begin(), test_res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });
    assert(res == test_res && "Test for sortMergeUneq failed");

    test_res = prtLRUneqSimple(L, R, SumNAgg<int>());
    std::sort(test_res.begin(), test_res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });
    assert(res == test_res && "Test for prtLRUneqSimple failed");

    test_res = prtLRUneq(L, R, SumNAgg<int>());
    std::sort(test_res.begin(), test_res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });
    assert(res == test_res && "Test for prtLRUneq failed");
}

void testSmallGJ(uint l_size, uint r_size, uint sel_fac)
{
    // Relation creation
    std::vector<int> val_pool = createValPool(sel_fac);
    IntRel L = createRel(l_size, val_pool);
    IntRel R = createRel(r_size, val_pool);

    auto res = nested(L, R, SumNAgg<int>(), std::less<int>());
    std::sort(res.begin(), res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });

    // start testing
    auto test_res = sortMergeLess(L, R, SumNAgg<int>());
    std::sort(test_res.begin(), test_res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });
    assert(res == test_res && "Test for sortMergeLess failed");

    test_res = prtLRLessSimple(L, R, SumNAgg<int>());
    std::sort(test_res.begin(), test_res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });
    assert(res == test_res && "Test for prtLRLessSimple failed");

    test_res = prtLRLess(L, R, SumNAgg<int>());
    std::sort(test_res.begin(), test_res.end(), [](const RowRes &t1, const RowRes &t2) { return t1.first.key < t2.first.key || (t1.first.key == t2.first.key && t1.first.other < t2.first.other); });
    assert(res == test_res && "Test for prtLRLess failed");
}
