#ifndef UTIL_H
#define UTIL_H

#include "basics.hpp"
#include "aggfuncs.hpp"

#include <tsl/robin_map.h>
#include <tbb/tbb.h>

#include <algorithm>
#include <unordered_set>

template <typename K = int, typename OA = int>
bool isLKeyUnique(const Rel<K, OA> &L)
{
    typedef tsl::robin_map<K, int> HMap;
    HMap hmap(L.size());

    for (const auto &r : L)
    {
        if (hmap.count(r.key) != 0)
            return false;
        hmap[r.key];
    }
    return true;
}

template <typename K = int, typename OA = int, typename OB = int>
double calcEqGJSelectivity(const Rel<K, OA> &L, const Rel<K, OB> &R)
{
    typedef unsigned long ulong;

    typedef tsl::robin_map<K, ulong> HMap;
    HMap hmap(L.size());

    for (const auto &r : L)
        ++hmap[r.key];

    auto end = hmap.end();
    auto count = tbb::parallel_reduce(
        tbb::blocked_range<int>(0, R.size()),
        (ulong) 0,
        [&](tbb::blocked_range<int> r, ulong total) {
            for (int i = r.begin(); i != r.end(); ++i)
            {
                auto it = hmap.find(R[i].key);
                if (it != end)
                    total += it->second;
            }
            return total;
        },
        std::plus<ulong>());
    return (double) count / L.size(); // chance for a rowA to find 1 partner
}

template <typename K, typename OA, typename OB>
double calcLessGJSelectivity(const Rel<K, OA> &L, const Rel<K, OB> &R)
{
    const long max_count = L.size() * R.size();
    long total = 0;

    std::sort(L.begin(), L.end(), [](const Row<K, OA> &ra1, const Row<K, OA> &ra2) { return ra1.key < ra2.key; });

    for (const auto &rb : R)
    {
        auto raIt = std::lower_bound(L.begin(), L.end(), rb.key, [](const Row<K, OA> &ra, const K b) { return ra.key < b; });
        total += raIt - L.begin(); // remove number of rows > tb.b
    }

    return (double) total / max_count;
}

template <typename K, typename OA, typename OB>
double calcUneqGJSelectivity(const Rel<K, OA> &L, const Rel<K, OB> &R)
{
    const long max_count = L.size() * R.size();
    long total = max_count;

    typedef std::unordered_multiset<K> HMap;
    HMap hmap(L.size());

    for (const auto &ra : L)
        hmap.emplace(ra.key);

    for (const auto &rb : R)
        total -= hmap.count(rb.key);

    return (double) total / max_count;
}

std::vector<int> createValPool(uint size);
std::vector<int> createUniqueValPool(uint size);
std::vector<Row<int, int>> createRel(uint rel_size, const std::vector<int> &val_pool);
std::vector<Row<int, int>> createUniqueRel(uint rel_size);

#endif