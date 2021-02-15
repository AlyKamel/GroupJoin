#include "aggfuncs.hpp"

#include "basics.hpp"
#include "tsl/robin_map.h"
#include "util.hpp"

#include <vector>

std::vector<int> createValPool(uint size)
{
    std::vector<int> val_pool;
    val_pool.reserve(size);
    for (uint i = 0; i != size; ++i)
        val_pool.push_back(rand());
    return val_pool;
}

std::vector<int> createUniqueValPool(uint size)
{
    std::vector<int> val_pool;
    val_pool.reserve(size);
    for (uint i = 0; i != size; ++i)
        val_pool.push_back(i);
    return val_pool;
}

std::vector<Row<int, int>> createRel(uint rel_size, const std::vector<int> &val_pool)
{
    const uint pool_size = val_pool.size();
    std::vector<Row<int, int>> rel;
    rel.reserve(rel_size);
    for (uint i = 0; i != rel_size; ++i)
        rel.emplace_back(val_pool[rand() % pool_size], i);
    return rel;
}

std::vector<Row<int, int>> createUniqueRel(uint rel_size)
{
    std::vector<Row<int, int>> rel;
    rel.reserve(rel_size);
    for (uint i = 0; i != rel_size; ++i)
        rel.emplace_back(i, i);
    return rel;
}
