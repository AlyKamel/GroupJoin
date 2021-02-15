#ifndef SMALLGJ_H
#define SMALLGJ_H

#include "basics.hpp"
#include "aggfuncs.hpp"

#include <tsl/robin_map.h>
#include <algorithm>
#include <functional>

namespace
{
    template <typename Key, typename Total, typename Hash, typename KeyEqual>
    using HashTable = tsl::robin_map<Key, Total, Hash, KeyEqual>;
}

/**
    Performs a <-GroupJoin by sorting both inputs first and then merging them.
    @param L left operand of the GroupJoin
    @param R right operand of the GroupJoin
    @param agg_struct aggregate function used for the calculation
    @param key_less function that returns true if the first operand is smaller than the second 
    operand, defaults to std::less
    @tparam Total type of the intermediate result of the aggregate function
    @tparam S type of the final result of the aggregate function
    @tparam Key type of the key values of L and R
    @tparam LRestValue type of the rest value of L
    @tparam RRestValue type of the rest value in R
*/
template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue, typename KeyLess = std::less<Key>>
GJResult_type<Key, LRestValue, S> sortMergeLess(L_type<Key, LRestValue>& L, R_type<Key, RRestValue>& R, const BasicAgg<Total, S, Key, RRestValue> &agg_struct, const KeyLess &key_less = KeyLess())
{
    typedef Row<Key, LRestValue> RowL;
    typedef Row<Key, RRestValue> RowR;
    typedef GJResult_type<Key, LRestValue, S> GJResult;

    auto rStart = R.begin();
    const auto &rEnd = R.end();
    
    std::sort(L.begin(), L.end(), [&](const RowL &r1, const RowL &r2) { return key_less(r2.key, r1.key); });
    std::sort(rStart, rEnd, [&](const RowR &r1, const RowR &r2) { return key_less(r2.key, r1.key); });

    GJResult rvec;
    rvec.reserve(L.size());
    Total total = {};
    for (const RowL &r : L)
    {
        while (rStart != rEnd && key_less(r.key, rStart->key))
            agg_struct.agg(total, *(rStart++));
        rvec.emplace_back(r, agg_struct.calc_final(total));
    }
    return rvec;
}

template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue, typename KeyLess = std::less<Key>>
void sortMergeLess(
    typename L_type<Key, LRestValue>::iterator lStart,
    const typename L_type<Key, LRestValue>::iterator& lEnd,
    typename R_type<Key, RRestValue>::iterator rStart,
    const typename R_type<Key, RRestValue>::iterator &rEnd,
    typename GJResult_type<Key, LRestValue, S>::iterator res,
    Total total,
    const BasicAgg<Total, S, Key, RRestValue> &agg_struct,
    const KeyLess &key_less = KeyLess())
{
    typedef Row<Key, LRestValue> RowL;
    typedef Row<Key, RRestValue> RowR;

    std::sort(lStart, lEnd, [&](const RowL &r1, const RowL &r2) { return key_less(r2.key, r1.key); });
    std::sort(rStart, rEnd, [&](const RowR &r1, const RowR &r2) { return key_less(r2.key, r1.key); });

    for (; lStart != lEnd; ++lStart, ++res)
    {
        while (rStart != rEnd && key_less(lStart->key, rStart->key))
            agg_struct.agg(total, *(rStart++));
        *res = {*lStart, agg_struct.calc_final(total)};
    }
}

/**
    Performs a <-GroupJoin by hashing the left input.
    @param L left operand of the GroupJoin
    @param R right operand of the GroupJoin
    @param agg_struct aggregate function used for the calculation
    @param key_less function that returns true if the first operand is smaller than the second 
    operand, defaults to std::less
    @param hash hash function used for building/probing the hash table, defaults to std::hash
    @param key_equal function to check for equality of keys, defaults to std::equal_to
    @tparam Total type of the intermediate result of the aggregate function
    @tparam S type of the final result of the aggregate function
    @tparam Key type of the key values of L and R
    @tparam LRestValue type of the rest value of L
    @tparam RRestValue type of the rest value in R
*/
template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue, typename KeyLess = std::less<Key>, typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
GJResult_type<Key, LRestValue, S> hashLess(L_type<Key, LRestValue> &L, const R_type<Key, RRestValue> &R, const CombineAgg<Total, S, Key, RRestValue> &agg_struct, const KeyLess &key_less = KeyLess(), const Hash &hash = Hash(), const KeyEqual &key_equal = KeyEqual())
{
    typedef Row<Key, LRestValue> RowL;
    typedef Row<Key, RRestValue> RowR;
    typedef GJResult_type<Key, LRestValue, S> GJResult;
    typedef HashTable<Key, Total, Hash, KeyEqual> HT;

    std::sort(L.begin(), L.end(), [&](const RowL &r1, const RowL &r2) { return key_less(r1.key, r2.key); });
    HT ht(L.size(), hash, key_equal);
    for (const RowL &r : L)
        ht.insert({r.key, Total{}});

    Key minKey = L.front().key;
    for (const RowR &r : R)
    {
        if (key_less(minKey, r.key)) // check if row has any join partners
        {
            auto it = --std::lower_bound(L.begin(), L.end(), r.key, [&](const RowL &rl, Key k) { return key_less(rl.key, k); }); // biggest element < tb.b
            agg_struct.agg(ht.find(it->key).value(), r);
        }
    }

    Total total = {};
    GJResult rvec;
    rvec.reserve(L.size());

    // first step
    Key prevKey = L.back().key;
    agg_struct.combine(total, ht.find(prevKey)->second);

    for (auto it = L.rbegin(); it != L.rend(); ++it)
    {
        if (key_less(it->key, prevKey)) // avoid combining the same subtotal more than once
        {
            prevKey = it->key;
            agg_struct.combine(total, ht.find(prevKey)->second);
        }
        rvec.emplace_back(*it, agg_struct.calc_final(total));
    }

    return rvec;
}

#endif