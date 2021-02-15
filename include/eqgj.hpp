#ifndef EQGJ_H
#define EQGJ_H

#include "basics.hpp"
#include "aggfuncs.hpp"

#include <tsl/robin_map.h>
#include <algorithm>

namespace
{
    template <typename Key, typename Total, typename Hash, typename KeyEqual>
    using HashTable = tsl::robin_map<Key, Total, Hash, KeyEqual>;
}

/// hash based approaches

/**
    Performs a =-GroupJoin by hashing the left input.
    @param L left operand of the GroupJoin
    @param R right operand of the GroupJoin
    @param agg_struct aggregate function used for the calculation
    @param hash hash function used for building/probing the hash table, defaults to std::hash
    @param key_equal function to check for equality of keys, defaults to std::equal_to
    @tparam Total type of the intermediate result of the aggregate function
    @tparam S type of the final result of the aggregate function
    @tparam Key type of the key values of L and R
    @tparam LRestValue type of the rest value of L
    @tparam RRestValue type of the rest value in R
*/
template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue, 
    typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
GJResult_type<Key, LRestValue, S> groupLEq(const L_type<Key, LRestValue> &L, 
    const R_type<Key, RRestValue> &R, const BasicAgg<Total, S, Key, RRestValue> &agg_struct,
    const Hash &hash = Hash(), const KeyEqual &key_equal = KeyEqual())
{
    typedef Row<Key, LRestValue> RowL;
    typedef Row<Key, RRestValue> RowR;
    typedef GJResult_type<Key, LRestValue, S> GJResult;
    typedef HashTable<Key, Total, Hash, KeyEqual> HT;

    // build the hash table with L
    HT ht(L.size(), hash, key_equal);
    for (const RowL &r : L)
        ht.insert({r.key, Total{}}); // initialize tuples with base value

    // probe the hash table with R
    const auto &ht_end = ht.end();
    for (const RowR &r : R)
    {
        auto it = ht.find(r.key);
        if (it != ht_end)
            agg_struct.agg(it.value(), r); // update the aggregate value
    }

    // build the result set
    GJResult rvec;
    rvec.reserve(L.size());
    for (const RowL &r : L)
        rvec.emplace_back(r, agg_struct.calc_final(ht.find(r.key)->second)); // search for the aggregate value
    return rvec;
}

/**
    Performs a =-GroupJoin by hashing the right input.
    @param L left operand of the GroupJoin
    @param R right operand of the GroupJoin
    @param agg_struct aggregate function used for the calculation
    @param hash hash function used for building/probing the hash table, defaults to std::hash
    @param key_equal function to check for equality of keys, defaults to std::equal_to
    @tparam Total type of the intermediate result of the aggregate function
    @tparam S type of the final result of the aggregate function
    @tparam Key type of the key values of L and R
    @tparam LRestValue type of the rest value of L
    @tparam RRestValue type of the rest value in R
*/
template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue, 
    typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
GJResult_type<Key, LRestValue, S> groupREq(const L_type<Key, LRestValue> &L, 
    const R_type<Key, RRestValue> &R, const BasicAgg<Total, S, Key, RRestValue> &agg_struct, 
    const Hash &hash = Hash(), const KeyEqual &key_equal = KeyEqual())
{
    typedef Row<Key, LRestValue> RowL;
    typedef Row<Key, RRestValue> RowR;
    typedef GJResult_type<Key, LRestValue, S> GJResult;
    typedef HashTable<Key, Total, Hash, KeyEqual> HT;

    // build the hash table with R
    HT ht(R.size(), hash, key_equal);
    for (const RowR &r : R)
        agg_struct.agg(ht[r.key], r); // initiate or update the aggregate value

    // build the result set
    GJResult rvec;
    rvec.reserve(L.size());
    const auto &ht_end = ht.end();
    for (const RowL &r : L)
    {
        const auto it = ht.find(r.key); // search for the aggregate value
        rvec.emplace_back(r, agg_struct.calc_final(it != ht_end ? it->second : Total{}));
    }

    return rvec;
}

/**
    Performs a =-GroupJoin by hashing one of the inputs depending on their sizes with the goal being 
    to minimize execution time. 
    @param L left operand of the GroupJoin
    @param R right operand of the GroupJoin
    @param agg_struct aggregate function used for the calculation
    @param hash hash function used for building/probing the hash table, defaults to std::hash
    @param key_equal function to check for equality of keys, defaults to std::equal_to
    @tparam Total type of the intermediate result of the aggregate function
    @tparam S type of the final result of the aggregate function
    @tparam Key type of the key values of L and R
    @tparam LRestValue type of the rest value of L
    @tparam RRestValue type of the rest value in R
*/
template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue, 
    typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
GJResult_type<Key, LRestValue, S> groupLREq(const L_type<Key, LRestValue> &L, 
    const R_type<Key, RRestValue> &R, const BasicAgg<Total, S, Key, RRestValue> &agg_struct, 
    const Hash &hash = Hash(), const KeyEqual &key_equal = KeyEqual())
{
    if (L.size() * 10 < R.size()) // based on an estimate to maximize performance
        return groupLEq(L, R, agg_struct, hash, key_equal);
    return groupREq(L, R, agg_struct, hash, key_equal);
}

// iterator-based versions

/**
    Performs a =-GroupJoin by hashing the left input.
    @param lStart iterator to the first tuple of the left operand of the GroupJoin
    @param lEnd iterator to one past the last tuple of the left operand of the GroupJoin
    @param rStart iterator to the first tuple of the right operand of the GroupJoin
    @param rEnd iterator to one past the last tuple of the right operand of the GroupJoin
    @param res iterator to the first tuple of the output
    @param agg_struct aggregate function used for the calculation
    @param hash hash function used for building/probing the hash table, defaults to std::hash
    @param key_equal function to check for equality of keys, defaults to std::equal_to
    @tparam Total type of the intermediate result of the aggregate function
    @tparam S type of the final result of the aggregate function
    @tparam Key type of the key values of L and R
    @tparam LRestValue type of the rest value of L
    @tparam RRestValue type of the rest value in R
*/
template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue,
          typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
void groupLEq(
    typename L_type<Key, LRestValue>::const_iterator lStart,
    const typename L_type<Key, LRestValue>::const_iterator &lEnd,
    typename R_type<Key, RRestValue>::const_iterator rStart,
    const typename R_type<Key, RRestValue>::const_iterator &rEnd,
    typename GJResult_type<Key, LRestValue, S>::iterator res,
    const BasicAgg<Total, S, Key, RRestValue> &agg_struct,
    const Hash &hash = Hash(), const KeyEqual &key_equal = KeyEqual())
{
    typedef HashTable<Key, Total, Hash, KeyEqual> HT;

    HT ht(lEnd - lStart, hash, key_equal);
    for (; lStart != lEnd; ++lStart)
        ht.emplace(lStart->key, Total{});

    for (const auto &ht_end = ht.end(); rStart != rEnd; ++rStart)
    {
        auto it = ht.find(rStart->key);
        if (it != ht_end)
            agg_struct.agg(it.value(), *rStart);
    }

    for (; lStart != lEnd; ++lStart, ++res)
        *res = {*lStart, agg_struct.calc_final(ht.find(lStart->key)->second)};
}

/**
    Performs a =-GroupJoin by hashing the right input.
    @param lStart iterator to the first tuple of the left operand of the GroupJoin
    @param lEnd iterator to one past the last tuple of the left operand of the GroupJoin
    @param rStart iterator to the first tuple of the right operand of the GroupJoin
    @param rEnd iterator to one past the last tuple of the right operand of the GroupJoin
    @param res iterator to the first tuple of the output
    @param agg_struct aggregate function used for the calculation
    @param hash hash function used for building/probing the hash table, defaults to std::hash
    @param key_equal function to check for equality of keys, defaults to std::equal_to
    @tparam Total type of the intermediate result of the aggregate function
    @tparam S type of the final result of the aggregate function
    @tparam Key type of the key values of L and R
    @tparam LRestValue type of the rest value of L
    @tparam RRestValue type of the rest value in R
*/
template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue,
          typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
void groupREq(
    typename L_type<Key, LRestValue>::const_iterator lStart,
    const typename L_type<Key, LRestValue>::const_iterator &lEnd,
    typename R_type<Key, RRestValue>::const_iterator rStart,
    const typename R_type<Key, RRestValue>::const_iterator &rEnd,
    typename GJResult_type<Key, LRestValue, S>::iterator res,
    const BasicAgg<Total, S, Key, RRestValue> &agg_struct,
    const Hash &hash = Hash(), const KeyEqual &key_equal = KeyEqual())
{
    typedef HashTable<Key, Total, Hash, KeyEqual> HT;

    HT ht(rEnd - rStart, hash, key_equal);
    for (; rStart != rEnd; ++rStart)
        agg_struct.agg(ht[rStart->key], *rStart);

    for (const auto &ht_end = ht.end(); lStart != lEnd; ++lStart, ++res)
    {
        const auto it = ht.find(lStart->key);
        *res = {*lStart, agg_struct.calc_final(it != ht_end ? it->second : Total{})};
    }
}

/**
    Performs a =-GroupJoin by hashing one of the inputs depending on their sizes with the goal being 
    to minimize execution time.
    @param lStart iterator to the first tuple of the left operand of the GroupJoin
    @param lEnd iterator to one past the last tuple of the left operand of the GroupJoin
    @param rStart iterator to the first tuple of the right operand of the GroupJoin
    @param rEnd iterator to one past the last tuple of the right operand of the GroupJoin
    @param res iterator to the first tuple of the output
    @param agg_struct aggregate function used for the calculation
    @param hash hash function used for building/probing the hash table, defaults to std::hash
    @param key_equal function to check for equality of keys, defaults to std::equal_to
    @tparam Total type of the intermediate result of the aggregate function
    @tparam S type of the final result of the aggregate function
    @tparam Key type of the key values of L and R
    @tparam LRestValue type of the rest value of L
    @tparam RRestValue type of the rest value in R
*/
template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue,
          typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
void groupLREq(
    const typename L_type<Key, LRestValue>::const_iterator &lStart,
    const typename L_type<Key, LRestValue>::const_iterator &lEnd,
    const typename R_type<Key, RRestValue>::const_iterator &rStart,
    const typename R_type<Key, RRestValue>::const_iterator &rEnd,
    typename GJResult_type<Key, LRestValue, S>::iterator res,
    const BasicAgg<Total, S, Key, RRestValue> &agg_struct,
    const Hash &hash = Hash(), const KeyEqual &key_equal = KeyEqual())
{
    if ((lEnd - lStart) * 10 < rEnd - rStart)
        return groupLEq<Total, S, Key, LRestValue>(lStart, lEnd, rStart, rEnd, res, agg_struct, hash, key_equal);
    return groupREq<Total, S, Key, LRestValue>(lStart, lEnd, rStart, rEnd, res, agg_struct, hash, key_equal);
}



/// Sorting based approaches

/**
    Performs a =-GroupJoin on sorted inputs by merging them.
    @param L left operand of the GroupJoin
    @param R right operand of the GroupJoin
    @param agg_struct aggregate function used for the calculation
    @param key_equal function to check for equality of keys, defaults to std::equal_to
    @param key_less function that returns true if the first operand is smaller than the second 
    operand, defaults to std::less
    @tparam Total type of the intermediate result of the aggregate function
    @tparam S type of the final result of the aggregate function
    @tparam Key type of the key values of L and R
    @tparam LRestValue type of the rest value of L
    @tparam RRestValue type of the rest value in R
*/
template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue, 
    typename KeyEqual = std::equal_to<Key>, typename KeyLess = std::less<Key>>
GJResult_type<Key, LRestValue, S> mergeEq(const L_type<Key, LRestValue> &L, 
    const R_type<Key, RRestValue> &R, const BasicAgg<Total, S, Key, RRestValue> &agg_struct, 
    const KeyEqual &key_equal = KeyEqual(), const KeyLess &key_less = KeyLess())
{
    typedef Row<Key, LRestValue> RowL;
    typedef GJResult_type<Key, LRestValue, S> GJResult;

    GJResult rvec;
    rvec.reserve(L.size());

    auto r_it = R.begin();
    const auto &r_end = R.end();
    Total total{};

    // first step
    Key prev_key = L.begin()->key;
    for (; r_it != r_end && key_less(r_it->key, prev_key); ++r_it){}
    for (; r_it != r_end && key_equal(r_it->key, prev_key); ++r_it)
        agg_struct.agg(total, *r_it);

    for (const RowL &r : L)
    {
        if (!key_equal(r.key, prev_key)) // spare recalculation of duplicates
        {
            total = Total{};
            for (; r_it != r_end && key_less(r_it->key, r.key); ++r_it){}
            for (; r_it != r_end && key_equal(r_it->key, r.key); ++r_it)
                agg_struct.agg(total, *r_it);
            prev_key = r.key;
        }
        rvec.emplace_back(r, agg_struct.calc_final(total));
    }
    return rvec;
}

/**
    Performs a =-GroupJoin by sorting both inputs first and then merging them.
    @param L left operand of the GroupJoin
    @param R right operand of the GroupJoin
    @param agg_struct aggregate function used for the calculation
    @param key_equal function to check for equality of keys, defaults to std::equal_to
    @param key_less function that returns true if the first operand is smaller than the second 
    operand, defaults to std::less
    @tparam Total type of the intermediate result of the aggregate function
    @tparam S type of the final result of the aggregate function
    @tparam Key type of the key values of L and R
    @tparam LRestValue type of the rest value of L
    @tparam RRestValue type of the rest value in R
*/
template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue, 
    typename KeyEqual = std::equal_to<Key>, typename KeyLess = std::less<Key>>
GJResult_type<Key, LRestValue, S> sortMergeEq(L_type<Key, LRestValue>& L, 
    R_type<Key, RRestValue>& R, const BasicAgg<Total, S, Key, RRestValue> &agg_struct, 
    const KeyEqual &key_equal = KeyEqual(), const KeyLess &key_less = KeyLess())
{
    std::sort(L.begin(), L.end(), [&](const Row<int, int> &r1, const Row<int, int> &r2) { return key_less(r1.key, r2.key); });
    std::sort(R.begin(), R.end(), [&](const Row<int, int> &r1, const Row<int, int> &r2) { return key_less(r1.key, r2.key); });
    return mergeEq(L, R, agg_struct, key_equal, key_less);
}

#endif