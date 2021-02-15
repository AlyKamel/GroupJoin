#ifndef ALTGJ_H
#define ALTGJ_H

#include "basics.hpp"
#include "aggfuncs.hpp"

#include <tsl/robin_map.h>
#include <vector>


// functions
template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue, typename KeyComp = std::equal_to<Key>>
GJResult_type<Key, LRestValue, S> nested(const L_type<Key, LRestValue> &L, const R_type<Key, RRestValue> &R, const BasicAgg<Total, S, Key, RRestValue> &agg_struct, const KeyComp &key_comp = KeyComp())
{
    typedef Row<Key, LRestValue> RowL;
    typedef Row<Key, RRestValue> RowR;
    typedef GJResult_type<Key, LRestValue, S> GJResult;

    GJResult rvec;
    rvec.reserve(L.size());

    for (const RowL &rowl : L) {
        Total total = {};
        for (const RowR &rowr : R) {
            if (key_comp(rowl.key, rowr.key))
                agg_struct.agg(total, rowr);
        }
        rvec.emplace_back(rowl, agg_struct.calc_final(total));
    }
    return rvec;
}

template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue, typename KeyComp = std::equal_to<Key>>
void nested(
    typename GJResult_type<Key, LRestValue, S>::iterator L_first,
    const typename GJResult_type<Key, LRestValue, S>::iterator &L_last,
    const typename R_type<Key, RRestValue>::const_iterator &R_first,
    const typename R_type<Key, RRestValue>::const_iterator &R_last,
    const BasicAgg<Total, S, Key, RRestValue> &agg_struct,
    const KeyComp &key_comp = KeyComp())
{
    for (auto rowl = L_first; rowl != L_last; ++rowl)
    {
        Total total = {};
        for (auto rowr = R_first; rowr != R_last; ++rowr)
        {
            if (key_comp(rowl->first.key, rowr->key))
                agg_struct.agg(total, *rowr);
        }
        rowl->second = agg_struct.calc_final(total);
    }
}

template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue,
          typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
GJResult_type<Key, LRestValue, S> hashEq(const L_type<Key, LRestValue> &L,
    const R_type<Key, RRestValue> &R, const BasicAgg<Total, S, Key, RRestValue> &agg_struct,
    const Hash &hash = Hash(), const KeyEqual &key_equal = KeyEqual())
{
    typedef Row<Key, LRestValue> RowL;
    typedef Row<Key, RRestValue> RowR;
    typedef GJResult_type<Key, LRestValue, S> GJResult;
    typedef std::pair<RRestValue, Total> Value;
    typedef tsl::robin_map<Key, std::vector<Value>> HashTable;

    HashTable ht(L.size(), hash, key_equal);

    for (const RowL &r : L)
        ht[r.key].emplace_back(r.other, Total{});

    const auto &ht_end = ht.end();
    for (const RowR &r : R)
    {
        auto group = ht.find(r.key);
        if (group != ht_end) {
            for (auto &v : group.value())
                agg_struct.agg(v.second, r);
        }
    }

    GJResult rvec;
    rvec.reserve(L.size());
    for (const auto &t_itr : ht)
    {
        Key key = t_itr.first;
        for (const auto &v : t_itr.second)
            rvec.emplace_back(RowL{key, v.first}, agg_struct.calc_final(v.second));
    }

    return rvec;
}

template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue,
          typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
GJResult_type<Key, LRestValue, S> hashUniqueEq(const L_type<Key, LRestValue> &L,
    const R_type<Key, RRestValue> &R, const BasicAgg<Total, S, Key, RRestValue> &agg_struct,
    const Hash &hash = Hash(), const KeyEqual &key_equal = KeyEqual())
{
    typedef Row<Key, LRestValue> RowL;
    typedef Row<Key, RRestValue> RowR;
    typedef GJResult_type<Key, LRestValue, S> GJResult;
    typedef std::pair<RRestValue, Total> Value;
    typedef tsl::robin_map<Key, Value> HashTable;

    HashTable ht(L.size(), hash, key_equal);

    for (const RowL &r : L)
        ht.insert({r.key, Value(r.other, Total{})});

    const auto &ht_end = ht.end();
    for (const RowR &r : R)
    {
        auto it = ht.find(r.key);
        if (it != ht_end)
            agg_struct.agg(it.value().second, r);
    }

    GJResult rvec;
    rvec.reserve(L.size());
    for (const auto &t_itr : ht)
        rvec.emplace_back(RowL{t_itr.first, t_itr.second.first}, agg_struct.calc_final(t_itr.second.second));

    return rvec;
}

#endif