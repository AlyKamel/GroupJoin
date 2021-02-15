#ifndef BASICS_H
#define BASICS_H

#include <utility>
#include <vector>

template <typename Key, typename RestValue>
struct Row
{
    Key key;
    RestValue other;
    Row(Key key = {}, RestValue other = {}) : key(key), other(other) {}
};

template<>
struct Row<int,int>
{
    int key;
    int other;
    Row(int key = {}, int other = {}) : key(key), other(other) {}

    bool operator==(const Row &r2) const
    {
        return this->key == r2.key && this->other == r2.other;
    };
};

// types
template <typename Key, typename LRestValue>
using L_type = std::vector<Row<Key, LRestValue>>;

template <typename Key, typename RRestValue>
using R_type = std::vector<Row<Key, RRestValue>>;

template <typename Key, typename LRestValue, typename S>
using RowResult = std::pair<Row<Key, LRestValue>, S>;

template <typename Key, typename LRestValue, typename S>
using GJResult_type = std::vector<RowResult<Key, LRestValue, S>>;

template <typename Key, typename RestValue>
using Rel = std::vector<Row<Key, RestValue>>;

using IntRel = Rel<int, int>;

#endif