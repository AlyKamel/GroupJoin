#ifndef AGGFUNCS_H
#define AGGFUNCS_H

#include "basics.hpp"

#include <utility>
#include <limits>

// abstract agg types
template <typename Total, typename S, typename Key, typename RRestValue>
struct BasicAgg
{
    virtual void agg(Total &total, const Row<Key, RRestValue>& rb) const = 0;
    virtual S calc_final(const Total& total) const = 0;
};

template <typename Total, typename S, typename Key, typename RRestValue>
struct CombineAgg : virtual BasicAgg<Total, S, Key, RRestValue>
{
    virtual void combine(Total &total1, const Total& total2) const = 0;
};

template <typename Total, typename S, typename Key, typename RRestValue>
struct SubtractAgg : virtual BasicAgg<Total, S, Key, RRestValue>
{
    virtual Total subtract(Total total1, const Total& total2) const = 0;
};

template <typename Total, typename S, typename Key, typename RRestValue>
struct CSAgg : CombineAgg<Total, S, Key, RRestValue>, SubtractAgg<Total, S, Key, RRestValue>
{
    virtual ~CSAgg(){}
};


// agg definitions
template <typename V>
struct Opt
{
    Opt(V val = {}) : value(val){};

    bool valid = false;
    V value;

    bool isValid() const { 
        return valid; 
    };
    
    V getValue() const
    {
        return value;
    };
};

struct OptMin : Opt<int>
{
    OptMin() { value = std::numeric_limits<int>::max(); };
};

struct OptMax : Opt<int>
{
    OptMax() { value = std::numeric_limits<int>::min(); };
};


// aggs
/**
    Calculates the sum of integers. If input is empty, it returns 0.
    @tparam Key type of the key value
*/
template <typename Key>
struct SumNAgg : CSAgg<int, int, Key, int>
{
    virtual void agg(int &total, const Row<Key, int>& rb) const override
    {
        total += rb.other;
    }

    virtual int calc_final(const int& total) const override
    {
        return total;
    }

    virtual void combine(int &total1, const int& total2) const override
    {
        total1 += total2;
    }

    virtual int subtract(int total1, const int& total2) const override
    {
        return total1 - total2;
    }
};

/**
    Calculates the sum of integers. The result is an Opt that contains the sum if the input is not empty.
    @tparam Key type of the key value
*/
template <typename Key>
struct SumAgg : CSAgg<Opt<int>, Opt<int>, Key, int>
{
    virtual void agg(Opt<int> &total, const Row<Key, int>& rb) const override
    {
        total.value += rb.other;
        total.valid = true;
    }

    virtual Opt<int> calc_final(const Opt<int>& total) const override
    {
        return total;
    }

    virtual void combine(Opt<int> &total1, const Opt<int>& total2) const override
    {
        if (total2.isValid()) {
            total1.value += total2.getValue();
            total1.valid = true;
        }
    }

    virtual Opt<int> subtract(Opt<int> total1, const Opt<int>& total2) const override
    {
        total1.value -= total2.value;
        total1.valid |= total2.valid;
        return total1;
    }
};

/**
    Calculates the minimum of integers. The result is an Opt that contains the minimum if the input is not empty.
    @tparam Key type of the key value
*/
template <typename Key>
struct MinAgg : CombineAgg<OptMin, Opt<int>, Key, int>
{
    virtual void agg(Opt<int> &total, const Row<Key, int>& rb) const override
    {
        if (rb.other < total.getValue()) {
            total.value = rb.other;
            total.valid = true;
        }
    }

    virtual Opt<int> calc_final(const Opt<int>& total) const override
    {
        return total;
    }

    virtual void combine(Opt<int> &total1, const Opt<int>& total2) const override
    {
        if (total2.getValue() < total1.getValue())
            total1 = total2;
    }
};

/**
    Calculates the maximum of integers. The result is an Opt that contains the maximum if the input is not empty.
    @tparam Key type of the key value
*/
template <typename Key>
struct MaxAgg : CombineAgg<OptMax, Opt<int>, Key, int>
{
    virtual void agg(Opt<int> &total, const Row<Key, int> &rb) const override
    {
        if (rb.other > total.getValue()) {
            total.value = rb.other;
            total.valid = true;
        }
    }

    virtual Opt<int> calc_final(const Opt<int>& total) const override
    {
        return total;
    }

    virtual void combine(Opt<int> &total1, const Opt<int>& total2) const override
    {
        if (total2.getValue() > total1.getValue())
            total1 = total2;
    }
};

/**
    Calculates the count of tuples.
    @tparam Key type of the key value
    @tparam RRestValue type of the rest value of R
*/
template <typename Key, typename RRestValue>
struct CountAgg : CSAgg<int, int, Key, RRestValue>
{
    virtual void agg(int &total, const Row<Key, RRestValue> &) const override
    {
        ++total;
    }

    virtual int calc_final(const int &total) const override
    {
        return total;
    }

    virtual void combine(int &total1, const int &total2) const override
    {
        total1 += total2;
    }

    virtual int subtract(int total1, int &total2) const override
    {
        return total1 - total2;
    }
};

/**
    Calculates the average of integers. The result is an Opt that contains the average if the input is not empty.
    @tparam Key type of the key value
*/
template <typename Key>
struct AvgAgg : CSAgg<std::pair<int, int>, Opt<double>, Key, int>
{
    virtual void agg(std::pair<int, int> &total, const Row<Key, int> &rb) const override
    {
        total.first += rb.other;
        ++total.second;
    }

    virtual Opt<double> calc_final(const std::pair<int, int>& total) const override
    {
        Opt<double> res;
        if (total.second != 0) {
            res.value = total.first / total.second;
            res.valid = true;
        }
        return res;
    }

    virtual void combine(std::pair<int, int> &total1, const std::pair<int, int>& total2) const override
    {
        total1.first += total2.first;
        total1.second += total2.second;
    }

    virtual std::pair<int, int> subtract(std::pair<int, int> total1, const std::pair<int, int> &total2) const override
    {
        return {total1.first - total2.first, total1.second - total2.second};
    }
};

#endif
