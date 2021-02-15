#ifndef PARAGJ_H
#define PARAGJ_H

#include "basics.hpp"
#include "aggfuncs.hpp"

#include <tbb/tbb.h>
#include <vector>
#include <thread>
#include <functional>

namespace parajoin
{
    extern int prt_size; // <= 1e6
    extern int num_threads;

    struct PFMod
    {
        PFMod(const uint prt_count) : prt_count(prt_count) {}
        uint operator()(const int x)
        {
            return x % prt_count;
        }
        const uint prt_count;
    };

    template <typename PrtFunc, typename Row>
    void prtfunc(tbb::task_arena &arena, std::vector<Row> &rel, const uint prt_count, std::vector<uint> &posPrts, PrtFunc pf)
    {
        const double th_work_size = (double)rel.size() / num_threads; // size of thread workload

        std::vector<std::vector<Row>> workloads(num_threads); // workload of each thread
        std::vector<uint> prt_sizes(num_threads * prt_count); // partition i of thread j has size prt_sizes[j * prt_count + i]

        // calculate the partition sizes for each thread
        arena.execute([&] {
            tbb::parallel_for(0, num_threads, [&, th_work_size](const int th_num) {
                auto &workload = workloads[th_num];
                workload.insert(workload.begin(), rel.begin() + th_work_size * th_num, rel.begin() + (uint)(th_work_size * (th_num + 1)));
                const uint start = th_num * prt_count;
                for (const Row &r : workload)
                    ++prt_sizes[start + pf(r.key)];
            });
        });

        // sum up the amount of work for each partition
        posPrts.reserve(prt_count + 1); // partition i starts at posPrts[i]
        posPrts.push_back(0);
        uint count = 0;
        for (uint prt_num = 0; prt_num != prt_count - 1; ++prt_num)
        {
            for (int th_num = 0; th_num != num_threads; ++th_num)
                count += prt_sizes[th_num * prt_count + prt_num];
            posPrts.push_back(count);
        }

        // give each thread its starting position for each partition
        std::vector<uint> th_posPrts; // start position of partition i for thread j: th_posPrts[j * prt_count + i]
        th_posPrts.reserve(num_threads * prt_count);
        for (int th_num = 0; th_num != num_threads; ++th_num)
        {
            const uint start = th_num * prt_count;
            for (uint prt_num = 0; prt_num != prt_count; ++prt_num)
            {
                th_posPrts.push_back(posPrts[prt_num]);
                posPrts[prt_num] += prt_sizes[start + prt_num];
            }
        }

        // set starting position of each partition
        for (uint prt_num = 0; prt_num != prt_count; ++prt_num)
            posPrts[prt_num] = th_posPrts[prt_num];
        posPrts.push_back(rel.size());

        // partition the relation in parallel
        arena.execute([&] {
            tbb::parallel_for(0, num_threads, [&](const int th_num) {
                const uint start = th_num * prt_count;
                for (const Row &r : workloads[th_num])
                    rel[th_posPrts[start + pf(r.key)]++] = r;
            });
        });
    }

    template <typename PrtFunc, typename Row, typename Total, typename... AggArgs>
    Total prtfuncUneq(tbb::task_arena &arena, std::vector<Row> &rel, const uint prt_count, std::vector<uint> &posPrts, PrtFunc pf, const CombineAgg<Total, AggArgs...> &agg_struct)
    {
        const double th_work_size = (double)rel.size() / num_threads; // size of thread workload

        std::vector<std::vector<Row>> workloads(num_threads); // workload of each thread
        std::vector<uint> prt_sizes(num_threads * prt_count); // partition i of thread j has size prt_sizes[j * prt_count + i]
        std::vector<Total> subtotals(num_threads);            // total sum of all tuples a thread is responsible for

        // calculate the partition sizes for each thread
        arena.execute([&] {
            tbb::parallel_for(0, num_threads, [&, th_work_size](const int th_num) {
                const uint start = th_num * prt_count;
                Total &subtotal = subtotals[th_num];
                auto &workload = workloads[th_num];
                workload.insert(workload.begin(), rel.begin() + th_work_size * th_num, rel.begin() + (uint)(th_work_size * (th_num + 1)));
                for (const Row &r : workload)
                {
                    ++prt_sizes[start + pf(r.key)];
                    agg_struct.agg(subtotal, r); // calculate aggregate total for thread
                }
            });
        });

        // sum up the amount of work for each partition
        posPrts.reserve(prt_count + 1); // partition i starts at posPrts[i]
        posPrts.push_back(0);
        uint count = 0;
        for (uint prt_num = 0; prt_num != prt_count - 1; ++prt_num)
        {
            for (int th_num = 0; th_num != num_threads; ++th_num)
                count += prt_sizes[th_num * prt_count + prt_num];
            posPrts.push_back(count);
        }

        // give each thread its starting position for each partition
        Total total = {};
        std::vector<uint> th_posPrts; // start position of partition i for thread j: th_posPrts[j * prt_count + i]
        th_posPrts.reserve(num_threads * prt_count);
        for (int th_num = 0; th_num != num_threads; ++th_num)
        {
            const uint start = th_num * prt_count;
            for (uint prt_num = 0; prt_num != prt_count; ++prt_num)
            {
                th_posPrts.push_back(posPrts[prt_num]);
                posPrts[prt_num] += prt_sizes[start + prt_num];
            }
            agg_struct.combine(total, subtotals[th_num]); // merge subtotals together
        }

        // set starting position of each partition
        for (uint prt_num = 0; prt_num != prt_count; ++prt_num)
            posPrts[prt_num] = th_posPrts[prt_num];
        posPrts.push_back(rel.size());

        // partition the relation in parallel
        arena.execute([&] {
            tbb::parallel_for(0, num_threads, [&](const int th_num) {
                const uint start = th_num * prt_count;
                for (const Row &r : workloads[th_num])
                    rel[th_posPrts[start + pf(r.key)]++] = r;
            });
        });

        return total;
    }

    template <typename PrtFunc, typename Row, typename Total, typename... AggArgs>
    std::vector<Total> prtfuncLess(tbb::task_arena &arena, std::vector<Row> &rel, const uint prt_count, std::vector<uint> &posPrts, PrtFunc pf, const CombineAgg<Total, AggArgs...> &agg_struct)
    {
        const double th_work_size = (double)rel.size() / num_threads; // size of thread workload

        std::vector<std::vector<Row>> workloads(num_threads); // workload of each thread
        std::vector<uint> prt_sizes(num_threads * prt_count); // partition i of thread j has size prt_sizes[j * prt_count + i]
        std::vector<Total> subtotals(num_threads * prt_count); // total sum of all tuples a thread is responsible for

        // calculate the partition sizes for each thread
        arena.execute([&] {
            tbb::parallel_for(0, num_threads, [&, th_work_size](const int th_num) {
                const uint start = th_num * prt_count;
                auto &workload = workloads[th_num];
                workload.insert(workload.begin(), rel.begin() + th_work_size * th_num, rel.begin() + (uint)(th_work_size * (th_num + 1)));
                for (const Row &r : workload) {
                    const uint pos = start + pf(r.key);
                    ++prt_sizes[pos];
                    agg_struct.agg(subtotals[pos], r); // calculate aggregate total of each partition
                }
            });
        });

        // sum up the amount of work for each partition
        posPrts.reserve(prt_count + 1); // partition i starts at posPrts[i]
        posPrts.push_back(0);
        uint count = 0;
        for (uint prt_num = 0; prt_num != prt_count - 1; ++prt_num)
        {
            for (int th_num = 0; th_num != num_threads; ++th_num)
                count += prt_sizes[th_num * prt_count + prt_num];
            posPrts.push_back(count);
        }

        // give each thread its starting position for each partition
        std::vector<Total> totals(prt_count + 1);
        std::vector<uint> th_posPrts; // start position of partition i for thread j: th_posPrts[j * prt_count + i]
        th_posPrts.reserve(num_threads * prt_count);
        for (int th_num = 0; th_num != num_threads; ++th_num)
        {
            const uint start = th_num * prt_count;
            for (uint prt_num = 0; prt_num != prt_count; ++prt_num)
            {
                th_posPrts.push_back(posPrts[prt_num]);
                posPrts[prt_num] += prt_sizes[start + prt_num];
                agg_struct.combine(totals[prt_num], subtotals[start + prt_num]);
            }
        }

        // combine subtotals
        totals[prt_count] = 0;
        for (int prt_num = prt_count - 2; prt_num != -1; --prt_num)
            agg_struct.combine(totals[prt_num], totals[prt_num + 1]);

        // set starting position of each partition
        for (uint prt_num = 0; prt_num != prt_count; ++prt_num)
            posPrts[prt_num] = th_posPrts[prt_num];
        posPrts.push_back(rel.size());

        arena.execute([&] {
            tbb::parallel_for(0, num_threads, [&](const int th_num) { 
                const uint start = th_num * prt_count;
                for (const Row &r : workloads[th_num])
                    rel[th_posPrts[start + pf(r.key)]++] = r;
            });
        });

        return totals;
    }

    // parallel partitioning
    template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue, typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>, typename PrtFunc = PFMod>
    GJResult_type<Key, LRestValue, S> prtLREq(L_type<Key, LRestValue> &L, R_type<Key, RRestValue> &R, const BasicAgg<Total, S, Key, RRestValue> &agg_struct, const Hash &hash = Hash(), const KeyEqual &key_equal = KeyEqual())
    {
        typedef GJResult_type<Key, LRestValue, S> GJResult;

        GJResult rvec; // result vector
        std::thread outputAllocator([&]() {
            rvec.resize(L.size());
        });

        const int prt_count = L.size() / prt_size;
        auto pf = PrtFunc(prt_count);
        tbb::task_arena limited_arena(num_threads); // limit the number of threads in use
        std::vector<uint> posPrtsL, posPrtsR;       // start position of each partition

        // partition inputs
        prtfunc(limited_arena, L, prt_count, posPrtsL, pf);
        prtfunc(limited_arena, R, prt_count, posPrtsR, pf);

        outputAllocator.join();

        // perform GroupJoin
        limited_arena.execute([&] {
            tbb::parallel_for(0, prt_count, [&](const int prt_num) {
                groupLREq<Total, S, Key, LRestValue>(
                    L.begin() + posPrtsL[prt_num],
                    L.begin() + posPrtsL[prt_num + 1],
                    R.begin() + posPrtsR[prt_num],
                    R.begin() + posPrtsR[prt_num + 1],
                    rvec.begin() + posPrtsL[prt_num],
                    agg_struct,
                    hash,
                    key_equal);
            });
        });

        return rvec;
    }

    template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue, typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>, typename PrtFunc = PFMod>
    GJResult_type<Key, LRestValue, S> prtLRUneq(L_type<Key, LRestValue> &L, R_type<Key, RRestValue> &R, const CSAgg<Total, S, Key, RRestValue> &agg_struct, const Hash &hash = Hash(), const KeyEqual &key_equal = KeyEqual())
    {
        typedef GJResult_type<Key, LRestValue, S> GJResult;

        GJResult rvec; // result vector
        std::thread outputAllocator([&]() {
            rvec.resize(L.size());
        });

        const int prt_count = L.size() / prt_size;
        auto pf = PrtFunc(prt_count);
        tbb::task_arena limited_arena(num_threads); // limit the number of threads in use
        std::vector<uint> posPrtsL, posPrtsR;       // start position of each partition

        // partition inputs
        prtfunc(limited_arena, L, prt_count, posPrtsL, pf);
        Total total = prtfuncUneq(limited_arena, R, prt_count, posPrtsR, pf, agg_struct);

        outputAllocator.join();

        // perform GroupJoin
        limited_arena.execute([&] {
            tbb::parallel_for(0, prt_count, [&](const int prt_num) {
                groupLRUneq<Total, S, Key, LRestValue>(
                    L.begin() + posPrtsL[prt_num],
                    L.begin() + posPrtsL[prt_num + 1],
                    R.begin() + posPrtsR[prt_num],
                    R.begin() + posPrtsR[prt_num + 1],
                    rvec.begin() + posPrtsL[prt_num],
                    total,
                    agg_struct,
                    hash,
                    key_equal);
            });
        });

        return rvec;
    }

    template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue, typename KeyLess = std::less<Key>, typename PrtFunc = PFMod>
    GJResult_type<Key, LRestValue, S> prtLRLess(L_type<Key, LRestValue> &L, R_type<Key, RRestValue> &R, const CombineAgg<Total, S, Key, RRestValue> &agg_struct, const KeyLess &key_less = KeyLess())
    {
        typedef GJResult_type<Key, LRestValue, S> GJResult;

        GJResult rvec; // result vector
        std::thread outputAllocator([&]() {
            rvec.resize(L.size());
        });

        const int prt_count = L.size() / prt_size;
        tbb::task_arena limited_arena(num_threads); // limit the number of threads in use
        std::vector<uint> posPrtsL, posPrtsR;       // start position of each partition

        // generate partitioning function
        std::vector<Key> prtDivs(prt_count - 1); // borders of the partitions (p0<pDivs[0]<=p1, .., pDivs[N-2]<=pN-1<pDivs[N-1]<=pN)
        const uint prtfac = L.size() / (prt_count - 1); // estimation to get best result if inputs are sorted
        for (int i = 0; i != prt_count - 1; ++i)
            prtDivs[i] = L[i * prtfac].key;
        std::sort(prtDivs.begin(), prtDivs.end());
        auto pf = [&](const Key &x) { return std::upper_bound(prtDivs.begin(), prtDivs.end(), x) - prtDivs.begin(); };

        // partition inputs
        prtfunc(limited_arena, L, prt_count, posPrtsL, pf);
        std::vector<Total> totals = prtfuncLess(limited_arena, R, prt_count, posPrtsR, pf, agg_struct);

        outputAllocator.join();

        // perform GroupJoin
        limited_arena.execute([&] {
            tbb::parallel_for(0, prt_count, [&](const int prt_num) {
                sortMergeLess<Total, S, Key, LRestValue>(
                    L.begin() + posPrtsL[prt_num],
                    L.begin() + posPrtsL[prt_num + 1],
                    R.begin() + posPrtsR[prt_num],
                    R.begin() + posPrtsR[prt_num + 1],
                    rvec.begin() + posPrtsL[prt_num],
                    totals[prt_num + 1],
                    agg_struct,
                    key_less);
            });
        });

        return rvec;
    }

    // serial partitioning
    template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue, typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>, typename PrtFunc = PFMod>
    GJResult_type<Key, LRestValue, S> prtLREqSimple(L_type<Key, LRestValue> &L, R_type<Key, RRestValue> &R, const BasicAgg<Total, S, Key, RRestValue> &agg_struct, const Hash &hash = Hash(), const KeyEqual &key_equal = KeyEqual())
    {
        typedef Row<Key, LRestValue> RowL;
        typedef Row<Key, RRestValue> RowR;
        typedef GJResult_type<Key, LRestValue, S> GJResult;

        GJResult rvec; // result vector
        std::thread outputAllocator([&]() {
            rvec.resize(L.size());
        });

        const int prt_count = L.size() / prt_size;
        auto pf = PrtFunc(prt_count);
        tbb::task_arena limited_arena(num_threads); // limit the number of threads in use

        // partition inputs
        std::vector<std::vector<RowL>> prtsL(prt_count);
        std::thread lPartitioner([&]() {
            for (const auto &r : L)
                prtsL[pf(r.key)].push_back(r);
        });

        std::vector<std::vector<RowR>> prtsR(prt_count);
        for (const auto &r : R)
            prtsR[pf(r.key)].push_back(r);

        lPartitioner.join();

        // calculate a unique insertion position for each partition
        std::vector<uint> posPrts;
        posPrts.reserve(prt_count);
        posPrts.push_back(0);
        for (int prt_num = 1; prt_num != prt_count; ++prt_num)
            posPrts.push_back(posPrts[prt_num - 1] + prtsL[prt_num - 1].size());

        outputAllocator.join();

        // perform GroupJoin
        limited_arena.execute([&] {
            tbb::parallel_for(0, prt_count, [&](const int prt_num) {
                groupLREq<Total, S, Key, LRestValue>(
                    prtsL[prt_num].begin(),
                    prtsL[prt_num].end(),
                    prtsR[prt_num].begin(),
                    prtsR[prt_num].end(),
                    rvec.begin() + posPrts[prt_num],
                    agg_struct,
                    hash,
                    key_equal);
            });
        });

        return rvec;
    }

    template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue, typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>, typename PrtFunc = PFMod>
    GJResult_type<Key, LRestValue, S> prtLRUneqSimple(L_type<Key, LRestValue> &L, R_type<Key, RRestValue> &R, const CSAgg<Total, S, Key, RRestValue> &agg_struct, const Hash &hash = Hash(), const KeyEqual &key_equal = KeyEqual())
    {
        typedef Row<Key, LRestValue> RowL;
        typedef Row<Key, RRestValue> RowR;
        typedef GJResult_type<Key, LRestValue, S> GJResult;

        GJResult rvec; // result vector
        std::thread outputAllocator([&]() {
            rvec.resize(L.size());
        });

        const int prt_count = L.size() / prt_size;
        auto pf = PrtFunc(prt_count);
        tbb::task_arena limited_arena(num_threads); // limit the number of threads in use

        // partition inputs
        std::vector<std::vector<RowL>> prtsL(prt_count);
        std::thread lPartitioner([&]() {
            for (const auto &r : L)
                prtsL[pf(r.key)].push_back(r);
        });

        Total total = {};
        std::vector<std::vector<RowR>> prtsR(prt_count);
        for (const auto &r : R) {
            prtsR[pf(r.key)].push_back(r);
            agg_struct.agg(total, r); // calculate total
        }

        lPartitioner.join();

        // calculate a unique insertion position for each partition
        std::vector<uint> posPrts;
        posPrts.reserve(prt_count);
        posPrts.push_back(0);
        for (int prt_num = 1; prt_num != prt_count; ++prt_num)
            posPrts.push_back(posPrts[prt_num - 1] + prtsL[prt_num - 1].size());

        outputAllocator.join();

        // perform GroupJoin
        limited_arena.execute([&] {
            tbb::parallel_for(0, prt_count, [&](const int prt_num) {
                groupLRUneq<Total, S, Key, LRestValue>(
                    prtsL[prt_num].begin(),
                    prtsL[prt_num].end(),
                    prtsR[prt_num].begin(),
                    prtsR[prt_num].end(),
                    rvec.begin() + posPrts[prt_num],
                    total,
                    agg_struct,
                    hash,
                    key_equal);
            });
        });

        return rvec;
    }

    template <typename Total, typename S, typename Key, typename LRestValue, typename RRestValue, typename KeyLess = std::less<Key>, typename PrtFunc = PFMod>
    GJResult_type<Key, LRestValue, S> prtLRLessSimple(L_type<Key, LRestValue> &L, R_type<Key, RRestValue> &R, const CombineAgg<Total, S, Key, RRestValue> &agg_struct, const KeyLess &key_less = KeyLess())
    {
        typedef Row<Key, LRestValue> RowL;
        typedef Row<Key, RRestValue> RowR;
        typedef GJResult_type<Key, LRestValue, S> GJResult;

        GJResult rvec; // result vector
        std::thread outputAllocator([&]() {
            rvec.resize(L.size());
        });

        const int prt_count = L.size() / prt_size;
        tbb::task_arena limited_arena(num_threads); // limit the number of threads in use
        std::vector<uint> posPrtsL, posPrtsR;       // start position of each partition

        // generate partitioning function
        std::vector<Key> prtDivs(prt_count - 1); // borders of the partitions (p0<pDivs[0]<=p1, .., pDivs[N-2]<=pN-1<pDivs[N-1]<=pN)
        const uint prtfac = L.size() / (prt_count - 1); // estimation to get best result if inputs are sorted
        for (int i = 0; i != prt_count - 1; ++i)
            prtDivs[i] = L[i * prtfac].key;
        std::sort(prtDivs.begin(), prtDivs.end());
        auto pf = [&](const Key &x) { return std::upper_bound(prtDivs.begin(), prtDivs.end(), x) - prtDivs.begin(); };

        // partition inputs
        std::vector<std::vector<RowL>> prtsL(prt_count);
        std::thread lPartitioner([&]() {
            for (const auto &r : L)
                prtsL[pf(r.key)].push_back(r);
        });

        std::vector<std::vector<RowR>> prtsR(prt_count);
        std::vector<Total> totals(prt_count + 1);
        for (const auto &r : R) {
            const uint prt_num = pf(r.key);
            prtsR[prt_num].push_back(r);
            agg_struct.agg(totals[prt_num], r); // calculate aggregate total of each partition
        }

        // combine subtotals
        totals[prt_count] = 0;
        for (int prt_num = prt_count - 2; prt_num != -1; --prt_num)
            agg_struct.combine(totals[prt_num], totals[prt_num + 1]);

        lPartitioner.join();

        // calculate a unique insertion position for each partition
        std::vector<uint> posPrts;
        posPrts.reserve(prt_count);
        posPrts.push_back(0);
        for (int prt_num = 1; prt_num != prt_count; ++prt_num)
            posPrts.push_back(posPrts[prt_num - 1] + prtsL[prt_num - 1].size());

        outputAllocator.join();

        // perform GroupJoin
        limited_arena.execute([&] {
            tbb::parallel_for(0, prt_count, [&](const int prt_num) {
                sortMergeLess<Total, S, Key, LRestValue>(
                    prtsL[prt_num].begin(),
                    prtsL[prt_num].end(),
                    prtsR[prt_num].begin(),
                    prtsR[prt_num].end(),
                    rvec.begin() + posPrts[prt_num],
                    totals[prt_num + 1],
                    agg_struct,
                    key_less);
            });
        });

        return rvec;
    }

}

#endif