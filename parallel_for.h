// Copyright (c) 2022, JD.com Inc.
// Autor: liheying <liheying3@jd.com>
//
#pragma once

#include <vector>
#include <functional>
#include <chrono>
#include <iterator>
#include <brpc/client/bthread_closure_help.h>
#include <bthread/bthread.h>

namespace comm_block
{

  template <class Iter>
  static std::vector<std::pair<Iter, Iter>> GetRangeVector(Iter begin, Iter end,
                                                           int parallelism, std::size_t &total_size, std::size_t &split_size)
  {
    // 1. 计算split size
    split_size = 1;
    total_size = std::distance(begin, end);
    if (parallelism > 0)
    {
      split_size = ceil(total_size * 1.0 / parallelism);
    }

    // 2. 创建分片对象
    std::vector<std::pair<Iter, Iter>> range_vec;
    std::size_t current_size = 0;
    auto range_begin = begin;
    for (auto iter = begin; iter != end;)
    {
      ++current_size;
      ++iter;
      if (current_size == split_size)
      {
        range_vec.push_back(std::make_pair(range_begin, iter));
        range_begin = iter;
        current_size = 0;
      }
    }

    if (current_size > 0)
    {
      range_vec.push_back(std::make_pair(range_begin, end));
    }

    VLOG(0) << "range info: " << split_size << " "
            << parallelism << " " << range_vec.size()
            << " " << current_size;

    return range_vec;
  }

  template <class Iter, class Func, class... Args,
            class ReturnType = typename std::enable_if<!std::is_void<typename std::result_of<Func(decltype(*((Iter)(0))), std::size_t, Args...)>::type>::value, std::vector<typename std::result_of<Func(decltype(*((Iter)(0))), std::size_t, Args...)>::type>>::type>
  ReturnType parallel_for(Iter begin, Iter end, int parallelism,
                          const Func &func, Args &&...args)
  {
    std::size_t split_size;
    std::size_t total_size;
    std::vector<std::pair<Iter, Iter>> range_vec = GetRangeVector(begin, end,
                                                                  parallelism, total_size, split_size);

    auto exec_block_func = [&](Iter block_begin, Iter block_end, std::size_t begin_pos)
    {
      ReturnType block_results;
      for (auto iter = block_begin; iter != block_end; ++iter)
      {
        auto ret = func(*iter, begin_pos++, args...);
        block_results.push_back(std::move(ret));
      }
      return block_results;
    };

    std::vector<bthread_t> tids;
    std::vector<ReturnType> all_block_results;
    all_block_results.resize(range_vec.size());

    for (std::size_t i = 0; i < range_vec.size(); ++i)
    {
      auto pair_begin = range_vec[i].first;
      auto pair_end = range_vec[i].second;
      std::size_t j = i * split_size;
      bthread_t tid;
      StartRunInBthreadBackground(&tid, [&exec_block_func, pair_begin, pair_end, &all_block_results, i, j]()
                                  { all_block_results[i] = exec_block_func(pair_begin, pair_end, j); });

      tids.push_back(tid);
    }

    std::for_each(tids.begin(), tids.end(), [](bthread_t tid)
                  { bthread_join(tid, nullptr); });

    ReturnType results;
    results.reserve(total_size);
    for (auto &block_result : all_block_results)
    {
      results.insert(results.end(), block_result.begin(), block_result.end());
    }

    return results;
  }

  template <class Iter, class Func, class... Args,
            class ReturnType = typename std::enable_if<std::is_void<typename std::result_of<Func(decltype(*((Iter)(0))), std::size_t, Args...)>::type>::value, void>::type>
  void parallel_for(Iter begin, Iter end, int parallelism,
                    const Func &func, Args &&...args)
  {
    std::size_t split_size;
    std::size_t total_size;
    std::vector<std::pair<Iter, Iter>> range_vec = GetRangeVector(begin, end,
                                                                  parallelism, total_size, split_size);

    auto exec_block_func = [&](Iter block_begin, Iter block_end, std::size_t begin_pos)
    {
      for (auto iter = block_begin; iter != block_end; ++iter)
      {
        func(*iter, begin_pos++, args...);
      }
    };

    std::vector<bthread_t> tids;
    for (std::size_t i = 0; i < range_vec.size(); ++i)
    {
      auto pair_begin = range_vec[i].first;
      auto pair_end = range_vec[i].second;
      std::size_t j = i * split_size;
      bthread_t tid;
      StartRunInBthreadBackground(&tid, [&exec_block_func, pair_begin, pair_end, j]()
                                  { exec_block_func(pair_begin, pair_end, j); });

      tids.push_back(tid);
    }

    std::for_each(tids.begin(), tids.end(), [](bthread_t tid)
                  { bthread_join(tid, nullptr); });
  }

} // namespace comm_block