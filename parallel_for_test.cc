// Copyright (c) 2022, JD.com Inc.
// Autor: liheying <liheying3@jd.com>
//
#include <gtest/gtest.h>
#include "comm_block/threads/parallel_task.h"

class CTest
{
public:
  int int_return(const char *s)
  {
    int len = strlen(s);
    int sum = 0;
    for (int i = 0; i < len; ++i)
    {
      int last = s[i] - '0';
      sum = sum * 10 + last;
    }

    return sum;
  }
};

TEST(ParallelTask, TestClassArray)
{
  const char *str_array[] = {
      "77",
      "66",
      "44",
      "22947",
      "20040922",
      "23951",
      "12345678",
      "19860311",
  };
  int vec_sz = sizeof(str_array) / sizeof(str_array[0]);
  CTest t;

  std::vector<int> result_vec;
  result_vec.resize(vec_sz);
  comm_block::parallel_for(str_array, str_array + vec_sz, 0, [&](const char *str, std::size_t idx)
                           {
    result_vec[idx] = t.int_return(str);
    LOG(INFO) << bthread_self() << " class set function test: " << str;
    EXPECT_EQ(result_vec[idx], atoi(str)); });

  for (int i = 0; i < vec_sz; ++i)
  {
    EXPECT_EQ(result_vec[i], atoi(str_array[i]));
  }
}

TEST(ParallelTask, TestClassSet)
{
  const char *str_array[] = {
      "77",
      "66",
      "44",
      "22947",
      "20040922",
      "23951",
      "12345678",
  };
  int vec_sz = sizeof(str_array) / sizeof(str_array[0]);
  CTest t;

  std::set<const char *> test_string_set;
  for (int idx = 0; idx < vec_sz; ++idx)
  {
    test_string_set.insert(str_array[idx]);
  }

  std::vector<int> result_vec_set = comm_block::parallel_for(test_string_set.begin(), test_string_set.end(), 2, [&](const char *str, std::size_t idx)
                                                             {
    int ret = t.int_return(str);
    EXPECT_EQ(ret, atoi(str));
    LOG(INFO) << bthread_self() << " class set function test: " << str;
    return ret; });

  std::size_t i = 0;
  for (auto iter = test_string_set.begin(); iter != test_string_set.end(); ++i, ++iter)
  {
    EXPECT_EQ(result_vec_set[i], atoi(*iter));
  }
}

TEST(ParallelTask, TestLambda)
{
  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  comm_block::parallel_for(a.begin(), a.end(), 3, [&](int &value, std::size_t idx, int test_int, const char *test_str)
                           {
      LOG(INFO) << bthread_self() << " parallel test: " << value << " " << test_int << " " << test_str;
      value = value * value; }, 1, "hello, world");

  for (size_t i = 0; i < a.size(); ++i)
  {
    EXPECT_EQ(a[i], (i + 1) * (i + 1));
  }
  std::for_each(a.begin(), a.end(), [](int value)
                { LOG(INFO) << bthread_self() << " parallel result: " << value; });
}