/**
 * extendible_hash_test.cpp
 */

#include <memory>
#include <thread>  // NOLINT

#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"

namespace bustub {

TEST(ExtendibleHashTableTest, SampleTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  table->Insert(1, "a");
  table->Insert(2, "b");
  table->Insert(3, "c");
  table->Insert(4, "d");
  table->Insert(5, "e");
  table->Insert(6, "f");
  table->Insert(7, "g");
  table->Insert(8, "h");
  table->Insert(9, "i");
  // test update
  table->Insert(9, "j");
  EXPECT_EQ(2, table->GetLocalDepth(0));
  EXPECT_EQ(3, table->GetLocalDepth(1));
  EXPECT_EQ(2, table->GetLocalDepth(2));
  EXPECT_EQ(2, table->GetLocalDepth(3));

  std::string result;
  table->Find(9, result);
  EXPECT_EQ("j", result);
  table->Find(8, result);
  EXPECT_EQ("h", result);
  table->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_FALSE(table->Find(10, result));

  EXPECT_TRUE(table->Remove(8));
  EXPECT_FALSE(table->Find(8, result));
  EXPECT_TRUE(table->Remove(4));
  EXPECT_TRUE(table->Remove(1));
  EXPECT_FALSE(table->Remove(20));

  table->Insert(11, "a");
  table->Insert(12, "b");
  table->Insert(13, "c");
  table->Insert(14, "d");
  table->Insert(15, "e");
  table->Insert(16, "f");
  table->Insert(17, "g");
  table->Insert(18, "h");
  table->Find(13, result);
  EXPECT_EQ("c", result);
  std::cout << table->GetNumBuckets() << '\n';
}

TEST(ExtendibleHashTableTest, Sample1Test) {
  auto table = std::make_unique<ExtendibleHashTable<int, int>>(4);
  table->Insert(4, 4);
  table->Insert(12, 12);
  table->Insert(16, 16);
  table->Insert(64, 64);
  table->Insert(5, 5);

  table->Insert(10, 10);
  table->Insert(51, 51);
  table->Insert(15, 15);
  table->Insert(18, 18);
  table->Insert(20, 20);
  table->Insert(7, 7);
  table->Insert(21, 21);

  ASSERT_EQ(2, table->GetLocalDepth(7));
}

TEST(ExtendibleHashTableTest, Sample2Test) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);
  table->Insert(0, "a");
  table->Insert(2, "b");
  table->Insert(4, "c");

  std::string result;
  table->Find(0, result);
  EXPECT_EQ("a", result);
  table->Find(2, result);
  EXPECT_EQ("b", result);
  table->Find(4, result);
  EXPECT_EQ("c", result);
}

TEST(ExtendibleHashTableTest, ConcurrentInsertTest) {
  const int num_runs = 50;
  const int num_threads = 100;

  // Run concurrent test multiple times to guarantee correctness.

  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(5);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() { table->Insert(tid, tid); });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_TRUE(table->Find(i, val));
      EXPECT_EQ(i, val);
    }
    threads.clear();
    threads.reserve(num_threads);
    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() { table->Remove(tid); });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }
    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_FALSE(table->Find(i, val));
    }
  }
}

TEST(ExtendibleHashTableTest, HardTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, int>>(7);
  for (int i = 1; i <= 1000; ++i) {
    table->Insert(i, i);
    table->DebugPrint();
  }
  for (int i = 1; i <= 1000; ++i) {
    int val;
    ASSERT_TRUE(table->Find(i, val));
    ASSERT_EQ(val, i);
  }

  for (int i = 1; i <= 1000; ++i) {
    EXPECT_TRUE(table->Remove(i));
    table->DebugPrint();
  }
}

}  // namespace bustub
