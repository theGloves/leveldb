#include <atomic>
#include <thread>

#include "leveldb/db.h"

#include "gtest/gtest.h"

using namespace leveldb;
class LevelDBLearn : public testing::Test {
 public:
  ~LevelDBLearn() { delete db_; }

  void SetUp() override {
    options_.create_if_missing = true;
    auto ret = DB::Open(options_, db_path_.c_str(), &db_);
    ASSERT_TRUE(ret.ok()) << "open failed, err:" << ret.ToString();
  }

  void TearDown() override { Test::TearDown(); }

  DB* db_{};
  std::string db_path_{"/tmp/leveldb_test"};
  Options options_{};
};

TEST_F(LevelDBLearn, Usage) {
  Status ret;
  std::string key = "key", val = "value";
  WriteOptions woption{};
  ret = db_->Put(woption, key, val);
  ASSERT_TRUE(ret.ok());

  ReadOptions roption{};
  std::string rval{};
  ret = db_->Get(roption, key, &rval);
  ASSERT_TRUE(ret.ok());
  ASSERT_EQ(val, rval);

  ret = db_->Delete(woption, key);
  ASSERT_TRUE(ret.ok());

  ret = db_->Get(roption, key, &rval);
  ASSERT_TRUE(ret.IsNotFound()) << ret.ToString();
}

TEST_F(LevelDBLearn, Snapshot) {
  std::string key = "key";
  std::atomic_int val{1};

  WriteOptions woption{};
  auto write = [&]() {
    Status ret;
    ret = db_->Put(woption, key, std::to_string(val++));
    ASSERT_TRUE(ret.ok()) << "err:" << ret.ToString();
  };

  auto read = [db = db_, key = key](const Snapshot* snapshot) -> std::string {
    ReadOptions roption{};
    roption.snapshot = snapshot;
    std::string rval{};
    Status ret;
    ret = db->Get(roption, key, &rval);
    return rval;
  };

  write();
  db_->Put(woption, "aaaa", "12341234");
  auto snapshot = db_->GetSnapshot();
  std::string rval;
  rval = read(snapshot);
  ASSERT_EQ(rval, "1");
  for (int i = 0; i < 10; ++i) {
    write();
  }
  rval = read(snapshot);
  ASSERT_EQ(rval, "1");
}

TEST_F(LevelDBLearn, SnapshotWithHoleSeq) {
  std::string key1 = "key11";
  std::string key2 = "key21";

  WriteOptions woption{};
  woption.sync = true;

  db_->Put(woption, key1, "val1");
  auto snapshot1 = db_->GetSnapshot();
  db_->Put(woption, key2, "val2");
  auto snapshot = db_->GetSnapshot();
  db_->Put(woption, key1, "val2");


  std::string rval{};
  ReadOptions roption{};
  roption.snapshot = snapshot;
  db_->Get(roption, key1, &rval);

  ASSERT_EQ("val2", rval);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}