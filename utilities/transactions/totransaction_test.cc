//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include <functional>
#include <string>
#include <thread>

#include "rocksdb/db.h"
#include "totransaction_db_impl.h"
#include "totransaction_impl.h"
#include "util/crc32c.h"
#include "util/logging.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/transaction_test_util.h"
#include "port/port.h"

using std::string;

namespace rocksdb {

class TOTransactionTest : public testing::Test {
 public:
  TOTransactionDB* txn_db;
  string dbname;
  Options options;
  TOTransactionDBOptions txndb_options;
  TOTransactionOptions txn_options;

  TOTransactionTest() {
    options.create_if_missing = true;
    options.max_write_buffer_number = 2;
    dbname = /*test::TmpDir() +*/ "./totransaction_testdb";

    DestroyDB(dbname, options);
    Open();
  }
  ~TOTransactionTest() {
    delete txn_db;
    DestroyDB(dbname, options);
  }

  void Reopen() {
    delete txn_db;
    txn_db = nullptr;
    Open();
  }

private:
  void Open() {
    Status s = TOTransactionDBImpl::Open(options, txndb_options, dbname, &txn_db);
    assert(s.ok());
    assert(txn_db != nullptr);
  }
};

TEST_F(TOTransactionTest, ValidateIOWithoutTimestamp) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;
  TOTransactionStat stat;

  ASSERT_OK(s);
  // txn1 test put and get
  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  s = txn->Put(Slice("foo"), Slice("bar"));
  ASSERT_OK(s);

  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;
  txn = txn_db->BeginTransaction(write_options, txn_options);

  txn->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");
  
  s = txn->Commit();
  ASSERT_OK(s);

  s = txn_db->Stat(&stat);
  ASSERT_OK(s);
  ASSERT_EQ(stat.commit_without_ts_times, 2);
  ASSERT_EQ(stat.read_without_ts_times, 2);
  ASSERT_EQ(stat.txn_commits, 2);
  ASSERT_EQ(stat.txn_aborts, 0);
  delete txn;
}

TEST_F(TOTransactionTest, ValidateIO) {
  WriteOptions write_options;
  ReadOptions read_options;
  read_options.read_timestamp = 50;
  string value;
  Status s;
  TOTransactionStat stat;

  s = txn_db->SetTimeStamp(kOldest, 10);
  ASSERT_OK(s);
  // txn1 test put and get
  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  s = txn->SetReadTimeStamp(50, 0);
  ASSERT_OK(s);

  s = txn->Put(Slice("foo"), Slice("bar"));
  ASSERT_OK(s); 
 
  s = txn->Put(Slice("key3"), Slice("value3"));
  ASSERT_OK(s); 

  s = txn->SetCommitTimeStamp(100);
  ASSERT_OK(s);
  
  // Read your write
  s = txn->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar");

  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;
  s = txn_db->Stat(&stat);
  ASSERT_OK(s);
  ASSERT_EQ(stat.commit_without_ts_times, 0);
  ASSERT_EQ(stat.read_without_ts_times, 0);
  ASSERT_EQ(stat.txn_commits, 1);
  ASSERT_EQ(stat.txn_aborts, 0);
  ASSERT_EQ(stat.read_q_walk_times, 0);
  ASSERT_EQ(stat.commit_q_walk_times, 0);

  //
  // txn2  test iterator
  s = txn_db->SetTimeStamp(kOldest, 10);

  txn = txn_db->BeginTransaction(write_options, txn_options);
  txn->SetReadTimeStamp(101, 0); 

  txn->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");
 
  s = txn->Put(Slice("foo"), Slice("bar2"));
  ASSERT_OK(s);

  s = txn->Put(Slice("key1"), Slice("value1"));
  ASSERT_OK(s);
  s = txn->Put(Slice("key2"), Slice("value2"));
  ASSERT_OK(s);
  
  s = txn->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");

  Iterator* iter = txn->GetIterator(read_options);
  ASSERT_TRUE(iter);

  iter->SeekToFirst();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());

  Slice key = iter->key();
  Slice val = iter->value();

  ASSERT_EQ(key.ToString(), "foo");
  ASSERT_EQ(val, "bar2");

  iter->Next();
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "key1");
  ASSERT_EQ(val, "value1");

  iter->Next();
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "key2");
  ASSERT_EQ(val, "value2");

  delete iter;
  
  s = txn->SetCommitTimeStamp(105);
  ASSERT_OK(s);
  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;
  s = txn_db->Stat(&stat);
  ASSERT_OK(s);
  ASSERT_EQ(stat.commit_without_ts_times, 0);
  ASSERT_EQ(stat.read_without_ts_times, 0);
  ASSERT_EQ(stat.txn_commits, 2);
  ASSERT_EQ(stat.txn_aborts, 0);
  ASSERT_EQ(stat.read_q_num, 1);
  ASSERT_EQ(stat.commit_q_num, 1);

  // txn3 test write conflict
  txn = txn_db->BeginTransaction(write_options, txn_options);
  s = txn->SetReadTimeStamp(101, 0); 
  ASSERT_OK(s);

  txn->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");
 
  s = txn->Put(Slice("key4"), Slice("value4"));
  ASSERT_OK(s);

  // Write Conflict here, there is a txn committed before
  // whose commit ts is greater than my read ts
  s = txn->Put(Slice("key1"), Slice("value1"));
  ASSERT_TRUE(s.IsBusy());

  s = txn->Rollback();
  ASSERT_OK(s);
  
  delete txn;
  s = txn_db->Stat(&stat);
  ASSERT_OK(s);
  ASSERT_EQ(stat.commit_without_ts_times, 0);
  ASSERT_EQ(stat.read_without_ts_times, 0);
  ASSERT_EQ(stat.txn_commits, 2);
  ASSERT_EQ(stat.txn_aborts, 1);
  ASSERT_EQ(stat.read_q_num, 1);
  ASSERT_EQ(stat.commit_q_num, 1);

  // txn4 
  txn = txn_db->BeginTransaction(write_options, txn_options);
  s = txn->SetReadTimeStamp(106, 0);
  ASSERT_OK(s);

  // No write conflict here
  s = txn->Put(Slice("key1"), Slice("value1"));
  ASSERT_OK(s);
  
  s = txn->SetCommitTimeStamp(110);
  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;

  // txn5 test delete
  txn = txn_db->BeginTransaction(write_options, txn_options);
  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);

  s = txn2->SetReadTimeStamp(110, 0);
  ASSERT_OK(s);
  s = txn->SetReadTimeStamp(110, 0);
  ASSERT_OK(s);

  s = txn2->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar2");

  s = txn->Delete(Slice("foo"));
  ASSERT_OK(s);

  s = txn->SetCommitTimeStamp(120);
  ASSERT_OK(s);

  s = txn->Commit();
  ASSERT_OK(s);

  // snapshot isolation
  s = txn2->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar2");

  s = txn2->SetCommitTimeStamp(121);
  ASSERT_OK(s);
  s = txn2->Commit();
  ASSERT_OK(s);

  s = txn_db->Stat(&stat);
  ASSERT_OK(s);
  ASSERT_EQ(stat.commit_without_ts_times, 0);
  ASSERT_EQ(stat.read_without_ts_times, 0);
  ASSERT_EQ(stat.txn_commits, 5);
  ASSERT_EQ(stat.txn_aborts, 1);

  delete txn;
  delete txn2;
  
}

TEST_F(TOTransactionTest, ValidateWriteConflict) {
  WriteOptions write_options;
  ReadOptions read_options;
  read_options.read_timestamp = 50;
  string value;
  Status s;

  s = txn_db->SetTimeStamp(kOldest, 10);
  ASSERT_OK(s);
  // txn1 test write conflict
  // txn1 and txn2 both modify foo
  // first update wins
  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  ASSERT_TRUE(txn->GetID() < txn2->GetID());

  s = txn->SetReadTimeStamp(50, 0);
  ASSERT_OK(s);

  s = txn->Put(Slice("foo"), Slice("bar"));
  ASSERT_OK(s); 
 
  s = txn2->Put(Slice("foo"), Slice("bar2"));
  ASSERT_TRUE(s.IsBusy()); 

  s = txn2->Rollback();
  ASSERT_OK(s);

  delete txn2;

  s = txn->SetCommitTimeStamp(100);
  ASSERT_OK(s);
  
  // Read your write
  s = txn->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar");

  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;
  // txn2  test write conflict
  // txn1 began before txn2, txn2 modified foo and commit
  // txn1 tried to modify foo
  s = txn_db->SetTimeStamp(kOldest, 10);

  txn = txn_db->BeginTransaction(write_options, txn_options);
  txn->SetReadTimeStamp(101, 0); 
  txn2 = txn_db->BeginTransaction(write_options, txn_options);
  txn2->SetReadTimeStamp(101, 0); 

  txn->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");
 
  s = txn2->Put(Slice("foo"), Slice("bar2"));
  ASSERT_OK(s);

  s = txn2->Put(Slice("key1"), Slice("value1"));
  ASSERT_OK(s);
  s = txn2->Put(Slice("key2"), Slice("value2"));
  ASSERT_OK(s);
  
  s = txn2->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");

  Iterator* iter = txn2->GetIterator(read_options);
  ASSERT_TRUE(iter);

  iter->SeekToFirst();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());

  Slice key = iter->key();
  Slice val = iter->value();

  ASSERT_EQ(key.ToString(), "foo");
  ASSERT_EQ(val, "bar2");

  iter->Next();
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "key1");
  ASSERT_EQ(val, "value1");

  iter->Next();
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "key2");
  ASSERT_EQ(val, "value2");

  delete iter;
  
  s = txn2->SetCommitTimeStamp(105);
  s = txn2->Commit();
  ASSERT_OK(s);

  s = txn->Put(Slice("foo"), Slice("bar3"));
  ASSERT_TRUE(s.IsBusy());

  s = txn->Rollback();
  ASSERT_OK(s);

  delete txn;
  delete txn2;
  // txn3 test write conflict
  txn = txn_db->BeginTransaction(write_options, txn_options);
  s = txn->SetReadTimeStamp(101, 0); 
  ASSERT_OK(s);

  txn->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");
 
  s = txn->Put(Slice("key4"), Slice("value4"));
  ASSERT_OK(s);

  // Write Conflict here, there is a txn committed before
  // whose commit ts is greater than my read ts
  s = txn->Put(Slice("key1"), Slice("value1_1"));
  ASSERT_TRUE(s.IsBusy());

  s = txn->Rollback();
  ASSERT_OK(s);
  
  delete txn;

  // txn4 
  txn = txn_db->BeginTransaction(write_options, txn_options);
  s = txn->SetReadTimeStamp(106, 0);
  ASSERT_OK(s);

  // No write conflict here
  s = txn->Put(Slice("key1"), Slice("value1"));
  ASSERT_OK(s);
  
  s = txn->SetCommitTimeStamp(110);
  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;
}

TEST_F(TOTransactionTest, ValidateIsolation) {
  WriteOptions write_options;
  ReadOptions read_options;
  read_options.read_timestamp = 50;
  string value;
  Status s;

  s = txn_db->SetTimeStamp(kOldest, 10);
  ASSERT_OK(s);
  // txn1 test snapshot isolation
  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  ASSERT_TRUE(txn->GetID() < txn2->GetID());

  s = txn->SetReadTimeStamp(50, 0);
  ASSERT_OK(s);

  s = txn->Put(Slice("A"), Slice("A-A"));
  ASSERT_OK(s); 

  s = txn->SetCommitTimeStamp(100);
  ASSERT_OK(s);

  s = txn->Commit();
  ASSERT_OK(s);
  
  RocksTimeStamp all_committed_ts;
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);

  s = txn2->Put(Slice("B"), Slice("B-B"));
  ASSERT_OK(s); 

  s = txn2->SetCommitTimeStamp(110);
  ASSERT_OK(s);

  TOTransaction* txn3 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn3);

  TOTransaction* txn4 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn4);

  s = txn3->SetReadTimeStamp(60, 0);
  ASSERT_OK(s);

  s = txn4->SetReadTimeStamp(110, 0);
  ASSERT_OK(s);

  s = txn3->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn4->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "A-A");
  
  s = txn3->Get(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn4->Get(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn2->Commit();
  ASSERT_OK(s);

  s = txn3->Get(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn4->Get(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn3->Rollback();
  ASSERT_OK(s);

  s = txn4->Rollback();
  ASSERT_OK(s);

  delete txn;
  delete txn2;
  delete txn3;
  delete txn4;

  txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  s = txn->SetReadTimeStamp(110, 0);
  ASSERT_OK(s);

  s = txn2->SetReadTimeStamp(110, 0);
  ASSERT_OK(s);

  s = txn->Put(Slice("C"), Slice("C-C"));
  ASSERT_OK(s);

  s = txn->Put(Slice("H"), Slice("H-H"));
  ASSERT_OK(s);

  s = txn->Put(Slice("J"), Slice("J-J"));
  ASSERT_OK(s);
  
  Iterator* iter = txn->GetIterator(read_options);
  ASSERT_TRUE(iter);

  iter->SeekToFirst();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());

  Slice key = iter->key();
  Slice val = iter->value();

  ASSERT_EQ(key.ToString(), "A");
  ASSERT_EQ(val, "A-A");

  iter->Next();
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "B");
  ASSERT_EQ(val, "B-B");

  s = txn2->Put(Slice("E"), Slice("E-E"));
  ASSERT_OK(s);

  s = txn2->SetCommitTimeStamp(120);
  ASSERT_OK(s);
  s = txn2->Commit();
  ASSERT_OK(s);

  iter->Next();
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "C");
  ASSERT_EQ(val, "C-C");

  s = txn->Put(Slice("D"), Slice("D-D"));
  ASSERT_OK(s);

  iter->Next();
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "D");
  ASSERT_EQ(val, "D-D");

  iter->Next();
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "H");
  ASSERT_EQ(val, "H-H");

  s = txn->Put(Slice("F"), Slice("F-F"));
  ASSERT_OK(s);

  iter->Next();
  key = iter->key();
  val = iter->value();
  ASSERT_EQ(key.ToString(), "J");
  ASSERT_EQ(val, "J-J");
  
  delete iter;
  
  s = txn->SetCommitTimeStamp(120);
  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;
  delete txn2;
}


TEST_F(TOTransactionTest, CommitTsCheck) {
  WriteOptions write_options;
  ReadOptions read_options;
  read_options.read_timestamp = 50;
  string value;
  Status s;

  s = txn_db->SetTimeStamp(kOldest, 10);
  ASSERT_OK(s);
  
  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);
  
  s = txn->SetReadTimeStamp(100, 0);
  ASSERT_OK(s);
  
  s = txn->SetCommitTimeStamp(120);
  ASSERT_OK(s);

  s = txn2->SetReadTimeStamp(100, 0);
  ASSERT_OK(s);
  
  s = txn2->SetCommitTimeStamp(130);
  ASSERT_OK(s);
  
  RocksTimeStamp all_committed_ts;
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_TRUE(s.IsNotFound());
  
  s = txn2->Commit();
  ASSERT_OK(s);
  
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(all_committed_ts, 119);

  s = txn->Commit();
  ASSERT_OK(s);
  
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 130);
  
  delete txn;
  delete txn2;
  
}

TEST_F(TOTransactionTest, CommitTsCheck2) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  s = txn_db->SetTimeStamp(kOldest, 10);
  ASSERT_OK(s);
  
  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);
  
  TOTransaction* txn3 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn3);
  
  s = txn->SetReadTimeStamp(100, 0);
  ASSERT_OK(s);
  
  s = txn->SetCommitTimeStamp(100);
  ASSERT_OK(s);

  s = txn2->SetReadTimeStamp(100, 0);
  ASSERT_OK(s);
  
  s = txn2->SetCommitTimeStamp(120);
  ASSERT_OK(s);
  
  s = txn3->SetReadTimeStamp(100, 0);
  ASSERT_OK(s);
  
  s = txn3->SetCommitTimeStamp(130);
  ASSERT_OK(s);
  
  RocksTimeStamp all_committed_ts;
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_TRUE(s.IsNotFound());
  s = txn->Put(Slice("1"), Slice("1"));
  ASSERT_TRUE(s.ok());
  
  s = txn->Commit();
  ASSERT_TRUE(s.ok());
  
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 100);
  
  s = txn3->Put(Slice("3"), Slice("3"));
  ASSERT_TRUE(s.ok());
  
  s = txn3->Commit();
  ASSERT_OK(s);
  
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 119);
  
  s = txn2->Put(Slice("2"), Slice("2"));
  ASSERT_TRUE(s.ok());
  
  s = txn2->Commit();
  ASSERT_OK(s);
  
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 130);
  
  delete txn;
  delete txn2;
  delete txn3;
  
}

//no put
TEST_F(TOTransactionTest, CommitTsCheck3) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  s = txn_db->SetTimeStamp(kOldest, 10);
  ASSERT_OK(s);
  
  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);
  
  TOTransaction* txn3 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn3);
  
  s = txn->SetReadTimeStamp(100, 0);
  ASSERT_OK(s);
  
  s = txn->SetCommitTimeStamp(100);
  ASSERT_OK(s);

  s = txn2->SetReadTimeStamp(100, 0);
  ASSERT_OK(s);
  
  s = txn2->SetCommitTimeStamp(120);
  ASSERT_OK(s);
  
  s = txn3->SetReadTimeStamp(100, 0);
  ASSERT_OK(s);
  
  s = txn3->SetCommitTimeStamp(130);
  ASSERT_OK(s);
  
  RocksTimeStamp all_committed_ts;
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_TRUE(s.IsNotFound());
  
  s = txn->Commit();
  ASSERT_TRUE(s.ok());
  
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 100);
  
  ASSERT_TRUE(s.ok());
  
  s = txn3->Commit();
  ASSERT_OK(s);
  
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 119);
  
  ASSERT_TRUE(s.ok());
  
  s = txn2->Commit();
  ASSERT_OK(s);
  
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 130);
  
  delete txn;
  delete txn2;
  delete txn3;
  
}

TEST_F(TOTransactionTest, CommitTsCheck4) {
  WriteOptions write_options;
  ReadOptions read_options;
  RocksTimeStamp all_committed_ts;
  string value;
  Status s;

  TOTransaction* txn_ori = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn_ori);
  s = txn_ori->SetCommitTimeStamp(4);
  ASSERT_OK(s);
  s = txn_ori->Commit();
  ASSERT_OK(s);

  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 4);

  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);
  
  TOTransaction* txn3 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn3);
 
  TOTransaction* txn4 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn4);

  s = txn->SetReadTimeStamp(all_committed_ts, 0); 
  ASSERT_OK(s);

  s = txn2->SetCommitTimeStamp(5);
  ASSERT_OK(s);
  
  s = txn3->SetCommitTimeStamp(6);
  ASSERT_OK(s);
  
  s = txn2->Commit();
  ASSERT_OK(s);
  s = txn3->Commit();
  ASSERT_OK(s);

  s = txn4->SetCommitTimeStamp(6);
  ASSERT_OK(s);
  s = txn4->Commit();
  ASSERT_OK(s);

  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 6);

  delete txn_ori;  
  delete txn;
  delete txn2;
  delete txn3;
  delete txn4;
}

TEST_F(TOTransactionTest, Rollback) {
  WriteOptions write_options;
  ReadOptions read_options;
  RocksTimeStamp all_committed_ts;
  string value;
  Status s;

  TOTransaction* txn_ori = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn_ori);
  s = txn_ori->SetCommitTimeStamp(4);
  ASSERT_OK(s);
  s = txn_ori->Commit();
  ASSERT_OK(s);

  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 4);

  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);
  
  TOTransaction* txn3 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn3);
 
  TOTransaction* txn4 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn4);

  s = txn->SetReadTimeStamp(all_committed_ts, 0); 
  ASSERT_OK(s);

  s = txn->SetCommitTimeStamp(5);
  ASSERT_OK(s);

  s = txn2->SetCommitTimeStamp(6);
  ASSERT_OK(s);
  
  s = txn3->SetCommitTimeStamp(7);
  ASSERT_OK(s);
  
  s = txn2->Commit();
  ASSERT_OK(s);
  s = txn3->Commit();
  ASSERT_OK(s);

  s = txn4->SetCommitTimeStamp(8);
  ASSERT_OK(s);
  s = txn4->Commit();
  ASSERT_OK(s);

  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 4);

  s = txn->Rollback();
  ASSERT_OK(s);

  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 8);

  delete txn_ori;  
  delete txn;
  delete txn2;
  delete txn3;
  delete txn4;
}

TEST_F(TOTransactionTest, AdvanceTSAndCleanInLock) {
  WriteOptions write_options;
  ReadOptions read_options;
  RocksTimeStamp all_committed_ts;
  RocksTimeStamp maxToCleanTs = 0;
  string value;
  Status s;

  s = txn_db->SetTimeStamp(kOldest, 3);
  ASSERT_OK(s);
  TOTransaction* txn_ori = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn_ori);
  s = txn_ori->SetCommitTimeStamp(6);
  ASSERT_OK(s);
  s = txn_ori->Commit();
  ASSERT_OK(s);

  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 6);

  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);
  
  TOTransaction* txn3 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn3);
 
  TOTransaction* txn4 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn4);

  s = txn->SetReadTimeStamp(0, 0); 
  ASSERT_TRUE(s.IsInvalidArgument());

  s = txn->SetReadTimeStamp(3, 0);
  ASSERT_OK(s);

  s = txn->SetCommitTimeStamp(8);
  ASSERT_OK(s);

  s = txn2->SetReadTimeStamp(5, 0);
  ASSERT_OK(s);  

  s = txn2->SetCommitTimeStamp(7);
  ASSERT_OK(s);
  
  s = txn3->SetReadTimeStamp(6, 0);
  ASSERT_OK(s);  

  s = txn3->SetCommitTimeStamp(9);
  ASSERT_OK(s);
  
  s = txn2->Commit();
  ASSERT_OK(s);
  ((TOTransactionDBImpl*)txn_db)->AdvanceTS(&maxToCleanTs);

  ASSERT_EQ(maxToCleanTs, 3);
  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 7);

  s = txn->Commit();
  ASSERT_OK(s);
  ((TOTransactionDBImpl*)txn_db)->AdvanceTS(&maxToCleanTs);
  ASSERT_EQ(maxToCleanTs, 3);

  s = txn_db->SetTimeStamp(kOldest, 7);
  ASSERT_OK(s);

  ((TOTransactionDBImpl*)txn_db)->AdvanceTS(&maxToCleanTs);
  ASSERT_EQ(maxToCleanTs, 6);
  
  s = txn3->Commit();
  ASSERT_OK(s);

  ((TOTransactionDBImpl*)txn_db)->AdvanceTS(&maxToCleanTs);
  ASSERT_EQ(maxToCleanTs, 7);

  s = txn4->SetCommitTimeStamp(10);
  ASSERT_OK(s);
  s = txn4->SetCommitTimeStamp(11);
  ASSERT_OK(s);
  s = txn4->Commit();
  ASSERT_OK(s);

  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 11);

  delete txn_ori;  
  delete txn;
  delete txn2;
  delete txn3;
  delete txn4;
}

TEST_F(TOTransactionTest, ThreadsTest) {
	
  WriteOptions write_options;
  ReadOptions read_options;
  string value;

  std::vector<TOTransaction*> txns(31);

  for (uint32_t i = 0; i < 31; i++) {
    txns[i] = txn_db->BeginTransaction(write_options, txn_options);
	ASSERT_TRUE(txns[i]);
	auto s = txns[i]->SetCommitTimeStamp(i+1);
    ASSERT_OK(s);
  }
  
  std::vector<port::Thread> threads;
  for (uint32_t i = 0; i < 31; i++) {
    std::function<void()> blocking_thread = [&, i] {
      auto s = txns[i]->Put(ToString(i + 1), ToString(i + 1));
      ASSERT_OK(s);
	  
	  //printf("threads %d\n",i);
	  s = txns[i]->Commit();
	  ASSERT_OK(s);
      delete txns[i];
    };
    threads.emplace_back(blocking_thread);
  }
  
  //printf("start to join\n");
	
  for (auto& t : threads) {
    t.join();
  }
}

TEST_F(TOTransactionTest, MultiCommitTs) {
  WriteOptions write_options;
  ReadOptions read_options;
  RocksTimeStamp all_committed_ts;
  string value;
  Status s;

  TOTransaction* txn_ori = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn_ori);
  s = txn_ori->SetCommitTimeStamp(4);
  ASSERT_OK(s);
  s = txn_ori->Commit();
  ASSERT_OK(s);
  delete txn_ori;

  s = txn_db->QueryTimeStamp(kAllCommitted, &all_committed_ts);
  ASSERT_OK(s);
  ASSERT_EQ(all_committed_ts, 4);

  TOTransaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn != nullptr);

  s = txn->SetCommitTimeStamp(6); 
  ASSERT_OK(s);

  // commit ts can not set back
  s = txn->SetCommitTimeStamp(5); 
  ASSERT_FALSE(s.ok());
  s = txn->Put("a", "aa");
  ASSERT_OK(s);
  s = txn->SetCommitTimeStamp(7); 
  ASSERT_OK(s);
  s = txn->Put("b", "bb");
  ASSERT_OK(s);
  ASSERT_OK(txn->Commit());
  delete txn;

  TOTransaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2 != nullptr);
  txn2->SetReadTimeStamp(7, 0);
  ASSERT_OK(txn2->Get(read_options, "b", &value));
  ASSERT_EQ(value, "bb");
  ASSERT_OK(txn2->Get(read_options, "a", &value));
  ASSERT_EQ(value, "aa");
  delete txn2;

  TOTransaction* txn3 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn3 != nullptr);
  txn3->SetReadTimeStamp(6, 0);
  ASSERT_OK(txn3->Get(read_options, "a", &value));
  ASSERT_EQ(value, "aa");
  ASSERT_FALSE(txn3->Get(read_options, "b", &value).ok());
  delete txn3;

  TOTransaction* txn4 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn4 != nullptr);
  txn4->SetReadTimeStamp(4, 0);
  ASSERT_FALSE(txn4->Get(read_options, "b", &value).ok());
  ASSERT_FALSE(txn4->Get(read_options, "a", &value).ok());
  delete txn4;
}

TEST_F(TOTransactionTest, MemUsage) {
  auto db_imp = dynamic_cast<TOTransactionDBImpl*>(txn_db);
  db_imp->SetMaxConflictBytes(25);
  WriteOptions write_options;
  ReadOptions read_options;
  TOTransactionStat stat;

  TOTransaction* txn = db_imp->BeginTransaction(write_options, txn_options);
  // 11
  ASSERT_OK(txn->Put("abc", "abc"));
  memset(&stat, 0, sizeof stat);
  db_imp->Stat(&stat);
  ASSERT_EQ(stat.uk_num, 1);
  ASSERT_EQ(stat.cur_conflict_bytes, 11);
  // 11+12=23
  ASSERT_OK(txn->Put("defg", "defg"));
  memset(&stat, 0, sizeof stat);
  db_imp->Stat(&stat);
  ASSERT_EQ(stat.uk_num, 2);
  ASSERT_EQ(stat.cur_conflict_bytes, 23);
  auto s = txn->Put("h", "h");
  ASSERT_FALSE(s.ok());
  db_imp->SetMaxConflictBytes(50);
  // 23+9=32
  ASSERT_OK(txn->Put("h", "h"));
  memset(&stat, 0, sizeof stat);
  db_imp->Stat(&stat);
  ASSERT_EQ(stat.uk_num, 3);
  ASSERT_EQ(stat.cur_conflict_bytes, 32);
  ASSERT_OK(txn->Commit());
  memset(&stat, 0, sizeof stat);
  db_imp->Stat(&stat);
  ASSERT_EQ(stat.uk_num, 0);
  ASSERT_EQ(stat.ck_num, 3);
  ASSERT_EQ(stat.cur_conflict_bytes, 3+16+4+16+1+16);
  delete txn;
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(
      stderr,
      "SKIPPED as optimistic_transaction is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
