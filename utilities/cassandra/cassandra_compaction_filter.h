// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <atomic>
#include <string>

#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "utilities/cassandra/format.h"
#include "utilities/cassandra/cassandra_options.h"

namespace ROCKSDB_NAMESPACE {
namespace cassandra {

/**
 * Compaction filter for removing expired/deleted Cassandra data.
 *
 * If option `purge_ttl_on_expiration` is set to true, expired data
 * will be directly purged. Otherwise expired data will be converted
 * tombstones first, then be eventally removed after gc grace period.
 * `purge_ttl_on_expiration` should only be on in the case all the
 * writes have same ttl setting, otherwise it could bring old data back.
 *
 * If option `ignore_range_tombstone_on_read` is set to true, when client
 * care more about disk space releasing and not what would be read after
 * range/partition, we will drop deleted data more aggressively without
 * considering gc grace period.
 *
 */
class CassandraCompactionFilter : public CompactionFilter {
 public:
  explicit CassandraCompactionFilter(bool purge_ttl_on_expiration,
                                     int32_t gc_grace_period_in_seconds);
  static const char* kClassName() { return "CassandraCompactionFilter"; }
  const char* Name() const override { return kClassName(); }

  virtual Decision FilterV2(int level, const Slice& key, ValueType value_type,
                            const Slice& existing_value, std::string* new_value,
                            std::string* skip_until) const override;

   void SetMetaCfHandle(DB* meta_db, ColumnFamilyHandle* meta_cf_handle);
 private:
  CassandraOptions options_;
};

class CassandraCompactionFilterFactory : public CompactionFilterFactory {
 public:
  explicit CassandraCompactionFilterFactory(bool purge_ttl_on_expiration,
                                            int32_t gc_grace_period_in_seconds);
  ~CassandraCompactionFilterFactory() override {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override;
  static const char* kClassName() { return "CassandraCompactionFilterFactory"; }
  const char* Name() const override { return kClassName(); }

 private:
  CassandraOptions options_;
};
}  // namespace cassandra
}  // namespace ROCKSDB_NAMESPACE
