// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/cassandra/cassandra_compaction_filter.h"

#include <string>

#include "rocksdb/slice.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "utilities/cassandra/format.h"
#include "utilities/cassandra/merge_operator.h"

namespace ROCKSDB_NAMESPACE {
namespace cassandra {
static std::unordered_map<std::string, OptionTypeInfo>
    cassandra_filter_type_info = {
#ifndef ROCKSDB_LITE
        {"purge_ttl_on_expiration",
         {offsetof(struct CassandraOptions, purge_ttl_on_expiration),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"gc_grace_period_in_seconds",
         {offsetof(struct CassandraOptions, gc_grace_period_in_seconds),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
#endif  // ROCKSDB_LITE
};

const char* CassandraCompactionFilter::Name() const {
  return "CassandraCompactionFilter";
}

CassandraCompactionFilter::CassandraCompactionFilter(
    bool purge_ttl_on_expiration, int32_t gc_grace_period_in_seconds)
    : options_(gc_grace_period_in_seconds, 0, purge_ttl_on_expiration) {
  RegisterOptions(&options_, &cassandra_filter_type_info);
}

void CassandraCompactionFilter::SetMetaCfHandle(
    DB* meta_db, ColumnFamilyHandle* meta_cf_handle) {
  meta_db_ = meta_db;
  meta_cf_handle_ = meta_cf_handle;
}

PartitionDeletion CassandraCompactionFilter::GetPartitionDelete(
    const Slice& key) const {
  if (!meta_db_) {
    // skip triming when parition meta db is not ready yet
    return PartitionDeletion::kDefault;
  }

  DB* meta_db = meta_db_.load();
  if (!meta_cf_handle_) {
    // skip triming when parition meta cf handle is not ready yet
    return PartitionDeletion::kDefault;
  }
  ColumnFamilyHandle* meta_cf_handle = meta_cf_handle_.load();

  auto it = unique_ptr<Iterator>(
    meta_db->NewIterator(rocksdb::ReadOptions(), meta_cf_handle));
  // partition meta key is encoded token+paritionkey
  it->SeekForPrev(key);
  if (!it->Valid()) {
    // skip trimming when
    return PartitionDeletion::kDefault;
  }

  if (!key.starts_with(it->key())) {
    // skip trimming when there is no parition meta data
    return PartitionDeletion::kDefault;
  }

  Slice value = it->value();
  return PartitionDeletion::Deserialize(value.data(), value.size());
}

bool CassandraCompactionFilter::ShouldDropByParitionDelete(
    const Slice& key,
    std::chrono::time_point<std::chrono::system_clock> row_timestamp) const {
  std::chrono::seconds gc_grace_period =
      ignore_range_delete_on_read_ ? std::chrono::seconds(0) : gc_grace_period_;
  return GetPartitionDelete(key).MarkForDeleteAt() >
         row_timestamp + gc_grace_period;
}

CompactionFilter::Decision CassandraCompactionFilter::FilterV2(
    int /*level*/, const Slice& key, ValueType value_type,
    const Slice& existing_value, std::string* new_value,
    std::string* /*skip_until*/) const {
  bool value_changed = false;
  RowValue row_value = RowValue::Deserialize(
    existing_value.data(), existing_value.size());

  if (ShouldDropByParitionDelete(key, row_value.LastModifiedTimePoint())) {
    return Decision::kRemove;
  }

  RowValue compacted =
      options_.purge_ttl_on_expiration
          ? row_value.RemoveExpiredColumns(&value_changed)
          : row_value.ConvertExpiredColumnsToTombstones(&value_changed);

  if (value_type == ValueType::kValue) {
    compacted = compacted.RemoveTombstones(options_.gc_grace_period_in_seconds);
  }

  if (compacted.Empty()) {
    return Decision::kRemove;
  }

  if (value_changed) {
    compacted.Serialize(new_value);
    return Decision::kChangeValue;
  }

  return Decision::kKeep;
}

CassandraCompactionFilterFactory::CassandraCompactionFilterFactory(
    bool purge_ttl_on_expiration, int32_t gc_grace_period_in_seconds)
    : options_(gc_grace_period_in_seconds, 0, purge_ttl_on_expiration) {
  RegisterOptions(&options_, &cassandra_filter_type_info);
}

std::unique_ptr<CompactionFilter>
CassandraCompactionFilterFactory::CreateCompactionFilter(
    const CompactionFilter::Context&) {
  std::unique_ptr<CompactionFilter> result(new CassandraCompactionFilter(
      options_.purge_ttl_on_expiration, options_.gc_grace_period_in_seconds));
  return result;
}

#ifndef ROCKSDB_LITE
int RegisterCassandraObjects(ObjectLibrary& library,
                             const std::string& /*arg*/) {
  library.AddFactory<MergeOperator>(
      CassandraValueMergeOperator::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<MergeOperator>* guard,
         std::string* /* errmsg */) {
        guard->reset(new CassandraValueMergeOperator(0));
        return guard->get();
      });
  library.AddFactory<CompactionFilter>(
      CassandraCompactionFilter::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<CompactionFilter>* /*guard */,
         std::string* /* errmsg */) {
        return new CassandraCompactionFilter(false, 0);
      });
  library.AddFactory<CompactionFilterFactory>(
      CassandraCompactionFilterFactory::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<CompactionFilterFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new CassandraCompactionFilterFactory(false, 0));
        return guard->get();
      });
  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
}
#endif  // ROCKSDB_LITE
}  // namespace cassandra
}  // namespace ROCKSDB_NAMESPACE
