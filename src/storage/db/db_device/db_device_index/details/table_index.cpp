#include "../table_index.h"

namespace xforce { namespace fooskv {

TableIndex::TableIndex() :
  table_index_buckets_(NULL),
  tid_recovery_(0) {}

bool TableIndex::Init(
    const Config& config, 
    NoTable no_table, 
    const std::string& name_table,
    size_t num_buckets,
    bool* end) {
  config_ = &config;
  no_table_ = no_table;
  name_table_ = &name_table;
  num_buckets_ = num_buckets;
  end_ = end;

  table_index_buckets_ = new TableIndexBucket [num_buckets];
  for (size_t i=0; i<num_buckets_; ++i) {
    table_index_buckets_[i] = NULL;
  }
  int ret = pthread_create(&tid_recovery_, NULL, );
  return true;

  ERROR_HANDLE:
  return false;
}

bool TableIndex::Dump() {
  for (size_t i=0; i<num_buckets_; ++i) {
    if (NULL != table_index_buckets_[i] && !table_index_buckets_[i]->Dump()) {
      return false;
    }
  }
  return true;
}

TableIndex::~TableIndex() {
  for (size_t i=0; i<num_buckets_; ++i) {
    XFC_DELETE(table_index_buckets_[i])
  }
  XFC_DELETE_ARRAY(table_index_buckets_)

  if (0 != tid_recovery_) {
    pthread_join(tid_recovery_, NULL);
  }
}

void TableIndex::ActivateBucket_(size_t i) {
  lock_.Lock();
  recovery_queue_.push(i);
  lock_.Unlock();
}

int TableIndex::RecoverBucket_() {
  lock_.Lock();
  if (0 == recovery_queue_.size()) {
    for (size_t i=0; i<num_buckets_; ++i) {
      if (NULL == table_index_buckets_[i]) {
        recovery_queue_.push(i);
        break;
      }
    }

    if (0 == recovery_queue_.size()) {
      return 1;
    }
  }

  size_t bucket_to_pop = recovery_queue_.front();
  recovery_queue_.pop();
  lock_.Unlock();

  XFC_NEW(table_index_buckets_[i], TableIndexBucket)
  return table_index_buckets_[i]->Init(
      *config_, 
      no_table_, 
      *name_table_, 
      bucket_to_pop, 
      end_) ? 0 : -1;
}

void* TableIndex::Recovery_(void* arg) {
  Self* self = RCAST<Self*>(arg);
  while (true) {
    int ret = RecoverBucket_();
    if (0!=ret) {
      break;
    }
  }
  return NULL;
}

}} 
