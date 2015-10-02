#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

template <typename DBIndex>
class DBBase {
 private:
  typedef DBBase<DBIndex> Self;
  typedef MPSCFixedPipe<DBMsg> Mailbox;

 public:
  DBBase();

  bool Init(
      const Config& config,
      bool* end);

  virtual ~DBBase();
  
 private: 
  bool StartWriteHandler_();
  void JoinWriteHandler_();

  bool HandleWrites_();

  ErrNo AddKVBatch_(const KVBatch* kv_batch);
  ErrNo RemoveKVBatch_(const KeyBatch* key_batch);

  static void* WriteHandler_(void* args);

 private:
  bool* end_;

  Mailbox* mailbox_;
  DBIndex* db_index_;
  DBDevice* db_device_;
  pthread_t tid_writer_handler_;
};

}}

namespace xforce { namespace fooskv {

template <typename DBIndex>
DBBase<DBIndex>::DBBase() :
  mailbox_(NULL),
  db_index_(NULL),
  db_device_(NULL),
  tid_writer_handler_(0) {}

template <typename DBIndex>
bool DBBase<DBIndex>::Init(
    const Config& config,
    bool* end) {
  end_ = end;

  XFC_NEW(mailbox_, Mailbox)

  XFC_NEW(db_index_, DBIndex)
  bool ret = db_index_->Init(config);
  XFC_FAIL_HANDLE_WARN(!ret, "fail_init_db_index")

  XFC_NEW(db_device_, DBDevice)
  ret = db_device_->Init(config);
  XFC_FAIL_HANDLE_WARN(!ret, "fail_init_db_device")

  ret = StartWriteHandler_();
  XFC_FAIL_HANDLE_WARN(!ret, "fail_start_writer_handler")
  return true;

  ERROR_HANDLE:
  JoinWriteHandler_();
  return false;
}

template <typename DBIndex>
DBBase<DBIndex>::~DBBase() {
  JoinWriteHandler_();
  XFC_DELETE(db_device_)
  XFC_DELETE(db_index_)
  XFC_DELETE(mailbox_)
}

template <typename DBIndex>
bool DBBase<DBIndex>::StartWriteHandler_() {
  return 0 == pthread_create(&tid_writer_handler_, NULL, WriteHandler_, RCAST<void*>(this));
}

template <typename DBIndex>
void DBBase<DBIndex>::JoinWriteHandler_() {
  if (0 != tid_writer_handler_) {
    pthread_join(tid_writer_handler_, NULL);
    tid_writer_handler_ = 0;
  }
}

template <typename DBIndex>
bool DBBase<DBIndex>::HandleWrites_() {
  for (size_t i=0; i<100; ++i) {
    Mailbox::Msg* msg = mailbox_->ReceiveMsg();
    if (NULL==msg) {
      return 0!=i ? true : false;
    }

    DBMsg& db_msg = msg->msg_header;
    switch (db_msg.no_cmd) {
      case AddKVMsg::kCmd : {
      }
      case RemoveKVMsg::kCmd : {
      }
      default : {
      }
    }

    mailbox_->MsgConsumed();
  }
  return true;
}

template <typename DBIndex>
ErrNo DBBase<DBIndex>::AddKVBatch_(const KVBatch* kv_batch) {
}

template <typename DBIndex>
ErrNo DBBase<DBIndex>::RemoveKVBatch_(const KeyBatch* key_batch) {
}

template <typename DBIndex>
void* DBBase<DBIndex>::WriteHandler_(void* args) {
  Self* self = RCAST<Self*>(args);
  while (!*(self->end_)) {
    bool ret = self->HandleWrites_();
    if (!ret) {
      usleep(10*1000);
    }
  }
  self->HandleWrites_();
}

}}
