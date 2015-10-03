#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

template <typename DBIndex>
class DBBase {
 private:
  typedef DBBase<DBIndex> Self;
  typedef MPSCFixedPipe<DBCmd*> Mailbox;

 public:
  DBBase();

  bool Init(
      const Config& config,
      bool* end);

  void SendWriteCmd(DBCmd& db_cmd);

  virtual ~DBBase();
  
 private: 
  bool StartWriteHandler_();
  void JoinWriteHandler_();

  bool HandleWrites_();

  inline ErrNo ModifyKVs_(const CmdModify& cmd_modify, LogicTime logic_time);
  inline ErrNo AddKV_(NoTable no_table, const KV& kv, LogicTime logic_time);
  inline ErrNo RemoveKV_(NoTable no_table, const KVBatch& kv_batch, LogicTime logic_time);

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
void DBBase<DBIndex>::SendWriteCmd(DBCmd& db_cmd) {
  db_cmd.endOfWriteCmd = false;

  if (mailbox_->SendMsg(&db_cmd)) {
    return;
  }

  size_t num_trials = 0;
  while (!mailbox_->SendMsg(&db_cmd)) {
    if (++num_trials % 500) {
      WARN("fail_send_write_cmd");
    }
    usleep(10);
  }

  num_trials = 0;
  while (!(*db_cmd.endOfWriteCmd)) {
    if (++num_trials % 500) {
      WARN("wait_for_end_of_write_cmd_for_too_long");
    }
    usleep(1);
  }
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

    DBCmd& db_cmd = *(msg->msg_header);
    switch (db_cmd.no_cmd.code) {
      case CmdModify::kCmd : {
        db_cmd.g_errno = ModifyKVs_(*(db_cmd.body.cmd_modify));
        break;
      }
      default : {
        FATAL("unknown_db_base_cmd[" << db_cmd.no_cmd << "]");
        break;
      }
    }
    mailbox_->MsgConsumed();
  }
  return true;
}

template <typename DBIndex>
ErrNo DBBase<DBIndex>::ModifyKVs_(const CmdModify& cmd_modify, LogicTime logic_time) {
  NoTable no_table = cmd_modify.req.no_table;
  const KVBatch& kv_batch = *(cmd_modify.req.kv_batch);
  ErrNo* errno = cmd_modify.resp.errno;
  switch (cmd_modify.category) {
    case CmdModify::kAdd : {
      for (size_t i=0; i < kv_batch.num; ++i) {
        errno[i] = AddKV_(no_table, kv_batch.kvs[i], logic_time+i);
      }
      break;
    }
    case CmdModify::kRemove : {
      for (size_t i=0; i < kv_batch.num; ++i) {
        errno[i] = RemoveKV_(no_table, kv_batch.kvs[i], logic_time+i);
      }
      break;
    }
    case CmdModify::kUpdate : {
      for (size_t i=0; i < kv_batch.num; ++i) {
        errno[i] = RemoveKV_(no_table, kv_batch.kvs[i], logic_time+i);
      }

      for (size_t i=0; i < kv_batch.num; ++i) {
        if (ErrNo::kOK == errno[i]) {
          errno[i] = AddKV_(no_table, kv_batch.kvs[i]);
        }
      }
      break;
    }
    default : {
      break;
    }
  }
  return ErrNo::kOK;
}

template <typename DBIndex>
ErrNo DBBase<DBIndex>::AddKV_(NoTable no_table, const KV& kv, LogicTime logic_time) {
  ErrNo errno = db_device_->Add(no_table, kv, logic_time);
  if (ErrNo::kOK != errno) {
    return errno;
  }
  return db_index_->Add(no_table, kv, logic_time);
}

template <typename DBIndex>
ErrNo DBBase<DBIndex>::RemoveKV_(NoTable no_table, const KV& kv, LogicTime logic_time) {
  ErrNo errno = db_index_->Remove(no_table, kv_batch, logic_time);
  if (ErrNo::kOK != errno) {
    return errno;;
  }
  return db_device_->Remove(no_table, kv_batch, logic_time);
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
