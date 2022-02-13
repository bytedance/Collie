// MIT License

// Copyright (c) 2021 ByteDance Inc. All rights reserved.
// Copyright (c) 2021 Duke University.  All rights reserved.

// See LICENSE for license information

#ifndef RDMA_ENDPOINT_HPP
#define RDMA_ENDPOINT_HPP
#include <queue>

#include "helper.hpp"
#include "memory.hpp"

namespace Collie {

class rdma_request {
 public:
  enum ibv_wr_opcode opcode;  // Opcode of this request
  int sge_num;                // sge_num of this request
  std::vector<struct ibv_sge> sglist;
};

class rdma_endpoint {
 private:
  struct ibv_qp *qp_ = nullptr;
  uint32_t id_ = 0;
  enum ibv_qp_type qp_type_;
  union ibv_gid remote_gid_;
  uint32_t send_credits_ = 0;
  uint32_t recv_credits_ = 0;
  // Remote Information
  std::string remote_server_;
  uint32_t remote_qpn_ = 0;
  // Remote info for UD
  uint16_t dlid_ = 0;
  uint8_t remote_sl_ = 0;
  // Remote memory pool id
  int rmem_id_ = -1;

  std::queue<int> send_batch_size_;
  std::queue<int> recv_batch_size_;

  bool activated_ = false;
  void *master_ = nullptr;
  void *context_ = nullptr;

  // For statistics
  uint64_t bytes_sent_last_ = 0;
  uint64_t bytes_sent_now_ = 0;
  uint64_t msgs_sent_last_ = 0;
  uint64_t msgs_sent_now_ = 0;
  uint64_t timestamp_ = 0;

 public:
  rdma_endpoint(uint32_t id, ibv_qp *qp)
      : qp_(qp),
        id_(id),
        qp_type_((enum ibv_qp_type)FLAGS_qp_type),
        send_credits_(FLAGS_send_wq_depth),
        recv_credits_(FLAGS_recv_wq_depth) {}
  ~rdma_endpoint() {
    if (qp_) ibv_destroy_qp(qp_);
  }

 public:
  int PostSend(const std::vector<rdma_request> &requests, size_t &req_idx,
               uint32_t batch_size,
               const std::vector<rdma_buffer *> &remote_buffer);
  int PostRecv(const std::vector<rdma_request> &requests, size_t &req_idx,
               uint32_t batch_size);
  int Activate(const union ibv_gid &remote_gid);
  int RestoreFromERR();
  int SendHandler(struct ibv_wc *wc);
  int RecvHandler(struct ibv_wc *wc);
  void PrintThroughput(uint64_t timestamp);

  enum ibv_qp_type GetType() { return qp_type_; }
  int GetQpn() { return qp_->qp_num; }
  int GetSendCredits() { return send_credits_; }
  int GetRecvCredits() { return recv_credits_; }
  int GetMemId() { return rmem_id_; }
  bool GetActivated() { return activated_; }
  void SetQpn(int qpn) { remote_qpn_ = qpn; }
  void SetLid(int lid) { dlid_ = lid; }
  void SetSl(int sl) { remote_sl_ = sl; }
  void SetContext(void *context) { context_ = context; }
  void SetMaster(void *master) { master_ = master; }
  void SetActivated(bool state) { activated_ = state; }
  void SetMemId(int remote_mem_id) { rmem_id_ = remote_mem_id; }
  void SetServer(const std::string &name) { remote_server_ = name; }
};
}  // namespace Collie

#endif
