// MIT License

// Copyright (c) 2021 ByteDance Inc. All rights reserved.
// Copyright (c) 2021 Duke University.  All rights reserved.

// See LICENSE for license information

#ifndef RDMA_CONTEXT_HPP
#define RDMA_CONTEXT_HPP
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <vector>

#include "endpoint.hpp"
#include "helper.hpp"
#include "memory.hpp"

namespace Collie {

union collie_cq {
  struct ibv_cq *cq;
  struct ibv_cq_ex *cq_ex;
};

class rdma_context {
 private:
  void *master_ = nullptr;
  // For connection setup (Local infomation)
  std::string devname_;
  union ibv_gid local_gid_;
  std::string local_ip_;
  struct ibv_context *ctx_ = nullptr;
  uint16_t lid_;
  uint8_t sl_;
  int port_;  // tcp port for server
  uint64_t completion_timestamp_mask_;

  // Memory Management
#ifdef GDR
  CUdevice cuDevice_;
  CUcontext cuContext_;
#endif
  std::vector<struct ibv_pd *> pds_;
  std::vector<std::vector<rdma_region *>> local_mempool_ =
      std::vector<std::vector<rdma_region *>>(2);
  std::vector<std::vector<rdma_buffer *>> remote_mempools_;
  std::mutex rmem_lock_;
  // For each remote host, we have a single mempool for it

  // Transportation
  std::vector<union collie_cq> send_cqs_;
  std::vector<union collie_cq> recv_cqs_;
  // For hardware timestamp
  std::vector<uint64_t> nic_process_time_;

  std::vector<rdma_endpoint *> endpoints_;
  std::vector<int> request_size_;
  std::queue<int> ids_;
  int num_of_hosts_ = 0;  // How many hosts to set up connections
  int num_per_host_ = 0;  // How many connections each host will set
  bool share_cq_ = false;
  bool share_pd_ = false;
  enum ibv_wr_opcode opcode_ = IBV_WR_RDMA_WRITE;

  int num_of_recv_ = 0;
  std::mutex numlock_;

  bool _print_thp;
  uint32_t current_buf_id_ = 0;
  rdma_buffer *CreateBufferFromInfo(struct connect_info *info);
  void SetInfoByBuffer(struct connect_info *info, rdma_buffer *buf);
  void SetEndpointInfo(rdma_endpoint *endpoint, struct connect_info *info);
  void GetEndpointInfo(rdma_endpoint *endpoint, struct connect_info *info);

  // Basic Initialization: connection setup info.
  int InitDevice();
  int InitIds();

  // Memory Management and Transportation Allocation
#ifdef GDR
  int InitCuda();
#endif
  int InitMemory();
  int InitTransport();

  int ConnectionSetup(const char *server, int port);
  int AcceptHandler(int connfd);

  int PollEach(struct ibv_cq *cq);
  int PollEachEx(struct ibv_cq_ex *cq_ex);
  int ParseEachEx(struct ibv_cq_ex *cq_ex);
  int PollCompletion();

  std::string GidToIP(
      const union ibv_gid &gid);  // Translate local gid to a IP string.
  std::vector<rdma_request> ParseReqFromStr();
  std::vector<rdma_request> ParseRecvFromStr();

 public:
  rdma_context(const char *dev_name, int gid_idx, int num_of_hosts,
               int num_per_host, bool print_thp)
      : devname_(dev_name),
        num_of_hosts_(num_of_hosts),
        num_per_host_(num_per_host),
        _print_thp(print_thp) {}

  int Init();

  // Connection Setup: Server side
  int Listen();
  int ServerDatapath();

  // Connection Setup: Client side
  int Connect(const char *server, int port, int connid);
  int ClientDatapath();

  // Assitant function: Randomly choose a buffer
  // 0 indicates send buffer
  // 1 indicates recv buffer
  rdma_buffer *PickNextBuffer(int idx) {
    if (idx != 0 && idx != 1) return nullptr;
    if (local_mempool_[idx].empty()) return nullptr;
    auto buf = local_mempool_[idx][current_buf_id_]->GetBuffer();
    current_buf_id_++;
    if (current_buf_id_ == local_mempool_[idx].size()) current_buf_id_ = 0;
    return buf;
  }

  struct ibv_cq *GetSendCq(int id) {
    if (share_cq_) id = 0;
    if (FLAGS_hw_ts)
      return ibv_cq_ex_to_cq(send_cqs_[id].cq_ex);
    else
      return send_cqs_[id].cq;
  }
  struct ibv_cq *GetRecvCq(int id) {
    if (share_cq_) id = 0;
    if (FLAGS_hw_ts)
      return ibv_cq_ex_to_cq(recv_cqs_[id].cq_ex);
    else
      return recv_cqs_[id].cq;
  }
  struct ibv_pd *GetPd(int id) {
    if (share_pd_) return pds_[0];
    return pds_[id];
  }
  std::string GetIp() { return local_ip_; }
};
}  // namespace Collie

#endif
