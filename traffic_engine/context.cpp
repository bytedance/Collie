// MIT License

// Copyright (c) 2021 ByteDance Inc. All rights reserved.
// Copyright (c) 2021 Duke University.  All rights reserved.

// See LICENSE for license information

#include "context.hpp"

#include <malloc.h>

#include <algorithm>
#include <thread>

namespace Collie {

std::vector<rdma_request> rdma_context::ParseRecvFromStr() {
  std::stringstream ss(FLAGS_receive);
  char c;
  int size;
  int sge_num;
  std::vector<rdma_request> requests;
  while (ss >> sge_num) {
    rdma_request req;
    for (int i = 0; i < sge_num; i++) {
      ss >> c >> size;
      struct ibv_sge sg;
      auto buf = PickNextBuffer(1);
      sg.addr = buf->addr_;
      sg.lkey = buf->local_K_;
      sg.length = size;
      req.sglist.push_back(sg);
    }
    req.sge_num = sge_num;
    requests.push_back(req);
  }
  return requests;
}

std::vector<rdma_request> rdma_context::ParseReqFromStr() {
  std::stringstream ss(FLAGS_request);
  char op;
  char c;
  int size;
  int sge_num;
  std::vector<rdma_request> requests;
  while (ss >> op >> c >> sge_num) {
    rdma_request req;
    if (op != 's' && FLAGS_qp_type == 4) {
      LOG(ERROR) << "UD does not support opcode other than SEND/RECV";
      exit(1);
    }
    if (op == 'r' && FLAGS_qp_type != 2) {
      LOG(ERROR) << "Only RC supports RDMA Read";
      exit(1);
    }
    switch (op) {
      case 'w':
        req.opcode = IBV_WR_RDMA_WRITE;
        if (FLAGS_imm_data) req.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
        break;
      case 'r':
        req.opcode = IBV_WR_RDMA_READ;
        break;
      case 's':
        req.opcode = IBV_WR_SEND;
        break;
      default:
        LOG(ERROR) << "Unsupported work request opcode";
        exit(1);
    }
    req.sge_num = sge_num;
    for (int i = 0; i < sge_num; i++) {
      ss >> c >> size;
      struct ibv_sge sge;
      auto buf = PickNextBuffer(0);
      sge.addr = buf->addr_;
      sge.length = size;
      sge.lkey = buf->local_K_;
      req.sglist.push_back(sge);
    }
    requests.push_back(req);
    if (ss.peek() == ',') ss.ignore();
  }
  return requests;
}

std::string rdma_context::GidToIP(const union ibv_gid &gid) {
  std::string ip =
      std::to_string(gid.raw[12]) + "." + std::to_string(gid.raw[13]) + "." +
      std::to_string(gid.raw[14]) + "." + std::to_string(gid.raw[15]);
  return ip;
}

int rdma_context::Init() {
#ifdef GDR
  if (InitCuda() < 0) {
    LOG(ERROR) << "InitCuda() failed";
    return -1;
  }
#endif
  if (InitDevice() < 0) {
    LOG(ERROR) << "InitDevice() failed";
    return -1;
  }
  if (InitMemory() < 0) {
    LOG(ERROR) << "InitMemory() failed";
    return -1;
  }
  if (InitTransport() < 0) {
    LOG(ERROR) << "InitTransport() failed";
    return -1;
  }
  return 0;
}

int rdma_context::InitDevice() {
  struct ibv_device *dev = nullptr;
  struct ibv_device **device_list = nullptr;
  int n;
  bool flag = false;
  device_list = ibv_get_device_list(&n);
  if (!device_list) {
    PLOG(ERROR) << "ibv_get_device_list() failed when initializing clients";
    return -1;
  }
  for (int i = 0; i < n; i++) {
    dev = device_list[i];
    if (!strncmp(ibv_get_device_name(dev), devname_.c_str(),
                 strlen(devname_.c_str()))) {
      flag = true;
      break;
    }
  }
  if (!flag) {
    LOG(ERROR) << "We didn't find device " << devname_ << ". So we exit.";
  }
  this->ctx_ = ibv_open_device(dev);
  if (!ctx_) {
    PLOG(ERROR) << "ibv_open_device() failed";
    return -1;
  }
  if (FLAGS_hw_ts) {
    struct ibv_device_attr_ex device_attrx;
    if (ibv_query_device_ex(ctx_, nullptr, &device_attrx)) {
      PLOG(ERROR) << "ibv_query_device_ex() failed";
      return -1;
    }
    if (!device_attrx.completion_timestamp_mask) {
      LOG(ERROR) << "This device does not support hardware timestamp mask";
      return -1;
    }
    LOG(INFO) << "The hca_clock of this device is "
              << device_attrx.hca_core_clock;
  }
  send_cqs_.clear();
  recv_cqs_.clear();
  share_pd_ = FLAGS_share_pd;
  share_cq_ = FLAGS_share_cq;
  if (ibv_query_gid(ctx_, 1, FLAGS_gid, &local_gid_) < 0) {
    PLOG(ERROR) << "ibv_query_gid() failed";
    return -1;
  }
  local_ip_ = GidToIP(local_gid_);
  auto num_of_qps = InitIds();
  endpoints_.resize(num_of_qps, nullptr);
  struct ibv_port_attr port_attr;
  memset(&port_attr, 0, sizeof(port_attr));
  if (ibv_query_port(ctx_, 1, &port_attr)) {
    PLOG(ERROR) << "ibv_query_port() failed";
    exit(1);
  }
  lid_ = port_attr.lid;
  sl_ = port_attr.sm_sl;
  port_ = FLAGS_port;
  return 0;
}
int rdma_context::InitIds() {
  while (!ids_.empty()) {
    ids_.pop();
  }
  auto num_of_qps = num_of_hosts_ * num_per_host_;
  for (int i = 0; i < num_of_qps; i++) {
    ids_.push(i);
  }
  return num_of_qps;
}

#ifdef GDR
int rdma_context::InitCuda() {
  if (FLAGS_use_cuda == false) return 0;
  int gpu_id = FLAGS_gpu_id;
  CUdevice cu_device;

  CUresult ret = cuInit(0);
  if (ret != CUDA_SUCCESS) {
    PLOG(ERROR) <<  "cuInit(0)";
    return -1;
  }

  int n = 0;
  ret = cuDeviceGetCount(&n);
  if (ret != CUDA_SUCCESS) {
    LOG(ERROR) << "cuDeviceGetCount() return " << ret;
    return -1;
  }
  if (n == 0) {
    LOG(ERROR) << "No available Cuda device";
    return -1;
  }
  if (gpu_id >= n) {
    LOG(ERROR) << "No " << gpu_id << " device";
    return -1;
  }
  ret = cuDeviceGet(&cuDevice_, gpu_id);
  if (ret != CUDA_SUCCESS) {
    LOG(ERROR) << "cuDeviceGet() failed with " << ret;
  }

  ret = cuCtxCreate(&cuContext_, CU_CTX_MAP_HOST, cuDevice_);
  if (ret != CUDA_SUCCESS) {
    LOG(ERROR) << "cuCtxCreate() failed with " << ret; 
    return -1;
  }

  ret = cuCtxSetCurrent(cuContext_);
  if (ret != CUDA_SUCCESS) {
    LOG(ERROR) << "cuCtxSetCurrent() failed with " << ret; 
    return -1;
  }
  return 0;
}
#endif

int rdma_context::InitMemory() {
  // Allocate PD
  auto pd_num = share_pd_ ? 1 : FLAGS_qp_num;
  for (int i = 0; i < pd_num; i++) {
    auto pd = ibv_alloc_pd(ctx_);
    if (!pd) {
      PLOG(ERROR) << "ibv_alloc_pd() failed";
      return -1;
    }
    pds_.push_back(pd);
  }

  auto buf_size = FLAGS_buf_size;
  // Allocate Memory and Register them
  if ((enum ibv_qp_type)FLAGS_qp_type == IBV_QPT_UD) {
    buf_size += kUdAddition;
  }
  for (int i = 0; i < FLAGS_mr_num; i++) {
    auto region =
        new rdma_region(GetPd(i), buf_size, FLAGS_buf_num, FLAGS_memalign, 0);
    if (region->Mallocate()) {
      LOG(ERROR) << "Region Memory allocation failed";
      break;
    }
    local_mempool_[0].push_back(region);
    region =
        new rdma_region(GetPd(i), buf_size, FLAGS_buf_num, FLAGS_memalign, 0);
    if (region->Mallocate()) {
      LOG(ERROR) << "Region Memory allocation failed";
      break;
    }
    local_mempool_[1].push_back(region);
  }

  // Allocate Send/Recv Completion Queue
  auto cqn = share_cq_ ? 1 : num_of_hosts_ * num_per_host_;
  for (int i = 0; i < cqn; i++) {
    union collie_cq send_cq;
    union collie_cq recv_cq;
    if (FLAGS_hw_ts) {
      struct ibv_cq_init_attr_ex send_attr_ex, recv_attr_ex;
      memset(&send_attr_ex, 0, sizeof(ibv_cq_init_attr_ex));
      memset(&recv_attr_ex, 0, sizeof(ibv_cq_init_attr_ex));
      send_attr_ex.cqe = recv_attr_ex.cqe = FLAGS_cq_depth / cqn;
      send_attr_ex.channel = recv_attr_ex.channel = nullptr;
      send_attr_ex.cq_context = recv_attr_ex.cq_context = nullptr;
      send_attr_ex.comp_vector = recv_attr_ex.comp_vector = 0;
      send_attr_ex.wc_flags = recv_attr_ex.wc_flags =
          IBV_WC_EX_WITH_COMPLETION_TIMESTAMP |
          IBV_WC_EX_WITH_COMPLETION_TIMESTAMP_WALLCLOCK;
      send_cq.cq_ex = ibv_create_cq_ex(ctx_, &send_attr_ex);
      if (!send_cq.cq_ex) {
        PLOG(ERROR) << "ibv_create_cq_ex() failed";
        return -1;
      }
      recv_cq.cq_ex = ibv_create_cq_ex(ctx_, &recv_attr_ex);
      if (!recv_cq.cq_ex) {
        PLOG(ERROR) << "ibv_create_cq_ex() failed";
        return -1;
      }
    } else {
      send_cq.cq =
          ibv_create_cq(ctx_, FLAGS_cq_depth / cqn, nullptr, nullptr, 0);
      if (!send_cq.cq) {
        PLOG(ERROR) << "ibv_create_cq() failed";
        return -1;
      }
      recv_cq.cq =
          ibv_create_cq(ctx_, FLAGS_cq_depth / cqn, nullptr, nullptr, 0);
      if (!recv_cq.cq) {
        PLOG(ERROR) << "ibv_create_cq() failed";
        return -1;
      }
    }
    send_cqs_.push_back(send_cq);
    recv_cqs_.push_back(recv_cq);
  }
  return 0;
}

int rdma_context::InitTransport() {
  rdma_endpoint *ep = nullptr;
  int cnt = 0;
  while (!ids_.empty()) {
    auto id = ids_.front();
    ids_.pop();
    if (endpoints_[id]) delete endpoints_[id];
    struct ibv_qp_init_attr qp_init_attr = MakeQpInitAttr(
        GetSendCq(id), GetRecvCq(id), FLAGS_send_wq_depth, FLAGS_recv_wq_depth);
    auto qp = ibv_create_qp(GetPd(id), &qp_init_attr);
    if (!qp) {
      PLOG(ERROR) << "ibv_create_qp() failed";
      delete ep;
      return -1;
    }
    ep = new rdma_endpoint(id, qp);
    ep->SetMaster(this);
    endpoints_[id] = ep;
  }
  return 0;
}

int rdma_context::Listen() {
  struct addrinfo *res, *t;
  struct addrinfo hints;
  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_flags = AI_PASSIVE;
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  char *service;
  int sockfd = -1, err, n;
  int *connfd;
  if (asprintf(&service, "%d", port_) < 0) return -1;
  if (getaddrinfo(nullptr, service, &hints, &res)) {
    LOG(ERROR) << gai_strerror(n) << " for port " << port_;
    free(service);
    return -1;
  }
  for (t = res; t; t = t->ai_next) {
    sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
    if (sockfd >= 0) {
      n = 1;
      setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof(n));
      if (!bind(sockfd, t->ai_addr, t->ai_addrlen)) break;
      close(sockfd);
      sockfd = -1;
    }
  }
  freeaddrinfo(res);
  free(service);
  if (sockfd < 0) {
    LOG(ERROR) << "Couldn't listen to port " << port_;
    return -1;
  }
  LOG(INFO) << "About to listen on port " << port_;
  err = listen(sockfd, 1024);
  if (err) {
    PLOG(ERROR) << "listen() failed";
    return -1;
  }
  LOG(INFO) << "Server listen thread starts";
  while (true) {
    connfd = (int *)malloc(sizeof(int));
    *connfd = accept(sockfd, nullptr, 0);
    if (*connfd < 0) {
      PLOG(ERROR) << "Accept Error";
      break;
    }
    // [TODO] connection handler
    std::thread handler =
        std::thread(&rdma_context::AcceptHandler, this, *connfd);
    handler.detach();
    free(connfd);
  }
  // The loop shall never end.
  free(connfd);
  close(sockfd);
  return -1;
}

void rdma_context::SetEndpointInfo(rdma_endpoint *endpoint,
                                   struct connect_info *info) {
  switch (endpoint->GetType()) {
    case IBV_QPT_UD:
      endpoint->SetLid((info->info.channel.dlid));
      endpoint->SetSl((info->info.channel.sl));
    case IBV_QPT_UC:
    case IBV_QPT_RC:
      endpoint->SetQpn((info->info.channel.qp_num));
      break;
    default:
      LOG(ERROR) << "Currently we don't support other type of QP";
  }
}

void rdma_context::GetEndpointInfo(rdma_endpoint *endpoint,
                                   struct connect_info *info) {
  memset(info, 0, sizeof(connect_info));
  info->type = (kChannelInfoKey);
  switch (endpoint->GetType()) {
    case IBV_QPT_UD:
      info->info.channel.dlid = (lid_);
      info->info.channel.sl = (sl_);
    case IBV_QPT_UC:
    case IBV_QPT_RC:
      info->info.channel.qp_num = (endpoint->GetQpn());
      break;
    default:
      LOG(ERROR) << "Currently we don't support other type of QP";
  }
}

rdma_buffer *rdma_context::CreateBufferFromInfo(struct connect_info *info) {
  uint64_t remote_addr = (info->info.memory.remote_addr);
  uint32_t rkey = (info->info.memory.remote_K);
  int size = (info->info.memory.size);
  return new rdma_buffer(remote_addr, size, 0, rkey);
}

void rdma_context::SetInfoByBuffer(struct connect_info *info,
                                   rdma_buffer *buf) {
  info->type = (kMemInfoKey);
  info->info.memory.size = (buf->size_);
  info->info.memory.remote_K = (buf->remote_K_);
  info->info.memory.remote_addr = (buf->addr_);
  return;
}

int rdma_context::AcceptHandler(int connfd) {
  int n, number_of_qp, number_of_mem, left, right;
  char *conn_buf = (char *)malloc(sizeof(connect_info));
  connect_info *info = (connect_info *)conn_buf;
  union ibv_gid gid;
  std::vector<rdma_buffer *> buffers;
  auto reqs = ParseRecvFromStr();
  int rbuf_id = -1;
  if (!conn_buf) {
    LOG(ERROR) << "Malloc for exchange buffer failed";
    return -1;
  }
  n = read(connfd, conn_buf, sizeof(connect_info));
  if (n != sizeof(connect_info)) {
    PLOG(ERROR) << "Server Read";
    LOG(ERROR) << n << "/" << (int)sizeof(connect_info)
               << ": Couldn't read remote address";
    goto out;
  }
  if ((info->type) != kHostInfoKey) {
    LOG(ERROR) << "The first exchange type should be " << kHostInfoKey;
    goto out;
  }
  number_of_qp = (info->info.host.number_of_qp);
  number_of_mem = (info->info.host.number_of_mem);
  if (number_of_qp <= 0) {
    LOG(ERROR) << "The number of qp should be positive";
    goto out;
  }
  numlock_.lock();
  if (num_of_recv_ + number_of_qp > num_per_host_ * num_of_hosts_) {
    LOG(ERROR) << "QP Overflow, request rejected";
    numlock_.unlock();
    memset(info, 0, sizeof(connect_info));
    if (write(connfd, conn_buf, sizeof(connect_info)) != sizeof(connect_info))
      PLOG(ERROR) << "Write Error";
    goto out;
  }
  left = num_of_recv_;
  num_of_recv_ += number_of_qp;
  numlock_.unlock();
  right = left + number_of_qp;
  // Copy the remote gid.
  memcpy(&gid, &info->info.host.gid, sizeof(union ibv_gid));

  // Put local info to connect_info and send
  memset(info, 0, sizeof(connect_info));
  info->type = (kHostInfoKey);
  memcpy(&info->info.host.gid, &local_gid_, sizeof(union ibv_gid));
  info->info.host.number_of_qp = (number_of_qp);
  if (write(connfd, conn_buf, sizeof(connect_info)) != sizeof(connect_info)) {
    LOG(ERROR) << "Couldn't send local address";
    goto out;
  }

  // Get the memory info from remote
  for (int i = 0; i < number_of_mem; i++) {
    n = read(connfd, conn_buf, sizeof(connect_info));
    if (n != sizeof(connect_info)) {
      PLOG(ERROR) << "Server read";
      LOG(ERROR) << n << "/" << (int)sizeof(connect_info) << ": Read " << i
                 << " mem's info failed";
      goto out;
    }
    if ((info->type) != kMemInfoKey) {
      LOG(ERROR) << "Exchange MemInfo failed. Type received is "
                 << (info->type);
      goto out;
    }
    auto remote_buf = CreateBufferFromInfo(info);
    buffers.push_back(remote_buf);
    auto buf = PickNextBuffer(1);
    if (!buf) {
      LOG(ERROR) << "Server using buffer error";
      goto out;
    }
    SetInfoByBuffer(info, buf);
    if (write(connfd, conn_buf, sizeof(connect_info)) != sizeof(connect_info)) {
      LOG(ERROR) << "Couldn't send " << i << " memory's info";
      goto out;
    }
  }

  rmem_lock_.lock();
  remote_mempools_.push_back(buffers);
  rbuf_id = remote_mempools_.size() - 1;
  rmem_lock_.unlock();

  // Get the connection channel info from remote

  for (int i = left; i < right; i++) {
    auto ep = (rdma_endpoint *)endpoints_[i];
    n = read(connfd, conn_buf, sizeof(connect_info));
    if (n != sizeof(connect_info)) {
      PLOG(ERROR) << "Server read";
      LOG(ERROR) << n << "/" << (int)sizeof(connect_info) << ": Read " << i
                 << " endpoint's info failed";
      goto out;
    }
    if ((info->type) != kChannelInfoKey) {
      LOG(ERROR) << "Exchange data failed. Type Error: " << (info->type);
      goto out;
    }
    SetEndpointInfo(ep, info);
    GetEndpointInfo(ep, info);
    if (write(connfd, conn_buf, sizeof(connect_info)) != sizeof(connect_info)) {
      LOG(ERROR) << "Couldn't send " << i << " endpoint's info";
      goto out;
    }
    if (ep->Activate(gid)) {
      LOG(ERROR) << "Activate Recv Endpoint " << i << " failed";
      goto out;
    }
    // Post The first batch
    int first_batch = FLAGS_recv_wq_depth;
    int batch_size = FLAGS_recv_batch;
    size_t idx = 0;
    while (ep->GetRecvCredits() > 0) {
      auto num_to_post = std::min(first_batch, batch_size);
      if (ep->PostRecv(reqs, idx, num_to_post)) {
        LOG(ERROR) << "The " << i << " Receiver Post first batch error";
        goto out;
      }
      first_batch -= num_to_post;
    }
    ep->SetActivated(true);
    ep->SetMemId(rbuf_id);
    ep->SetServer(GidToIP(gid));
    LOG(INFO) << "Endpoint " << i << " has started";
  }

  // After connection setup. Tell remote that they can send.
  n = read(connfd, conn_buf, sizeof(connect_info));
  if (n != sizeof(connect_info)) {
    PLOG(ERROR) << "Server read";
    LOG(ERROR) << n << "/" << (int)sizeof(connect_info)
               << ": Read Send request failed";
    goto out;
  }
  if ((info->type) != kGoGoKey) {
    LOG(ERROR) << "GOGO request failed";
    goto out;
  }
  memset(info, 0, sizeof(connect_info));
  info->type = (kGoGoKey);
  if (write(connfd, conn_buf, sizeof(connect_info)) != sizeof(connect_info)) {
    LOG(ERROR) << "Couldn't send GOGO!!";
    goto out;
  }
  close(connfd);
  free(conn_buf);
  return 0;
out:
  close(connfd);
  free(conn_buf);
  return -1;
}

int rdma_context::ConnectionSetup(const char *server, int port) {
  struct addrinfo *res, *t;
  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  char *service;
  int n;
  int sockfd = -1;
  int err;
  if (asprintf(&service, "%d", port) < 0) return -1;
  n = getaddrinfo(server, service, &hints, &res);
  if (n < 0) {
    LOG(ERROR) << gai_strerror(n) << " for " << server << ":" << port;
    free(service);
    return -1;
  }
  for (t = res; t; t = t->ai_next) {
    sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
    if (sockfd >= 0) {
      if (!connect(sockfd, t->ai_addr, t->ai_addrlen)) break;
      close(sockfd);
      sockfd = -1;
    }
  }
  freeaddrinfo(res);
  free(service);
  if (sockfd < 0) {
    LOG(ERROR) << "Couldn't connect to " << server << ":" << port;
    return -1;
  }
  return sockfd;
}

int rdma_context::Connect(const char *server, int port, int connid) {
  int sockfd = -1;
  for (int i = 0; i < kMaxConnRetry; i++) {
    sockfd = ConnectionSetup(server, port);
    if (sockfd > 0) break;
    LOG(INFO) << "Try connect to " << server << ":" << port << " failed for "
              << i + 1 << " times...";
    sleep(1);
  }
  if (sockfd < 0) return -1;
  rdma_endpoint *ep;
  union ibv_gid remote_gid;
  char *conn_buf = (char *)malloc(sizeof(connect_info));
  if (!conn_buf) {
    LOG(ERROR) << "Malloc for metadata failed";
    return -1;
  }
  connect_info *info = (connect_info *)conn_buf;
  int number_of_qp, n = 0, rbuf_id = -1;
  std::vector<rdma_buffer *> buffers;
  memset(info, 0, sizeof(connect_info));
  info->info.host.number_of_qp = (num_per_host_);
  info->info.host.number_of_mem = (FLAGS_buf_num);
  memcpy(&info->info.host.gid, &local_gid_, sizeof(union ibv_gid));
  if (write(sockfd, conn_buf, sizeof(connect_info)) != sizeof(connect_info)) {
    LOG(ERROR) << "Couldn't send local address";
    n = -1;
    goto out;
  }
  n = read(sockfd, conn_buf, sizeof(connect_info));
  if (n != sizeof(connect_info)) {
    PLOG(ERROR) << "client read";
    LOG(ERROR) << "Read only " << n << "/" << sizeof(connect_info) << " bytes";
    goto out;
  }
  if (info->type != kHostInfoKey) {
    LOG(ERROR) << "The First exchange should be host info";
    goto out;
  }
  number_of_qp = (info->info.host.number_of_qp);
  if (number_of_qp != num_per_host_) {
    LOG(ERROR) << "Receiver does not support " << num_per_host_ << " senders";
    goto out;
  }
  memcpy(&remote_gid, &info->info.host.gid, sizeof(union ibv_gid));
  for (int i = 0; i < FLAGS_buf_num; i++) {
    auto buf = PickNextBuffer(1);
    if (!buf) {
      LOG(ERROR) << "Client using buffer error";
      goto out;
    }
    SetInfoByBuffer(info, buf);
    if (write(sockfd, conn_buf, sizeof(connect_info)) != sizeof(connect_info)) {
      LOG(ERROR) << "Couldn't send " << i << " memory's info";
      goto out;
    }
    n = read(sockfd, conn_buf, sizeof(connect_info));
    if (n != sizeof(connect_info)) {
      PLOG(ERROR) << "Client read";
      LOG(ERROR) << n << "/" << (int)sizeof(connect_info) << ": Read " << i
                 << " mem's info failed";
      goto out;
    }
    if ((info->type) != kMemInfoKey) {
      LOG(ERROR) << "Exchange MemInfo failde. Type received is "
                 << (info->type);
      goto out;
    }
    auto remote_buf = CreateBufferFromInfo(info);
    buffers.push_back(remote_buf);
  }

  rmem_lock_.lock();
  rbuf_id = remote_mempools_.size();
  remote_mempools_.push_back(buffers);
  rmem_lock_.unlock();

  for (int i = 0; i < num_per_host_; i++) {
    ep = endpoints_[i + connid * num_per_host_];
    GetEndpointInfo(ep, info);
    if (write(sockfd, conn_buf, sizeof(connect_info)) != sizeof(connect_info)) {
      LOG(ERROR) << "Couldn't send " << i << "endpoint's info";
      goto out;
    }
    n = read(sockfd, conn_buf, sizeof(connect_info));
    if (n != sizeof(connect_info)) {
      PLOG(ERROR) << "Client Read";
      LOG(ERROR) << "Read only " << n << "/" << sizeof(connect_info)
                 << " bytes";
      goto out;
    }
    if ((info->type) != kChannelInfoKey) {
      LOG(ERROR) << "Exchange Data Failed. Type Received is " << (info->type)
                 << ", expected " << kChannelInfoKey;
      goto out;
    }
    SetEndpointInfo(ep, info);
    if (ep->Activate(remote_gid)) {
      LOG(ERROR) << "Activate " << i << " endpoint failed";
      goto out;
    }
  }
  memset(info, 0, sizeof(connect_info));
  info->type = (kGoGoKey);
  if (write(sockfd, conn_buf, sizeof(connect_info)) != sizeof(connect_info)) {
    LOG(ERROR) << "Ask GOGO send failed";
    goto out;
  }
  n = read(sockfd, conn_buf, sizeof(connect_info));
  if (n != sizeof(connect_info)) {
    PLOG(ERROR) << "Client Read";
    LOG(ERROR) << "Read only " << n << " / " << sizeof(connect_info)
               << " bytes";
    goto out;
  }
  if ((info->type) != kGoGoKey) {
    LOG(ERROR) << "Ask to Send failed. Receiver reply with " << (info->type)
               << " But we expect " << kGoGoKey;
    goto out;
  }
  for (int i = 0; i < num_per_host_; i++) {
    auto ep = endpoints_[i + connid * num_per_host_];
    ep->SetActivated(true);
    ep->SetServer(GidToIP(remote_gid));
    ep->SetMemId(rbuf_id);
  }
  close(sockfd);
  free(conn_buf);
  return 0;
out:
  close(sockfd);
  free(conn_buf);
  return -1;
}

inline int rdma_context::ParseEachEx(struct ibv_cq_ex *cq_ex) {
  if (cq_ex->status != IBV_WC_SUCCESS) {
    LOG(ERROR) << "God bad completion status with " << cq_ex->status;
    return -1;
  }
  auto opcode = cq_ex->read_opcode(cq_ex);
  auto ep = reinterpret_cast<rdma_endpoint *>(cq_ex->wr_id);
  switch (opcode) {
    case IBV_WC_RDMA_WRITE:
    case IBV_WC_RDMA_READ:
    case IBV_WC_SEND:
      // Client Handle CQE
      ep->SendHandler(nullptr);
      break;
    case IBV_WC_RECV:
    case IBV_WC_RECV_RDMA_WITH_IMM:
      // Server Handle CQE
      ep->RecvHandler(nullptr);
      break;
    default:
      LOG(ERROR) << "Unknown opcode " << opcode;
      break;
  }
  auto nic_clock_cycles = ibv_wc_read_completion_ts(cq_ex);
  auto nic_wall_ts = ibv_wc_read_completion_wallclock_ns(cq_ex);
  auto cpu_wall_ts = Now64Ns();
  struct mlx5dv_clock_info clock_info;
  int ret = mlx5dv_get_clock_info(ctx_, &clock_info);
  if (ret < 0) {
    PLOG(ERROR) << "mlx5dv_get_clock_info() failed";
  }
  struct ibv_values_ex values_ex;
  memset(&values_ex, 0, sizeof(struct ibv_values_ex));
  values_ex.comp_mask = IBV_VALUES_MASK_RAW_CLOCK;
  ret = ibv_query_rt_values_ex(ctx_, &values_ex);
  if (ret < 0) {
    PLOG(ERROR) << "ibv_query_rt_values_ex() failed";
  }
  auto cqe_wall_ts = mlx5dv_ts_to_ns(&clock_info, values_ex.raw_clock.tv_nsec);
  nic_process_time_.push_back(cqe_wall_ts - nic_wall_ts);
  if (nic_process_time_.size() >= 200000) {
    std::sort(nic_process_time_.begin(), nic_process_time_.end());
    LOG(INFO) << "The size of the vector " << nic_process_time_.size();
    LOG(INFO) << "The min: " << nic_process_time_[0]
              << ", median: " << nic_process_time_[nic_process_time_.size() / 2]
              << ", p95: "
              << nic_process_time_[(int)(nic_process_time_.size() * 0.95)]
              << ", p99: "
              << nic_process_time_[(int)(nic_process_time_.size() * 0.99)]
              << ", max: " << nic_process_time_[nic_process_time_.size() - 1];
    nic_process_time_.clear();
  }
  return 0;
}

int rdma_context::PollEachEx(struct ibv_cq_ex *cq_ex) {
  struct ibv_poll_cq_attr attr = {};
  int ret = ENOENT;
  ret = ibv_start_poll(cq_ex, &attr);
  if (ret == ENOENT) return 0;
  if (ret && ret != ENOENT) {
    LOG(ERROR) << "ibv_start_poll() failed with " << ret;
    return ret;
  }
  // Then we parse the completion
  if (ParseEachEx(cq_ex)) {
    ibv_end_poll(cq_ex);
    return -1;
  }
  while (true) {
    ret = ibv_next_poll(cq_ex);
    if (ret) break;
    if (ret = ParseEachEx(cq_ex)) break;
  }
  ibv_end_poll(cq_ex);
  if (ret == ENOENT)
    return 0;
  else
    return ret;
}

int rdma_context::PollEach(struct ibv_cq *cq) {
  int n = 0, ret = 0;
  struct ibv_wc wc[kCqPollDepth];
  do {
    n = ibv_poll_cq(cq, kCqPollDepth, wc);
    if (n < 0) {
      PLOG(ERROR) << "ibv_poll_cq() failed";
      return -1;
    }
    for (int i = 0; i < n; i++) {
      if (wc[i].status != IBV_WC_SUCCESS) {
        LOG(ERROR) << "Got bad completion status with " << wc[i].status;
        return -1;
      }
      auto ep = reinterpret_cast<rdma_endpoint *>(wc[i].wr_id);
      switch (wc[i].opcode) {
        case IBV_WC_RDMA_WRITE:
        case IBV_WC_RDMA_READ:
        case IBV_WC_SEND:
          // Client Handle CQE
          ep->SendHandler(&wc[i]);
          break;
        case IBV_WC_RECV:
        case IBV_WC_RECV_RDMA_WITH_IMM:
          // Server Handle CQE
          ep->RecvHandler(&wc[i]);
          break;
        default:
          LOG(ERROR) << "Unknown opcode " << wc[i].opcode;
          return -1;
      }
    }
    ret += n;
  } while (n);
  return ret;
}

int rdma_context::ServerDatapath() {
  int batch_size = FLAGS_recv_batch;
  auto reqs = ParseRecvFromStr();
  size_t idx = 0;
  while (true) {
    // Replenesh Recv Buffer
    for (auto ep : endpoints_) {
      if (!ep || !ep->GetActivated() || ep->GetRecvCredits() <= 0) continue;
      auto credits = ep->GetRecvCredits();
      while (credits > 0) {
        auto toPostRecv = std::min(credits, batch_size);
        for (auto &req : reqs) {
          for (int i = 0; i < req.sge_num; i++) {
            auto buf = PickNextBuffer(1);
            req.sglist[i].addr = buf->addr_;
            req.sglist[i].lkey = buf->local_K_;
          }
        }
        if (ep->PostRecv(reqs, idx, toPostRecv)) {
          LOG(ERROR) << "PostRecv() failed";
          break;
        }
        credits -= toPostRecv;
      }
    }
    // Poll out the possible completion
    for (auto cq : recv_cqs_) {
      if (FLAGS_hw_ts) {
        if (PollEachEx(cq.cq_ex) != 0) {
          LOG(ERROR) << "PollEachEx() failed";
          exit(0);
        }
      } else {
        if (PollEach(cq.cq) < 0) {
          LOG(ERROR) << "PollEach() failed";
          exit(0);
        }
      }
    }
  }
  // Never reach here
  return 0;
}

int rdma_context::ClientDatapath() {
  auto req_vec = ParseReqFromStr();
  uint32_t batch_size = FLAGS_send_batch;
  size_t j = 0;
  int iterations_left = FLAGS_iters;
  bool run_infinitely = FLAGS_run_infinitely;
  while (true) {
    if (!run_infinitely && iterations_left <= 0) break;
    iterations_left--;
    for (auto ep : endpoints_) {
      if (!ep) continue;                  // Ignore those dead ones
      if (!ep->GetActivated()) continue;  // YOU ARE NOT PREPARED!
      if (batch_size > ep->GetSendCredits()) continue;  // YOU DON'T HAVE MONEY!
      size_t i = 0;
      // Shuffle the buffer that is used.
      for (auto &req : req_vec) {
        for (int i = 0; i < req.sge_num; i++) {
          auto buf = PickNextBuffer(0);
          req.sglist[i].addr = buf->addr_;
          req.sglist[i].lkey = buf->local_K_;
        }
      }
      ep->PostSend(req_vec, j, batch_size, remote_mempools_[ep->GetMemId()]);
    }
    for (auto cq : send_cqs_) {
      if (FLAGS_hw_ts) {
        if (PollEachEx(cq.cq_ex) != 0) {
          LOG(ERROR) << "PollEachEx() failed";
          exit(1);
        }
      } else {
        if (PollEach(cq.cq) < 0) {
          LOG(ERROR) << "PollEach() failed";
          exit(1);
        }
      }
    }
    auto ts = Now64();
    if (_print_thp)
      for (auto ep : endpoints_) {
        if (!ep) continue;                  // Ignore those dead ones
        if (!ep->GetActivated()) continue;  // YOU ARE NOT PREPARED!
        ep->PrintThroughput(ts);
      }
  }
  // Never reach here.
  return 0;
}

}  // namespace Collie
