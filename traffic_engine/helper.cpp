// MIT License

// Copyright (c) 2021 ByteDance Inc. All rights reserved.
// Copyright (c) 2021 Duke University.  All rights reserved.

// See LICENSE for license information

#include "helper.hpp"
// Control Path parameter
DEFINE_string(dev, "mlx5_0", "ib device to use, default mlx5_0");
DEFINE_int32(gid, 3, "Global id index");

DEFINE_bool(server, false, "Set up a server");
DEFINE_string(connect, "", "connect to the other end");
DEFINE_int32(port, 12000, "Tcp port");

DEFINE_int32(min_rnr_timer, 14, "Minimal Receive Not Ready error");
DEFINE_int32(hop_limit, 16, "Hop limit");
DEFINE_int32(tos, 0, "Type of Service value");
DEFINE_int32(qp_timeout, 0, "QP timeout value");
DEFINE_int32(retry_cnt, 7, "QP retry count");
DEFINE_int32(rnr_retry, 7, "Receive Not Ready retry count");
DEFINE_int32(max_qp_rd_atom, 16, "max_qp_rd_atom");

DEFINE_int32(
    qp_type, 2,
    "QP type: 2 for RC, 3 for UC, 4 for UD. Others are not supported for now.");
DEFINE_int32(mtu, IBV_MTU_1024,
             "IBV_MTU value:\n\
                    \t 1 indicates 256\n\
                    \t 2 indicates 512\n\
                    \t 3 indicates 1024(default)\n\
                    \t 4 indicates 2048\n\
                    \t 5 indicates 4096");

// Resource Management Parameter
DEFINE_bool(share_cq, false, "All qps inside one thread share cq");
DEFINE_bool(share_mr, false, "All qps inside one thread share mr");
DEFINE_bool(share_pd, true, "All elements inside one thread share pd");
DEFINE_bool(memalign, true, "memalign instead of malloc");

DEFINE_int32(send_wq_depth, 1024, "Send Work Queue depth");
DEFINE_int32(recv_wq_depth, 1024, "Recv Work Queue depth");
DEFINE_int32(cq_depth, 65536, "CQ depth");
DEFINE_int32(buf_size, 65536, "Buffer/Message Size");
DEFINE_int32(buf_num, 1, "The number of buffers one QP owns");
DEFINE_int32(mr_num, 1, "The number of MR one thread contains.");
DEFINE_bool(use_cuda, false, "Whether use cuda or not");
DEFINE_int32(gpu_id, 0, "Cuda device id");
DEFINE_bool(hw_ts, false, "Hardware timestamp enable?");

DEFINE_bool(run_infinitely, false, "Will run infinitely");
DEFINE_int32(iters, 200000, "Iterations one QP will send");

DEFINE_int32(send_sge_batch_size, 1, "The sge_num for client");
DEFINE_int32(recv_sge_batch_size, 1,
             "The sge_num for server to post recv requests");
DEFINE_int32(send_batch, 1, "The wr posted inside one post_send call");
DEFINE_int32(recv_batch, 1, "The wr posted inside one post_recv call");
DEFINE_string(request, "w_1_65536",
              "The send request vector: \
                                    e.g., s_1024_1024 indicates traffic patterns as 1K, 1K");
DEFINE_string(receive, "1_65536",
              "The receive request vector: \
                                    e.g., 1024_65536 means to post a receive buffer with pattern 1K, 64K");
DEFINE_bool(imm_data, false, "Use immediate data for all WRITE");

DEFINE_int32(host_num, 1, "The number of host to connect or get connected");
DEFINE_int32(qp_num, 1, "The number of qp each host has");

DEFINE_bool(print_thp, false, "To print throughput or not");

namespace Collie {
uint64_t Now64() {
  struct timespec tv;
  int res = clock_gettime(CLOCK_REALTIME, &tv);
  return (uint64_t)tv.tv_sec * 1000000llu + (uint64_t)tv.tv_nsec / 1000;
}

uint64_t Now64Ns() {
  struct timespec tv;
  int res = clock_gettime(CLOCK_REALTIME, &tv);
  return (uint64_t)tv.tv_sec * 1000000000llu + (uint64_t)tv.tv_nsec;
}

int PrintQpAttr(struct ibv_qp *qp) {
  struct ibv_qp_init_attr qp_init_attr;
  struct ibv_qp_attr qp_attr;
  memset(&qp_attr, 0, sizeof(qp_attr));
  memset(&qp_init_attr, 0, sizeof(qp_init_attr));
  int attr_mask = IBV_QP_PATH_MTU | IBV_QP_PORT | IBV_QP_CAP | IBV_QP_TIMEOUT |
                  IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MIN_RNR_TIMER |
                  IBV_QP_DEST_QPN;

  int rc = ibv_query_qp(qp, &qp_attr, attr_mask, &qp_init_attr);
  if (rc != 0) {
    PLOG(ERROR) << "ibv_query_qp error";
    return -1;
  }

  LOG(INFO) << "qp attr: ";
  LOG(INFO) << "path mtu: " << qp_attr.path_mtu;
  LOG(INFO) << "local phycial port number: "
            << static_cast<int>(qp_attr.port_num);
  LOG(INFO) << "dest qp number: " << qp_attr.dest_qp_num;
  LOG(INFO) << "max send wr: " << qp_attr.cap.max_send_wr;
  LOG(INFO) << "max recv wr: " << qp_attr.cap.max_recv_wr;
  LOG(INFO) << "max send sge: " << qp_attr.cap.max_send_sge;
  LOG(INFO) << "max recv sge: " << qp_attr.cap.max_recv_sge;
  LOG(INFO) << "max inline data: " << qp_attr.cap.max_inline_data;
  LOG(INFO) << "min rnr timer: " << static_cast<int>(qp_attr.min_rnr_timer);
  LOG(INFO) << "timeout: " << static_cast<int>(qp_attr.timeout);
  LOG(INFO) << "retry cnt: " << static_cast<int>(qp_attr.retry_cnt);
  LOG(INFO) << "rnr retry: " << static_cast<int>(qp_attr.rnr_retry);
  return 0;
}

struct ibv_qp_init_attr MakeQpInitAttr(struct ibv_cq *send_cq,
                                       struct ibv_cq *recv_cq,
                                       int send_wq_depth, int recv_wq_depth) {
  struct ibv_qp_init_attr qp_init_attr;
  memset(&qp_init_attr, 0, sizeof(qp_init_attr));
  qp_init_attr.qp_type = (enum ibv_qp_type)FLAGS_qp_type;
  qp_init_attr.sq_sig_all = 0;
  qp_init_attr.send_cq = send_cq;
  qp_init_attr.recv_cq = recv_cq;
  qp_init_attr.cap.max_send_wr = send_wq_depth;
  qp_init_attr.cap.max_recv_wr = recv_wq_depth;
  qp_init_attr.cap.max_send_sge = kMaxSge;
  qp_init_attr.cap.max_recv_sge = kMaxSge;
  qp_init_attr.cap.max_inline_data = kMaxInline;
  return qp_init_attr;
}

struct ibv_qp_attr MakeQpAttr(enum ibv_qp_state state, enum ibv_qp_type qp_type,
                              int remote_qpn, const union ibv_gid &remote_gid,
                              int *attr_mask) {
  struct ibv_qp_attr attr;
  memset(&attr, 0, sizeof(attr));
  *attr_mask = 0;
  switch (state) {
    case IBV_QPS_INIT:
      attr.port_num = 1;
      attr.qp_state = IBV_QPS_INIT;
      switch (qp_type) {
        case IBV_QPT_UD:
          attr.qkey = 0;
          *attr_mask |= IBV_QP_QKEY;
          break;
        case IBV_QPT_UC:
        case IBV_QPT_RC:
          attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE |
                                 IBV_ACCESS_REMOTE_READ |
                                 IBV_ACCESS_REMOTE_ATOMIC;
          *attr_mask |= IBV_QP_ACCESS_FLAGS;
          break;
        default:
          LOG(ERROR) << "Unsupported QP type: " << qp_type;
          break;
      }
      *attr_mask |= IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT;
      break;
    case IBV_QPS_RTR:
      attr.qp_state = IBV_QPS_RTR;
      *attr_mask |= IBV_QP_STATE;
      switch (qp_type) {
        case IBV_QPT_RC:
          attr.max_dest_rd_atomic = FLAGS_max_qp_rd_atom;
          attr.min_rnr_timer = FLAGS_min_rnr_timer;
          *attr_mask |= IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
        case IBV_QPT_UC:
          attr.path_mtu = (enum ibv_mtu)FLAGS_mtu;
          attr.dest_qp_num = remote_qpn;
          attr.rq_psn = 0;
          attr.ah_attr.is_global = 1;
          attr.ah_attr.grh.flow_label = 0;
          attr.ah_attr.grh.sgid_index = FLAGS_gid;
          attr.ah_attr.grh.hop_limit = FLAGS_hop_limit;
          attr.ah_attr.grh.traffic_class = FLAGS_tos;
          memcpy(&attr.ah_attr.grh.dgid, &remote_gid, 16);
          attr.ah_attr.dlid = 0;
          attr.ah_attr.sl = 0;
          attr.ah_attr.src_path_bits = 0;
          attr.ah_attr.port_num = 1;
          *attr_mask |=
              IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;
          break;
        case IBV_QPT_UD:
          break;
      }
      break;
    case IBV_QPS_RTS:
      attr.qp_state = IBV_QPS_RTS;
      attr.sq_psn = 0;
      *attr_mask |= IBV_QP_STATE | IBV_QP_SQ_PSN;
      switch (qp_type) {
        case IBV_QPT_RC:
          attr.timeout = FLAGS_qp_timeout;
          attr.retry_cnt = FLAGS_retry_cnt;
          attr.rnr_retry = FLAGS_rnr_retry;  // This is the retry counter, 7
                                             // means that try infinitely.
          attr.max_rd_atomic = FLAGS_max_qp_rd_atom;
          *attr_mask |= IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                        IBV_QP_MAX_QP_RD_ATOMIC;
        case IBV_QPT_UC:
        case IBV_QPT_UD:
          break;
      }
      break;
    default:
      break;
  }
  return attr;
}

std::vector<std::string> ParseHostlist(const std::string &hostlist) {
  std::vector<std::string> result;
  std::stringstream s_stream(hostlist);
  while (s_stream.good()) {
    std::string substr;
    getline(s_stream, substr, ',');
    result.push_back(substr);
  }
  return result;
}

bool ParametersCheck() {
  if (FLAGS_connect == "" && !FLAGS_server) {
    LOG(ERROR) << "You are not connecting to anyone and you are not a server";
    LOG(ERROR) << "So why do you want to wake me up?";
    return false;
  }
  if (!FLAGS_share_pd) {
    LOG(WARNING) << "High priority warning: PD is better to share";
  }
  if (FLAGS_send_batch > kMaxBatch) {
    LOG(WARNING)
        << "Send batch size is larger than the maximum batch we can set : "
        << FLAGS_send_batch << " > " << kMaxBatch;
    LOG(WARNING) << "Set send_batch = " << kMaxBatch;
    FLAGS_send_batch = kMaxBatch;
  }
  if (FLAGS_recv_batch > kMaxBatch) {
    LOG(WARNING)
        << "RECV batch size is larger than the maximum batch we can set : "
        << FLAGS_recv_batch << " > " << kMaxBatch;
    LOG(WARNING) << "Set recv_batch = " << kMaxBatch;
    FLAGS_recv_batch = kMaxBatch;
  }
  if (FLAGS_run_infinitely) {
    LOG(WARNING)
        << "Running infinitely. The iterations parameters will be of no use.";
  }
  return true;
}

int Initialize(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (!ParametersCheck()) return -1;
  return 0;
}

};  // namespace Collie