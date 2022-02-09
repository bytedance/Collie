// MIT License

// Copyright (c) 2021 ByteDance Inc. All rights reserved.
// Copyright (c) 2021 Duke University.  All rights reserved.

// See LICENSE for license information

#include <thread>

#include "context.hpp"
int main(int argc, char **argv) {
  ibv_fork_init();
  if (Collie::Initialize(argc, argv)) return -1;

  std::thread listen_thread;
  std::thread server_thread;
  // Set up server
  LOG(INFO) << "Grfwork starts";
  if (FLAGS_server) {
    auto pici_server =
        new Collie::rdma_context(FLAGS_dev.c_str(), FLAGS_gid, FLAGS_host_num,
                                 FLAGS_qp_num, FLAGS_print_thp);
    if (pici_server->Init()) {
      LOG(ERROR) << "Collie server initialization failed. Exit...";
      return -1;
    }
    listen_thread = std::thread(&Collie::rdma_context::Listen, pici_server);
    server_thread =
        std::thread(&Collie::rdma_context::ServerDatapath, pici_server);
    LOG(INFO) << "Collie server has started.";
  }
  // Set up client
  if (FLAGS_connect != "") {
    auto host_vec = Collie::ParseHostlist(FLAGS_connect);
    auto pici_client =
        new Collie::rdma_context(FLAGS_dev.c_str(), FLAGS_gid, host_vec.size(),
                                 FLAGS_qp_num, FLAGS_print_thp);
    if (pici_client->Init()) {
      LOG(ERROR) << "Collie client initialization failed. Exit... ";
      return -1;
    }
    for (size_t i = 0; i < host_vec.size(); i++) {
      if (pici_client->Connect(host_vec[i].c_str(), FLAGS_port, i)) {
        LOG(ERROR) << "Collie client connect to " << host_vec[i] << " failed";
      }
    }
    pici_client->ClientDatapath();
  }
  listen_thread.join();
  server_thread.join();
  return 0;
}