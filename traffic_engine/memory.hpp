// MIT License

// Copyright (c) 2021 ByteDance Inc. All rights reserved.
// Copyright (c) 2021 Duke University.  All rights reserved.

// See LICENSE for license information

#ifndef RMEMORY_HPP
#define RMEMORY_HPP
#include <queue>

#include "helper.hpp"

namespace Collie {

// local_memory_pool is a queue of regions.
class rdma_buffer {
 public:
  uint64_t addr_;
  uint32_t size_;
  uint32_t local_K_;  // K short for Key because key is a sensitive word due to
                      // NDA :)
  uint32_t remote_K_;
  rdma_buffer(uint64_t addr, uint32_t size, uint32_t local_K, uint32_t remote_K)
      : addr_(addr), size_(size), local_K_(local_K), remote_K_(remote_K) {}
};

class rdma_region {
 private:
  struct ibv_mr *mr_ = nullptr;
  struct ibv_pd *pd_ = nullptr;
  int numa_ = -1;
  int num_ = 0;
  uint32_t size_ = 0;
  bool align_ = false;
  std::queue<rdma_buffer *> buffers_;

 public:
  rdma_region(struct ibv_pd *pd, size_t size, int n, bool align, int numa)
      : pd_(pd), numa_(numa), num_(n), size_(size), align_(align) {}

  // Allocate from main memory
  int Mallocate();
  // Allocate from GPU memory
  int Gallocate();
  // Pick a buffer in the order of FIFO
  rdma_buffer *GetBuffer();
};
};  // namespace Collie

#endif