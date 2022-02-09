// MIT License

// Copyright (c) 2021 ByteDance Inc. All rights reserved.
// Copyright (c) 2021 Duke University.  All rights reserved.

// See LICENSE for license information

#include "memory.hpp"

#include <malloc.h>

namespace Collie {

int rdma_region::Mallocate() {
  auto buf_size = num_ * size_;
  int mrflags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
  char *buffer = nullptr;
#ifdef GDR
  if (FLAGS_use_cuda) {
    CUdeviceptr ptr;
    CUresult ret;
    const size_t gpu_page_size = 64 * 1024;
    size_t size = (buf_size + gpu_page_size - 1) & ~(gpu_page_size - 1);

    LOG(INFO) << "cuMemAlloc() of a " << buf_size << " bytes GPU buffer";
    ret = cuMemAlloc(&ptr, size);
    if (ret != CUDA_SUCCESS) {
      LOG(ERROR) << "cuMemAlloc failed with " << ret;
      return -1;
    }
    buffer = (char *)ptr;
  } else {
    if (align_) {
      buffer = (char *)memalign(sysconf(_SC_PAGESIZE), buf_size);
    } else {
      buffer = (char *)malloc(buf_size);
    }
  }
#else
  if (align_) {
    buffer = (char *)memalign(sysconf(_SC_PAGESIZE), buf_size);
  } else {
    buffer = (char *)malloc(buf_size);
  }
#endif
  if (!buffer) {
    PLOG(ERROR) << "Memory Allocation Failed";
    return -1;
  }
  mr_ = ibv_reg_mr(pd_, buffer, buf_size, mrflags);
  if (!mr_) {
    PLOG(ERROR) << "ibv_reg_mr() failed";
    return -1;
  }
  for (size_t i = 0; i < num_; i++) {
    rdma_buffer *rbuf = new rdma_buffer((uint64_t)(buffer + size_ * i), size_,
                                        mr_->lkey, mr_->rkey);
    buffers_.push(rbuf);
  }
  return 0;
}

rdma_buffer *rdma_region::GetBuffer() {
  if (buffers_.empty()) {
    LOG(ERROR) << "The MR's buffer is empty";
    return nullptr;
  }
  auto rbuf = buffers_.front();
  buffers_.pop();
  buffers_.push(rbuf);
  return rbuf;
}

};  // namespace Collie