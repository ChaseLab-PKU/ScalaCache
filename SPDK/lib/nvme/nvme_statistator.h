/*  HUST PengLi  */

#ifndef __NVME_STATISTATOR_H__
#define __NVME_STATISTATOR_H__

#include <stdint.h>
#include <stdalign.h>

#include "spdk/assert.h"
#include "spdk/queue.h"

struct spdk_nvme_qpair;
struct nvme_request;

enum spdk_statistator_type {
  SPDK_STATISTATOR_TYPE_READ,
  SPDK_STATISTATOR_TYPE_WRITE,
  SPDK_STATISTATOR_TYPE_ALL_RW,
  SPDK_STATISTATOR_TYPE_UNPIN,
  SPDK_STATISTATOR_TYPE_ERROR,
  SPDK_STATISTATOR_TYPE_COUNT,
};

struct spdk_statistator {
  uint64_t req_count[SPDK_STATISTATOR_TYPE_COUNT];
  uint64_t req_pg_num[SPDK_STATISTATOR_TYPE_COUNT];
  uint64_t pg_cc_hit_num[SPDK_STATISTATOR_TYPE_COUNT];
  uint64_t pg_cc_miss_num[SPDK_STATISTATOR_TYPE_COUNT];
  uint64_t pg_cc_pre_consume_time[SPDK_STATISTATOR_TYPE_COUNT];
  uint64_t pg_cc_post_consume_time[SPDK_STATISTATOR_TYPE_COUNT];
  uint64_t req_sending_time[SPDK_STATISTATOR_TYPE_COUNT];
  uint64_t req_pending_time[SPDK_STATISTATOR_TYPE_COUNT];
  // uint64_t pg_cc_hit_lat[SPDK_STATISTATOR_TYPE_COUNT];
  // uint64_t pg_cc_miss_lat[SPDK_STATISTATOR_TYPE_COUNT];
};

void spdk_add_statis(struct spdk_nvme_qpair *qpair, struct nvme_request *req);
void spdk_print_statis(struct spdk_nvme_qpair *qpair);


#endif
