#include "nvme_statistator.h"
#include "nvme_internal.h"

static const char *spdk_statistator_type_str[] = {
  "read",
  "write",
  "all_rw",
  "unpin",
  "error",
  "count",
};

static int convert_to_enum(uint16_t opc)
{
  switch (opc)
  {
  case SPDK_NVME_OPC_PG_CC_READ:
    return SPDK_STATISTATOR_TYPE_READ;
  case SPDK_NVME_OPC_PG_CC_WRITE:
    return SPDK_STATISTATOR_TYPE_WRITE;
  case SPDK_NVME_OPC_UN_PIN:
    return SPDK_STATISTATOR_TYPE_UNPIN;
  default:
    return SPDK_STATISTATOR_TYPE_ERROR;
  }
}

void spdk_add_statis(struct spdk_nvme_qpair *qpair, struct nvme_request *req) {
  assert(qpair->stat);

  if (req->cmd.opc == SPDK_NVME_OPC_READ || req->cmd.opc == SPDK_NVME_OPC_WRITE) {
    return ;
  }

  int origin_opc = convert_to_enum(req->cmd.opc);
  if (origin_opc == SPDK_STATISTATOR_TYPE_ERROR) {
    printf("SPDK STAT ERROR: unknown opc %d\n", req->cmd.opc);
  }
  assert(origin_opc != SPDK_STATISTATOR_TYPE_ERROR);

  qpair->stat->req_count[origin_opc]++;
  qpair->stat->req_pg_num[origin_opc] += req->stat.pg_num;
  qpair->stat->pg_cc_hit_num[origin_opc] += req->stat.pg_hit_num;
  qpair->stat->pg_cc_miss_num[origin_opc] += req->stat.pg_miss_num;

  assert(req->stat.precache_end_time > req->stat.precache_start_time);
  assert(req->stat.postcache_end_time > req->stat.postcache_start_time || (req->stat.postcache_end_time == 0 && req->stat.postcache_start_time == 0));
  assert(req->stat.precache_start_time != 0);

  assert(req->stat.req_sending_end_time > req->stat.req_sending_start_time);
  assert((req->stat.req_pending_end_time > req->stat.req_pending_start_time && req->stat.req_pending_start_time != 0) || (req->stat.req_pending_start_time == 0 && req->stat.req_pending_end_time == 0));

  qpair->stat->pg_cc_pre_consume_time[origin_opc] += (req->stat.precache_end_time - req->stat.precache_start_time);
  qpair->stat->pg_cc_post_consume_time[origin_opc] += (req->stat.postcache_end_time - req->stat.postcache_start_time);
  qpair->stat->req_sending_time[origin_opc] += (req->stat.req_sending_end_time - req->stat.req_sending_start_time);
  qpair->stat->req_pending_time[origin_opc] += (req->stat.req_pending_end_time - req->stat.req_pending_start_time);
}

void spdk_print_statis(struct spdk_nvme_qpair *qpair) {
  assert(qpair->stat);

  printf("SPDK STAT BEGIN\n");

  for (int type = 0; type < SPDK_STATISTATOR_TYPE_ERROR; type++) {
    if (qpair->stat->req_count[type] == 0) {
      continue;
    }

    double sending_time = (double)qpair->stat->req_sending_time[type] * (double)SPDK_SEC_TO_USEC / spdk_get_ticks_hz() / qpair->stat->req_count[type];
    double pending_time = (double)qpair->stat->req_pending_time[type] * (double)SPDK_SEC_TO_USEC / spdk_get_ticks_hz() / qpair->stat->req_count[type];

    double pg_cc_pre_time = (double)qpair->stat->pg_cc_pre_consume_time[type] * (double)SPDK_SEC_TO_USEC / spdk_get_ticks_hz()  / qpair->stat->req_count[type];
    double pg_cc_post_time = (double)qpair->stat->pg_cc_post_consume_time[type] * (double)SPDK_SEC_TO_USEC / spdk_get_ticks_hz() / qpair->stat->req_count[type];

    double cache_hit_rate = (double)qpair->stat->pg_cc_hit_num[type] / (double)qpair->stat->req_pg_num[type];
    double cache_miss_rate = (double)qpair->stat->pg_cc_miss_num[type] / (double)qpair->stat->req_pg_num[type];

    printf("SPDK %s spdk_req_count %lu spdk_req_pg_num %lu spdk_hit_num %lu spdk_miss_num %lu spdk_hit_rate %.3lf spdk_miss_rate %.3lf spdk_avg_sending_time %.2lf spdk_avg_pending_time %.2lf spdk_avg_pre_time %.2lf spdk_avg_post_time %.2lf\n",
           spdk_statistator_type_str[type], qpair->stat->req_count[type], qpair->stat->req_pg_num[type],
           qpair->stat->pg_cc_hit_num[type], qpair->stat->pg_cc_miss_num[type], 
           cache_hit_rate, cache_miss_rate,
           sending_time, pending_time,
           pg_cc_pre_time, pg_cc_post_time);
  }

  printf("SPDK STAT END\n");

  fflush(stdout);
}
