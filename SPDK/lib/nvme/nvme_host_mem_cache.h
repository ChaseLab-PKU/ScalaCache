/*  HUST PengLi  */

#ifndef __NVME_HOST_MEM_CACHE_H__
#define __NVME_HOST_MEM_CACHE_H__

#include <stdint.h>
#include <stdalign.h>

#include "spdk/assert.h"
#include "spdk/queue.h"


#define CACHELINE_SIZE (64)
#define CACHELINE_PACK_PAGES (6)
#define INVALID_REQ_CMD (~(0ULL))
#define CACHELINE_EMPTY_TAG ((1ULL << 58) - 1)
#define CACHELINE_EMPTY_PPAGE_ID ((1ULL << 32) - 1)

struct spdk_nvme_qpair;
struct spdk_nvme_ctrlr;
struct nvme_request;

struct unpacked_pte_header {
  bool exist;
  bool busy;
  bool dirty;
  uint16_t ref_count;
  uint64_t ppage_id;
};

#define REF_COUNT_BIT (12)

struct packed_pte_header {
  uint16_t exist : 1;
  uint16_t dirty : 1;
  uint16_t busy  : 1;
  uint16_t accessed : 1;
  uint16_t ref_count : REF_COUNT_BIT;
};

SPDK_STATIC_ASSERT(sizeof(struct packed_pte_header) == sizeof(uint16_t), "page table header size is not 1 bytes");
// alignas(CACHELINE_SIZE)

 struct page_table_cacheline {
  uint64_t tag : 58;
  uint64_t num_using : 4;
  uint32_t id;
  struct packed_pte_header header[CACHELINE_PACK_PAGES];
  uint32_t ppage_ids[CACHELINE_PACK_PAGES];
  struct page_table_cacheline *next;
  STAILQ_ENTRY(page_table_cacheline) stailq;
} __attribute__((aligned(CACHELINE_SIZE)));

SPDK_STATIC_ASSERT(sizeof(struct page_table_cacheline) == CACHELINE_SIZE,
          "page table cacheline size is not 64 bytes");

struct pending_nvme_request {
  STAILQ_HEAD(, nvme_request)		queued_req[CACHELINE_PACK_PAGES];
};

void initialize_page_table_cache(struct spdk_nvme_qpair *qpair);
void destroy_page_table_cache(struct spdk_nvme_qpair *qpair);

void pending_req_push_back(struct spdk_nvme_qpair *qpair, uint64_t cacheline_id, uint64_t cacheline_offset, struct nvme_request *req);

bool cacheline_pin_full(struct spdk_nvme_qpair *qpair);
struct unpacked_pte_header to_unpacked(struct page_table_cacheline *cacheline, uint64_t offset);

uint64_t trans_pgid2addr(struct spdk_nvme_ctrlr *ctrlr, uint64_t vir_pgid);
uint32_t peek_victim(struct spdk_nvme_qpair *qpair, uint32_t *is_dirty);

uint64_t get_cacheline_pos(struct page_table_cacheline *cacheline);

uint64_t get_total_mapping_size(struct spdk_nvme_qpair *qpair);
uint64_t get_total_pin_size(struct spdk_nvme_qpair *qpair);

bool check_need_pre_unpin(struct spdk_nvme_qpair *qpair, struct nvme_request *req);

void peek_batch_victim(struct spdk_nvme_qpair *qpair, uint32_t *page_id, uint32_t *is_dirty, uint16_t *need_pg_num, uint32_t batch_size, uint64_t start_lpa, uint64_t nlpn);

struct unpacked_pte_header get_pte(struct spdk_nvme_qpair *qpair, uint64_t vir_addr, struct page_table_cacheline *hint);
struct page_table_cacheline *get_cacheline(struct spdk_nvme_qpair *qpair, uint64_t vir_addr);
struct page_table_cacheline *get_pre_cacheline(struct spdk_nvme_qpair *qpair, const uint64_t tag, const struct page_table_cacheline *end);

struct page_table_cacheline *get_or_create_cacheline(struct spdk_nvme_qpair *qpair, uint64_t vir_addr);
struct page_table_cacheline *create_cacheline(struct spdk_nvme_qpair *qpair, const uint64_t tag);
struct page_table_cacheline *delete_cacheline(struct spdk_nvme_qpair *qpair, struct page_table_cacheline *cacheline);

bool create_mapping(struct spdk_nvme_qpair *qpair, uint64_t vir_addr, uint64_t phy_pg_id, bool is_write, struct page_table_cacheline *hint);
struct page_table_cacheline * delete_mapping(struct spdk_nvme_qpair *qpair, uint64_t vir_addr, struct page_table_cacheline *hint);

void unset_busy_and_set_phy_pg_id(struct spdk_nvme_qpair *qpair, uint64_t vir_pg_id, uint64_t phy_addr, struct page_table_cacheline *cacheline);

bool pin_page_with_hint(struct spdk_nvme_qpair *qpair, uint64_t vir_addr, uint64_t *phy_addr, struct page_table_cacheline *hint);
void unpin_page(struct spdk_nvme_qpair *qpair, uint64_t vir_addr, bool is_write);


#endif
