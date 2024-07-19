#include "nvme_host_mem_cache.h"
#include "nvme_internal.h"

#define CACHELINE_RATIO (4)

static uint64_t get_bucket(struct spdk_nvme_qpair *qpair, uint64_t tag) {
  return tag % qpair->cacheline_bucket_size;
}

static inline uint16_t is_accessed(struct page_table_cacheline *cacheline, uint64_t offset) {
  return (cacheline->header[offset]).accessed;
}

static inline void set_accessed(struct page_table_cacheline *cacheline, uint64_t offset) {
  cacheline->header[offset].accessed = 1;
}

static inline void clear_accessed(struct page_table_cacheline *cacheline, uint64_t offset) {
  cacheline->header[offset].accessed = 0;
}

inline uint64_t trans_pgid2addr(struct spdk_nvme_ctrlr *ctrlr, uint64_t vir_pgid) {
  assert(vir_pgid < ctrlr->host_mem_pg_size);
  return ctrlr->host_mem_vir_addr[vir_pgid];
}

void initialize_page_table_cache(struct spdk_nvme_qpair *qpair) {
  struct spdk_nvme_ctrlr *ctrlr = qpair->ctrlr;
  // TODO: check the parameter setting

  uint64_t cachesize = ctrlr->host_mem_size / 4096;
  
  qpair->cacheline_size = cachesize;
  qpair->cacheline_bucket_size = cachesize * 2;

  int rc = posix_memalign((void **)(&qpair->cacheline_store), CACHELINE_SIZE, (qpair->cacheline_size + qpair->cacheline_bucket_size) * sizeof(struct page_table_cacheline));
  assert(rc == 0);

  qpair->pending_req = malloc(sizeof(*qpair->pending_req) * (qpair->cacheline_size + qpair->cacheline_bucket_size));
  assert(qpair->pending_req);

  for (uint64_t i = 0; i < qpair->cacheline_size + qpair->cacheline_bucket_size; i++) {
    qpair->pending_req[i] = malloc(sizeof(*qpair->pending_req[i]) * (CACHELINE_PACK_PAGES));
    assert(qpair->pending_req[i]);
    for (int j = 0; j < CACHELINE_PACK_PAGES; j++) {
      STAILQ_INIT(&qpair->pending_req[i][j]);
    }
  }

  assert(ctrlr->opts.io_queue_size != 0);
  #define MAX_REQ_SIZE (4096)
  qpair->free_pending_req_size = ctrlr->opts.io_queue_size * CACHELINE_PACK_PAGES * MAX_REQ_SIZE;
  qpair->free_pending_req_entry = malloc(sizeof(*qpair->free_pending_req_entry) * qpair->free_pending_req_size);
  assert(qpair->free_pending_req_entry);

  STAILQ_INIT(&(qpair->free_pending_req_head));
  for (uint64_t i = 0; i < qpair->free_pending_req_size; i++) {
    STAILQ_INSERT_TAIL(&(qpair->free_pending_req_head), &qpair->free_pending_req_entry[i], stailq);
  }

  int ftl_num = spdk_nvme_ctrlr_get_ftl_num(ctrlr);
  for (int i = 0; i < ftl_num; i++) {
    qpair->pin_size[i] = 0;
    qpair->mapping_size[i] = 0;
  }

  printf("qpair initialize page table cache, cacheline size: %lu\n", qpair->cacheline_size);

  struct page_table_cacheline *cacheline = qpair->cacheline_store;

  for (uint64_t i = 0; i < qpair->cacheline_size + qpair->cacheline_bucket_size; i++) {
    cacheline[i].tag = CACHELINE_EMPTY_TAG;
    cacheline[i].num_using = 0;
    cacheline[i].next = NULL;
    cacheline[i].id = (uint32_t)(i + 1); // FIXME: through calculating address offset to get the id
    for (int j = 0; j < CACHELINE_PACK_PAGES; j++) {
      cacheline[i].header[j].exist = false;
      cacheline[i].header[j].dirty = false;
      cacheline[i].header[j].busy = false;
      cacheline[i].header[j].ref_count = 0;
      cacheline[i].header[j].accessed = 0;
      cacheline[i].ppage_ids[j] = CACHELINE_EMPTY_PPAGE_ID;

    }
  }

  STAILQ_INIT(&qpair->cacheline_pool);
  for (uint64_t i = 0; i < qpair->cacheline_size; i++) {
    STAILQ_INSERT_HEAD(&qpair->cacheline_pool, &cacheline[qpair->cacheline_bucket_size + i], stailq);
  }
}

void destroy_page_table_cache(struct spdk_nvme_qpair *qpair) {
  for (uint64_t i = 0; i < qpair->cacheline_size + qpair->cacheline_bucket_size; i++) {
    free(qpair->pending_req[i]);
  }
  free(qpair->pending_req);
  free(qpair->free_pending_req_entry);

  free(qpair->cacheline_store);
}

uint64_t get_cacheline_pos(struct page_table_cacheline *cacheline) {
  assert(cacheline->id > 0);
  return cacheline->id - 1;
}

void pending_req_push_back(struct spdk_nvme_qpair *qpair, uint64_t cacheline_id, uint64_t cacheline_offset, struct nvme_request *req) {
  assert(req);
  assert(!STAILQ_EMPTY(&qpair->free_pending_req_head));
  struct spdk_nvme_pending_req_entry *entry = STAILQ_FIRST(&qpair->free_pending_req_head);
  STAILQ_REMOVE_HEAD(&qpair->free_pending_req_head, stailq);
  entry->req = req;
  STAILQ_INSERT_TAIL(&qpair->pending_req[cacheline_id][cacheline_offset], entry, stailq);
  // printf("push back req %p, cacheline_id: %lu, cacheline_offset: %lu, ptr: %lu\n", req, cacheline_id, cacheline_offset, ptr);
}


struct page_table_cacheline *get_pre_cacheline(struct spdk_nvme_qpair *qpair, const uint64_t tag, const struct page_table_cacheline *end) {
  struct page_table_cacheline *cacheline = &qpair->cacheline_store[get_bucket(qpair, tag)];
  while (cacheline != NULL) {
    if (cacheline->next == end) {
      return cacheline;
    }
    cacheline = cacheline->next;
  }
  return NULL;
}

bool cacheline_pin_full(struct spdk_nvme_qpair *qpair) {
  struct page_table_cacheline* entry = STAILQ_FIRST(&qpair->cacheline_pool);
  return entry == NULL;
}

struct page_table_cacheline *get_cacheline(struct spdk_nvme_qpair *qpair, uint64_t vir_addr) {
  uint64_t tag = vir_addr / CACHELINE_PACK_PAGES;

  struct page_table_cacheline *cacheline = &qpair->cacheline_store[get_bucket(qpair, tag)];
  while (cacheline != NULL) {
    if (cacheline->tag == tag) {
      return cacheline;
    }
    cacheline = cacheline->next;
  }

  return NULL;
}

struct page_table_cacheline *get_or_create_cacheline(struct spdk_nvme_qpair *qpair, uint64_t vir_addr) {
  uint64_t tag = vir_addr / CACHELINE_PACK_PAGES;

  struct page_table_cacheline *cacheline = get_cacheline(qpair, vir_addr);
  if (cacheline)
    return cacheline;
  else return create_cacheline(qpair, tag);
}

static void check_cacheline_empty(struct page_table_cacheline *cacheline, bool skip_check_next) {
  assert(cacheline);
  assert(cacheline->id != 0);
  assert(cacheline->tag == CACHELINE_EMPTY_TAG);
  assert(cacheline->num_using == 0);
  if (!skip_check_next)
    assert(cacheline->next == NULL);
  for (int i = 0; i < CACHELINE_PACK_PAGES; i++) {
    assert(cacheline->header[i].exist == false);
    assert(cacheline->header[i].dirty == false);
    assert(cacheline->header[i].busy == false);
    assert(cacheline->header[i].ref_count == 0);
    assert(cacheline->header[i].accessed == false);
    assert(cacheline->ppage_ids[i] == CACHELINE_EMPTY_PPAGE_ID);
  }
}

static void clear_cacheline(struct page_table_cacheline *cacheline) {
  assert(cacheline);
  cacheline->tag = CACHELINE_EMPTY_TAG;
  cacheline->num_using = 0;
  cacheline->next = NULL;
  assert(cacheline->id != 0);
  for (int i = 0; i < CACHELINE_PACK_PAGES; i++) {
    cacheline->header[i].exist = false;
    cacheline->header[i].dirty = false;
    cacheline->header[i].busy = false;
    cacheline->header[i].accessed = false;
    cacheline->header[i].ref_count = 0;
    cacheline->ppage_ids[i] = CACHELINE_EMPTY_PPAGE_ID;
  }
}

static void deep_copy_cacheline(struct page_table_cacheline *dst, struct page_table_cacheline *src) {
  assert(dst);
  assert(src);
  uint64_t det_cacheline_ig = dst->id;
  memcpy(dst, src, sizeof(struct page_table_cacheline));
  dst->id = det_cacheline_ig;
  assert(dst->id > 0);
  assert(src->id > 0);
  assert(dst->id != src->id);
}

struct unpacked_pte_header to_unpacked(struct page_table_cacheline *cacheline, uint64_t offset) {
  assert(cacheline);
  assert(offset < CACHELINE_PACK_PAGES);
  return (struct unpacked_pte_header){
    .exist = cacheline->header[offset].exist,
    .busy = cacheline->header[offset].busy,
    .dirty = cacheline->header[offset].dirty,
    .ref_count = cacheline->header[offset].ref_count,
    .ppage_id = cacheline->ppage_ids[offset]
  };
}

struct unpacked_pte_header get_pte(struct spdk_nvme_qpair *qpair, uint64_t vir_addr, struct page_table_cacheline *hint)
{
  uint64_t tag = vir_addr / CACHELINE_PACK_PAGES;
  uint64_t offset = vir_addr % CACHELINE_PACK_PAGES;

  struct page_table_cacheline *cacheline = hint ? hint : get_cacheline(qpair, vir_addr);
  if (!cacheline || cacheline->tag != tag) {
    return (struct unpacked_pte_header){false, false, false, 0, CACHELINE_EMPTY_PPAGE_ID};
  }

  return to_unpacked(cacheline, offset);
}

struct page_table_cacheline *create_cacheline(struct spdk_nvme_qpair *qpair, const uint64_t tag)
{  
  struct page_table_cacheline *store = qpair->cacheline_store;
  struct page_table_cacheline *cacheline = &store[get_bucket(qpair, tag)];
  if (cacheline->tag == CACHELINE_EMPTY_TAG) {
    check_cacheline_empty(cacheline, true);
    cacheline->tag = tag;
    return cacheline;
  } else {
    /* TODO: speed the finding process up*/
    assert(cacheline->tag != tag);
    while (cacheline->next != NULL) {
      cacheline = cacheline->next;
      assert(cacheline->tag != tag);
    }

    assert(cacheline->next == NULL);

    assert(!STAILQ_EMPTY(&qpair->cacheline_pool));
    struct page_table_cacheline* entry = STAILQ_FIRST(&qpair->cacheline_pool);
    STAILQ_REMOVE_HEAD(&qpair->cacheline_pool, stailq);

    check_cacheline_empty(entry, false);
    cacheline->next = entry;
    entry->tag = tag;

    return entry;
  }
}

struct page_table_cacheline *delete_cacheline(struct spdk_nvme_qpair *qpair, struct page_table_cacheline *cacheline) {
  assert(cacheline);
  assert(get_cacheline(qpair, cacheline->tag * CACHELINE_PACK_PAGES) == cacheline);

  uint64_t origin_tag = cacheline->tag;

  struct page_table_cacheline *pre_cacheline = get_pre_cacheline(qpair, cacheline->tag, cacheline);
  assert((&qpair->cacheline_store[get_bucket(qpair, cacheline->tag)] == cacheline) || (pre_cacheline->next && pre_cacheline->next == cacheline));

  cacheline->tag = CACHELINE_EMPTY_TAG;

  if (pre_cacheline) {
    pre_cacheline->next = cacheline->next;
    cacheline->next = NULL;
    STAILQ_INSERT_HEAD(&qpair->cacheline_pool, cacheline, stailq);
  }

  check_cacheline_empty(cacheline, (&qpair->cacheline_store[get_bucket(qpair, origin_tag)] == cacheline));

  if (pre_cacheline) {
    return pre_cacheline->next;
  } else {
    return cacheline->next;
  }
}

uint64_t get_total_mapping_size(struct spdk_nvme_qpair *qpair) {
  uint64_t total_mapping_size = 0;
  uint64_t ftl_num = spdk_nvme_ctrlr_get_ftl_num(qpair->ctrlr);
  for (uint64_t i = 0; i < ftl_num; i++) {
    total_mapping_size += qpair->mapping_size[i];
  }
  return total_mapping_size;
}

uint64_t get_total_pin_size(struct spdk_nvme_qpair *qpair) {
  uint64_t total_pin_size = 0;
  uint64_t ftl_num = spdk_nvme_ctrlr_get_ftl_num(qpair->ctrlr);
  for (uint64_t i = 0; i < ftl_num; i++) {
    total_pin_size += qpair->pin_size[i];
  }
  return total_pin_size;
}

bool check_need_pre_unpin(struct spdk_nvme_qpair *qpair, struct nvme_request *req)
{
  // assert(spdk_nvme_ctrlr_get_cachesize_per_qpair(qpair->ctrlr) >= qpair->mapping_size);

  assert(spdk_nvme_ctrlr_get_cachesize_per_qpair(qpair->ctrlr) >= get_total_pin_size(qpair));
  assert(spdk_nvme_ctrlr_get_cachesize_per_qpair(qpair->ctrlr) - get_total_pin_size(qpair) >= req->total_need_pg_num);

  uint64_t lba = req->cmd.cdw10;
	uint64_t lba_count = req->cmd.cdw12 & 0xFFFF;
	uint64_t req_length = (lba_count + 1) * 512;
	uint64_t req_offset = lba * 512;

	uint64_t start_lpn = req_offset / 4096;
	uint64_t end_lpn = (req_offset + req_length - 1) / 4096;

  uint64_t cachesize_limit = spdk_nvme_ctrlr_get_cachesize_per_qpair_per_ftl(qpair->ctrlr);
  uint64_t ftl_num = spdk_nvme_ctrlr_get_ftl_num(qpair->ctrlr);

  bool need_pre_unpin[ftl_num];
  for (uint64_t i = 0; i < ftl_num; i++) {
    need_pre_unpin[i] = false;
  }

  for (uint64_t lpn = start_lpn; lpn <= end_lpn; lpn++) {
    need_pre_unpin[lpn % ftl_num] = true;
  }

  for (uint64_t i = 0; i < ftl_num; i++) {
    if (need_pre_unpin[i] && qpair->mapping_size[i] > cachesize_limit) {
      return true;
    }
  }

  return false;
}

static void print_cacheline(struct spdk_nvme_qpair *qpair) {
  uint64_t cacheline_size = qpair->cacheline_bucket_size + qpair->cacheline_size;
  uint32_t shard_pg_num = spdk_nvme_ctrlr_get_shard_pg_num(qpair->ctrlr);
  uint32_t ftl_num = spdk_nvme_ctrlr_get_ftl_num(qpair->ctrlr);

  uint64_t pin_page[ftl_num], mapping_page[ftl_num];
  memset(pin_page, 0, sizeof(pin_page));
  memset(mapping_page, 0, sizeof(mapping_page));

  printf("ftl_num: %u, shard_pg_num: %u\n", ftl_num, shard_pg_num);

  for (uint64_t i = 0; i < cacheline_size; i++) {
    struct page_table_cacheline *cacheline = &qpair->cacheline_store[i];
    if (cacheline->tag != CACHELINE_EMPTY_TAG)
      for (int j = 0; j < CACHELINE_PACK_PAGES; j++) {
        if (cacheline->header[j].exist) {
          uint64_t lpn = cacheline->tag * CACHELINE_PACK_PAGES + j;
          uint64_t belong_ftl = (lpn / shard_pg_num) % ftl_num;
          uint64_t ref = cacheline->header[j].ref_count;
          printf("lpn %lu, busy %d, ftl %lu, ref:%lu, aced: %d\n", lpn, cacheline->header[j].busy, belong_ftl, ref, cacheline->header[j].accessed);

          mapping_page[belong_ftl] ++;
          if (ref > 0) {
            pin_page[belong_ftl]++;
          }
        }
      }
  }

  for (uint32_t i = 0; i < ftl_num; i++) {
    printf("mapping pg: %lu, pin pg: %lu\n", mapping_page[i], pin_page[i]);
  }
}

void peek_batch_victim(struct spdk_nvme_qpair *qpair, uint32_t *page_id, uint32_t *is_dirty, uint16_t *need_pg_num, uint32_t batch_size, uint64_t start_lpa, uint64_t nlpn)
{
  struct spdk_nvme_ctrlr *ctrlr = qpair->ctrlr;
  uint32_t ftl_num = spdk_nvme_ctrlr_get_ftl_num(ctrlr);
  uint32_t shard_pg_num = spdk_nvme_ctrlr_get_shard_pg_num(ctrlr);

  uint64_t origin_victim_bucket = qpair->victim_bucket;
  qpair->victim_bucket = (qpair->victim_bucket + 1) % qpair->cacheline_bucket_size;
  struct page_table_cacheline *cacheline = &qpair->cacheline_store[qpair->victim_bucket];
  struct page_table_cacheline *next_cacheline;
  uint32_t lpn, belong_ftl;
  uint32_t count = 0;
  uint32_t round_count = 0;
  uint32_t meet_page = 0;
  uint32_t meet_cacheline = 0;
  // bool retry = false;

  while (true) {
#if 0
    if (qpair->victim_bucket == origin_victim_bucket) {
      if (retry)
        assert(0);
      else
        retry = true;
      /* FIXME: retry will consume much time */
    }
#endif

    if (!cacheline) {
      if (spdk_unlikely(qpair->victim_bucket == origin_victim_bucket)) {
        round_count++;
        if (round_count == 3) {
          printf("Err Req: start_lpa: %lu, nlpn: %lu\n", start_lpa, nlpn);
          for (uint64_t i = 0; i < ftl_num; i++) {
            printf("need_pg_num[%lu]: %d\n", i, need_pg_num[i]);
          }
          print_cacheline(qpair);
          assert(0);
        }
      }
      qpair->victim_bucket = (qpair->victim_bucket + 1) % qpair->cacheline_bucket_size;
      cacheline = &qpair->cacheline_store[qpair->victim_bucket];
      continue ;
    }
    while (cacheline) {
      bool found_next = false;
      for (int i = 0; i < CACHELINE_PACK_PAGES; i++) {
        if (!cacheline->header[i].busy && cacheline->header[i].exist && cacheline->header[i].ref_count == 0) {
          lpn = cacheline->tag * CACHELINE_PACK_PAGES + i;
          belong_ftl = (lpn / shard_pg_num) % ftl_num;

          if (need_pg_num[belong_ftl] == 0)
            continue ;

          meet_page++;
          
          if (is_accessed(cacheline, i)) {
            clear_accessed(cacheline, i);
            continue ;
          }

          if (lpn >= start_lpa && lpn < start_lpa + nlpn) {
            continue ;
          }

          page_id[count] = lpn;
          is_dirty[count] = cacheline->header[i].dirty;
          next_cacheline = delete_mapping(qpair, lpn, cacheline);
          count++;

          need_pg_num[belong_ftl]--;
          if (count == batch_size) {
            return ;
          }

          if (next_cacheline != cacheline) {
            found_next = true;
            cacheline = next_cacheline;
            break ;
          }
        }
      }
      if (!found_next) {
        cacheline = cacheline->next;
      }
    }
  }
}

uint32_t peek_victim(struct spdk_nvme_qpair *qpair, uint32_t *is_dirty) {
  uint64_t origin_victim_bucket = qpair->victim_bucket;
  qpair->victim_bucket = (qpair->victim_bucket + 1) % qpair->cacheline_bucket_size;
  struct page_table_cacheline *cacheline = &qpair->cacheline_store[qpair->victim_bucket];
  uint32_t lpn;
  
  while (true) {
    assert(qpair->victim_bucket != origin_victim_bucket);
    if (!cacheline) {
      qpair->victim_bucket = (qpair->victim_bucket + 1) % qpair->cacheline_bucket_size;
      cacheline = &qpair->cacheline_store[qpair->victim_bucket];
      continue ;
    }
    while (cacheline) {
      for (int i = 0; i < CACHELINE_PACK_PAGES; i++) {
        if (!cacheline->header[i].busy && cacheline->header[i].exist && cacheline->header[i].ref_count == 0) {
          if (is_accessed(cacheline, i)) {
            clear_accessed(cacheline, i);
            continue ;
          }
          lpn = cacheline->tag * CACHELINE_PACK_PAGES + i;
          *is_dirty = cacheline->header[i].dirty;
          delete_mapping(qpair, lpn, cacheline);
          return lpn;
        }
      }
      cacheline = cacheline->next;
    }
  }
}

bool create_mapping(struct spdk_nvme_qpair *qpair, uint64_t vir_addr, uint64_t phy_pg_id, bool is_write, struct page_table_cacheline *hint) {
  uint64_t tag = vir_addr / CACHELINE_PACK_PAGES;
  uint64_t offset = vir_addr % CACHELINE_PACK_PAGES;

  struct page_table_cacheline *cacheline = hint ? hint : get_cacheline(qpair, vir_addr);
  assert(cacheline && cacheline->tag == tag);

  assert(cacheline->header[offset].exist == false);
  assert(cacheline->header[offset].dirty == false);
  assert(cacheline->header[offset].busy == false);
  assert(cacheline->header[offset].ref_count == 0);
  assert(cacheline->header[offset].accessed == false);
  assert(cacheline->ppage_ids[offset] == CACHELINE_EMPTY_PPAGE_ID);

  cacheline->header[offset].exist = true;
  cacheline->header[offset].busy = true;
  cacheline->header[offset].dirty = is_write;
  cacheline->header[offset].ref_count = 0;
  cacheline->ppage_ids[offset] = phy_pg_id;

  cacheline->num_using++;

  // qpair->mapping_size++;

  return true;
}

struct page_table_cacheline *delete_mapping(struct spdk_nvme_qpair *qpair, uint64_t vir_addr, struct page_table_cacheline *hint) {
  uint64_t tag = vir_addr / CACHELINE_PACK_PAGES;
  uint64_t offset = vir_addr % CACHELINE_PACK_PAGES;

  struct page_table_cacheline *cacheline = hint ? hint : get_cacheline(qpair, vir_addr);
  assert(cacheline && cacheline->tag == tag);
  assert(cacheline->header[offset].exist == true);
  assert(cacheline->header[offset].busy == false);
  assert(cacheline->header[offset].ref_count == 0);
  assert(cacheline->header[offset].accessed == false);

  cacheline->header[offset].exist = false;
  cacheline->header[offset].dirty = false;
  cacheline->ppage_ids[offset] = CACHELINE_EMPTY_PPAGE_ID;

  uint64_t ftl_id = spdk_nvme_ctrlr_trans_pgid2ftlid(qpair->ctrlr, vir_addr);
  assert(qpair->mapping_size[ftl_id] > 0);
  qpair->mapping_size[ftl_id]--;
  cacheline->num_using--;

  if (!cacheline->num_using) {
    return delete_cacheline(qpair, cacheline);
  }

  return cacheline;
}

void unset_busy_and_set_phy_pg_id(struct spdk_nvme_qpair *qpair, uint64_t vir_pg_id, uint64_t phy_addr, struct page_table_cacheline *cacheline) {
  uint64_t tag = vir_pg_id / CACHELINE_PACK_PAGES;
  uint64_t offset = vir_pg_id % CACHELINE_PACK_PAGES;

  assert(cacheline && cacheline->tag == tag);
  assert(cacheline->header[offset].exist == true);
  assert(cacheline->header[offset].busy == true);

  cacheline->header[offset].busy = false;
  cacheline->ppage_ids[offset] = phy_addr;
}

bool pin_page_with_hint(struct spdk_nvme_qpair *qpair, uint64_t vir_addr, uint64_t *phy_addr, struct page_table_cacheline *hint) {
  uint64_t tag = vir_addr / CACHELINE_PACK_PAGES;
  uint64_t offset = vir_addr % CACHELINE_PACK_PAGES;

  struct page_table_cacheline *cacheline = hint ? hint : get_cacheline(qpair, vir_addr);
  assert(cacheline && cacheline->tag == tag);

  cacheline->header[offset].ref_count++;
  assert(cacheline->header[offset].ref_count < (1ULL << REF_COUNT_BIT));

  if (cacheline->header[offset].ref_count == 1) {
    qpair->pin_size[spdk_nvme_ctrlr_trans_pgid2ftlid(qpair->ctrlr, vir_addr)]++;
  }

  if (phy_addr) {
    *phy_addr = cacheline->ppage_ids[offset];
  }

  return true;
}

void unpin_page(struct spdk_nvme_qpair *qpair, uint64_t vir_addr, bool is_write) {
  uint64_t tag = vir_addr / CACHELINE_PACK_PAGES;
  uint64_t offset = vir_addr % CACHELINE_PACK_PAGES;

  struct page_table_cacheline *cacheline = get_cacheline(qpair, vir_addr);
  assert(cacheline && cacheline->tag == tag);
  assert(cacheline->header[offset].ref_count > 0);

  cacheline->header[offset].ref_count--;
  if (cacheline->header[offset].ref_count == 0) {
    qpair->pin_size[spdk_nvme_ctrlr_trans_pgid2ftlid(qpair->ctrlr, vir_addr)]--;
    set_accessed(cacheline, offset);
  }

  if (is_write) {
    cacheline->header[offset].dirty = true;
  }

}
