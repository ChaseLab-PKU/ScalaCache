// Li Peng Huazhong University of Science and Technology

#ifndef __FEMU_PAGE_CACHE_H
#define __FEMU_PAGE_CACHE_H

#include "../nvme.h"

#define FEMU_DEBUG_PAGE_CACHE
#ifdef FEMU_DEBUG_PAGE_CACHE
#define pg_cc_assert(expression) assert(expression)
#else
#define pg_cc_assert(expression)
#endif

#define INVALID_SEG_ID (~0ULL)

enum {
  PAGE_ERROR = 0,
  PAGE_UNINITED = 1,
  PAGE_USED_EVICTABLE = 2,
  PAGE_USED_ACCESSED = 3,
  PAGE_PINNED = 4,
  PAGE_EVICTING = 5,
};

typedef struct free_pg {
  uint64_t pg_id;
  struct free_pg *next, *pre;
  QTAILQ_ENTRY(free_pg) entry;
} free_pg;

struct page {
  uint16_t ref_count;
  bool is_dirty;
  uint8_t status;
  uint64_t remap;
  uint64_t phy_addr;
  uint64_t vir_addr;
  uint64_t valid_at;
  uint64_t global_pg_id;
  struct free_pg *rplc_ptr;
};

typedef struct pg_cc_look_up_context {
  NvmeRequest *req;
  NvmeSubRequest *subReq;
  uint64_t start_lpa;
  uint64_t nlp;
} pg_cc_look_up_context;

typedef struct pg_cc_unpin_context {
  uint64_t start_lpa;
  uint64_t nlp;
  bool is_dirty;
} pg_cc_unpin_context;

typedef struct pg_cc_write_context {
  uint64_t *wlpn;
  uint64_t nlp;
  uint64_t stime;
  bool is_fast_flush;
} pg_cc_write_context;

typedef struct pg_cc_read_context {
  uint64_t slpa;
  uint64_t nlp;
  uint64_t stime;
} pg_cc_read_context;

typedef struct pg_cc_flush_context {
  uint64_t wlpn[550];
  uint64_t lpn;
} pg_cc_flush_context;

struct page_cache_mgr;
struct sub_ssd;

typedef struct page_cache_replacer_ops {
  void        (*access)(struct page_cache_mgr *mgr, uint64_t page_id);
  void        (*push)(struct page_cache_mgr *mgr, uint64_t page_id);
  void        (*erase)(struct page_cache_mgr *mgr, uint64_t page_id);
  uint64_t    (*pop)(struct page_cache_mgr *mgr, uint64_t start_lpn, uint64_t nlp);
} page_cache_replacer_ops;

typedef struct page_cache_mgr_ops {
  void        (*init)(struct page_cache_mgr *mgr, uint64_t page_cache_size, uint64_t begin_global_pg_id);
  void        (*look_up)(struct page_cache_mgr *mgr, NvmeSubRequest *subReq);
  void        (*unpin)(struct page_cache_mgr *mgr, pg_cc_unpin_context *ctx);
  void        (*prefetch)(struct page_cache_mgr *mgr, uint64_t lpn);
  void        (*flush)(struct page_cache_mgr *mgr);
  void        (*clear)(struct sub_ssd *sub_ssd);
} page_cache_mgr_ops;

struct page_cache_mgr {
  struct ssd  *ssd;
  struct sub_ssd *sub_ssd;
  struct page **pages;
  struct {
    free_pg *free_pgs_head;
    free_pg *replacer_wp;
  };
  struct free_pg *free_pg_frames;
  QTAILQ_HEAD(mgr_free_pg_frame_list, free_pg) free_pg_frame_list;
  uint64_t max_page_num;
  uint64_t free_page_count;
  uint64_t inited_page_num;
  struct page_cache_replacer_ops replacer_ops;
  struct pg_cc_flush_context flush_ctx;
};


void sub_ssd_register_page_cache_mgr(struct sub_ssd *sub_ssd, uint64_t begin_global_pg_id);
uint64_t trans_single_pgid2_phyaddr(struct sub_ssd *sub_ssd, uint64_t page_id);

#endif