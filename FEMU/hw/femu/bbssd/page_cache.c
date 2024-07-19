#include "page_cache.h"
#include "replacement.h"
#include "ftl.h"

#define PAGE_CACHE_EXIST_BIT (1ULL << 63)

static bool is_cached_ppa(struct ppa *ppa) {
  return ppa->g.is_cached;
}

/*
static struct ppa get_free_page(struct page_cache_mgr *mgr, struct ssd *ssd) {
  struct ppa ppa;
  struct page *page;
  bool found = false;
  uint64_t origin_ptr = mgr->clock_ptr;
  uint64_t *ptr = &mgr->clock_ptr;

  while (!found) {
    page = mgr->pages[*ptr];
    if (!page->is_valid) {
      ppa.ppa = *ptr;
      ppa.g.is_cached = 1;
      found = true;
    }
    *ptr = (*ptr + 1) % mgr->n_pages;
    ftl_assert(*ptr != origin_ptr);
  }

  return ppa;
}
*/

// need to update remap if flush async
#if 0
static void async_flush(struct page_cache_mgr *mgr) {
  uint64_t flush_count = 8;
  uint64_t find_count = 0;
  uint64_t origin_ptr = mgr->flush_wp;
  struct page **page = mgr->pages;
  uint64_t *write_page = (uint64_t*)malloc(sizeof(uint64_t) * flush_count);
  for (uint64_t i = mgr->flush_wp; find_count < flush_count; i++) {
    if (page[i]->is_dirty && page[i]->status == PAGE_USED_EVICTABLE) {
      page[i]->status = PAGE_EVICTING;
      write_page[find_count++] = page[i]->remap;
    }
  }

  uint64_t valid_at = ssd_page_cache_write(mgr->ssd, write_page, find_count);

  for (uint64_t i = 0; i < find_count; i++) {
    page[write_page[i]]->is_dirty = false;
    page[write_page[i]]->valid_at = valid_at;
  }
}
#endif

static inline bool is_partion_write(struct ssd *ssd, struct NvmeSubRequest *subReq, uint64_t lpa) {
  uint64_t slba = subReq->parent_req->slba;
  uint64_t len = subReq->parent_req->nlb;
  uint64_t s_lpa = fast_transLBA2LPN(slba);
  uint64_t e_lpa = fast_transLBA2LPN(slba + len - 1);

  if (slba % ssd->sp.secs_per_pg != 0 && lpa == s_lpa) {
    return true; 
  } else if ((slba + len) % ssd->sp.secs_per_pg != 0 
                  && lpa == e_lpa) {
    return true;
  }

  return false;
}

static inline void print_pg_desptr(struct sub_ssd *sub_ssd) {
  HMBEntryDesptr *desptr = sub_ssd->page_cache_desptr;
  for (int i = 0; i < sub_ssd->page_cache_desptr_sz; i++) {
    printf("desptr %d, start_pg_id %u, size %u, phy_addr %lu, vir_addr %lu\n", 
            i, desptr[i].start_pg_id, desptr[i].size, desptr[i].phy_addr, desptr[i].vir_addr);
  }
}

uint64_t trans_single_pgid2_phyaddr(struct sub_ssd *sub_ssd, uint64_t page_id) {
  struct page *page = sub_ssd->page_cache_mgr->pages[page_id];
  return page->phy_addr;
}

static inline void trans_pg_id2addr(struct sub_ssd *sub_ssd, uint64_t page_id, NvmeRequest *req, uint64_t wp) {
  struct page *page = sub_ssd->page_cache_mgr->pages[page_id];
  req->page_phy_addr[wp] = page->phy_addr;
  return ;
}

static void __attribute__((unused)) page_cache_warm_up(struct page_cache_mgr *mgr) {
  assert(0);
}

static int __attribute__((unused)) shard(uint64_t lpa, struct sub_ssd *sub_ssd) {
  struct ssd *ssd = sub_ssd->parent_ssd;
  int ftl_num = ssd->sub_ssd_num;

  return (lpa * 4096 / (ssd->sp.shard_length)) % ftl_num;
}

static void page_cache_look_up(struct page_cache_mgr *mgr, NvmeSubRequest *subReq)
{
  struct ssd *ssd = mgr->ssd;
  struct sub_ssd *sub_ssd = mgr->sub_ssd;
  NvmeRequest    *req = subReq->parent_req;
  struct ppa ppa, ftl_addr;
  bool is_existed;
  uint64_t page_id;
  uint64_t start_lpa, nlp;
  uint64_t s_lpa = fast_transLBA2LPN(req->slba);
  uint64_t e_lpa = fast_transLBA2LPN(req->slba + req->nlb - 1);
  uint64_t req_lpn = e_lpa - s_lpa + 1;
  uint64_t lat = 0, max_lat = 0;
  uint64_t stime = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
  struct pg_cc_read_context read_ctx;
  NvmeLBARangeEntry *lba_range, *next;

  femu_debug("page_cache_look_up: start_lpa %lu, nlp %lu, s_lpa %lu, slba %lu, nlb %u\n", 
                  start_lpa, ctx->nlp, s_lpa, req->slba, req->nlb);

  // clear pg_cc_flush_context
  mgr->flush_ctx.lpn = 0;

  QTAILQ_FOREACH_SAFE(lba_range, &subReq->lba_list, entry, next) {
    
    start_lpa = fast_transLBA2LPN(lba_range->slba);
    nlp = fast_transLBA2LPN(lba_range->slba + lba_range->nlb - 1) - start_lpa + 1;

    for (uint64_t i = start_lpa, ptr = start_lpa - s_lpa; i < start_lpa + nlp; 
                      i++, ptr++) {
      pg_cc_assert(shard(i, sub_ssd) == sub_ssd->id);
      ppa = get_maptbl_ent(ssd, i);
      if (ppa.ppa == UNMAPPED_PPA) {
        is_existed = false;
      } else if (is_cached_ppa(&ppa)) {
        is_existed = true;
      } else {
        is_existed = false;
      }

      if (!req->need_pin[ptr]) {
        pg_cc_assert(is_existed);
        continue ;
      }

      if (is_existed) {
        ftl_addr = ppa;
        pg_cc_assert(ftl_addr.g.is_cached == 1);
        ftl_addr.g.is_cached = 0;

        /* update stat */
        subReq->pg_cc_hit_num++;

        pg_cc_assert(i == mgr->pages[ftl_addr.ppa]->remap);

        /* if the page is reading to page cache, the request should be blocked util the read is finished */
        if (sub_ssd->avaliable_time[ftl_addr.ppa] > stime) {
          max_lat = sub_ssd->avaliable_time[ftl_addr.ppa] - stime > max_lat ? 
                    sub_ssd->avaliable_time[ftl_addr.ppa] - stime : max_lat;
        }
      } else {
        /* update stat */
        subReq->pg_cc_miss_num++;

        ftl_addr.ppa = mgr->replacer_ops.pop(mgr, s_lpa, req_lpn);

        pg_cc_assert(mgr->pages[ftl_addr.ppa]->remap == INVALID_LPN);

      }

      page_id = ftl_addr.ppa;
      /* check page_id is valid */
      pg_cc_assert(ftl_addr.g.is_cached == 0);
      req->page_id[ptr] = mgr->pages[page_id]->global_pg_id;
      pg_cc_assert(mgr->pages[page_id]->ref_count <= 65536);
      mgr->pages[page_id]->ref_count++;
      if (mgr->pages[page_id]->ref_count == 1) {
        mgr->replacer_ops.erase(mgr, page_id);
      }
      if (req->is_write) {
        mgr->pages[page_id]->is_dirty = true;
      }
      if (!is_existed) {
        /* copy data to page cache */
        if (!req->is_write || unlikely(is_partion_write(ssd, subReq, i))) {
          read_ctx.slpa = i;
          read_ctx.nlp = 1;
          read_ctx.stime = stime;
          lat = ssd_pg_cc_read_page(ssd, sub_ssd, &read_ctx);
          req->need_dram_rw[ptr] = true;
          max_lat = lat > max_lat ? lat : max_lat;
        }

        sub_ssd->avaliable_time[ftl_addr.ppa] = stime + lat;

        ftl_addr.g.is_cached = 1;
        set_maptbl_ent(ssd, i, &ftl_addr);
        ssd_pg_cc_set_bp_maptbl_ent(ssd, sub_ssd, page_id, &ppa);
        mgr->pages[page_id]->remap = i;
      }

      trans_pg_id2addr(sub_ssd, page_id, req, ptr);
    }
  
  }

  subReq->expire_time = stime + max_lat;
  subReq->nand_lat = max_lat;

  pg_cc_write_context write_ctx = {
      .wlpn = mgr->flush_ctx.wlpn,
      .nlp = mgr->flush_ctx.lpn,
      .stime = stime,
      .is_fast_flush = false,
  };
  pg_cc_assert(mgr->flush_ctx.lpn <= 504);
  ssd_pg_cc_write_page(mgr->ssd, mgr->sub_ssd, &write_ctx);
}

static void page_cache_unpin(struct page_cache_mgr *mgr, pg_cc_unpin_context *ctx) {
  struct page* page;
  uint64_t i;
  struct ppa ppa, ftl_addr;
  struct ssd *ssd = mgr->ssd;

  for (i = ctx->start_lpa; i < ctx->start_lpa + ctx->nlp; i++) {
    pg_cc_assert(shard(i, mgr->sub_ssd) == mgr->sub_ssd->id);
    ppa = get_maptbl_ent(ssd, i);
    pg_cc_assert(ppa.g.is_cached == 1);
    ftl_addr = ppa;
    ftl_addr.g.is_cached = 0;
    page = mgr->pages[ftl_addr.ppa];
    pg_cc_assert(page);
    pg_cc_assert(page->remap == i);
    page->ref_count--;
    
    if (ctx->is_dirty)
      page->is_dirty = true;
      
    if (page->ref_count == 0) {
      mgr->replacer_ops.push(mgr, ftl_addr.ppa);
    }
  }
}

#if 0
static void flush(struct page_cache_mgr *mgr) {
  struct page* page;
  struct ppa ppa;
  struct ssd* ssd = mgr->ssd;
  uint64_t i;

  for (i = 0; i < mgr->n_pages; i++) {
    page = mgr->pages[i];
    if (page->is_dirty) {
      ppa = get_maptbl_ent(ssd, page->remap);
      ppa.g.is_cached = 0;
      // TODO: write page
      // write_page(ssd, &ppa, page->data);
      page->is_dirty = false;
    }
  }
}
#endif

static void page_cache_init(struct page_cache_mgr *mgr, uint64_t page_cache_size, uint64_t begin_global_pg_id) {
  uint64_t i, seg_id = 0, count = 0;
  struct sub_ssd *sub_ssd = mgr->sub_ssd;
  femu_debug("page_cache_init, page_cache_size %ld", page_cache_size);
  mgr->max_page_num = page_cache_size;
  mgr->pages = (struct page**)malloc(sizeof(struct page*) * mgr->max_page_num);
  for (i = 0; i < mgr->max_page_num; i++) {
    mgr->pages[i] = (struct page*)malloc(sizeof(struct page));
    mgr->pages[i]->status = PAGE_UNINITED;
    mgr->pages[i]->ref_count = 0;
    mgr->pages[i]->phy_addr = sub_ssd->page_cache_desptr[seg_id].phy_addr + count * 4096;
    mgr->pages[i]->vir_addr = sub_ssd->page_cache_desptr[seg_id].vir_addr + count * 4096;
    mgr->pages[i]->global_pg_id = begin_global_pg_id + i;
    mgr->pages[i]->remap = INVALID_LPN;
    mgr->pages[i]->is_dirty = false;
    mgr->pages[i]->valid_at = 0;
    mgr->pages[i]->rplc_ptr = NULL;

    count++;
    if (count == sub_ssd->page_cache_desptr[seg_id].size) {
      seg_id++;
      count = 0;
    }
  }
  mgr->inited_page_num = 0;
  mgr->free_page_count = mgr->max_page_num;

  mgr->free_pgs_head = NULL;
  mgr->replacer_wp = NULL;
  mgr->free_pg_frames = g_malloc0(sizeof(*mgr->free_pg_frames) * mgr->max_page_num);
  QTAILQ_INIT(&mgr->free_pg_frame_list);
  for (i = 0; i < mgr->max_page_num; i++) {
    QTAILQ_INSERT_TAIL(&mgr->free_pg_frame_list, &mgr->free_pg_frames[i], entry);
  }

  femu_debug("page_cache_init, mgr->max_page_num %ld\n", mgr->max_page_num);
  page_cache_register_replacer(mgr);
}

static void read_dirty_page(struct page_cache_mgr *mgr, uint64_t wp) {
  uint64_t phy_addr = trans_single_pgid2_phyaddr(mgr->sub_ssd, wp);
  bool need_cp = true;

  struct pg_cc_dram_rw_context rw_ctx = {
    .as = mgr->sub_ssd->as,
    .page_addr = &phy_addr,
    .page_num = 1,
    .start_addr = (mgr->pages[wp]->remap) * ((uint64_t)mgr->ssd->sp.secs_per_pg * mgr->ssd->sp.secsz),
    .is_write = true,
    .need_cp = &need_cp,
  };

  /* cost too much time */
  backend_pg_cc_rw(mgr->sub_ssd->mbe, &rw_ctx);
}

static void write_dirty_page_to_ssd(struct page_cache_mgr *mgr, uint64_t wp) {
  pg_cc_write_context write_ctx = {
    .wlpn = &mgr->pages[wp]->remap,
    .nlp = 1,
    .stime = 0,
    .is_fast_flush = true,
  };

  ssd_pg_cc_write_page(mgr->ssd, mgr->sub_ssd, &write_ctx);
}

static void page_cache_clear(struct sub_ssd *sub_ssd) {
  struct page_cache_mgr *mgr = sub_ssd->page_cache_mgr;
  // struct ssd *ssd = mgr->ssd;

  for (int i = 0; i < mgr->max_page_num; i++) {
    if (mgr->pages[i]->status != PAGE_UNINITED) {
      read_dirty_page(mgr, i);
      pre_invalidate_maptbl_ent(mgr->ssd, mgr->sub_ssd, i);
      write_dirty_page_to_ssd(mgr, i);
#if 0
      } else {
        struct ppa ppa = ssd_pg_cc_get_bp_maptbl_ent(ssd, sub_ssd, i);
        assert(ppa.g.is_cached == 0);
        set_maptbl_ent(ssd, mgr->pages[i]->remap, &ppa);
        struct ppa invalid_ppa;
        invalid_ppa.ppa = INVALID_PPA;
        ssd_pg_cc_set_bp_maptbl_ent(ssd, sub_ssd, i, &invalid_ppa);
      }
#endif      
    }
    free(mgr->pages[i]);
  }

  free(mgr->pages);
  mgr->pages = NULL;

  mgr->max_page_num = 0;
  mgr->inited_page_num = 0;
  mgr->free_page_count = 0;
  free(mgr->free_pg_frames);
  mgr->free_pg_frames = NULL;
  QTAILQ_INIT(&mgr->free_pg_frame_list);
}

void sub_ssd_register_page_cache_mgr(struct sub_ssd *sub_ssd, uint64_t begin_global_pg_id) {
  femu_err("sub_ssd_register_page_cache_mgr, sub_ssd->page_cache_size %ld\n", sub_ssd->page_cache_size);
  sub_ssd->page_cache_mgr = (struct page_cache_mgr*)malloc(sizeof(struct page_cache_mgr));

  sub_ssd->page_cache_ops = (struct page_cache_mgr_ops) {
    .init = page_cache_init,
    .look_up = page_cache_look_up,
    .unpin = page_cache_unpin,
    .prefetch = NULL, //page_cache_prefetch,
    .flush = NULL,
    .clear = page_cache_clear,
  };

  sub_ssd->page_cache_mgr->ssd = sub_ssd->parent_ssd;
  sub_ssd->page_cache_mgr->sub_ssd = sub_ssd;
  sub_ssd->backup_maptbl = (struct ppa*)malloc(sizeof(struct ppa) * sub_ssd->page_cache_size);
  for (int i = 0; i < sub_ssd->page_cache_size; i++) {
    sub_ssd->backup_maptbl[i].ppa = INVALID_PPA;
  }

  sub_ssd->avaliable_time = calloc(sub_ssd->page_cache_size, sizeof(*sub_ssd->avaliable_time));

  femu_debug("sub_ssd_register_page_cache_mgr: sub_ssd->id = %d, sub_ssd->page_cache_size = %ld\n", sub_ssd->id, sub_ssd->page_cache_size);
  
  sub_ssd->page_cache_ops.init(sub_ssd->page_cache_mgr, sub_ssd->page_cache_size, begin_global_pg_id);
}