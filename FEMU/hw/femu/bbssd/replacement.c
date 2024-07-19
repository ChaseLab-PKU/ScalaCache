#include "page_cache.h"
#include "replacement.h"
#include "ftl.h"

struct clock_replacer {
  
};

static void clock_access(struct page_cache_mgr *mgr, uint64_t page_id) {
  // struct pages **pages = mgr->pages;
  // pages[page_id]->is_accessed = true;
}

static inline void insert_free_pg_list(struct page_cache_mgr *mgr, uint64_t page_id) {
  ftl_assert(!QTAILQ_EMPTY(&mgr->free_pg_frame_list));
  struct free_pg *new_pg = QTAILQ_FIRST(&mgr->free_pg_frame_list);
  QTAILQ_REMOVE(&mgr->free_pg_frame_list, new_pg, entry);

  ftl_assert(new_pg);

  new_pg->pg_id = page_id;
  mgr->pages[page_id]->rplc_ptr = new_pg;

  ftl_assert(mgr->pages[page_id]->status == PAGE_USED_ACCESSED);
  ftl_assert(mgr->pages[page_id]->rplc_ptr);
  // printf("push %ld\n", page_id);

  new_pg->pre = NULL;
  new_pg->next = mgr->free_pgs_head;

  if (mgr->free_pgs_head)
    mgr->free_pgs_head->pre = new_pg;

  mgr->free_pgs_head = new_pg;
  ftl_assert(mgr->free_pgs_head);

}

static inline void erase_free_pg_list(struct page_cache_mgr *mgr, uint64_t page_id) {
  struct free_pg *free_pg = mgr->pages[page_id]->rplc_ptr;
  if (!free_pg) {
    return;
  }
  // printf("erase %ld\n", page_id);
  if (free_pg->pre) {
    free_pg->pre->next = free_pg->next;
  }
  if (free_pg->next) {
    free_pg->next->pre = free_pg->pre;
  }

  if (free_pg == mgr->free_pgs_head) {
    mgr->free_pgs_head = free_pg->next;
  }

  if (free_pg == mgr->replacer_wp) {
    mgr->replacer_wp = mgr->replacer_wp->next;
  }

  free_pg->pre = free_pg->next = NULL;
  QTAILQ_INSERT_TAIL(&mgr->free_pg_frame_list, free_pg, entry);
  mgr->pages[page_id]->rplc_ptr = NULL;
}

static void push(struct page_cache_mgr *mgr, uint64_t page_id) {
  struct page **pages = mgr->pages;
  assert(pages[page_id]->status == PAGE_PINNED);
  assert(pages[page_id]->ref_count == 0);
  pages[page_id]->status = PAGE_USED_ACCESSED;
  mgr->free_page_count++; 

  insert_free_pg_list(mgr, page_id);
}

static void __attribute__((unused)) page_init(struct page_cache_mgr *mgr, uint64_t page_id) {
  struct page **pages = mgr->pages;
  pages[page_id]->status = PAGE_UNINITED;
  assert(pages[page_id]->ref_count == 0);
  pages[page_id]->is_dirty = false;
  pages[page_id]->remap = INVALID_LPN;
}

static inline bool is_inclusive_page(struct page_cache_mgr *mgr, uint64_t wp, uint64_t start_lpa, uint64_t nlp) {
  if (mgr->pages[wp]->remap != INVALID_LPN) {
    return mgr->pages[wp]->remap >= start_lpa && mgr->pages[wp]->remap < start_lpa + nlp;
  } else {
    return true;
  }
}

static inline void move_wp_pointer(struct page_cache_mgr *mgr) {
  ftl_assert(mgr->free_pgs_head);
  ftl_assert(mgr->replacer_wp);
  mgr->replacer_wp = (mgr->replacer_wp->next)? mgr->replacer_wp->next : mgr->free_pgs_head;
  ftl_assert(mgr->replacer_wp);
}

// may need evict
static uint64_t pop(struct page_cache_mgr *mgr, uint64_t start_lpa, uint64_t nlp) {
  struct page **pages = mgr->pages;
  uint64_t wp;

  if (unlikely(mgr->inited_page_num < mgr->max_page_num)) {
    uint64_t page_pos = mgr->inited_page_num++;
    femu_debug("peek page from free page, inited_page_num %ld, page_pos %ld\n", 
                mgr->inited_page_num, page_pos);
    return page_pos;
  } else {
    ftl_assert(mgr->free_page_count > 0);
    ftl_assert(mgr->free_pgs_head != NULL);

    if (!mgr->replacer_wp) {
      mgr->replacer_wp = mgr->free_pgs_head;
      ftl_assert(mgr->free_pgs_head);
    }

    ftl_assert(mgr->replacer_wp);

//    int count = 0;

    while(true) {
      ftl_assert(mgr->replacer_wp);
      wp = mgr->replacer_wp->pg_id;
#if 0
      count++;

      if (count > 2 * mgr->free_page_count) {
        for (int i = 0; i < mgr->free_page_count; i++) {
          wp = mgr->replacer_wp->pg_id;
          const char *in = is_inclusive_page(mgr, wp, start_lpa, nlp) ? "inclusive" : "exclusive";
          printf("SSD ID %d, page %ld, status %d, remap %ld, %s\n", mgr->sub_ssd->id, wp, pages[wp]->status, pages[wp]->remap, in);
          move_wp_pointer(mgr);
        }
        ftl_assert(0);
      }
#endif
      // printf("travel %ld\n", wp);
      ftl_assert(pages[wp]->status == PAGE_USED_ACCESSED || pages[wp]->status == PAGE_USED_EVICTABLE);
      if (is_inclusive_page(mgr, wp, start_lpa, nlp)) {
        move_wp_pointer(mgr);
        continue;
      }
      if (pages[wp]->status == PAGE_USED_ACCESSED) {
        pages[wp]->status = PAGE_USED_EVICTABLE;
        move_wp_pointer(mgr);
        continue ;
      } else {
        mgr->replacer_wp = mgr->replacer_wp->next;
        // delay to erase phase
        // erase_free_pg_list(mgr, wp);
        break ;
      }
    }
    /*
    uint64_t del_wp = wp;
    uint64_t prefetch_loop = 1024;
    while (pages[wp]->is_valid && --prefetch_loop) {
      wp = (wp + 1) % max_page_num;
    }
    */
    // printf("peek %ld\n", wp);
    pg_cc_assert(pages[wp]->ref_count == 0);
    // update latency
    if (pages[wp]->is_dirty) { 
#if 0
      uint64_t phy_addr = trans_single_pgid2_phyaddr(mgr->sub_ssd, wp);
      bool need_cp = true;

      struct pg_cc_dram_rw_context rw_ctx = {
        .as = mgr->sub_ssd->as,
        .page_addr = &phy_addr,
        .page_num = 1,
        .start_addr = (pages[wp]->remap) * ((uint64_t)mgr->ssd->sp.secs_per_pg * mgr->ssd->sp.secsz),
        .is_write = true,
        .need_cp = &need_cp,
      };

      /* cost too much time */
      backend_pg_cc_rw(mgr->sub_ssd->mbe, &rw_ctx);
#endif

      mgr->flush_ctx.wlpn[mgr->flush_ctx.lpn] = pages[wp]->remap;
      mgr->flush_ctx.lpn++;

      // pre-invalidate the bp_maptbl_ent
      // WARN: before actually call pg_cc_write, the maptbl entry of pages[wp]->remap is incorrect
      pre_invalidate_maptbl_ent(mgr->ssd, mgr->sub_ssd, wp);

      pages[wp]->is_dirty = false;
    } else {
      struct ppa origin_ppa = ssd_pg_cc_get_bp_maptbl_ent(mgr->ssd, mgr->sub_ssd, wp);
      // pg_cc_assert(origin_ppa.ppa != INVALID_PPA);
      pg_cc_assert(pages[wp]->remap != INVALID_LPN);
      set_maptbl_ent(mgr->ssd, pages[wp]->remap, &origin_ppa);
    }

    struct ppa invalid_ppa;
    invalid_ppa.ppa = INVALID_PPA;
    ssd_pg_cc_set_bp_maptbl_ent(mgr->ssd, mgr->sub_ssd, wp, &invalid_ppa);
    pages[wp]->remap = INVALID_LPN;
    pages[wp]->is_dirty = false;

    return wp;
  }
}

#if 0
static int size(struct page_cache_mgr *mgr) {
  return mgr->free_page_count;
}
#endif

// do not need evict
static void erase(struct page_cache_mgr *mgr, uint64_t page_id) {
  struct page **pages = mgr->pages;
  ftl_assert(pages[page_id]->status != PAGE_PINNED);
  erase_free_pg_list(mgr, page_id);

  pages[page_id]->status = PAGE_PINNED;
  mgr->free_page_count--;
}

void page_cache_register_replacer(struct page_cache_mgr *mgr) {
  mgr->replacer_ops = (struct page_cache_replacer_ops) {
    .access = clock_access,
    .push = push,
    .pop = pop,
    .erase = erase,
  };
}