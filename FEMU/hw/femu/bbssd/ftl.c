#include "ftl.h"

//#define FEMU_DEBUG_FTL
#define SPDK_MOCK
#define LOG_PERIOD 2000000
#define FTL_MAX_LATENCY 0

static void *ftl_thread(void *arg);
struct ftl_arg {
    int ftl_id;
    FemuCtrl *n;
};

static inline bool should_gc(struct ssd *ssd, struct sub_ssd *sub_ssd)
{
    /* TODO: need to provide correct parameter for GC */
    return (sub_ssd->lm.free_line_cnt <= ssd->sp.gc_thres_lines);
}

static inline bool should_gc_high(struct ssd *ssd, struct sub_ssd *sub_ssd)
{
    return (sub_ssd->lm.free_line_cnt <= ssd->sp.gc_thres_lines_high);
}

struct ppa get_maptbl_ent(struct ssd *ssd, uint64_t lpn)
{
    return ssd->maptbl[lpn];
}

void set_maptbl_ent(struct ssd *ssd, uint64_t lpn, struct ppa *ppa)
{
    ftl_assert(lpn < ssd->sp.tt_pgs);
    ssd->maptbl[lpn] = *ppa;
}

static uint64_t ppa2pgidx(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    uint64_t pgidx;

    pgidx = ppa->g.ch  * spp->pgs_per_ch  + \
            ppa->g.lun * spp->pgs_per_lun + \
            ppa->g.pl  * spp->pgs_per_pl  + \
            ppa->g.blk * spp->pgs_per_blk + \
            ppa->g.pg;

    ftl_assert(pgidx < spp->tt_pgs);

    return pgidx;
}

static inline uint64_t get_rmap_ent(struct ssd *ssd, struct ppa *ppa)
{
    uint64_t pgidx = ppa2pgidx(ssd, ppa);

    return ssd->rmap[pgidx];
}

/* set rmap[page_no(ppa)] -> lpn */
static inline void set_rmap_ent(struct ssd *ssd, uint64_t lpn, struct ppa *ppa)
{
    uint64_t pgidx = ppa2pgidx(ssd, ppa);

    ssd->rmap[pgidx] = lpn;
}

static inline int victim_line_cmp_pri(pqueue_pri_t next, pqueue_pri_t curr)
{
    return (next > curr);
}

static inline pqueue_pri_t victim_line_get_pri(void *a)
{
    return ((struct line *)a)->vpc;
}

static inline void victim_line_set_pri(void *a, pqueue_pri_t pri)
{
    ((struct line *)a)->vpc = pri;
}

static inline size_t victim_line_get_pos(void *a)
{
    return ((struct line *)a)->pos;
}

static inline void victim_line_set_pos(void *a, size_t pos)
{
    ((struct line *)a)->pos = pos;
}

static void ssd_init_lines(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;
    struct line_mgmt *lm;
    struct line *line;

    for (int i = 0; i < ssd->sub_ssd_num; i++) {
        lm = &(ssd->sub_ssds[i].lm);
        lm->tt_lines = spp->tt_lines;
        lm->lines = g_malloc0(sizeof(struct line) * lm->tt_lines);

        QTAILQ_INIT(&lm->free_line_list);
        lm->victim_line_pq = pqueue_init(spp->tt_lines, victim_line_cmp_pri,
                victim_line_get_pri, victim_line_set_pri,
                victim_line_get_pos, victim_line_set_pos);
        QTAILQ_INIT(&lm->full_line_list);

        lm->free_line_cnt = 0;
        lm->victim_line_cnt = 0;
        lm->full_line_cnt = 0;

    }

    for (int ssd_index = 0; ssd_index < ssd->sub_ssd_num; ssd_index++) {
        lm = &(ssd->sub_ssds[ssd_index].lm);
        for (int i = 0; i < spp->tt_lines; i++) {
            line = &lm->lines[i];
            if (line->id != 0) {
                assert(0);
            }
            line->id = i;
            line->ipc = 0;
            line->vpc = 0;
            line->pos = 0;
            /* initialize all the lines as free lines */
            QTAILQ_INSERT_TAIL(&lm->free_line_list, line, entry);
            lm->free_line_cnt++;
        }
        ftl_assert(lm->free_line_cnt == spp->tt_lines);
    }
}

static void get_init_write_pointer(struct ssd *ssd, struct sub_ssd* sub_ssd)
{
    struct ssdparams *spp = &ssd->sp;
    struct write_pointer *wpp;
    struct line_mgmt *lm;
    struct line *curline = NULL;

    lm = &(sub_ssd->lm);
    wpp = &(sub_ssd->wp);
    curline = QTAILQ_FIRST(&lm->free_line_list);
    if (!curline) {
        printf("no free line\n");
        assert(0);
    }
    QTAILQ_REMOVE(&lm->free_line_list, curline, entry);
    lm->free_line_cnt--;

    uint64_t tt_luns = spp->tt_luns;
    uint64_t init_id = sub_ssd->id * (tt_luns / ssd->sub_ssd_num);
    uint64_t init_ch = init_id / spp->luns_per_ch;
    uint64_t init_luns = init_id % spp->luns_per_ch;
    assert(tt_luns % ssd->sub_ssd_num == 0);

    /* wpp->curline is always our next-to-write super-block */
    wpp->curline = curline;
    wpp->ch = init_ch;
    wpp->lun = init_luns;
    wpp->pg = 0;
    wpp->blk = curline->id;
    wpp->pl = 0;
    wpp->prl_id = init_id;
}

static void ssd_init_write_pointer(struct ssd *ssd)
{
    for (int i = 0; i < ssd->sub_ssd_num; i++) {
        get_init_write_pointer(ssd, &ssd->sub_ssds[i]);
    }
}

static inline void check_addr(int a, int max)
{
    ftl_assert(a >= 0 && a < max);
}

#if 0
static struct line *get_next_free_line(struct ssd *ssd, struct sub_ssd *sub_ssd)
{
    struct line_mgmt *lm = &sub_ssd->lm;
    struct line *curline = NULL;

    curline = QTAILQ_FIRST(&lm->free_line_list);
    if (!curline) {
        ftl_err("No free lines left in [%s] !!!!\n", ssd->ssdname);
        return NULL;
    }

    QTAILQ_REMOVE(&lm->free_line_list, curline, entry);
    lm->free_line_cnt--;
    return curline;
}
#endif

static void ssd_advance_write_pointer(struct ssd *ssd, struct sub_ssd *sub_ssd)
{
    struct ssdparams *spp = &ssd->sp;
    struct write_pointer *wpp = &sub_ssd->wp;
    struct line_mgmt *lm = &sub_ssd->lm;
    uint64_t end_prl_id = (sub_ssd->id + 1) * (spp->tt_luns / ssd->sub_ssd_num);
    bool is_new_pg = false;

    wpp->prl_id++;
    if (wpp->prl_id == end_prl_id) {
        wpp->prl_id = sub_ssd->id * (spp->tt_luns / ssd->sub_ssd_num);
        is_new_pg = true;
    }
    wpp->ch = wpp->prl_id / spp->luns_per_ch;
    wpp->lun = wpp->prl_id % spp->luns_per_ch;
    check_addr(wpp->ch, spp->nchs);
    check_addr(wpp->lun, spp->luns_per_ch);
    if (is_new_pg)
    {
        /* go to next page in the block */
        check_addr(wpp->pg, spp->pgs_per_blk);
        wpp->pg++;
        if (wpp->pg == spp->pgs_per_blk)
        {
            wpp->pg = 0;
            /* move current line to {victim,full} line list */
            if (wpp->curline->vpc == spp->pgs_per_line_per_subssd)
            {
                /* all pgs are still valid, move to full line list */
                ftl_assert(wpp->curline->ipc == 0);
                QTAILQ_INSERT_TAIL(&lm->full_line_list, wpp->curline, entry);
                lm->full_line_cnt++;
            }
            else
            {
                ftl_assert(wpp->curline->vpc >= 0 && wpp->curline->vpc < spp->pgs_per_line_per_subssd);
                /* there must be some invalid pages in this line */
                ftl_assert(wpp->curline->ipc > 0);
                pqueue_insert(lm->victim_line_pq, wpp->curline);
                lm->victim_line_cnt++;
            }
            /* current line is used up, pick another empty line */
            check_addr(wpp->blk, spp->blks_per_pl);
            get_init_write_pointer(ssd, sub_ssd);
        }
    }
}

static struct ppa get_new_page(struct ssd *ssd, struct sub_ssd *sub_ssd)
{
    struct write_pointer *wpp = &sub_ssd->wp;
    struct ppa ppa;
    ppa.ppa = 0;
    ppa.g.ch = wpp->ch;
    ppa.g.lun = wpp->lun;
    ppa.g.pg = wpp->pg;
    ppa.g.blk = wpp->blk;
    ppa.g.pl = wpp->pl;
    ftl_assert(ppa.g.pl == 0);

    return ppa;
}

static void check_params(struct ssdparams *spp)
{
    /*
     * we are using a general write pointer increment method now, no need to
     * force luns_per_ch and nchs to be power of 2
     */

    //ftl_assert(is_power_of_2(spp->luns_per_ch));
    //ftl_assert(is_power_of_2(spp->nchs));
}

static void ssd_init_params(struct FemuCtrl *n, struct ssdparams *spp)
{
    spp->secsz = 512;
    spp->secs_per_pg = 8;
    spp->pgs_per_blk = 2048;
    spp->blks_per_pl = 1024; /* 16GB */
    spp->pls_per_lun = 1;
    spp->luns_per_ch = 4;
    spp->nchs = 8;

    assert(spp->secsz == 512);
    assert(spp->secs_per_pg == 8);

    spp->pg_rd_lat = n->read_lat;
    spp->pg_wr_lat = n->write_lat;
    spp->blk_er_lat = n->erase_lat;
    spp->ch_xfer_lat = 0;

    /* calculated values */
    spp->secs_per_blk = spp->secs_per_pg * spp->pgs_per_blk;
    spp->secs_per_pl = spp->secs_per_blk * spp->blks_per_pl;
    spp->secs_per_lun = spp->secs_per_pl * spp->pls_per_lun;
    spp->secs_per_ch = spp->secs_per_lun * spp->luns_per_ch;
    spp->tt_secs = spp->secs_per_ch * spp->nchs;

    spp->pgs_per_pl = spp->pgs_per_blk * spp->blks_per_pl;
    spp->pgs_per_lun = spp->pgs_per_pl * spp->pls_per_lun;
    spp->pgs_per_ch = spp->pgs_per_lun * spp->luns_per_ch;
    spp->tt_pgs = spp->pgs_per_ch * spp->nchs;

    spp->blks_per_lun = spp->blks_per_pl * spp->pls_per_lun;
    spp->blks_per_ch = spp->blks_per_lun * spp->luns_per_ch;
    spp->tt_blks = spp->blks_per_ch * spp->nchs; /* total blocks of SSD */

    spp->pls_per_ch =  spp->pls_per_lun * spp->luns_per_ch;
    spp->tt_pls = spp->pls_per_ch * spp->nchs;  /* total planes of SSD */

    spp->tt_luns = spp->luns_per_ch * spp->nchs;

    /* line is special, put it at the end */
    spp->blks_per_line = spp->tt_luns; /* TODO: to fix under multiplanes */
    spp->pgs_per_line = spp->blks_per_line * spp->pgs_per_blk;
    spp->pgs_per_line_per_subssd = spp->pgs_per_line / n->num_ftl;
    assert(n->num_ftl != 0);
    assert(spp->pgs_per_line % n->num_ftl == 0);
    spp->secs_per_line = spp->pgs_per_line * spp->secs_per_pg;
    spp->tt_lines = spp->blks_per_lun; /* TODO: to fix under multiplanes */

    spp->gc_thres_pcent = 0.75;
    spp->gc_thres_lines = (int)((1 - spp->gc_thres_pcent) * spp->tt_lines);
    spp->gc_thres_pcent_high = 0.95;
    spp->gc_thres_lines_high = (int)((1 - spp->gc_thres_pcent_high) * spp->tt_lines);
    spp->enable_gc_delay = true;

    // spp->shard_length = (uint64_t)spp->blks_per_line * spp->secs_per_pg * spp->secsz;
    spp->shard_length = 4096;

    check_params(spp);
}

static void ssd_init_nand_page(struct nand_page *pg, struct ssdparams *spp)
{
    pg->nsecs = spp->secs_per_pg;
    pg->sec = g_malloc0(sizeof(nand_sec_status_t) * pg->nsecs);
    for (int i = 0; i < pg->nsecs; i++) {
        pg->sec[i] = SEC_FREE;
    }
    pg->status = PG_FREE;
}

static void ssd_init_nand_blk(struct nand_block *blk, struct ssdparams *spp)
{
    blk->npgs = spp->pgs_per_blk;
    blk->pg = g_malloc0(sizeof(struct nand_page) * blk->npgs);
    for (int i = 0; i < blk->npgs; i++) {
        ssd_init_nand_page(&blk->pg[i], spp);
    }
    blk->ipc = 0;
    blk->vpc = 0;
    blk->erase_cnt = 0;
    blk->wp = 0;
}

static void ssd_init_nand_plane(struct nand_plane *pl, struct ssdparams *spp)
{
    pl->nblks = spp->blks_per_pl;
    pl->blk = g_malloc0(sizeof(struct nand_block) * pl->nblks);
    for (int i = 0; i < pl->nblks; i++) {
        ssd_init_nand_blk(&pl->blk[i], spp);
    }
}

static void ssd_init_nand_lun(struct nand_lun *lun, struct ssdparams *spp)
{
    lun->npls = spp->pls_per_lun;
    lun->pl = g_malloc0(sizeof(struct nand_plane) * lun->npls);
    for (int i = 0; i < lun->npls; i++) {
        ssd_init_nand_plane(&lun->pl[i], spp);
    }
    lun->next_lun_avail_time = 0;
    if (pthread_mutex_init(&lun->mutex, NULL) != 0) {
        femu_err("mutex init failed\n");
        assert(0);
    }
    lun->busy = false;
}

static void ssd_init_ch(struct ssd_channel *ch, struct ssdparams *spp)
{
    ch->nluns = spp->luns_per_ch;
    ch->lun = g_malloc0(sizeof(struct nand_lun) * ch->nluns);
    for (int i = 0; i < ch->nluns; i++) {
        ssd_init_nand_lun(&ch->lun[i], spp);
    }
    ch->next_ch_avail_time = 0;
    ch->busy = 0;
}

static void ssd_init_maptbl(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;

    ssd->maptbl = g_malloc0(sizeof(struct ppa) * spp->tt_pgs);
    for (int i = 0; i < spp->tt_pgs; i++) {
        ssd->maptbl[i].ppa = UNMAPPED_PPA;
    }
}

static void ssd_init_rmap(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;

    ssd->rmap = g_malloc0(sizeof(uint64_t) * spp->tt_pgs);
    for (int i = 0; i < spp->tt_pgs; i++) {
        ssd->rmap[i] = INVALID_LPN;
    }
}

void pg_cc_init(FemuCtrl *n)
{
    struct ssd *ssd = n->ssd;
    struct sub_ssd *sub_ssd = NULL;
    uint64_t global_pg_id = 0;
    femu_debug("Initializing page cache manager, hmb_desptr_size %u, sub_ssd_num %u\n", 
                n->g_hmb_desptr_size, ssd->sub_ssd_num);
    // assert(n->g_hmb_desptr_size % ssd->sub_ssd_num == 0);
    uint64_t last_index = 0;
    for (int i = 0; i < ssd->sub_ssd_num; i++) {
        sub_ssd = &ssd->sub_ssds[i];
        assert(sub_ssd);
        sub_ssd->page_cache_desptr_sz = (n->g_hmb_desptr_size / ssd->sub_ssd_num);
        if ((n->g_hmb_desptr_size % ssd->sub_ssd_num) > i) {
            sub_ssd->page_cache_desptr_sz += 1;
        }
        sub_ssd->page_cache_desptr = &n->g_hmb_desptr[last_index];
        last_index += sub_ssd->page_cache_desptr_sz;

        femu_debug("sub_ssd %u, page_cache_size %lu, page_cache_desptr_sz %u, page_cache_desptr %p\n",
                    i, sub_ssd->page_cache_size, sub_ssd->page_cache_desptr_sz, sub_ssd->page_cache_desptr);
        
        sub_ssd->page_cache_desptr[0].start_pg_id = 0;
        sub_ssd->page_cache_size = sub_ssd->page_cache_desptr[0].size;
        for (int j = 1; j < sub_ssd->page_cache_desptr_sz; j++) {
            sub_ssd->page_cache_desptr[j].start_pg_id = 
                sub_ssd->page_cache_desptr[j - 1].start_pg_id + sub_ssd->page_cache_desptr[j - 1].size;
            
            sub_ssd->page_cache_size += sub_ssd->page_cache_desptr[j].size;
            
            femu_debug("sub_ssd %u, page_cache_desptr[%u].start_pg_id %u, size %u, phy_addr %lx\n",
                        i, j, sub_ssd->page_cache_desptr[j].start_pg_id, sub_ssd->page_cache_desptr[j].size,
                        sub_ssd->page_cache_desptr[j].phy_addr);
        }
        sub_ssd_register_page_cache_mgr(sub_ssd, global_pg_id);
        global_pg_id += sub_ssd->page_cache_size;
    }
    // assert(n->pg_cc_enabled == false);
    n->pg_cc_enabled = true;
}

void pg_cc_clear(FemuCtrl *n) {
    struct ssd *ssd = n->ssd;

    for (int i = 0; i < ssd->sub_ssd_num; i++) {
        ssd->sub_ssds[i].page_cache_size = 0;
        ssd->sub_ssds[i].page_cache_desptr_sz = 0;
        ssd->sub_ssds[i].page_cache_desptr = NULL;
    }
    
    // n->pg_cc_enabled = false;
}

void ssd_init(FemuCtrl *n)
{
    struct ssd *ssd = n->ssd;
    struct ssdparams *spp = &ssd->sp;

    ftl_assert(ssd);

    ssd_init_params(n, spp);
    // n->shard_length = (uint64_t)spp->pgs_per_line / spp->blks_per_line * spp->secs_per_pg * spp->secsz;
    n->shard_length = spp->shard_length;
    ((uint32_t *)(n->id_ctrl.vs))[1] = (n->shard_length) / 4096;
    assert(n->shard_length != 0);
    n->secs_per_pg = spp->secs_per_pg;
    n->secsz = spp->secsz;
    n->pgsz = spp->secsz * spp->secs_per_pg;
    
    ssd->sub_ssd_num = n->num_ftl;
    ssd->sub_ssds = g_malloc0(sizeof(struct sub_ssd) * ssd->sub_ssd_num);

    spp->gc_thres_lines /= ssd->sub_ssd_num;
    spp->gc_thres_lines_high /= ssd->sub_ssd_num;

    ftl_assert(ssd->sub_ssds);
    for (int i = 0; i < ssd->sub_ssd_num; i++) {
        ssd->sub_ssds[i].id = i;
        ssd->sub_ssds[i].parent_ssd = ssd;
        ssd->sub_ssds[i].as = pci_get_address_space(&n->parent_obj);
        ssd->sub_ssds[i].mbe = n->mbe;
    }

    /* initialize ssd internal layout architecture */
    ssd->ch = g_malloc0(sizeof(struct ssd_channel) * spp->nchs);
    for (int i = 0; i < spp->nchs; i++) {
        ssd_init_ch(&ssd->ch[i], spp);
    }

    /* initialize maptbl */
    ssd_init_maptbl(ssd);

    /* initialize rmap */
    ssd_init_rmap(ssd);

    /* initialize all the lines */
    ssd_init_lines(ssd);

    /* initialize write pointer, this is how we allocate new pages for writes */
    ssd_init_write_pointer(ssd);

    printf("femu create %u ftl_thread\n", n->num_ftl);

    ssd->ftl_thread = malloc(sizeof(QemuThread) * (n->num_ftl + 1));

    for (int i = 1; i <= n->num_ftl; i++) {
        struct ftl_arg *arg = malloc(sizeof(struct ftl_arg));
        arg->n = n;
        arg->ftl_id = i;
        qemu_thread_create(&ssd->ftl_thread[i], "FEMU-FTL-Thread", ftl_thread, arg,
                       QEMU_THREAD_JOINABLE);
    }
}

static inline bool valid_ppa(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    int ch = ppa->g.ch;
    int lun = ppa->g.lun;
    int pl = ppa->g.pl;
    int blk = ppa->g.blk;
    int pg = ppa->g.pg;
    int sec = ppa->g.sec;

    if (ch >= 0 && ch < spp->nchs && lun >= 0 && lun < spp->luns_per_ch && pl >=
        0 && pl < spp->pls_per_lun && blk >= 0 && blk < spp->blks_per_pl && pg
        >= 0 && pg < spp->pgs_per_blk && sec >= 0 && sec < spp->secs_per_pg)
        return true;

    return false;
}

static inline bool valid_lpn(struct ssd *ssd, uint64_t lpn)
{
    return (lpn < ssd->sp.tt_pgs);
}

static inline bool mapped_ppa(struct ppa *ppa)
{
    return !(ppa->ppa == UNMAPPED_PPA);
}

static inline struct ssd_channel *get_ch(struct ssd *ssd, struct ppa *ppa)
{
    return &(ssd->ch[ppa->g.ch]);
}

static inline struct nand_lun *get_lun(struct ssd *ssd, struct ppa *ppa)
{
    struct ssd_channel *ch = get_ch(ssd, ppa);
    return &(ch->lun[ppa->g.lun]);
}

static inline struct nand_plane *get_pl(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_lun *lun = get_lun(ssd, ppa);
    return &(lun->pl[ppa->g.pl]);
}

static inline struct nand_block *get_blk(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_plane *pl = get_pl(ssd, ppa);
    return &(pl->blk[ppa->g.blk]);
}

static inline struct line *get_line(struct ssd *ssd, struct ppa *ppa, struct sub_ssd *sub_ssd)
{
    return &(sub_ssd->lm.lines[ppa->g.blk]);
}

static inline struct nand_page *get_pg(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_block *blk = get_blk(ssd, ppa);
    return &(blk->pg[ppa->g.pg]);
}

static uint64_t ssd_advance_status(struct ssd *ssd, struct ppa *ppa, struct
        nand_cmd *ncmd)
{
    int c = ncmd->cmd;
    uint64_t cmd_stime = (ncmd->stime == 0) ? \
        qemu_clock_get_ns(QEMU_CLOCK_REALTIME) : ncmd->stime;
    uint64_t nand_stime;
    struct ssdparams *spp = &ssd->sp;
    struct nand_lun *lun = get_lun(ssd, ppa);
    uint64_t lat = 0;

    pthread_mutex_lock(&lun->mutex);

    switch (c) {
    case NAND_READ:
        /* read: perform NAND cmd first */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->pg_rd_lat;
        lat = lun->next_lun_avail_time - cmd_stime;
#if 0
        lun->next_lun_avail_time = nand_stime + spp->pg_rd_lat;

        /* read: then data transfer through channel */
        chnl_stime = (ch->next_ch_avail_time < lun->next_lun_avail_time) ? \
            lun->next_lun_avail_time : ch->next_ch_avail_time;
        ch->next_ch_avail_time = chnl_stime + spp->ch_xfer_lat;

        lat = ch->next_ch_avail_time - cmd_stime;
#endif
        break;

    case NAND_WRITE:
        /* write: transfer data through channel first */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        if (likely(ncmd->type == USER_IO)) {
            lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat;
        } else if (ncmd->type == GC_IO) {
            lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat;
        } else if (ncmd->type == FAST_FLUSH_IO) {
            lun->next_lun_avail_time = nand_stime;
        } else {
            printf("Error: unknown ncmd type\n");
            assert(0);
        }
        lat = lun->next_lun_avail_time - cmd_stime;

#if 0
        chnl_stime = (ch->next_ch_avail_time < cmd_stime) ? cmd_stime : \
                     ch->next_ch_avail_time;
        ch->next_ch_avail_time = chnl_stime + spp->ch_xfer_lat;

        /* write: then do NAND program */
        nand_stime = (lun->next_lun_avail_time < ch->next_ch_avail_time) ? \
            ch->next_ch_avail_time : lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat;

        lat = lun->next_lun_avail_time - cmd_stime;
#endif
        break;

    case NAND_ERASE:
        /* erase: only need to advance NAND status */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->blk_er_lat;

        lat = lun->next_lun_avail_time - cmd_stime;
        break;

    default:
        ftl_err("Unsupported NAND command: 0x%x\n", c);
    }

    pthread_mutex_unlock(&lun->mutex);

    return lat;
}

#ifdef SPDK_MOCK
/* update SSD status about one page from PG_VALID -> PG_VALID */
static void mark_page_invalid(struct ssd *ssd, struct ppa *ppa, struct sub_ssd *sub_ssd)
{
    struct line_mgmt *lm = &sub_ssd->lm;
    struct ssdparams *spp = &ssd->sp;
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;
    bool was_full_line = false;
    struct line *line;

    /* update corresponding page status */
    pg = get_pg(ssd, ppa);
    ftl_assert(pg->status == PG_VALID);
    pg->status = PG_INVALID;

    /* update corresponding block status */
    blk = get_blk(ssd, ppa);
    ftl_assert(blk->ipc >= 0 && blk->ipc < spp->pgs_per_blk);
    blk->ipc++;
    ftl_assert(blk->vpc > 0 && blk->vpc <= spp->pgs_per_blk);
    blk->vpc--;

    /* update corresponding line status */
    line = get_line(ssd, ppa, sub_ssd);
    ftl_assert(line->ipc >= 0 && line->ipc < spp->pgs_per_line_per_subssd);
    if (line->vpc == spp->pgs_per_line_per_subssd) {
        ftl_assert(line->ipc == 0);
        was_full_line = true;
    }
    line->ipc++;
    if (line->vpc <= 0 || line->vpc > spp->pgs_per_line_per_subssd) {
        ftl_err("sub_ssd id: %d\n", sub_ssd->id);
        ftl_err("ppa ch:%d lun:%d blk:%d pg:%d\n", ppa->g.ch, ppa->g.lun, ppa->g.blk, ppa->g.pg);
        ftl_err("line->id: %d, line->vpc: %d, pgs_per_line_per_subssd %d\n", line->id, line->vpc, spp->pgs_per_line_per_subssd);
    }
    ftl_assert(line->vpc > 0 && line->vpc <= spp->pgs_per_line_per_subssd);
    /* Adjust the position of the victime line in the pq under over-writes */
    if (line->pos) {
        /* Note that line->vpc will be updated by this call */
        pqueue_change_priority(lm->victim_line_pq, line->vpc - 1, line);
    } else {
        line->vpc--;
    }

    if (was_full_line) {
        /* move line: "full" -> "victim" */
        QTAILQ_REMOVE(&lm->full_line_list, line, entry);
        lm->full_line_cnt--;
        pqueue_insert(lm->victim_line_pq, line);
        lm->victim_line_cnt++;
    }
}
#endif

static void mark_page_valid(struct ssd *ssd, struct ppa *ppa, struct sub_ssd *sub_ssd)
{
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;
    struct line *line;

    /* update page status */
    pg = get_pg(ssd, ppa);
    ftl_assert(pg->status == PG_FREE);
    pg->status = PG_VALID;

    /* update corresponding block status */
    blk = get_blk(ssd, ppa);
    ftl_assert(blk->vpc >= 0 && blk->vpc < ssd->sp.pgs_per_blk);
    blk->vpc++;

    /* update corresponding line status */
    line = get_line(ssd, ppa, sub_ssd);
    if (line->vpc < 0 || line->vpc >= ssd->sp.pgs_per_line_per_subssd) {
        printf("sub_ssd id: %d\n", sub_ssd->id);
        printf("vpc %d, pgs_per_line_per_subssd %d\n", line->vpc, ssd->sp.pgs_per_line_per_subssd);
    }
    ftl_assert(line->vpc >= 0 && line->vpc < ssd->sp.pgs_per_line_per_subssd);
    line->vpc++;
}

static void mark_block_free(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    struct nand_block *blk = get_blk(ssd, ppa);
    struct nand_page *pg = NULL;

    for (int i = 0; i < spp->pgs_per_blk; i++) {
        /* reset page status */
        pg = &blk->pg[i];
        ftl_assert(pg->nsecs == spp->secs_per_pg);
        pg->status = PG_FREE;
    }

    /* reset block status */
    ftl_assert(blk->npgs == spp->pgs_per_blk);
    blk->ipc = 0;
    blk->vpc = 0;
    blk->erase_cnt++;
}

static void gc_read_page(struct ssd *ssd, struct ppa *ppa)
{
    /* advance ssd status, we don't care about how long it takes */
    if (ssd->sp.enable_gc_delay) {
        struct nand_cmd gcr;
        gcr.type = GC_IO;
        gcr.cmd = NAND_READ;
        gcr.stime = 0;
        ssd_advance_status(ssd, ppa, &gcr);
    }
}

static int __attribute__((unused)) shard(uint64_t lpa, struct sub_ssd *sub_ssd) {
  struct ssd *ssd = sub_ssd->parent_ssd;
  int ftl_num = ssd->sub_ssd_num;

  return (lpa * 4096 / (ssd->sp.shard_length)) % ftl_num;
}

/* move valid page data (already in DRAM) from victim line to a new page */
static uint64_t gc_write_page(struct ssd *ssd, struct ppa *ssd_ppa, struct sub_ssd *sub_ssd)
{
    struct ppa new_ppa, old_ppa, backup_ppa;
    struct nand_lun *new_lun;
    uint64_t page_id = INVALID_LPN;
    bool is_cached = false;
    uint64_t lpn = get_rmap_ent(ssd, ssd_ppa);

    pg_cc_assert(shard(lpn, sub_ssd) == sub_ssd->id);

    old_ppa = get_maptbl_ent(ssd, lpn);
    if (old_ppa.g.is_cached) {
        is_cached = true;
        old_ppa.g.is_cached = 0;
        page_id = old_ppa.ppa;
        backup_ppa = ssd_pg_cc_get_bp_maptbl_ent(ssd, sub_ssd, page_id);
        old_ppa.g.is_cached = 1;
    }

    if (is_cached) {
        if (backup_ppa.ppa != ssd_ppa->ppa) {
            ftl_err("lpn: %lu, page_id: %lu, backup_ppa.ppa: %lx, ssd_ppa->ppa: %lx, old_ppa.ppa: %lx\n", 
            lpn, page_id, backup_ppa.ppa, ssd_ppa->ppa, old_ppa.ppa);
        }
        ftl_assert(backup_ppa.ppa == ssd_ppa->ppa);
    }

    // printf("gc_write_page: lpn: %lu, page_id %lu, backup_ppa.ppa: %lx, ssd_ppa->ppa: %lx, old_ppa.ppa: %lx\n", 
    //        lpn, page_id, backup_ppa.ppa, ssd_ppa->ppa, old_ppa.ppa);

    ftl_assert(valid_lpn(ssd, lpn));
    new_ppa = get_new_page(ssd, sub_ssd);
    assert(get_rmap_ent(ssd, &new_ppa) == INVALID_LPN);

    /* update maptbl */
    if (!is_cached) {
        set_maptbl_ent(ssd, lpn, &new_ppa);
        
        set_rmap_ent(ssd, INVALID_LPN, &old_ppa);
    }
    else {
        ssd_pg_cc_set_bp_maptbl_ent(ssd, sub_ssd, page_id, &new_ppa);

        set_rmap_ent(ssd, INVALID_LPN, &backup_ppa);
    }
    /* update rmap */
    set_rmap_ent(ssd, lpn, &new_ppa);

    mark_page_valid(ssd, &new_ppa, sub_ssd);

    /* need to advance the write pointer here */
    ssd_advance_write_pointer(ssd, sub_ssd);

    if (ssd->sp.enable_gc_delay) {
        struct nand_cmd gcw;
        gcw.type = GC_IO;
        gcw.cmd = NAND_WRITE;
        gcw.stime = 0;
        ssd_advance_status(ssd, &new_ppa, &gcw);
    }

    /* advance per-ch gc_endtime as well */
#if 0
    new_ch = get_ch(ssd, &new_ppa);
    new_ch->gc_endtime = new_ch->next_ch_avail_time;
#endif

    new_lun = get_lun(ssd, &new_ppa);
    new_lun->gc_endtime = new_lun->next_lun_avail_time;

    return 0;
}

static struct line *select_victim_line(struct ssd *ssd, bool force, struct sub_ssd* sub_ssd)
{
    struct line_mgmt *lm = &sub_ssd->lm;
    struct line *victim_line = NULL;

    victim_line = pqueue_peek(lm->victim_line_pq);
    if (!victim_line) {
        return NULL;
    }

    if (!force && victim_line->ipc < ssd->sp.pgs_per_line_per_subssd / 8) {
        return NULL;
    }

    pqueue_pop(lm->victim_line_pq);
    victim_line->pos = 0;
    lm->victim_line_cnt--;

    /* victim_line is a danggling node now */
    return victim_line;
}

/* here ppa identifies the block we want to clean */
static void clean_one_block(struct ssd *ssd, struct ppa *ppa, struct sub_ssd *sub_ssd)
{
    struct ssdparams *spp = &ssd->sp;
    struct nand_page *pg_iter = NULL;
    int cnt = 0;

    for (int pg = 0; pg < spp->pgs_per_blk; pg++) {
        ppa->g.pg = pg;
        pg_iter = get_pg(ssd, ppa);
        /* there shouldn't be any free page in victim blocks */
        ftl_assert(pg_iter->status != PG_FREE);
        if (pg_iter->status == PG_VALID) {
            gc_read_page(ssd, ppa);



            /* delay the maptbl update until "write" happens */
            gc_write_page(ssd, ppa, sub_ssd);
            cnt++;
        }
    }

    ftl_assert(get_blk(ssd, ppa)->vpc == cnt);
}

static void mark_line_free(struct ssd *ssd, struct ppa *ppa, struct sub_ssd *sub_ssd)
{
    struct line_mgmt *lm = &sub_ssd->lm;
    struct line *line = get_line(ssd, ppa, sub_ssd);
    line->ipc = 0;
    line->vpc = 0;
    /* move this line to free line list */
    QTAILQ_INSERT_TAIL(&lm->free_line_list, line, entry);
    lm->free_line_cnt++;
}

static int do_gc(struct ssd *ssd, bool force, struct sub_ssd *sub_ssd)
{
    struct line *victim_line = NULL;
    struct ssdparams *spp = &ssd->sp;
    struct nand_lun *lunp;
    struct ppa ppa;

    static int no_victim_line = 0;

    victim_line = select_victim_line(ssd, force, sub_ssd);
    if (!victim_line) {
        no_victim_line++;
        if (no_victim_line % 100000 == 0)
            printf("do_gc: no victim line, %d\n", no_victim_line);
        return -1;
    }

    ppa.g.blk = victim_line->id;
    ppa.g.sec = 0;
    ftl_debug("GC-ing line:%d,ipc=%d,vpc=%d,victim=%d,full=%d,free=%d\n", ppa.g.blk,
              victim_line->ipc, victim_line->vpc, sub_ssd->lm.victim_line_cnt, sub_ssd->lm.full_line_cnt,
              sub_ssd->lm.free_line_cnt);

    /* copy back valid data */
    uint64_t start_prl_id = sub_ssd->id * (spp->tt_luns / ssd->sub_ssd_num);
    uint64_t end_prl_id = start_prl_id + (spp->tt_luns / ssd->sub_ssd_num);
    for (uint64_t i = start_prl_id; i < end_prl_id; i++)
    {
        ppa.g.ch = i / spp->luns_per_ch;
        ppa.g.lun = i % spp->luns_per_ch;
        ppa.g.pl = 0;
        lunp = get_lun(ssd, &ppa);
        clean_one_block(ssd, &ppa, sub_ssd);
        mark_block_free(ssd, &ppa);

        if (spp->enable_gc_delay)
        {
            struct nand_cmd gce;
            gce.type = GC_IO;
            gce.cmd = NAND_ERASE;
            gce.stime = 0;
            ssd_advance_status(ssd, &ppa, &gce);
        }

        lunp->gc_endtime = lunp->next_lun_avail_time;
    }

    /* update line status */
    mark_line_free(ssd, &ppa, sub_ssd);

    return 0;
}

#ifdef SPDK_MOCK
static void __attribute__((unused)) ssd_read(struct ssd *ssd, NvmeSubRequest *subReq)
{
    struct ssdparams *spp = &ssd->sp;
    struct ppa ppa;
    
    uint64_t sublat, maxlat = 0;
    uint64_t stime = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    NvmeLBARangeEntry *lba_entry, *next;    

    uint64_t lba_count = subReq->lba_count;
    assert(lba_count > 0);

    /* normal IO read path */
    QTAILQ_FOREACH_SAFE(lba_entry, &subReq->lba_list, entry, next) {
        assert(lba_count > 0);
        lba_count--;

        uint64_t lba = lba_entry->slba;
        int nsecs = lba_entry->nlb;
        uint64_t start_lpn = lba / spp->secs_per_pg;
        uint64_t end_lpn = (lba + nsecs - 1) / spp->secs_per_pg;

        if (end_lpn >= spp->tt_pgs) {
            ftl_err("start_lpn=%"PRIu64",tt_pgs=%d\n", start_lpn, ssd->sp.tt_pgs);
        }

        for (uint64_t lpn = start_lpn; lpn <= end_lpn; lpn++) {
            ppa = get_maptbl_ent(ssd, lpn);
            if (!mapped_ppa(&ppa) || !valid_ppa(ssd, &ppa)) {
                //printf("%s,lpn(%" PRId64 ") not mapped to valid ppa\n", ssd->ssdname, lpn);
                //printf("Invalid ppa,ch:%d,lun:%d,blk:%d,pl:%d,pg:%d,sec:%d\n",
                //ppa.g.ch, ppa.g.lun, ppa.g.blk, ppa.g.pl, ppa.g.pg, ppa.g.sec);
                continue;
            }

            struct nand_cmd srd;
            srd.type = USER_IO;
            srd.cmd = NAND_READ;
            srd.stime = stime;
            sublat = ssd_advance_status(ssd, &ppa, &srd);
            maxlat = (sublat > maxlat) ? sublat : maxlat;
        }
    }
    

    /* expire_time indicates the expected end time of requests */
    subReq->expire_time = maxlat + stime;
    subReq->nand_lat = maxlat;
}

static void __attribute__((unused)) ssd_write(struct ssd *ssd, NvmeSubRequest *subReq, struct sub_ssd *sub_ssd)
{
    struct ssdparams *spp = &ssd->sp;
    struct ppa ppa;
    uint64_t curlat = 0, maxlat = 0;
    int r;
    uint64_t stime = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    NvmeLBARangeEntry *lba_entry, *next;

    while (should_gc_high(ssd, sub_ssd)) {
        /* perform GC here until !should_gc(ssd) */
        r = do_gc(ssd, true, sub_ssd);
        if (r == -1) {
            break;
        }
    }

    uint64_t lba_count = subReq->lba_count;
    assert(lba_count > 0);

    QTAILQ_FOREACH_SAFE(lba_entry, &subReq->lba_list, entry, next) {

        assert(lba_count > 0);
        lba_count--;

        uint64_t lba = lba_entry->slba;
        int len = lba_entry->nlb;
        uint64_t start_lpn = lba / spp->secs_per_pg;
        uint64_t end_lpn = (lba + len - 1) / spp->secs_per_pg;

        if (end_lpn >= spp->tt_pgs) {
            ftl_err("ssd_write start_lpn=%"PRIu64",tt_pgs=%d\n", start_lpn, ssd->sp.tt_pgs);
        }

        for (uint64_t lpn = start_lpn; lpn <= end_lpn; lpn++) {
            ppa = get_maptbl_ent(ssd, lpn);
            if (mapped_ppa(&ppa)) {
                /* update old page information first */
                mark_page_invalid(ssd, &ppa, sub_ssd);
                set_rmap_ent(ssd, INVALID_LPN, &ppa);
            }

            /* new write */
            ppa = get_new_page(ssd, sub_ssd);
            /* update maptbl */
            set_maptbl_ent(ssd, lpn, &ppa);
            /* update rmap */
            set_rmap_ent(ssd, lpn, &ppa);

            mark_page_valid(ssd, &ppa, sub_ssd);

            /* need to advance the write pointer here */
            ssd_advance_write_pointer(ssd, sub_ssd);

            struct nand_cmd swr;
            swr.type = USER_IO;
            swr.cmd = NAND_WRITE;
            swr.stime = stime;
            /* get latency statistics */
            curlat = ssd_advance_status(ssd, &ppa, &swr);
            maxlat = (curlat > maxlat) ? curlat : maxlat;
        }
    }

    subReq->expire_time = maxlat + stime;
    subReq->nand_lat = maxlat;
}
#endif

struct ppa ssd_pg_cc_get_bp_maptbl_ent(struct ssd *ssd, struct sub_ssd *sub_ssd, uint64_t page_id) {
    ftl_assert(page_id < sub_ssd->page_cache_size);
    return sub_ssd->backup_maptbl[page_id];
}

void ssd_pg_cc_set_bp_maptbl_ent(struct ssd *ssd, struct sub_ssd *sub_ssd, uint64_t page_id, struct ppa *ppa) {
    ftl_assert(page_id < sub_ssd->page_cache_size);
    sub_ssd->backup_maptbl[page_id].ppa = ppa->ppa;
}

uint64_t ssd_pg_cc_read_page(struct ssd *ssd, struct sub_ssd *sub_ssd, pg_cc_read_context *ctx) {
    struct ssdparams *spp = &ssd->sp;
    struct ppa ppa;
    uint64_t start_lpn = ctx->slpa;
    uint64_t end_lpn = ctx->slpa + ctx->nlp - 1;
    uint64_t lpn;
    uint64_t sublat, maxlat = 0;

    if (end_lpn >= spp->tt_pgs) {
        ftl_err("start_lpn=%"PRIu64",tt_pgs=%d\n", start_lpn, ssd->sp.tt_pgs);
    }

    /* normal IO read path */
    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        ppa = get_maptbl_ent(ssd, lpn);
        if (!mapped_ppa(&ppa) || !valid_ppa(ssd, &ppa)) {
            //printf("%s,lpn(%" PRId64 ") not mapped to valid ppa\n", ssd->ssdname, lpn);
            //printf("Invalid ppa,ch:%d,lun:%d,blk:%d,pl:%d,pg:%d,sec:%d\n",
            //ppa.g.ch, ppa.g.lun, ppa.g.blk, ppa.g.pl, ppa.g.pg, ppa.g.sec);
            continue;
        }

        struct nand_cmd srd;
        srd.type = USER_IO;
        srd.cmd = NAND_READ;
        srd.stime = ctx->stime;
        sublat = ssd_advance_status(ssd, &ppa, &srd);
        maxlat = (sublat > maxlat) ? sublat : maxlat;
    }

    return maxlat;
}

void pre_invalidate_maptbl_ent(struct ssd *ssd, struct sub_ssd *sub_ssd, uint64_t pg_id) {
    struct ppa ppa = ssd_pg_cc_get_bp_maptbl_ent(ssd, sub_ssd, pg_id);
    if (mapped_ppa(&ppa)) {
        /* update old page information first */
        mark_page_invalid(ssd, &ppa, sub_ssd);
        set_rmap_ent(ssd, INVALID_LPN, &ppa);
    }
}

uint64_t ssd_pg_cc_write_page(struct ssd *ssd, struct sub_ssd *sub_ssd, pg_cc_write_context *ctx) {
    struct ppa ppa;
    uint64_t lpn;
    uint64_t curlat = 0, maxlat = 0;
    int i = 0, r;

    while (should_gc_high(ssd, sub_ssd)) {
        /* perform GC here until !should_gc(ssd) */
        r = do_gc(ssd, true, sub_ssd);
        if (r == -1) {
            break;
        }
    }
    
    for (; i < ctx->nlp; i++) {
        lpn = ctx->wlpn[i];
        pg_cc_assert(shard(lpn, sub_ssd) == sub_ssd->id);

        /* new write */
        ppa = get_new_page(ssd, sub_ssd);
        /* update maptbl */
        set_maptbl_ent(ssd, lpn, &ppa);
        /* update rmap */
        set_rmap_ent(ssd, lpn, &ppa);

        mark_page_valid(ssd, &ppa, sub_ssd);

        /* need to advance the write pointer here */
        ssd_advance_write_pointer(ssd, sub_ssd);

        struct nand_cmd swr;
        swr.type = USER_IO;
        if (unlikely(ctx->is_fast_flush)) {
            swr.type = FAST_FLUSH_IO;
        }
        swr.cmd = NAND_WRITE;
        swr.stime = ctx->stime;
        /* get latency statistics */
        curlat = ssd_advance_status(ssd, &ppa, &swr);
        maxlat = (curlat > maxlat) ? curlat : maxlat;
    }

    return maxlat;
}

static void ssd_warm_up(FemuCtrl *n, struct sub_ssd *sub_ssd)
{
    struct ssd *ssd = n->ssd;
    struct ssdparams *spp = &ssd->sp;
    struct ppa ppa;

    uint32_t warm_up_ratio = n->warm_up_ratio;
    uint32_t warm_up_pages = spp->tt_pgs / n->num_ftl * warm_up_ratio / 100;
    int sub_ssd_id = sub_ssd->id;

    uint64_t shard_pg_num = n->shard_length / 4096;
    uint64_t lpn = sub_ssd_id * shard_pg_num;


    for (int i = 0; i < warm_up_pages; i++) {
        if (i != 0 && i % shard_pg_num == 0) {
            lpn += shard_pg_num * (n->num_ftl - 1);
        }

        assert(shard(lpn, sub_ssd) == sub_ssd_id);

        if (lpn >= spp->tt_pgs) {
            ftl_err("warm up error lpn=%"PRIu64",tt_pgs=%d\n", lpn, ssd->sp.tt_pgs);
            assert(0);
        }

        if (should_gc(ssd, sub_ssd)) {
            ftl_err("should gc when warm up error");
            assert(0);
        }

        ppa = get_maptbl_ent(ssd, lpn);
        if (mapped_ppa(&ppa)) {
            femu_err("lpn %lu is already mapped to ppa %lu\n", lpn, ppa.ppa);
            assert(0);
        }
        ppa = get_new_page(ssd, sub_ssd);

        set_maptbl_ent(ssd, lpn, &ppa);
        set_rmap_ent(ssd, lpn, &ppa);
        mark_page_valid(ssd, &ppa, sub_ssd);
        ssd_advance_write_pointer(ssd, sub_ssd);

        lpn++;
    }
}

static void *ftl_thread(void *arg)
{
    struct ftl_arg *ftl_arg = (struct ftl_arg *)arg;
    FemuCtrl *n = ftl_arg->n;
    struct ssd *ssd = n->ssd;
    struct sub_ssd *sub_ssd = &ssd->sub_ssds[ftl_arg->ftl_id - 1];
    int ftl_id = ftl_arg->ftl_id;
    NvmeRequest *req = NULL;
    NvmeSubRequest *subReq = NULL;
//    uint64_t lat = 0;
    int rc, i;
    struct rte_ring *to_ftl, *to_poller;
#ifdef PG_CC_BREAKDOWN
    int64_t ftl_start_time, ftl_end_time;
#endif
    // struct pg_cc_look_up_context rw_ctx;
    struct pg_cc_unpin_context unpin_ctx;

    // NvmeLBARangeEntry *lba_range, *next;

    int serverd_req = 0;

    ftl_assert(sub_ssd);

    ssd_warm_up(n, sub_ssd);
    
    while (!*(ssd->dataplane_started_ptr)) {
        usleep(100000);
    }

    /* FIXME: not safe, to handle ->to_ftl and ->to_poller gracefully */
    ssd->to_ftl = n->to_ftl;
    ssd->to_poller = n->to_poller;
    ssd->admin2ftl = n->admin2ftl;
    ssd->ftl2admin = n->ftl2admin;

    while (1) {
        if (unlikely(femu_ring_count(ssd->admin2ftl[ftl_id]))) {
            // femu_err("admin2ftl begin clear\n");
            femu_ring_dequeue(ssd->admin2ftl[ftl_id], (void *)&req, 1);
            // if (sub_ssd->page_cache_ops.clear) {
            //    sub_ssd->page_cache_ops.clear(sub_ssd);
            //}
            femu_ring_enqueue(ssd->ftl2admin[ftl_id], (void *)&req, 1);
        }

        for (i = 1; i <= n->num_poller; i++) {

            to_ftl = ssd->to_ftl[i][ftl_id];
            to_poller = ssd->to_poller[i][ftl_id];

            if (!to_ftl || !femu_ring_count(to_ftl))
                continue;

            rc = femu_ring_dequeue(to_ftl, (void *)&subReq, 1);
            if (rc != 1) {
                printf("FEMU: FTL to_ftl dequeue failed\n");
                assert(0);
            }
            ftl_assert(subReq);
            req = subReq->parent_req;
#ifdef PG_CC_BREAKDOWN
            ftl_start_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
            subReq->rev_ftl_time = ftl_start_time;
#endif
            if (serverd_req % 100000 == 0) {
                // femu_log("FTL %d servered %d requests\n", ftl_id, serverd_req);
            }
            ftl_assert(req);

            if (n->pg_cc_enabled) {
                switch (req->cmd_opcode) {
                case NVME_CMD_PG_CC_WRITE:
                case NVME_CMD_PG_CC_READ:
                    serverd_req++;
                    sub_ssd->page_cache_ops.look_up(sub_ssd->page_cache_mgr, subReq);
                    break;
                case NVME_CMD_UN_PIN_RW:
                    assert(subReq->nand_lat == 0);
                    for (int pg_ptr = 0; pg_ptr < subReq->unpin_pg_num; pg_ptr++) {
                        unpin_ctx.start_lpa = subReq->unpin_pg_id[pg_ptr];
                        unpin_ctx.nlp = 1;
                        unpin_ctx.is_dirty = subReq->unpin_is_dirty[pg_ptr];
                        sub_ssd->page_cache_ops.unpin(sub_ssd->page_cache_mgr, &unpin_ctx);
                    }
                    femu_log("ftl: unpin %lu %lu\n", subReq->unpin_pg_num, sub_ssd->page_cache_mgr->free_page_count);
                    assert(subReq->nand_lat == 0);

                    if (serverd_req >= 55000) {
                        femu_log("FTL %d: unpin %ld, %ld\n", ftl_id, subReq->unpin_pg_num, sub_ssd->page_cache_mgr->free_page_count);
                    }

                    break;
                case NVME_CMD_DSM:
                    femu_err("FTL:nvme_dsm\n");
                    break;
                case NVME_CMD_WRITE:
                    femu_err("FTL:ssd_write but pg_cc_enabled\n");
                    ssd_write(ssd, subReq, sub_ssd);
                    break;
                case NVME_CMD_READ:
                    femu_err("FTL:ssd_read but pg_cc_enabled\n");
                    ssd_read(ssd, subReq);
                    break;
                default:
                    ftl_err("FTL received unkown request type, ERROR %u\n", req->cmd_opcode);
                    ftl_assert(0);
                    break;
                }
            } else {
                switch (req->cmd.opcode) {
                case NVME_CMD_WRITE:
                    ssd_write(ssd, subReq, sub_ssd);
                    break;
                case NVME_CMD_READ:
                    ssd_read(ssd, subReq);
                    break;
                case NVME_CMD_DSM:
                    break;
                default:
                    ftl_err("FTL received unkown request type %d, ERROR\n", req->cmd.opcode);
                    ftl_assert(0);
                    ;
                }
            }
#ifdef PG_CC_BREAKDOWN
            ftl_end_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
            subReq->ftl_time = ftl_end_time - ftl_start_time;

            subReq->send_poller_time = ftl_end_time;
#endif

            rc = femu_ring_enqueue(to_poller, (void *)&subReq, 1);
            if (rc != 1) {
                ftl_err("FTL to_poller enqueue failed\n");
            }

            /* clean one line if needed (in the background) */
            if (should_gc(ssd, sub_ssd)) {
                femu_debug("FTL %d should GC\n", ftl_id);
                do_gc(ssd, false, sub_ssd);
            }

            
        }
    }

    return NULL;
}

