#include "../nvme.h"
#include "./ftl.h"

static void bb_init_ctrl_str(FemuCtrl *n)
{
    static int fsid_vbb = 0;
    const char *vbbssd_mn = "FEMU BlackBox-SSD Controller";
    const char *vbbssd_sn = "vSSD";

    nvme_set_ctrl_name(n, vbbssd_mn, vbbssd_sn, &fsid_vbb);
}

/* bb <=> black-box */
static void bb_init(FemuCtrl *n, Error **errp)
{
    struct ssd *ssd = n->ssd = g_malloc0(sizeof(struct ssd));

    bb_init_ctrl_str(n);

    ssd->dataplane_started_ptr = &n->dataplane_started;
    ssd->ssdname = (char *)n->devname;
    femu_debug("Starting FEMU in Blackbox-SSD mode ...\n");
    ssd_init(n);
}

static void bb_flip(FemuCtrl *n, NvmeCmd *cmd)
{
    struct ssd *ssd = n->ssd;
    int64_t cdw10 = le64_to_cpu(cmd->cdw10);

    switch (cdw10) {
    case FEMU_ENABLE_GC_DELAY:
        ssd->sp.enable_gc_delay = true;
        femu_log("%s,FEMU GC Delay Emulation [Enabled]!\n", n->devname);
        break;
    case FEMU_DISABLE_GC_DELAY:
        ssd->sp.enable_gc_delay = false;
        femu_log("%s,FEMU GC Delay Emulation [Disabled]!\n", n->devname);
        break;
    case FEMU_ENABLE_DELAY_EMU:
        ssd->sp.pg_rd_lat = NAND_READ_LATENCY;
        ssd->sp.pg_wr_lat = NAND_PROG_LATENCY;
        ssd->sp.blk_er_lat = NAND_ERASE_LATENCY;
        ssd->sp.ch_xfer_lat = 0;
        femu_log("%s,FEMU Delay Emulation [Enabled]!\n", n->devname);
        break;
    case FEMU_DISABLE_DELAY_EMU:
        ssd->sp.pg_rd_lat = 0;
        ssd->sp.pg_wr_lat = 0;
        ssd->sp.blk_er_lat = 0;
        ssd->sp.ch_xfer_lat = 0;
        femu_log("%s,FEMU Delay Emulation [Disabled]!\n", n->devname);
        break;
    case FEMU_RESET_ACCT:
        n->nr_tt_ios = 0;
        n->nr_tt_late_ios = 0;
        femu_log("%s,Reset tt_late_ios/tt_ios,%lu/%lu\n", n->devname,
                n->nr_tt_late_ios, n->nr_tt_ios);
        break;
    case FEMU_ENABLE_LOG:
        n->print_log = true;
        femu_log("%s,Log print [Enabled]!\n", n->devname);
        break;
    case FEMU_DISABLE_LOG:
        n->print_log = false;
        femu_log("%s,Log print [Disabled]!\n", n->devname);
        break;
    default:
        printf("FEMU:%s,Not implemented flip cmd (%lu)\n", n->devname, cdw10);
    }
}

static uint16_t bb_nvme_rw(FemuCtrl *n, NvmeNamespace *ns, NvmeCmd *cmd,
                           NvmeRequest *req)
{
    return nvme_rw(n, ns, cmd, req);
}

static uint16_t bb_io_cmd(FemuCtrl *n, NvmeNamespace *ns, NvmeCmd *cmd,
                          NvmeRequest *req)
{
    switch (cmd->opcode) {
    case NVME_CMD_READ:
    case NVME_CMD_WRITE:
        return bb_nvme_rw(n, ns, cmd, req);
    default:
        return NVME_INVALID_OPCODE | NVME_DNR;
    }
}

static uint16_t bb_admin_cmd(FemuCtrl *n, NvmeCmd *cmd)
{
    switch (cmd->opcode) {
    case NVME_ADM_CMD_FEMU_FLIP:
        bb_flip(n, cmd);
        return NVME_SUCCESS;
    default:
        return NVME_INVALID_OPCODE | NVME_DNR;
    }
}

static void bb_pg_cc_init(FemuCtrl *n)
{
    pg_cc_init(n);
}

static void bb_pg_cc_clear(FemuCtrl *n)
{
    pg_cc_clear(n);
}

static void bb_stat(NvmeCQueue *cq, NvmeRequest *req)
{
    if (req->cmd_opcode != NVME_CMD_PG_CC_WRITE && req->cmd_opcode != NVME_CMD_PG_CC_READ && req->cmd_opcode != NVME_CMD_UN_PIN_RW) {
        return ;
    }
    int origin_ops;
    // int cache_ops;
    if (req->cmd_opcode == NVME_CMD_PG_CC_WRITE) {
        origin_ops = PG_CC_CACHE_WRITE;
        // if (req->pg_cc_hit_num)
        //    cache_ops = PG_CC_CACHE_WRITE_HIT;
        //else cache_ops = PG_CC_CACHE_WRITE_MISS;
    } else if (req->cmd_opcode == NVME_CMD_PG_CC_READ) {
        origin_ops = PG_CC_CACHE_READ;
        // if (req->pg_cc_hit_num)
        //     cache_ops = PG_CC_CACHE_READ_HIT;
        // else cache_ops = PG_CC_CACHE_READ_MISS;
    } else {
        origin_ops = PG_CC_CACHE_UNPIN;
    }

    // check the correctness of the request
    if (req->cmd_opcode != NVME_CMD_UN_PIN_RW) {
        assert(req->pg_cc_hit_num + req->pg_cc_miss_num == req->total_need_pin_num);

        /* FIXME: when running macro-benchmark, the cache_ops is not correct */
        #if 0
        cq->req_num[cache_ops]++;
        cq->sq_time[cache_ops] += req->sq_time;
        cq->cq_time[cache_ops] += req->cq_time;
        cq->ftl_time[cache_ops] += req->ftl_time;
        cq->nand_time[cache_ops] += req->nand_time;
        cq->total_time[cache_ops] += req->end_time - req->start_time;
        cq->diff_time[cache_ops] += req->end_time - req->expire_time;
        cq->to_ftl_time[cache_ops] += req->to_ftl_time;
        cq->to_poller_time[cache_ops] += req->to_poller_time;
        #endif

        cq->req_num[PG_CC_CACHE_RW_ALL]++;
        cq->pg_num[PG_CC_CACHE_RW_ALL] += req->total_need_pin_num;
        cq->hit_pg_num[PG_CC_CACHE_RW_ALL] += req->pg_cc_hit_num;
        cq->miss_pg_num[PG_CC_CACHE_RW_ALL] += req->pg_cc_miss_num;
        cq->sq_time[PG_CC_CACHE_RW_ALL] += req->sq_time;
        cq->cq_time[PG_CC_CACHE_RW_ALL] += req->cq_time;
        cq->ftl_time[PG_CC_CACHE_RW_ALL] += req->ftl_time;
        cq->nand_time[PG_CC_CACHE_RW_ALL] += req->nand_time;
        cq->total_time[PG_CC_CACHE_RW_ALL] += req->end_time - req->start_time;
        cq->diff_time[PG_CC_CACHE_RW_ALL] += req->end_time - req->expire_time;
        cq->to_ftl_time[PG_CC_CACHE_RW_ALL] += req->to_ftl_time;
        cq->to_poller_time[PG_CC_CACHE_RW_ALL] += req->to_poller_time;

        cq->pg_num[origin_ops] += req->total_need_pin_num;
        cq->hit_pg_num[origin_ops] += req->pg_cc_hit_num;
        cq->miss_pg_num[origin_ops] += req->pg_cc_miss_num;
    }   else {
        cq->pg_num[origin_ops] += req->unpin_pg_size;
    }

    cq->req_num[origin_ops]++;
    
    cq->sq_time[origin_ops] += req->sq_time;
    cq->cq_time[origin_ops] += req->cq_time;
    cq->ftl_time[origin_ops] += req->ftl_time;
    cq->nand_time[origin_ops] += req->nand_time;
    cq->total_time[origin_ops] += req->end_time - req->start_time;
    cq->diff_time[origin_ops] += req->end_time - req->expire_time;
    cq->to_ftl_time[origin_ops] += req->to_ftl_time;
    cq->to_poller_time[origin_ops] += req->to_poller_time;
}

static void bb_sum_stat(FemuCtrl *n, NvmeCQueue *cq)
{
    for (int i = 0; i < PG_CC_CACHE_ENUM_COUNT; i++) {
        n->req_num[i] += cq->req_num[i];
        n->sq_time[i] += cq->sq_time[i];
        n->cq_time[i] += cq->cq_time[i];
        n->ftl_time[i] += cq->ftl_time[i];
        n->nand_time[i] += cq->nand_time[i];
        n->total_time[i] += cq->total_time[i];
        n->diff_time[i] += cq->diff_time[i];
        n->to_ftl_time[i] += cq->to_ftl_time[i];
        n->to_poller_time[i] += cq->to_poller_time[i];
        n->pg_num[i] += cq->pg_num[i];
        n->hit_pg_num[i] += cq->hit_pg_num[i];
        n->miss_pg_num[i] += cq->miss_pg_num[i];
    }
}

static void bb_print_stat(FemuCtrl *n)
{
    static const char *pg_cc_cache_ops_str[] = {
        [PG_CC_CACHE_READ_HIT] = "read_hit",
        [PG_CC_CACHE_READ_MISS] = "read_miss",
        [PG_CC_CACHE_WRITE_HIT] = "write_hit",
        [PG_CC_CACHE_WRITE_MISS] = "write_miss",
        [PG_CC_CACHE_READ] = "read",
        [PG_CC_CACHE_WRITE] = "write",
        [PG_CC_CACHE_UNPIN] = "unpin",
        [PG_CC_CACHE_RW_ALL] = "rw_all",
    };

    printf("FEMU STAT BEGIN\n");

    double nsec_to_usec = 1000.0;

    for (int i = PG_CC_CACHE_READ; i < PG_CC_CACHE_ENUM_COUNT; i++) {
        const char *ops = pg_cc_cache_ops_str[i];
        if (n->req_num[i] != 0) {
            printf("STAT %s ssd_req_num %ld ssd_pg_num %ld ssd_pg_num_per_req %.2lf ssd_avg_total_time %.2lf ssd_sq %.2lf ssd_cq %.2lf ssd_ftl %.2lf ssd_nand_time_per_req %.2lf ssd_nand_time_per_page %.2lf ssd_to_ftl_time %.2lf ssd_to_poller_time %.2lf ssd_diff_time %.2lf\n",
                    ops,
                    n->req_num[i],
                    n->pg_num[i], (double)n->pg_num[i] / n->req_num[i],
                    n->total_time[i] / n->req_num[i] / nsec_to_usec,
                    n->sq_time[i] / n->req_num[i] / nsec_to_usec, n->cq_time[i] / n->req_num[i] / nsec_to_usec,
                    n->ftl_time[i] / n->req_num[i] / nsec_to_usec, 
                    n->nand_time[i] / n->req_num[i] / nsec_to_usec, n->nand_time[i] / n->pg_num[i] / nsec_to_usec,
                    n->to_ftl_time[i] / n->req_num[i] / nsec_to_usec, n->to_poller_time[i] / n->req_num[i] / nsec_to_usec,
                    n->diff_time[i] / n->req_num[i] / nsec_to_usec);
        }
    }
    
    const int enum_trace[] = {PG_CC_CACHE_READ, PG_CC_CACHE_WRITE, PG_CC_CACHE_RW_ALL};
    for (int i = 0; i < sizeof(enum_trace) / sizeof(int); i++) {
        if (n->req_num[enum_trace[i]] != 0) {
            printf("STAT cache %s hit_ratio %.4lf\n", 
                pg_cc_cache_ops_str[enum_trace[i]], 
                (double)n->hit_pg_num[enum_trace[i]] / (double)n->pg_num[enum_trace[i]]);
            printf("STAT cache %s miss_ratio %.4lf\n", 
                pg_cc_cache_ops_str[enum_trace[i]], 
                (double)n->miss_pg_num[enum_trace[i]] / (double)n->pg_num[enum_trace[i]]);
        }
    }

    printf("FEMU STAT END\n");
}

static void bb_reset_stat(FemuCtrl *n)
{
    for (int i = 0; i < PG_CC_CACHE_ENUM_COUNT; i++) {
        n->req_num[i] = 0;
        n->sq_time[i] = 0;
        n->cq_time[i] = 0;
        n->ftl_time[i] = 0;
        n->pg_cc_time[i] = 0;
        n->nand_time[i] = 0;
        n->total_time[i] = 0;
        n->diff_time[i] = 0;
        n->to_ftl_time[i] = 0;
        n->to_poller_time[i] = 0;
        n->pg_num[i] = 0;
        n->hit_pg_num[i] = 0;
        n->miss_pg_num[i] = 0;
    }
}

int nvme_register_bbssd(FemuCtrl *n)
{
    n->ext_ops = (FemuExtCtrlOps) {
        .state            = NULL,
        .init             = bb_init,
        .exit             = NULL,
        .rw_check_req     = NULL,
        .admin_cmd        = bb_admin_cmd,
        .io_cmd           = bb_io_cmd,
        .get_log          = NULL,
        .pg_cc_init       = bb_pg_cc_init,
        .pg_cc_clear      = bb_pg_cc_clear,
        .stat             = bb_stat,
        .sum_stat         = bb_sum_stat,
        .print_stat       = bb_print_stat,
        .reset_stat       = bb_reset_stat,
    };

    return 0;
}

