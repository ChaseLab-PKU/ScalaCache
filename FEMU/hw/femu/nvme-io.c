#include "./nvme.h"

// #define SPDK_MOCK
// #define SPDK_LOG
#define LOG_PERIOD 2000000

#define NVME_MAX_PRP_LIST_ENTRY (503)

static uint16_t __attribute__((unused)) nvme_io_cmd (FemuCtrl *n, NvmeCmd *cmd, NvmeRequest *req);
#ifdef SPDK_LOG
static int64_t sq_time, cq_time, io_cmd_time;
static int64_t sq_cnt, cq_cnt, io_cmd_cnt;
static int64_t max_time, max_t1, max_t2, avg_time, avg_cnt;
#endif

static void nvme_update_sq_eventidx(const NvmeSQueue *sq)
{
    if (sq->eventidx_addr_hva) {
        *((uint32_t *)(sq->eventidx_addr_hva)) = sq->tail;
        return;
    }

    if (sq->eventidx_addr) {
        nvme_addr_write(sq->ctrl, sq->eventidx_addr, (void *)&sq->tail,
                        sizeof(sq->tail));
    }
}

static inline void nvme_copy_cmd(NvmeCmd *dst, NvmeCmd *src)
{
#if defined(__AVX__)
    __m256i *d256 = (__m256i *)dst;
    const __m256i *s256 = (const __m256i *)src;

    _mm256_store_si256(&d256[0], _mm256_load_si256(&s256[0]));
    _mm256_store_si256(&d256[1], _mm256_load_si256(&s256[1]));
#elif defined(__SSE2__)
    __m128i *d128 = (__m128i *)dst;
    const __m128i *s128 = (const __m128i *)src;

    _mm_store_si128(&d128[0], _mm_load_si128(&s128[0]));
    _mm_store_si128(&d128[1], _mm_load_si128(&s128[1]));
    _mm_store_si128(&d128[2], _mm_load_si128(&s128[2]));
    _mm_store_si128(&d128[3], _mm_load_si128(&s128[3]));
#else
    *dst = *src;
#endif
}

static inline void nvme_clear_req_stat(NvmeRequest *req) {
    req->subReq_lat = 0;
    req->pg_cc_hit_num = req->pg_cc_miss_num = 0;
    req->nand_time = 0;
    req->total_need_pin_num = 0;
    req->ftl_time = 0;
    req->to_ftl_time = 0;
    req->to_poller_time = 0;
}

static inline void nvme_clear_subReq_stat(NvmeSubRequest *subReq) {
    subReq->total_lat = 0;
    subReq->nand_lat = 0;
    subReq->ftl_time = 0;
    subReq->send_ftl_time = 0;
    subReq->rev_ftl_time = 0;
    subReq->send_poller_time = 0;
    subReq->rev_poller_time = 0;
}

static inline bool is_pg_cc_cmd(uint16_t opcode) {
    return (opcode == NVME_CMD_PG_CC_READ || opcode == NVME_CMD_PG_CC_WRITE || opcode == NVME_CMD_UN_PIN_RW);
}

static inline bool is_raw_rw_cmd(uint16_t opcode) {
    return (opcode == NVME_CMD_READ || opcode == NVME_CMD_WRITE);
}

static uint16_t nvme_pg_cc_init_unpin(FemuCtrl *n, NvmeCmd *cmd, NvmeRequest *req) {
    req->unpin_pg_size = cmd->res2_low;
    assert(req->unpin_pg_size > 0);

    req->unpin_lpa[0] = cmd->cdw10;
    req->unpin_lpa[1] = cmd->cdw11;
    req->unpin_lpa[2] = cmd->cdw12;
    req->unpin_lpa[3] = cmd->cdw13;
    req->unpin_lpa[4] = cmd->cdw14;
    req->unpin_lpa[5] = cmd->cdw15;

    for (int i = 0; i < MIN(6, req->unpin_pg_size); i++) {
        req->unpin_is_dirty[i] = (cmd->res2_hi >> i) & 1;
    }

    if (req->unpin_pg_size <= 6) {
        return NVME_SUCCESS;
    }
    
    assert(req->unpin_pg_size <= NVME_MAX_PRP_LIST_ENTRY + 6);

    uint64_t prp_trans = NVME_MAX_PRP_LIST_ENTRY;
    if (req->unpin_pg_size - 6 < prp_trans) {
        prp_trans = req->unpin_pg_size - 6;
    }

    uint64_t prp_list[prp_trans];
    nvme_addr_read(n, cmd->dptr.prp1, (void *)prp_list, prp_trans * sizeof(uint64_t));

    for (uint64_t i = 6; i < req->unpin_pg_size; i++) {
        req->unpin_lpa[i] = prp_list[i - 6] & 0xFFFFFFFFULL;
        req->unpin_is_dirty[i] = (prp_list[i - 6] >> 32) & 0x1;
    }

    return NVME_SUCCESS;
}

static uint16_t nvme_pg_cc_init_rw(FemuCtrl *n, NvmeCmd *cmd, NvmeRequest *req) {
    NvmeRwCmd *rw = (NvmeRwCmd *)cmd;
    uint32_t nlb  = le16_to_cpu(rw->nlb) + 1;
    uint64_t slba = le64_to_cpu(rw->slba);
    /* process io request */

    req->slba = slba;
    req->nlb = nlb;

    uint64_t start_lpn = fast_transLBA2LPN(slba);
    uint64_t end_lpn = fast_transLBA2LPN(slba + nlb - 1);

    for (int i = 0; i < end_lpn - start_lpn + 1; i++) {
        req->page_id[i] = PG_CC_INVALID_PAGE_ID;
        req->page_phy_addr[i] = PG_CC_INVALID_PHY_ADDR;
        req->unpin_lpa[i] = PG_CC_INVALID_LPA;
        req->need_dram_rw[i] = false;
        req->need_pin[i] = true;
    }

    assert(req->total_need_pin_num == 0);

    if (end_lpn - start_lpn + 1 <= 31) {
        assert(cmd->res2_low != 0xFFFFFFFF);
        uint32_t rsvd2 = cmd->res2_low;
        for (int i = 0; i < end_lpn - start_lpn + 1; i++) {
            if (rsvd2 & 0x1)
                req->need_pin[i] = false;
            else {
                req->need_pin[i] = true;
                req->total_need_pin_num++;
            }
            rsvd2 = rsvd2 >> 1;
        }
    } else {
        uint64_t prp_list[NVME_MAX_PRP_LIST_ENTRY];
        nvme_addr_read(n, cmd->dptr.prp1, (void *)prp_list, NVME_MAX_PRP_LIST_ENTRY * sizeof(uint64_t));

        uint64_t index = 0;
        uint64_t offset = 64;
        for (int i = 0; i < end_lpn - start_lpn + 1; i++) {
            if (prp_list[index] & 0x1)
                req->need_pin[i] = false;
            else {
                req->need_pin[i] = true;
                req->total_need_pin_num++;
            }  
            prp_list[index] = prp_list[index] >> 1;
            offset--;
            if (!offset) {
                index++;
                offset = 64;
            }
        }

        assert(cmd->res2_low == req->total_need_pin_num);
    }

    return NVME_SUCCESS;
}

// TODO: separate the function into two parts:
static uint16_t nvme_pg_cc_cmd_init(FemuCtrl *n, NvmeCmd *cmd, NvmeRequest *req) {
    NvmeNamespace *ns;
    uint32_t nsid = le32_to_cpu(cmd->nsid);

    if (nsid == 0 || nsid > n->num_namespaces) {
        femu_err("%s, NVME_INVALID_NSID %" PRIu32 "\n", __func__, nsid);
        return NVME_INVALID_NSID | NVME_DNR;
    }

    req->ns = ns = &n->namespaces[nsid - 1];

    NvmeRwCmd *rw = (NvmeRwCmd *)cmd;
    uint16_t ctrl = le16_to_cpu(rw->control);
    uint32_t nlb  = le16_to_cpu(rw->nlb) + 1;
    uint64_t slba = le64_to_cpu(rw->slba);
    const uint8_t lba_index = NVME_ID_NS_FLBAS_INDEX(ns->id_ns.flbas);
    const uint16_t ms = le16_to_cpu(ns->id_ns.lbaf[lba_index].ms);
    const uint8_t data_shift = ns->id_ns.lbaf[lba_index].lbads;
    uint64_t data_size = (uint64_t)nlb << data_shift;
    uint64_t meta_size = nlb * ms;
    uint64_t elba = slba + nlb;
    uint16_t err;

    assert(rw->opcode == NVME_CMD_PG_CC_READ || rw->opcode == NVME_CMD_PG_CC_WRITE || rw->opcode == NVME_CMD_UN_PIN_RW);
    req->is_write = (rw->opcode == NVME_CMD_PG_CC_WRITE) ? 1 : 0;
    req->is_unpin = rw->opcode == NVME_CMD_UN_PIN_RW;
    nvme_clear_req_stat(req);    

    req->status = NVME_SUCCESS;

    if (req->is_unpin) {
        return nvme_pg_cc_init_unpin(n, cmd, req);
    } 
    
    err = femu_nvme_rw_check_req(n, ns, cmd, req, slba, elba, nlb, ctrl,
                                 data_size, meta_size);
    if (err)
        return err;
    
    return nvme_pg_cc_init_rw(n, cmd, req);
}

static int shards_index(FemuCtrl *n, uint64_t offset) {
    return (offset / n->shard_length) % (n->num_ftl);
}

static void deliver_subUnpinReq(FemuCtrl *n, NvmeSQueue *sq, int index_poller, NvmeRequest *req) {
    NvmeSubRequest *subReqs[n->num_ftl];
    int ftl_id;
    struct rte_ring *to_ftl;

    memset(subReqs, 0, sizeof(subReqs));
    assert(req);

    for (int i = 0; i < req->unpin_pg_size; i++) {
        ftl_id = shards_index(n, req->unpin_lpa[i] * 4096);
        if (!subReqs[ftl_id]) {
            assert(!QTAILQ_EMPTY(&sq->subReq_list));
            subReqs[ftl_id] = QTAILQ_FIRST(&sq->subReq_list);
            QTAILQ_REMOVE(&sq->subReq_list, subReqs[ftl_id], entry);
            
            subReqs[ftl_id]->is_unpin = true;
            subReqs[ftl_id]->parent_req = req;
            req->subReq_num++;
            
            subReqs[ftl_id]->unpin_pg_num = 0;

            nvme_clear_subReq_stat(subReqs[ftl_id]);
#ifdef PG_CC_BREAKDOWN
            subReqs[ftl_id]->send_ftl_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
#endif
            subReqs[ftl_id]->pg_cc_hit_num = subReqs[ftl_id]->pg_cc_miss_num = 0;
        }

        subReqs[ftl_id]->unpin_pg_id[subReqs[ftl_id]->unpin_pg_num] = req->unpin_lpa[i];
        subReqs[ftl_id]->unpin_is_dirty[subReqs[ftl_id]->unpin_pg_num] = req->unpin_is_dirty[i];
        subReqs[ftl_id]->unpin_pg_num++;
    }

    for (int ftl_id = 0; ftl_id < n->num_ftl; ftl_id++) {
        if (subReqs[ftl_id]) {
            to_ftl = n->to_ftl[index_poller][ftl_id + 1];

            assert(subReqs[ftl_id]->parent_req);
            int rc = femu_ring_enqueue(to_ftl, (void *)&subReqs[ftl_id], 1);
            if (rc != 1) {
                femu_err("enqueue failed, ret=%d\n", rc);
            }
        }
    }
}

static void deliver_subIOReq(FemuCtrl *n, NvmeSQueue *sq, int index_poller, NvmeRequest *req) {
    uint64_t req_point = req->slba * n->secsz;
    int ftl_id = shards_index(n, req_point);
    uint64_t length = req->nlb * n->secsz;
    uint64_t req_end_point = req_point + length;
    struct rte_ring *to_ftl;

    NvmeSubRequest *subReqs[n->num_ftl];
    memset(subReqs, 0, sizeof(subReqs));
    NvmeLBARangeEntry *lba_entry;

    /* get the end of shard */
    uint64_t shard_point = req_point / n->shard_length;
    shard_point = (shard_point + 1) * n->shard_length;

    assert(is_raw_rw_cmd(req->cmd.opcode) || (is_pg_cc_cmd(req->cmd.opcode) && req->cmd.opcode != NVME_CMD_UN_PIN_RW));

    while (length > 0) {
        uint64_t remain_len = MIN(shard_point, req_end_point) - req_point;
        assert(remain_len <= n->shard_length);

        if (!subReqs[ftl_id]) {
            assert(!QTAILQ_EMPTY(&sq->subReq_list));
            subReqs[ftl_id] = QTAILQ_FIRST(&sq->subReq_list);
            QTAILQ_REMOVE(&sq->subReq_list, subReqs[ftl_id], entry);
            subReqs[ftl_id]->is_write = req->is_write;
            subReqs[ftl_id]->parent_req = req;
            nvme_clear_subReq_stat(subReqs[ftl_id]);

            assert(QTAILQ_EMPTY(&subReqs[ftl_id]->lba_list));
#ifdef PG_CC_BREAKDOWN
            subReqs[ftl_id]->send_ftl_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
#endif
            subReqs[ftl_id]->pg_cc_hit_num = subReqs[ftl_id]->pg_cc_miss_num = 0;
            req->subReq_num++;
        }        

        assert(!QTAILQ_EMPTY(&sq->lba_list));
        lba_entry = QTAILQ_FIRST(&sq->lba_list);
        QTAILQ_REMOVE(&sq->lba_list, lba_entry, entry);
        lba_entry->slba = req_point / n->secsz;
        lba_entry->nlb = remain_len / n->secsz;
        QTAILQ_INSERT_TAIL(&subReqs[ftl_id]->lba_list, lba_entry, entry);
        subReqs[ftl_id]->lba_count++;
        
        if (n->pg_cc_enabled) {
            /* TODO: remove the check */
            // assert(subReq->nlb == 8);
        }

        req_point += remain_len;
        shard_point += n->shard_length;

        ftl_id = (ftl_id + 1) % n->num_ftl;
        length -= remain_len;
    }    
    /* hack to set req->subReq_num to 1 */
    if (n->pg_cc_enabled) {
        //assert(req->subReq_num == 1);
    }

    for (int ftl_id = 0; ftl_id < n->num_ftl; ftl_id++) {
        if (subReqs[ftl_id]) {
            to_ftl = n->to_ftl[index_poller][ftl_id + 1];

            int rc = femu_ring_enqueue(to_ftl, (void *)&subReqs[ftl_id], 1);
            if (rc != 1) {
                femu_err("enqueue failed, ret=%d\n", rc);
            }
        }
    }
        
}

static void deliver_otherReq(FemuCtrl *n, NvmeSQueue *sq, int index_poller, NvmeRequest *req) {
    assert(!is_raw_rw_cmd(req->cmd.opcode) && !is_pg_cc_cmd(req->cmd.opcode));

    assert(!QTAILQ_EMPTY(&sq->subReq_list));
    NvmeSubRequest *subReq = QTAILQ_FIRST(&sq->subReq_list);
    QTAILQ_REMOVE(&sq->subReq_list, subReq, entry);
    nvme_clear_subReq_stat(subReq);

    subReq->parent_req = req;
    req->subReq_num++;

    // HINT: HACK TO SEND OTHER REQ TO FTL 0
    int rc = femu_ring_enqueue(n->to_ftl[index_poller][1], (void *)&subReq, 1);
    if (rc != 1) {
        femu_err("enqueue failed, ret=%d\n", rc);
    }
}

static uint64_t receive_req = 0;

static void nvme_process_sq_io(void *opaque, int index_poller)
{
    NvmeSQueue *sq = opaque;
    FemuCtrl *n = sq->ctrl;

    uint16_t status = 0;
    hwaddr addr;
    NvmeCmd cmd;
    NvmeRequest *req;
    int processed = 0;
    #ifdef PG_CC_BREAKDOWN
    uint64_t start_time = 0, end_time = 0;
    #endif

    nvme_update_sq_tail(sq);
    while (!(nvme_sq_empty(sq))) {
        #ifdef PG_CC_BREAKDOWN
        start_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
        #endif

        if (sq->phys_contig) {
            addr = sq->dma_addr + sq->head * n->sqe_size;
            nvme_copy_cmd(&cmd, (void *)&(((NvmeCmd *)sq->dma_addr_hva)[sq->head]));
        } else {
            addr = nvme_discontig(sq->prp_list, sq->head, n->page_size,
                                  n->sqe_size);
            nvme_addr_read(n, addr, (void *)&cmd, sizeof(cmd));
        }

        req = QTAILQ_FIRST(&sq->req_list);
        QTAILQ_REMOVE(&sq->req_list, req, entry);
        memset(&req->cqe, 0, sizeof(req->cqe));

        if (is_pg_cc_cmd(cmd.opcode)) {
            assert(n->pg_cc_enabled);
            status = nvme_pg_cc_cmd_init(n, &cmd, req);
            if (unlikely(status != NVME_SUCCESS)) {
                femu_err("IO status %hu in pg_cc mode\n", status);
                nvme_inc_sq_head(sq);
                continue ;
            }
        }
        nvme_inc_sq_head(sq);
            
        /* Coperd: record req->stime at earliest convenience */
        req->expire_time = req->stime = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
#ifdef PG_CC_BREAKDOWN
        req->start_time = start_time;
#endif
        req->cqe.cid = cpu_to_le16(cmd.cid);
        req->cmd_opcode = cmd.opcode;
        req->subReq_num = 0;
        memcpy(&req->cmd, &cmd, sizeof(NvmeCmd));

        if (n->print_log) {
            femu_debug("%s,cid:%d\n", __func__, cmd.cid);
        }

        if (!is_pg_cc_cmd(cmd.opcode)) {
            if (!unlikely(n->pg_cc_enabled)) {
                femu_err("warn: pg_cc_enabled but raw rw_opc %d\n", cmd.opcode);
            }
            printf("normal mode opc %d\n", cmd.opcode);
            status = nvme_io_cmd(n, &cmd, req);
            if (status != NVME_SUCCESS) {
                femu_err("IO status %hu in normal mode\n", status);
                assert(0);
            }
        }

        receive_req++;
#if 0
        if (receive_req % 100000 == 0) {  
            printf("NVMe Queue: receive_req %lu\n", receive_req);
        } else if (receive_req >= 100000) {
            // printf("NVMe Queue:%lu %d %u %u\n", receive_req, req->cmd.opcode, req->cmd.cdw10, req->cmd.cdw12 & 0xFFFF);   
            //if (receive_req == 198785) {
            //    signal(SIGTRAP, display);
            }
        }
#endif
        if (1 && status == NVME_SUCCESS) {
            req->status = status;
            #ifdef PG_CC_BREAKDOWN
            end_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
            req->sq_time = end_time - start_time;
            #endif
            if (req->cmd.opcode == NVME_CMD_UN_PIN_RW) {
                assert(req->is_unpin);
                deliver_subUnpinReq(n, sq, index_poller, req);
            } else if (is_raw_rw_cmd(req->cmd.opcode) || is_pg_cc_cmd(req->cmd.opcode)) {
                deliver_subIOReq(n, sq, index_poller, req);
            } else {
                deliver_otherReq(n, sq, index_poller, req);
            }
        } else if (status == NVME_SUCCESS) {
            /* Normal I/Os that don't need delay emulation */
            req->status = status;
        } else {
            femu_err("IO status %hu\n", status);
            femu_err("Error IO processed!\n");
        }

        processed++;
    }

    nvme_update_sq_eventidx(sq);
    sq->completed += processed;
}

static void nvme_post_cqe(NvmeCQueue *cq, NvmeRequest *req)
{
    // uint64_t now = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    FemuCtrl *n = cq->ctrl;
    NvmeSQueue *sq = req->sq;
    NvmeCqe *cqe = &req->cqe;
    uint8_t phase = cq->phase;
    hwaddr addr;


    if (n->print_log) {
        femu_debug("%s,req,lba:%lu,lat:%lu\n", n->devname, req->slba, req->reqlat);
    }
    cqe->status = cpu_to_le16((req->status << 1) | phase);
    cqe->sq_id = cpu_to_le16(sq->sqid);
    cqe->sq_head = cpu_to_le16(sq->head);

    switch (req->cmd_opcode) {
        case NVME_CMD_PG_CC_WRITE:
        case NVME_CMD_PG_CC_READ:
        case NVME_CMD_WRITE:
        case NVME_CMD_READ:
        case NVME_CMD_UN_PIN_RW:
            break;
        default:
            femu_err("Unknown opcode %d\n", req->cmd_opcode);
            break;
    }
    
    if (cq->phys_contig) {
        addr = cq->dma_addr + cq->tail * n->cqe_size;
        ((NvmeCqe *)cq->dma_addr_hva)[cq->tail] = *cqe;
        // nvme_addr_write(n, addr, (void *)cqe, sizeof(*cqe));
    } else {
        addr = nvme_discontig(cq->prp_list, cq->tail, n->page_size, n->cqe_size);
        nvme_addr_write(n, addr, (void *)cqe, sizeof(*cqe));
    }

    femu_debug("CQ cid:%hu,cqe->sq_id:%hu,cqe->sq_head:%hu,cq_head:%hu,cq_tail:%hu\n", 
                cqe->cid, req->sq->sqid, cqe->sq_head, cq->head, cq->tail);

    nvme_inc_cq_tail(cq);
    // uint64_t now2 = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
}

static inline void nvme_update_stat(NvmeRequest *req, NvmeSubRequest *subReq) {
    req->subReq_lat = subReq->total_lat;
    req->nand_time = subReq->nand_lat;
    req->ftl_time = subReq->ftl_time;
    req->to_ftl_time = subReq->rev_ftl_time - subReq->send_ftl_time;
    req->to_poller_time = subReq->rev_poller_time - subReq->send_poller_time;
}

// static uint64_t sending_req = 0;

static void nvme_process_cq_cpl(void *arg, int index_poller, int index_ftl)
{
    FemuCtrl *n = (FemuCtrl *)arg;
    NvmeCQueue *cq = NULL;
    NvmeRequest *req = NULL;
    NvmeSubRequest *subReq = NULL;
    NvmeLBARangeEntry *lba_entry;
    struct rte_ring *rp = n->to_ftl[index_poller][index_ftl];
    pqueue_t *pq = n->pq[index_poller];  
    uint64_t now;
    #ifdef PG_CC_BREAKDOWN
    uint64_t next_now;
    #endif
    int processed = 0;
    int rc;


    // pg_cc_dram_rw_context dram_rw_ctx; 
    // dram_rw_ctx.as = pci_get_address_space(&n->parent_obj);


    if (BBSSD(n)) {
        rp = n->to_poller[index_poller][index_ftl];
    }

    while (femu_ring_count(rp)) {
        subReq = NULL;
        rc = femu_ring_dequeue(rp, (void *)&subReq, 1);
        if (rc != 1) {
            femu_err("dequeue from to_poller request failed\n");
        }
        assert(subReq);
#ifdef PG_CC_BREAKDOWN
        subReq->rev_poller_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
#endif
        req = subReq->parent_req;

        assert(req);

        assert(req->subReq_num != 0);
        req->subReq_num--;
        
        if (is_pg_cc_cmd(req->cmd_opcode)) {
            if (req->cmd_opcode != NVME_CMD_UN_PIN_RW) {
                req->pg_cc_hit_num += subReq->pg_cc_hit_num;
                req->pg_cc_miss_num += subReq->pg_cc_miss_num;
            }

             req->expire_time = req->expire_time > subReq->expire_time ? req->expire_time : subReq->expire_time;
        
            subReq->total_lat = subReq->rev_poller_time - subReq->send_ftl_time;
            if (req->subReq_lat <= subReq->total_lat) {
                nvme_update_stat(req, subReq);
            }
            
            while (!QTAILQ_EMPTY(&subReq->lba_list)) {
                lba_entry = QTAILQ_FIRST(&subReq->lba_list);
                QTAILQ_REMOVE(&subReq->lba_list, lba_entry, entry);
                QTAILQ_INSERT_TAIL(&req->sq->lba_list, lba_entry, entry);
            }
        }

        subReq->parent_req = NULL;

        QTAILQ_INSERT_TAIL(&req->sq->subReq_list, subReq, entry);
        
        if (req->subReq_num != 0) {
            continue;
        }

        if (n->pg_cc_enabled) {
            switch (req->cmd_opcode) {
            case NVME_CMD_PG_CC_READ:
            case NVME_CMD_PG_CC_WRITE:

                nvme_pg_cc_write_prp(req, n);
#if 0
                NvmeNamespace *ns = req->ns;
                const uint8_t lba_index = NVME_ID_NS_FLBAS_INDEX(ns->id_ns.flbas);
                const uint8_t data_shift = ns->id_ns.lbaf[lba_index].lbads;
                uint64_t data_offset = (req->slba) << data_shift;

                dram_rw_ctx.is_write = false;
                dram_rw_ctx.page_addr = req->page_phy_addr;
                dram_rw_ctx.page_num = fast_transLBA2LPN(req->slba + req->nlb - 1) - fast_transLBA2LPN(req->slba) + 1;
                dram_rw_ctx.start_addr = data_offset;
                dram_rw_ctx.need_cp = req->need_dram_rw;

                // backend_pg_cc_rw(n->mbe, &dram_rw_ctx);
#endif
                break;
            case NVME_CMD_UN_PIN_RW:
                /* TODO: check sq is active */
                break;
            default:
                break;
            }
        }

        pqueue_insert(pq, req);
    }

    while ((req = pqueue_peek(pq))) {
        now = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
        assert(req->subReq_num == 0);

        if (now < req->expire_time) {
            break;
        }
        
        processed++;
        if (processed >= 10000) {
            printf("processed %d\n", processed);
        }
#if 0
        sending_req++;
        if (sending_req % 100000 == 0) {
            printf("sending_req %lu\n", sending_req);
        } else if (sending_req > 100000) {
            printf("sending_req%lu %d %u %u\n", sending_req, req->cmd.opcode, req->cmd.cdw10, req->cmd.cdw12 & 0xFFFF);   
            // printf("sending_req %lu\n", sending_req);
        }
#endif
        cq = n->cq[req->sq->sqid];
        assert(cq);
        if (!cq->is_active)
            continue;
        nvme_post_cqe(cq, req);
        QTAILQ_INSERT_TAIL(&req->sq->req_list, req, entry);
        pqueue_pop(pq);
        
        n->nr_tt_ios++;
 #if 0
        if (now - req->expire_time >= 20000) {
            n->nr_tt_late_ios++;
            if (n->print_log) {
                femu_debug("%s,diff,pq.count=%lu,%" PRId64 ", %lu/%lu\n",
                           n->devname, pqueue_size(pq), now - req->expire_time,
                           n->nr_tt_late_ios, n->nr_tt_ios);
            }
        }
#endif
        n->should_isr[req->sq->sqid] = true;
        #ifdef PG_CC_BREAKDOWN
        next_now = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
        req->cq_time =  next_now - now;
        req->end_time = next_now;
        n->ext_ops.stat(cq, req);
        #endif
    }

    if (processed == 0)
        return;

    switch (n->multipoller_enabled) {
    case 1:
        int shard_length = n->num_io_queues / n->num_poller;
        for (int qid = (index_poller - 1) * shard_length + 1; qid <= index_poller * shard_length; qid++) {
            if (n->should_isr[qid]) {
                nvme_isr_notify_io(n->cq[qid]);
                n->should_isr[qid] = false;
            }
        }
        break;
    default:
        for (int i = 1; i <= n->num_io_queues; i++) {
            if (n->should_isr[i]) {
                nvme_isr_notify_io(n->cq[i]);
                n->should_isr[i] = false;
            }
        }
        break;
    }
}

static void *nvme_poller(void *arg)
{
    FemuCtrl *n = ((NvmePollerThreadArgument *)arg)->n;
    int index = ((NvmePollerThreadArgument *)arg)->index;
    assert(n->shard_length != 0);

    switch (n->multipoller_enabled)
    {
    case 1:
        assert(n->num_io_queues % n->num_poller == 0);
        int shard_length = n->num_io_queues / n->num_poller;
        while (1)
        {
            if ((!n->dataplane_started))
            {
                usleep(1000);
                continue;
            }
            for (int qid = (index - 1) * shard_length + 1; qid <= index * shard_length; qid++) {
                NvmeSQueue *sq = n->sq[qid];
                NvmeCQueue *cq = n->cq[qid];
                if (sq && sq->is_active && cq && cq->is_active) {
                    nvme_process_sq_io(sq, index);
                }
                for (int i = 1; i <= n->num_ftl; i++) 
                    if (sq && sq->is_active && cq && cq->is_active) {
                        nvme_process_cq_cpl(n, index, i);
                    }
            }
            
        }
        break;
    default:
        while (1) {
            if ((!n->dataplane_started)) {
                usleep(1000);
                continue;
            }

            for (int i = 1; i <= n->num_io_queues; i++) {
                NvmeSQueue *sq = n->sq[i];
                NvmeCQueue *cq = n->cq[i];
                if (sq && sq->is_active && cq && cq->is_active) {
                    nvme_process_sq_io(sq, index);
                }
                
                for (int j = 1; j <= n->num_ftl; j++) {
                    if (sq && sq->is_active && cq && cq->is_active) {
                        nvme_process_cq_cpl(n, index, j);
                    }
                }
                
                // uint64_t now2 = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
                // if (now2-now >= 10000)
                //    femu_log("diff %"PRId64"\n", now2 - now);
            }
            // for (int i = 1; i <= n->num_ftl; i++) {
            //     nvme_process_cq_cpl(n, index, i);
            // }
        }
        break;
    }

    return NULL;
}

static int cmp_pri(pqueue_pri_t next, pqueue_pri_t curr)
{
    return (next > curr);
}

static pqueue_pri_t get_pri(void *a)
{
    return ((NvmeRequest *)a)->expire_time;
}

static void set_pri(void *a, pqueue_pri_t pri)
{
    ((NvmeRequest *)a)->expire_time = pri;
}

static size_t get_pos(void *a)
{
    return ((NvmeRequest *)a)->pos;
}

static void set_pos(void *a, size_t pos)
{
    ((NvmeRequest *)a)->pos = pos;
}

void nvme_create_poller(FemuCtrl *n)
{
    n->should_isr = g_malloc0(sizeof(bool) * (n->num_io_queues + 1));

    assert(n->num_poller > 0);
    assert(n->num_io_queues % n->num_poller == 0);
    n->num_poller = n->multipoller_enabled ? n->num_poller : 1;
    /* Coperd: we put NvmeRequest into these rings */
    n->to_ftl = malloc(sizeof(struct rte_ring **) * (n->num_poller + 1));
    for (int i = 1; i <= n->num_poller; i++) {
        n->to_ftl[i] = malloc(sizeof(struct rte_ring *) * (n->num_ftl + 1));
        for (int j = 1; j <= n->num_ftl; j++) {
            n->to_ftl[i][j] = femu_ring_create(FEMU_RING_TYPE_SP_SC, FEMU_MAX_INF_REQS);
            if (!n->to_ftl[i][j])
            {
                femu_err("failed to create ring (n->to_ftl) ...\n");
                abort();
            }
            assert(rte_ring_empty(n->to_ftl[i][j]));
        }
    }

    n->to_poller = malloc(sizeof(struct rte_ring **) * (n->num_poller + 1));
    for (int i = 1; i <= n->num_poller; i++) {
        n->to_poller[i] = malloc(sizeof(struct rte_ring *) * (n->num_ftl + 1));
        for (int j = 1; j <= n->num_ftl; j++) {
            /* TODO: ensure single-producer and single-consumer  */
            n->to_poller[i][j] = femu_ring_create(FEMU_RING_TYPE_SP_SC, FEMU_MAX_INF_REQS);
            if (!n->to_poller[i][j]) {
                femu_err("failed to create ring (n->to_poller) ...\n");
                abort();
            }
            assert(rte_ring_empty(n->to_poller[i][j]));
        }
    }

    n->admin2ftl = malloc(sizeof(struct rte_ring *) * (n->num_ftl + 1));
    n->ftl2admin = malloc(sizeof(struct rte_ring *) * (n->num_ftl + 1));
    for (int i = 1; i <= n->num_ftl; i++) {
        n->admin2ftl[i] = femu_ring_create(FEMU_RING_TYPE_SP_SC, FEMU_MAX_INF_REQS);
        if (!n->admin2ftl[i]) {
            femu_err("failed to create ring (n->admin2ftl) ...\n");
            abort();
        }
        assert(rte_ring_empty(n->admin2ftl[i]));

        n->ftl2admin[i] = femu_ring_create(FEMU_RING_TYPE_SP_SC, FEMU_MAX_INF_REQS);
        if (!n->ftl2admin[i]) {
            femu_err("failed to create ring (n->ftl2admin) ...\n");
            abort();
        }
        assert(rte_ring_empty(n->ftl2admin[i]));
    }

    n->pq = malloc(sizeof(pqueue_t *) * (n->num_poller + 1));
    for (int i = 1; i <= n->num_poller; i++) {
        n->pq[i] = pqueue_init(FEMU_MAX_INF_REQS, cmp_pri, get_pri, set_pri,
                               get_pos, set_pos);
        if (!n->pq[i]) {
            femu_err("failed to create pqueue (n->pq) ...\n");
            abort();
        }
    }

    n->poller = malloc(sizeof(QemuThread) * (n->num_poller + 1));
    NvmePollerThreadArgument *args = malloc(sizeof(NvmePollerThreadArgument) *
                                            (n->num_poller + 1));
    for (int i = 1; i <= n->num_poller; i++) {
        args[i].n = n;
        args[i].index = i;
        qemu_thread_create(&n->poller[i], "nvme-poller", nvme_poller, &args[i],
                           QEMU_THREAD_JOINABLE);
        // qemu_thread_create(&n->cq_pusher[i], "nvme-cq-pusher", nvme_cq_pusher,
        //                    &args[i], QEMU_THREAD_JOINABLE);
        femu_debug("nvme-poller [%d] created ...\n", i - 1);
    }
}

uint16_t nvme_rw(FemuCtrl *n, NvmeNamespace *ns, NvmeCmd *cmd, NvmeRequest *req)
{
    NvmeRwCmd *rw = (NvmeRwCmd *)cmd;
    uint16_t ctrl = le16_to_cpu(rw->control);
    uint32_t nlb  = le16_to_cpu(rw->nlb) + 1;
    uint64_t slba = le64_to_cpu(rw->slba);
    uint64_t prp1 = le64_to_cpu(rw->prp1);
    uint64_t prp2 = le64_to_cpu(rw->prp2);
    const uint8_t lba_index = NVME_ID_NS_FLBAS_INDEX(ns->id_ns.flbas);
    const uint16_t ms = le16_to_cpu(ns->id_ns.lbaf[lba_index].ms);
    const uint8_t data_shift = ns->id_ns.lbaf[lba_index].lbads;
    uint64_t data_size = (uint64_t)nlb << data_shift;
    // uint64_t data_offset = slba << data_shift;
    int ret;
    uint64_t meta_size = nlb * ms;
    uint64_t elba = slba + nlb;
    uint16_t err;

    req->is_write = (rw->opcode == NVME_CMD_WRITE || rw->opcode == NVME_CMD_PG_CC_WRITE) ? 1 : 0;

    err = femu_nvme_rw_check_req(n, ns, cmd, req, slba, elba, nlb, ctrl,
                                 data_size, meta_size);
    if (err)
        return err;

    if (nvme_map_prp(&req->qsg, &req->iov, prp1, prp2, data_size, n)) {
        nvme_set_error_page(n, req->sq->sqid, cmd->cid, NVME_INVALID_FIELD,
                            offsetof(NvmeRwCmd, prp1), 0, ns->id);
        return NVME_INVALID_FIELD | NVME_DNR;
    }

    assert((nlb << data_shift) == req->qsg.size);

    req->slba = slba;
    req->status = NVME_SUCCESS;
    req->nlb = nlb;

    ret = 0; //backend_rw(n->mbe, &req->qsg, &data_offset, req->is_write);
    if (!ret) {
        return NVME_SUCCESS;
    }

    return NVME_DNR;
}

static uint16_t nvme_dsm(FemuCtrl *n, NvmeNamespace *ns, NvmeCmd *cmd,
                         NvmeRequest *req)
{
    uint32_t dw10 = le32_to_cpu(cmd->cdw10);
    uint32_t dw11 = le32_to_cpu(cmd->cdw11);
    uint64_t prp1 = le64_to_cpu(cmd->dptr.prp1);
    uint64_t prp2 = le64_to_cpu(cmd->dptr.prp2);

    if (dw11 & NVME_DSMGMT_AD) {
        uint16_t nr = (dw10 & 0xff) + 1;

        uint64_t slba;
        uint32_t nlb;
        NvmeDsmRange range[nr];

        if (dma_write_prp(n, (uint8_t *)range, sizeof(range), prp1, prp2)) {
            nvme_set_error_page(n, req->sq->sqid, cmd->cid, NVME_INVALID_FIELD,
                                offsetof(NvmeCmd, dptr.prp1), 0, ns->id);
            return NVME_INVALID_FIELD | NVME_DNR;
        }

        req->status = NVME_SUCCESS;
        for (int i = 0; i < nr; i++) {
            slba = le64_to_cpu(range[i].slba);
            nlb = le32_to_cpu(range[i].nlb);
            if (slba + nlb > le64_to_cpu(ns->id_ns.nsze)) {
                nvme_set_error_page(n, req->sq->sqid, cmd->cid, NVME_LBA_RANGE,
                                    offsetof(NvmeCmd, cdw10), slba + nlb, ns->id);
                return NVME_LBA_RANGE | NVME_DNR;
            }

            bitmap_clear(ns->util, slba, nlb);
        }
    }

    return NVME_SUCCESS;
}

static uint16_t nvme_compare(FemuCtrl *n, NvmeNamespace *ns, NvmeCmd *cmd,
                             NvmeRequest *req)
{
    NvmeRwCmd *rw = (NvmeRwCmd *)cmd;
    uint32_t nlb  = le16_to_cpu(rw->nlb) + 1;
    uint64_t slba = le64_to_cpu(rw->slba);
    uint64_t prp1 = le64_to_cpu(rw->prp1);
    uint64_t prp2 = le64_to_cpu(rw->prp2);

    uint64_t elba = slba + nlb;
    uint8_t lba_index = NVME_ID_NS_FLBAS_INDEX(ns->id_ns.flbas);
    uint8_t data_shift = ns->id_ns.lbaf[lba_index].lbads;
    uint64_t data_size = nlb << data_shift;
    uint64_t offset  = ns->start_block + (slba << data_shift);

    if ((slba + nlb) > le64_to_cpu(ns->id_ns.nsze)) {
        nvme_set_error_page(n, req->sq->sqid, cmd->cid, NVME_LBA_RANGE,
                            offsetof(NvmeRwCmd, nlb), elba, ns->id);
        return NVME_LBA_RANGE | NVME_DNR;
    }
    if (n->id_ctrl.mdts && data_size > n->page_size * (1 << n->id_ctrl.mdts)) {
        nvme_set_error_page(n, req->sq->sqid, cmd->cid, NVME_INVALID_FIELD,
                            offsetof(NvmeRwCmd, nlb), nlb, ns->id);
        return NVME_INVALID_FIELD | NVME_DNR;
    }
    if (nvme_map_prp(&req->qsg, &req->iov, prp1, prp2, data_size, n)) {
        nvme_set_error_page(n, req->sq->sqid, cmd->cid, NVME_INVALID_FIELD,
                            offsetof(NvmeRwCmd, prp1), 0, ns->id);
        return NVME_INVALID_FIELD | NVME_DNR;
    }
    if (find_next_bit(ns->uncorrectable, elba, slba) < elba) {
        return NVME_UNRECOVERED_READ;
    }

    for (int i = 0; i < req->qsg.nsg; i++) {
        uint32_t len = req->qsg.sg[i].len;
        uint8_t tmp[2][len];

        nvme_addr_read(n, req->qsg.sg[i].base, tmp[1], len);
        if (memcmp(tmp[0], tmp[1], len)) {
            qemu_sglist_destroy(&req->qsg);
            return NVME_CMP_FAILURE;
        }
        offset += len;
    }

    qemu_sglist_destroy(&req->qsg);

    return NVME_SUCCESS;
}

static uint16_t nvme_flush(FemuCtrl *n, NvmeNamespace *ns, NvmeCmd *cmd,
                           NvmeRequest *req)
{
    return NVME_SUCCESS;
}

static uint16_t nvme_write_zeros(FemuCtrl *n, NvmeNamespace *ns, NvmeCmd *cmd,
                                 NvmeRequest *req)
{
    NvmeRwCmd *rw = (NvmeRwCmd *)cmd;
    uint64_t slba = le64_to_cpu(rw->slba);
    uint32_t nlb  = le16_to_cpu(rw->nlb) + 1;

    if ((slba + nlb) > ns->id_ns.nsze) {
        nvme_set_error_page(n, req->sq->sqid, cmd->cid, NVME_LBA_RANGE,
                            offsetof(NvmeRwCmd, nlb), slba + nlb, ns->id);
        return NVME_LBA_RANGE | NVME_DNR;
    }

    return NVME_SUCCESS;
}

static uint16_t nvme_write_uncor(FemuCtrl *n, NvmeNamespace *ns, NvmeCmd *cmd,
                                 NvmeRequest *req)
{
    NvmeRwCmd *rw = (NvmeRwCmd *)cmd;
    uint64_t slba = le64_to_cpu(rw->slba);
    uint32_t nlb  = le16_to_cpu(rw->nlb) + 1;

    if ((slba + nlb) > ns->id_ns.nsze) {
        nvme_set_error_page(n, req->sq->sqid, cmd->cid, NVME_LBA_RANGE,
                            offsetof(NvmeRwCmd, nlb), slba + nlb, ns->id);
        return NVME_LBA_RANGE | NVME_DNR;
    }

    bitmap_set(ns->uncorrectable, slba, nlb);

    return NVME_SUCCESS;
}

static uint16_t nvme_io_cmd(FemuCtrl *n, NvmeCmd *cmd, NvmeRequest *req)
{
    NvmeNamespace *ns;
    uint32_t nsid = le32_to_cpu(cmd->nsid);

    if (nsid == 0 || nsid > n->num_namespaces) {
        femu_err("%s, NVME_INVALID_NSID %" PRIu32 "\n", __func__, nsid);
        return NVME_INVALID_NSID | NVME_DNR;
    }

    req->ns = ns = &n->namespaces[nsid - 1];

    switch (cmd->opcode) {
    case NVME_CMD_FLUSH:
        if (!n->id_ctrl.vwc || !n->features.volatile_wc) {
            return NVME_SUCCESS;
        }
        return nvme_flush(n, ns, cmd, req);
    case NVME_CMD_DSM:
        if (NVME_ONCS_DSM & n->oncs) {
            return nvme_dsm(n, ns, cmd, req);
        }
        return NVME_INVALID_OPCODE | NVME_DNR;
    case NVME_CMD_COMPARE:
        if (NVME_ONCS_COMPARE & n->oncs) {
            return nvme_compare(n, ns, cmd, req);
        }
        return NVME_INVALID_OPCODE | NVME_DNR;
    case NVME_CMD_WRITE_ZEROES:
        if (NVME_ONCS_WRITE_ZEROS & n->oncs) {
            return nvme_write_zeros(n, ns, cmd, req);
        }
        return NVME_INVALID_OPCODE | NVME_DNR;
    case NVME_CMD_WRITE_UNCOR:
        if (NVME_ONCS_WRITE_UNCORR & n->oncs) {
            return nvme_write_uncor(n, ns, cmd, req);
        }
        return NVME_INVALID_OPCODE | NVME_DNR;
    default:
        if (n->ext_ops.io_cmd) {
            return n->ext_ops.io_cmd(n, ns, cmd, req);
        }

        femu_err("%s, NVME_INVALID_OPCODE\n", __func__);
        return NVME_INVALID_OPCODE | NVME_DNR;
    }
}

void nvme_post_cqes_io(void *opaque)
{
    NvmeCQueue *cq = opaque;
    NvmeRequest *req, *next;
    int64_t cur_time, ntt = 0;
    int processed = 0;

    QTAILQ_FOREACH_SAFE(req, &cq->req_list, entry, next) {
        if (nvme_cq_full(cq)) {
            break;
        }

        cur_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
        if (cq->cqid != 0 && cur_time < req->expire_time) {
            ntt = req->expire_time;
            break;
        }

        nvme_post_cqe(cq, req);
        processed++;
    }

    if (ntt == 0) {
        ntt = qemu_clock_get_ns(QEMU_CLOCK_REALTIME) + CQ_POLLING_PERIOD_NS;
    }

    /* Only interrupt guest when we "do" complete some I/Os */
    if (processed > 0) {
        nvme_isr_notify_io(cq);
    }
}
