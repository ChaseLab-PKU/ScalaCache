#include "./nvme.h"

int nvme_check_sqid(FemuCtrl *n, uint16_t sqid)
{
    return sqid <= n->num_io_queues && n->sq[sqid] != NULL ? 0 : -1;
}

int nvme_check_cqid(FemuCtrl *n, uint16_t cqid)
{
    return cqid <= n->num_io_queues && n->cq[cqid] != NULL ? 0 : -1;
}

void nvme_inc_cq_tail(NvmeCQueue *cq)
{
    cq->tail++;
    if (cq->tail >= cq->size) {
        cq->tail = 0;
        cq->phase = !cq->phase;
    }
}

void nvme_inc_sq_head(NvmeSQueue *sq)
{
    sq->head = (sq->head + 1) % sq->size;
}

void nvme_update_sq_tail(NvmeSQueue *sq)
{
    if (sq->db_addr_hva) {
        sq->tail = *((uint32_t *)sq->db_addr_hva);
        return;
    }

    if (sq->db_addr) {
        nvme_addr_read(sq->ctrl, sq->db_addr, &sq->tail, sizeof(sq->tail));
    }
}

uint32_t nvme_update_and_get_cq_head(NvmeCQueue *cq) {
    uint32_t head = cq->head;
    nvme_update_cq_head(cq);
    return head;
}

void nvme_update_cq_head(NvmeCQueue *cq)
{
    if (cq->db_addr_hva) {
        cq->head = *(uint32_t *)(cq->db_addr_hva);
        return;
    }

    if (cq->db_addr) {
        nvme_addr_read(cq->ctrl, cq->db_addr, &cq->head, sizeof(cq->head));
    }
}

uint8_t nvme_cq_full(NvmeCQueue *cq)
{
    nvme_update_cq_head(cq);

    return (cq->tail + 1) % cq->size == cq->head;
}

uint8_t nvme_sq_empty(NvmeSQueue *sq)
{
    return sq->head == sq->tail;
}

uint64_t *nvme_setup_discontig(FemuCtrl *n, uint64_t prp_addr, uint16_t
                               queue_depth, uint16_t entry_size)
{
    uint16_t prps_per_page = n->page_size >> 3;
    uint64_t prp[prps_per_page];
    uint16_t total_prps = DIV_ROUND_UP(queue_depth * entry_size, n->page_size);
    uint64_t *prp_list = g_malloc0(total_prps * sizeof(*prp_list));

    for (int i = 0; i < total_prps; i++) {
        if (i % prps_per_page == 0 && i < total_prps - 1) {
            if (!prp_addr || prp_addr & (n->page_size - 1)) {
                g_free(prp_list);
                return NULL;
            }
            nvme_addr_write(n, prp_addr, (uint8_t *)&prp, sizeof(prp));
            prp_addr = le64_to_cpu(prp[prps_per_page - 1]);
        }
        prp_list[i] = le64_to_cpu(prp[i % prps_per_page]);
        if (!prp_list[i] || prp_list[i] & (n->page_size - 1)) {
            g_free(prp_list);
            return NULL;
        }
    }

    return prp_list;
}

void nvme_set_error_page(FemuCtrl *n, uint16_t sqid, uint16_t cid, uint16_t
                         status, uint16_t location, uint64_t lba, uint32_t nsid)
{
    NvmeErrorLog *elp;

    elp = &n->elpes[n->elp_index];
    elp->error_count = n->error_count++;
    elp->sqid = sqid;
    elp->cid = cid;
    elp->status_field = status;
    elp->param_error_location = location;
    elp->lba = lba;
    elp->nsid = nsid;
    n->elp_index = (n->elp_index + 1) % n->elpe;
    ++n->num_errors;
}

uint16_t femu_nvme_rw_check_req(FemuCtrl *n, NvmeNamespace *ns, NvmeCmd *cmd,
                                NvmeRequest *req, uint64_t slba, uint64_t elba,
                                uint32_t nlb, uint16_t ctrl, uint64_t data_size,
                                uint64_t meta_size)
{

    if (elba > le64_to_cpu(ns->id_ns.nsze)) {
        nvme_set_error_page(n, req->sq->sqid, cmd->cid, NVME_LBA_RANGE,
                            offsetof(NvmeRwCmd, nlb), elba, ns->id);
        return NVME_LBA_RANGE | NVME_DNR;
    }
    if (n->id_ctrl.mdts && data_size > n->page_size * (1 << n->id_ctrl.mdts)) {
        nvme_set_error_page(n, req->sq->sqid, cmd->cid, NVME_INVALID_FIELD,
                            offsetof(NvmeRwCmd, nlb), nlb, ns->id);
        return NVME_INVALID_FIELD | NVME_DNR;
    }
    if (meta_size) {
        nvme_set_error_page(n, req->sq->sqid, cmd->cid, NVME_INVALID_FIELD,
                            offsetof(NvmeRwCmd, control), ctrl, ns->id);
        return NVME_INVALID_FIELD | NVME_DNR;
    }
    if ((ctrl & NVME_RW_PRINFO_PRACT) && !(ns->id_ns.dps & DPS_TYPE_MASK)) {
        nvme_set_error_page(n, req->sq->sqid, cmd->cid, NVME_INVALID_FIELD,
                            offsetof(NvmeRwCmd, control), ctrl, ns->id);
        /* Not contemplated in LightNVM for now */
        if (OCSSD(n)) {
            return 0;
        }
        return NVME_INVALID_FIELD | NVME_DNR;
    }
    if (!req->is_write && find_next_bit(ns->uncorrectable, elba, slba) < elba) {
        nvme_set_error_page(n, req->sq->sqid, cmd->cid, NVME_UNRECOVERED_READ,
                            offsetof(NvmeRwCmd, slba), elba, ns->id);
        return NVME_UNRECOVERED_READ;
    }

    return 0;
}

static void nvme_clear_sq(NvmeSQueue *sq, FemuCtrl *n)
{
    for (int i = 0; i < sq->size; i++) {
        free(sq->io_req[i].page_id);
        free(sq->io_req[i].page_phy_addr);
        free(sq->io_req[i].need_dram_rw);
        free(sq->io_req[i].need_pin);
        free(sq->io_req[i].unpin_lpa);
    }
    g_free(sq->io_req);

    for (int i = 0; i < sq->size; i++) {
        free(sq->io_subReq[i].unpin_is_dirty);
        free(sq->io_subReq[i].unpin_pg_id);
    }
    g_free(sq->io_subReq);

    if (sq->prp_list) {
        g_free(sq->prp_list);
    }
    if (sq->sqid) {
        g_free(sq);
    }
}

void nvme_free_sq(NvmeSQueue *sq, FemuCtrl *n)
{
    n->sq[sq->sqid] = NULL;
    nvme_clear_sq(sq, n);
}

uint16_t nvme_init_sq(NvmeSQueue *sq, FemuCtrl *n, uint64_t dma_addr, uint16_t
                      sqid, uint16_t cqid, uint16_t size, enum NvmeQueueFlags
                      prio, int contig)
{
    uint8_t stride = n->db_stride;
    int dbbuf_entry_sz = 1 << (2 + stride);
    AddressSpace *as = pci_get_address_space(&n->parent_obj);
    dma_addr_t sqsz = (dma_addr_t)size;
    NvmeCQueue *cq;

    sq->ctrl = n;
    sq->sqid = sqid;
    sq->size = size;
    sq->cqid = cqid;
    sq->head = sq->tail = 0;
    sq->phys_contig = contig;
    if (sq->phys_contig) {
        sq->dma_addr = dma_addr;
        sq->dma_addr_hva = (uint64_t)dma_memory_map(as, dma_addr, &sqsz, 0, MEMTXATTRS_UNSPECIFIED);
    } else {
        sq->prp_list = nvme_setup_discontig(n, dma_addr, size, n->sqe_size);
        if (!sq->prp_list) {
            return NVME_INVALID_FIELD | NVME_DNR;
        }
    }

    sq->io_req = g_malloc0(sq->size * sizeof(*sq->io_req));
    QTAILQ_INIT(&sq->req_list);
    QTAILQ_INIT(&sq->subReq_list);
    QTAILQ_INIT(&sq->out_req_list);
    QTAILQ_INIT(&sq->lba_list);
    for (int i = 0; i < sq->size; i++) {
        sq->io_req[i].sq = sq;
        sq->io_req[i].page_id = malloc(sizeof(uint64_t) * NVME_MAX_REQUEST_SIZE);
        sq->io_req[i].page_phy_addr = malloc(sizeof(uint64_t) * NVME_MAX_REQUEST_SIZE);
        sq->io_req[i].unpin_lpa = malloc(sizeof(uint64_t) * NVME_MAX_REQUEST_SIZE);
        sq->io_req[i].need_dram_rw = malloc(sizeof(bool) * NVME_MAX_REQUEST_SIZE);
        sq->io_req[i].need_pin = malloc(sizeof(bool) * NVME_MAX_REQUEST_SIZE);
        sq->io_req[i].unpin_is_dirty = malloc(sizeof(bool) * NVME_MAX_REQUEST_SIZE);
        QTAILQ_INSERT_TAIL(&(sq->req_list), &sq->io_req[i], entry);
    }
    sq->io_subReq = g_malloc0(sq->size * n->num_ftl * 2 * sizeof(*sq->io_subReq));
    for (int i = 0; i < sq->size * n->num_ftl * 2; i++) {
        sq->io_subReq[i].unpin_pg_id = malloc(sizeof(uint64_t) * NVME_MAX_REQUEST_SIZE);
        sq->io_subReq[i].unpin_is_dirty = malloc(sizeof(bool) * NVME_MAX_REQUEST_SIZE);
        QTAILQ_INIT(&sq->io_subReq[i].lba_list);
        QTAILQ_INSERT_TAIL(&(sq->subReq_list), &sq->io_subReq[i], entry);
    }

    uint64_t lba_entry_size = sq->size * n->num_ftl * 8;
    sq->io_lba_entry = g_malloc0(lba_entry_size * sizeof(*sq->io_lba_entry));
    for (int i = 0; i < lba_entry_size; i++) {
        sq->io_lba_entry[i].slba = 0;
        sq->io_lba_entry[i].nlb = 0;
        QTAILQ_INSERT_TAIL(&(sq->lba_list), &sq->io_lba_entry[i], entry);
    }

    switch (prio) {
    case NVME_Q_PRIO_URGENT:
        sq->arb_burst = (1 << NVME_ARB_AB(n->features.arbitration));
        break;
    case NVME_Q_PRIO_HIGH:
        sq->arb_burst = NVME_ARB_HPW(n->features.arbitration) + 1;
        break;
    case NVME_Q_PRIO_NORMAL:
        sq->arb_burst = NVME_ARB_MPW(n->features.arbitration) + 1;
        break;
    case NVME_Q_PRIO_LOW:
    default:
        sq->arb_burst = NVME_ARB_LPW(n->features.arbitration) + 1;
        break;
    }

    if (sqid && n->dbs_addr && n->eis_addr) {
        sq->db_addr = n->dbs_addr + 2 * sqid * dbbuf_entry_sz;
        sq->db_addr_hva = n->dbs_addr_hva + 2 * sqid * dbbuf_entry_sz;
        sq->eventidx_addr = n->eis_addr + 2 * sqid * dbbuf_entry_sz;
        sq->eventidx_addr = n->eis_addr_hva + 2 * sqid + dbbuf_entry_sz;
        femu_debug("SQ[%d],db=%" PRIu64 ",ei=%" PRIu64 "\n", sqid, sq->db_addr,
                sq->eventidx_addr);
    }

    assert(n->cq[cqid]);
    cq = n->cq[cqid];
    QTAILQ_INSERT_TAIL(&(cq->sq_list), sq, entry);
    if (n->sq[sqid]) {
        nvme_clear_sq(n->sq[sqid], n);
    }
    n->sq[sqid] = sq;

    return NVME_SUCCESS;
}

uint16_t nvme_init_cq(NvmeCQueue *cq, FemuCtrl *n, uint64_t dma_addr, uint16_t
                      cqid, uint16_t vector, uint16_t size, uint16_t
                      irq_enabled, int contig)
{
    cq->ctrl = n;
    cq->cqid = cqid;
    cq->size = size;
    cq->phase = 1;
    cq->irq_enabled = irq_enabled;
    cq->vector = vector;
    cq->head = cq->tail = 0;
    cq->phys_contig = contig;

    uint8_t stride = n->db_stride;
    int dbbuf_entry_sz = 1 << (2 + stride);
    AddressSpace *as = pci_get_address_space(&n->parent_obj);
    dma_addr_t cqsz = (dma_addr_t)size;

    if (cq->phys_contig) {
        cq->dma_addr = dma_addr;
        cq->dma_addr_hva = (uint64_t)dma_memory_map(as, dma_addr, &cqsz, 1, MEMTXATTRS_UNSPECIFIED);
    } else {
        cq->prp_list = nvme_setup_discontig(n, dma_addr, size,
                n->cqe_size);
        if (!cq->prp_list) {
            return NVME_INVALID_FIELD | NVME_DNR;
        }
    }

    QTAILQ_INIT(&cq->req_list);
    QTAILQ_INIT(&cq->sq_list);
#if 0
    QTAILQ_INIT(&cq->pg_cc_req_list);
#endif 
    if (cqid && n->dbs_addr && n->eis_addr) {
        cq->db_addr = n->dbs_addr + (2 * cqid + 1) * dbbuf_entry_sz;
        cq->db_addr_hva = n->dbs_addr_hva + (2 * cqid + 1) * dbbuf_entry_sz;
        cq->eventidx_addr = n->eis_addr + (2 * cqid + 1) * dbbuf_entry_sz;
        cq->eventidx_addr_hva = n->eis_addr_hva + (2 * cqid + 1) * dbbuf_entry_sz;
        femu_debug("CQ, db_addr=%" PRIu64 ", eventidx_addr=%" PRIu64 "\n",
                cq->db_addr, cq->eventidx_addr);
    }
    msix_vector_use(&n->parent_obj, cq->vector);
    n->cq[cqid] = cq;

    return NVME_SUCCESS;
}

void nvme_free_cq(NvmeCQueue *cq, FemuCtrl *n)
{
    n->cq[cq->cqid] = NULL;
    msix_vector_unuse(&n->parent_obj, cq->vector);
    if (cq->prp_list) {
        g_free(cq->prp_list);
    }
    if (cq->cqid) {
        g_free(cq);
    }
}

void nvme_set_ctrl_name(FemuCtrl *n, const char *mn, const char *sn, int *dev_id)
{
    NvmeIdCtrl *id = &n->id_ctrl;
    char *subnqn;
    char serial[MN_MAX_LEN], dev_id_str[ID_MAX_LEN];

    memset(serial, 0, MN_MAX_LEN);
    memset(dev_id_str, 0, ID_MAX_LEN);
    strcat(serial, sn);

    sprintf(dev_id_str, "%d", *dev_id);
    strcat(serial, dev_id_str);
    (*dev_id)++;
    strpadcpy((char *)id->mn, sizeof(id->mn), mn, ' ');

    memset(n->devname, 0, MN_MAX_LEN);
    g_strlcpy(n->devname, serial, sizeof(serial));

    strpadcpy((char *)id->sn, sizeof(id->sn), serial, ' ');
    strpadcpy((char *)id->fr, sizeof(id->fr), "1.0", ' ');

    subnqn = g_strdup_printf("nqn.2021-05.org.femu:%s", serial);
    strpadcpy((char *)id->subnqn, sizeof(id->subnqn), subnqn, '\0');
}

