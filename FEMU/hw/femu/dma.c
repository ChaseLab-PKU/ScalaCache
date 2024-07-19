#include "./nvme.h"

void nvme_addr_read(FemuCtrl *n, hwaddr addr, void *buf, int size)
{
    if (n->cmbsz && addr >= n->ctrl_mem.addr &&
        addr < (n->ctrl_mem.addr + int128_get64(n->ctrl_mem.size))) {
        memcpy(buf, (void *)&n->cmbuf[addr - n->ctrl_mem.addr], size);
    } else {
        pci_dma_read(&n->parent_obj, addr, buf, size);
    }
}

void nvme_addr_write(FemuCtrl *n, hwaddr addr, void *buf, int size)
{
    if (n->cmbsz && addr >= n->ctrl_mem.addr &&
        addr < (n->ctrl_mem.addr + int128_get64(n->ctrl_mem.size))) {
        memcpy((void *)&n->cmbuf[addr - n->ctrl_mem.addr], buf, size);
    } else {
        pci_dma_write(&n->parent_obj, addr, buf, size);
    }
}

uint16_t nvme_pg_cc_write_prp(NvmeRequest *req, FemuCtrl *n)
{
    hwaddr addr;
    NvmeRwCmd *rw = (NvmeRwCmd *)&req->cmd;
    uint64_t prp1 = le64_to_cpu(rw->prp1);
    uint64_t len = (le32_to_cpu(rw->nlb) + 1) << 9;
    uint64_t offset = (le64_to_cpu(rw->slba)) << 9;
    uint64_t page_mask = 4095;

    assert(n->page_size == 4096);
    
    int wp = 0;
    hwaddr trans_len = MIN(n->page_size - (offset & page_mask), len);

    if (req->need_pin[wp])
        assert(req->page_id[wp] != PG_CC_INVALID_PAGE_ID);
    else 
        assert(req->page_id[wp] == PG_CC_INVALID_PAGE_ID);

    req->cqe.res64 = req->page_id[wp];

    wp++;
    len -= trans_len;
    
    if (!len) {
       return NVME_SUCCESS;
    }
    
    /* TODO: add 2 page id to res64 */
    if (1) {
        uint32_t nents, prp_trans;

        nents = (len + n->page_size - 1) >> n->page_bits;
        prp_trans = MIN(n->max_prp_ents, nents) * sizeof(uint64_t);
        addr = prp1;
        while (len) {

            nents = (len + n->page_size - 1) >> n->page_bits;
            prp_trans = MIN(n->max_prp_ents, nents);

            if (len <= prp_trans * n->page_size) {
                nvme_addr_write(n, addr, &(req->page_id[wp]), sizeof(uint64_t) * prp_trans);
                // femu_log("write_prp:1 %lu page_id %lu prp_trans %u\n", addr, req->page_id[wp], prp_trans);

                for (int i = wp; i < wp + prp_trans; i++) {
                    if (req->need_pin[i])
                        assert(req->page_id[i] != PG_CC_INVALID_PAGE_ID);
                    else 
                        assert(req->page_id[i] == PG_CC_INVALID_PAGE_ID);
                }
                
            } else {
                nvme_addr_write(n, addr, &(req->page_id[wp]), sizeof(uint64_t) * (n->max_prp_ents - 1));
                // femu_log("write_pip:2 %lu page_id %lu prp_trans %u\n", addr, req->page_id[wp], prp_trans - 1);

                nvme_addr_read(n, addr + sizeof(uint64_t) * (n->max_prp_ents - 1), &addr, sizeof(uint64_t));
                prp_trans--;

                for (int i = wp; i < wp + prp_trans; i++) {
                    if (req->need_pin[i])
                        assert(req->page_id[i] != PG_CC_INVALID_PAGE_ID);
                    else 
                        assert(req->page_id[i] == PG_CC_INVALID_PAGE_ID);
                }

                /* Note that SPDK does not support such prp mapping method */
                assert(0);
            }

            len -= MIN(prp_trans * n->page_size, len);
            wp += prp_trans;
        }
    }

    return NVME_SUCCESS;
}

uint16_t nvme_map_prp(QEMUSGList *qsg, QEMUIOVector *iov, uint64_t prp1,
                      uint64_t prp2, uint32_t len, FemuCtrl *n)
{
    hwaddr trans_len = n->page_size - (prp1 % n->page_size);
    trans_len = MIN(len, trans_len);
    int num_prps = (len >> n->page_bits) + 1;
    bool cmb = false;

    if (!prp1) {
        return NVME_INVALID_FIELD | NVME_DNR;
    } else if (n->cmbsz && prp1 >= n->ctrl_mem.addr &&
               prp1 < n->ctrl_mem.addr + int128_get64(n->ctrl_mem.size)) {
        cmb = true;
        qsg->nsg = 0;
        qemu_iovec_init(iov, num_prps);
        qemu_iovec_add(iov, (void *)&n->cmbuf[prp1-n->ctrl_mem.addr], trans_len);
    } else {
        pci_dma_sglist_init(qsg, &n->parent_obj, num_prps);
        qemu_sglist_add(qsg, prp1, trans_len);
    }

    len -= trans_len;
    if (len) {
        if (!prp2) {
            goto unmap;
        }
        if (len > n->page_size) {
            uint64_t prp_list[n->max_prp_ents];
            uint32_t nents, prp_trans;
            int i = 0;

            nents = (len + n->page_size - 1) >> n->page_bits;
            prp_trans = MIN(n->max_prp_ents, nents) * sizeof(uint64_t);
            nvme_addr_read(n, prp2, (void *)prp_list, prp_trans);
            while (len != 0) {
                uint64_t prp_ent = le64_to_cpu(prp_list[i]);

                if (i == n->max_prp_ents - 1 && len > n->page_size) {
                    if (!prp_ent || prp_ent & (n->page_size - 1)) {
                        goto unmap;
                    }

                    i = 0;
                    nents = (len + n->page_size - 1) >> n->page_bits;
                    prp_trans = MIN(n->max_prp_ents, nents) * sizeof(uint64_t);
                    nvme_addr_read(n, prp_ent, (void *)prp_list,
                                   prp_trans);
                    prp_ent = le64_to_cpu(prp_list[i]);
                }

                if (!prp_ent || prp_ent & (n->page_size - 1)) {
                    goto unmap;
                }

                trans_len = MIN(len, n->page_size);
                if (!cmb){
                    qemu_sglist_add(qsg, prp_ent, trans_len);
                } else {
                    uint64_t off = prp_ent - n->ctrl_mem.addr;
                    qemu_iovec_add(iov, (void *)&n->cmbuf[off], trans_len);
                }
                len -= trans_len;
                i++;
            }
        } else {
            if (prp2 & (n->page_size - 1)) {
                goto unmap;
            }
            if (!cmb) {
                qemu_sglist_add(qsg, prp2, len);
            } else {
                uint64_t off = prp2 - n->ctrl_mem.addr;
                qemu_iovec_add(iov, (void *)&n->cmbuf[off], trans_len);
            }
        }
    }

    return NVME_SUCCESS;

unmap:
    if (!cmb) {
        qemu_sglist_destroy(qsg);
    } else {
        qemu_iovec_destroy(iov);
    }

    return NVME_INVALID_FIELD | NVME_DNR;
}

uint16_t dma_write_prp(FemuCtrl *n, uint8_t *ptr, uint32_t len, uint64_t prp1,
                       uint64_t prp2)
{
    QEMUSGList qsg;
    QEMUIOVector iov;
    uint16_t status = NVME_SUCCESS;

    if (nvme_map_prp(&qsg, &iov, prp1, prp2, len, n)) {
        return NVME_INVALID_FIELD | NVME_DNR;
    }
    if (qsg.nsg > 0) {
        if (dma_buf_write(ptr, len, NULL, &qsg, MEMTXATTRS_UNSPECIFIED)) {
            status = NVME_INVALID_FIELD | NVME_DNR;
        }
        qemu_sglist_destroy(&qsg);
    } else {
        if (qemu_iovec_from_buf(&iov, 0, ptr, len) != len) {
            status = NVME_INVALID_FIELD | NVME_DNR;
        }
        qemu_iovec_destroy(&iov);
    }

    return status;
}

uint16_t dma_read_prp(FemuCtrl *n, uint8_t *ptr, uint32_t len, uint64_t prp1,
                      uint64_t prp2)
{
    QEMUSGList qsg;
    QEMUIOVector iov;
    uint16_t status = NVME_SUCCESS;

    if (nvme_map_prp(&qsg, &iov, prp1, prp2, len, n)) {
        return NVME_INVALID_FIELD | NVME_DNR;
    }
    if (qsg.nsg > 0) {
        if (dma_buf_read(ptr, len, NULL, &qsg, MEMTXATTRS_UNSPECIFIED)) {
            status = NVME_INVALID_FIELD | NVME_DNR;
        }
        qemu_sglist_destroy(&qsg);
    } else {
        if (qemu_iovec_to_buf(&iov, 0, ptr, len) != len) {
            status = NVME_INVALID_FIELD | NVME_DNR;
        }
        qemu_iovec_destroy(&iov);
    }

    return status;
}
