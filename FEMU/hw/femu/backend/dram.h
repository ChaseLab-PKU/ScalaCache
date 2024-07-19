#ifndef __FEMU_MEM_BACKEND
#define __FEMU_MEM_BACKEND

#include <stdint.h>

/* DRAM backend SSD address space */
typedef struct SsdDramBackend {
    void    *logical_space;
    int64_t size; /* in bytes */
    int     femu_mode;
} SsdDramBackend;

typedef struct pg_cc_dram_rw_context {
    AddressSpace *as;
    uint64_t *page_addr;
    uint64_t page_num;
    uint64_t start_addr;
    bool is_write;
    bool *need_cp;
} pg_cc_dram_rw_context;

int init_dram_backend(SsdDramBackend **mbe, int64_t nbytes);
void free_dram_backend(SsdDramBackend *);

int backend_rw(SsdDramBackend *, QEMUSGList *, uint64_t *, bool);
void backend_pg_cc_rw(SsdDramBackend *b, pg_cc_dram_rw_context *ctx);

#endif
