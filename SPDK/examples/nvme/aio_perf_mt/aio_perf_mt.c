/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2015 Intel Corporation.
 *   All rights reserved.
 *
 *   Copyright (c) 2019-2021 Mellanox Technologies LTD. All rights reserved.
 *   Copyright (c) 2021, 2022 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

#include "spdk/stdinc.h"

#include "spdk/env.h"
#include "spdk/fd.h"
#include "spdk/nvme.h"
#include "spdk/vmd.h"
#include "spdk/queue.h"
#include "spdk/string.h"
#include "spdk/nvme_intel.h"
#include "spdk/histogram_data.h"
#include "spdk/endian.h"
#include "spdk/dif.h"
#include "spdk/util.h"
#include "spdk/log.h"
#include "spdk/likely.h"
#include "spdk/sock.h"
#include "spdk/zipf.h"

#include <libaio.h>
#include <sys/time.h>

struct ctrlr_entry {
	struct spdk_nvme_ctrlr			*ctrlr;
	enum spdk_nvme_transport_type		trtype;
	struct spdk_nvme_intel_rw_latency_page	*latency_page;

	struct spdk_nvme_qpair			**unused_qpairs;

	TAILQ_ENTRY(ctrlr_entry)		link;
	char					name[1024];
};

enum entry_type {
	ENTRY_TYPE_NVME_NS,
	ENTRY_TYPE_AIO_FILE,
	ENTRY_TYPE_URING_FILE,
};

struct ns_fn_table;

struct ns_entry {
	enum entry_type		type;
	const struct ns_fn_table	*fn_table;

	union {
		struct {
			int                     fd;
		} aio;
	} u;

	TAILQ_ENTRY(ns_entry)	link;
	uint32_t		io_size_blocks;
	uint32_t		num_io_requests;
	uint64_t		size_in_ios;
	uint32_t		block_size;
	uint32_t		md_size;
	bool			md_interleave;
	unsigned int		seed;
	struct spdk_zipf	*zipf;
	bool			pi_loc;
	enum spdk_nvme_pi_type	pi_type;
	uint32_t		io_flags;
	char			name[1024];
};

static const double g_latency_cutoffs[] = {
	0.01,
	0.10,
	0.25,
	0.50,
	0.75,
	0.90,
	0.95,
	0.98,
	0.99,
	0.995,
	0.999,
	0.9999,
	0.99999,
	0.999999,
	0.9999999,
	-1,
};

struct ns_worker_stats {
	uint64_t		io_submitted;
	uint64_t		io_completed;
	uint64_t    io_lba_completed;
	uint64_t		last_io_completed;
	uint64_t    last_io_lba_completed;
	double		total_tsc;
	double		min_tsc;
	double		max_tsc;
	double		last_tsc;
	double		busy_tsc;
	double		idle_tsc;
	double		last_busy_tsc;
	double		last_idle_tsc;
	double		rw_max_tsc[2];
	double		rw_min_tsc[2];
	double		rw_total_tsc[2];
	uint64_t		rw_io_completed[2];
	uint64_t		rw_io_lba_completed[2];
};

struct ns_worker_ctx {
	struct ns_entry		*entry;
	struct ns_worker_stats	stats;
	uint64_t		current_queue_depth;
	uint64_t		offset_in_ios;
	bool			is_draining;
	uint32_t    worker_id;

	union {
		struct {
			struct io_event		*events;
			io_context_t		ctx;
		} aio;
	} u;

	TAILQ_ENTRY(ns_worker_ctx)	link;

	struct spdk_histogram_data	*histogram;
};

enum {
	PERF_IO_READ	= 0,
	PERF_IO_WRITE	= 1,
};

struct perf_task {
	struct ns_worker_ctx	*ns_ctx;
	struct iovec		*iovs; /* array of iovecs to transfer. */
	int			iovcnt; /* Number of iovecs in iovs array. */
	int			iovpos; /* Current iovec position. */
	uint32_t		iov_offset; /* Offset in current iovec. */
	struct iovec		md_iov;
	struct timespec		submit_tsc;
	bool			is_read;
	uint64_t    start_lba;
	uint64_t    lba_count;
	struct spdk_dif_ctx	dif_ctx;
  struct iocb		iocb;
};

struct perf_task_context {
	struct perf_task	*task;
	uint64_t lba;
	uint64_t io_size_blocks;
};

struct worker_thread {
	TAILQ_HEAD(, ns_worker_ctx)	ns_ctx;
	TAILQ_ENTRY(worker_thread)	link;
	unsigned			lcore;
	uint32_t  		id;
};

struct ns_fn_table {
	void	(*setup_payload)(struct perf_task *task, uint8_t pattern);

	int	(*submit_io)(struct perf_task_context *task_context, struct ns_worker_ctx *ns_ctx,
			     struct ns_entry *entry);

	int64_t	(*check_io)(struct ns_worker_ctx *ns_ctx);

	void	(*verify_io)(struct perf_task *task, struct ns_entry *entry);

	int	(*init_ns_worker_ctx)(struct ns_worker_ctx *ns_ctx);

	void	(*cleanup_ns_worker_ctx)(struct ns_worker_ctx *ns_ctx);
	void	(*dump_transport_stats)(uint32_t lcore, struct ns_worker_ctx *ns_ctx);
};

static uint32_t g_io_unit_size = (UINT32_MAX & (~0x03));

static int g_outstanding_commands;

static bool g_latency_ssd_tracking_enable;
static int g_latency_sw_tracking_level;

static bool g_vmd;
static const char *g_workload_type;
static TAILQ_HEAD(, ctrlr_entry) g_controllers = TAILQ_HEAD_INITIALIZER(g_controllers);
static TAILQ_HEAD(, ns_entry) g_namespaces = TAILQ_HEAD_INITIALIZER(g_namespaces);
static int g_num_namespaces;
static TAILQ_HEAD(, worker_thread) g_workers = TAILQ_HEAD_INITIALIZER(g_workers);
static int g_num_workers = 0;
static uint32_t g_main_core;
static pthread_barrier_t g_worker_sync_barrier;

static uint64_t g_print_period_in_ticks;

static struct timespec g_tsc_start;

static bool g_monitor_perf_cores = false;

static uint32_t g_io_align = 0x200;
static bool g_io_align_specified;
static uint32_t g_io_size_bytes;
static uint32_t g_max_trace_io_size;
static uint32_t g_metacfg_pract_flag;
static uint32_t g_metacfg_prchk_flags;
static int g_rw_percentage = -1;
static int g_is_random;
static uint32_t g_queue_depth;
static int g_nr_io_queues_per_ns = 1;
static int g_nr_unused_io_queues;
static uint64_t g_number_ios;
static uint64_t g_elapsed_time_in_usec;
static int g_warmup_time_in_sec;
static uint32_t g_max_completions;
static uint32_t g_disable_sq_cmb;
static bool g_use_uring;
static bool g_warn;
static bool g_header_digest;
static bool g_host_mem_buf;
static bool g_data_digest;
static bool g_no_shn_notification;
static bool g_mix_specified;
/* The flag is used to exit the program while keep alive fails on the transport */
static bool g_exit;
/* Default to 10 seconds for the keep alive value. This value is arbitrary. */
static uint32_t g_keep_alive_timeout_in_ms = 10000;
static uint32_t g_quiet_count = 1;
static double g_zipf_theta;
/* Set default io_queue_size to UINT16_MAX, NVMe driver will then reduce this
 * to MQES to maximize the io_queue_size as much as possible.
 */
static uint32_t g_io_queue_size = UINT16_MAX;

static uint32_t g_sock_zcopy_threshold;
static char *g_sock_threshold_impl;

static uint8_t g_transport_tos = 0;

static char *g_io_trace_path;
static uint64_t g_trace_line = 0;
static bool g_enable_io_trace = false;
static int g_core_mask = 0;

struct trace_entry {
	uint64_t start_lba;
	uint64_t lba_count;
	uint32_t pid;
	bool is_read;
};

static struct trace_entry *g_trace_entry = NULL;

/* When user specifies -Q, some error messages are rate limited.  When rate
 * limited, we only print the error message every g_quiet_count times the
 * error occurs.
 *
 * Note: the __count is not thread safe, meaning the rate limiting will not
 * be exact when running perf with multiple thread with lots of errors.
 * Thread-local __count would mean rate-limiting per thread which doesn't
 * seem as useful.
 */
#define RATELIMIT_LOG(...) \
	{								\
		static uint64_t __count = 0;				\
		if ((__count % g_quiet_count) == 0) {			\
			if (__count > 0 && g_quiet_count > 1) {		\
				fprintf(stderr, "Message suppressed %" PRIu32 " times: ",	\
					g_quiet_count - 1);		\
			}						\
			fprintf(stderr, __VA_ARGS__);			\
		}							\
		__count++;						\
	}

static bool g_dump_transport_stats;
static pthread_mutex_t g_stats_mutex;

#define MAX_ALLOWED_PCI_DEVICE_NUM 128
static struct spdk_pci_addr g_allowed_pci_addr[MAX_ALLOWED_PCI_DEVICE_NUM];

struct trid_entry {
	struct spdk_nvme_transport_id	trid;
	uint16_t			nsid;
	char				hostnqn[SPDK_NVMF_NQN_MAX_LEN + 1];
	TAILQ_ENTRY(trid_entry)		tailq;
};

static TAILQ_HEAD(, trid_entry) g_trid_list = TAILQ_HEAD_INITIALIZER(g_trid_list);

static int g_file_optind; /* Index of first filename in argv */

static inline void task_complete(struct perf_task_context *task_context);

static double diff_time(struct timespec start) {
	struct timespec end;
	double diff;

	int rc = clock_gettime(CLOCK_MONOTONIC, &end);
	if (rc != 0) {
		perror("clock_gettime in diff_time");
		exit(1);
	}
	diff = (end.tv_sec - start.tv_sec) * 1000000.0 + (end.tv_nsec - start.tv_nsec) / 1000.0;

	return diff;
}

static void
perf_set_sock_opts(const char *impl_name, const char *field, uint32_t val, const char *valstr)
{
	struct spdk_sock_impl_opts sock_opts = {};
	size_t opts_size = sizeof(sock_opts);
	int rc;

	rc = spdk_sock_impl_get_opts(impl_name, &sock_opts, &opts_size);
	if (rc != 0) {
		if (errno == EINVAL) {
			fprintf(stderr, "Unknown sock impl %s\n", impl_name);
		} else {
			fprintf(stderr, "Failed to get opts for sock impl %s: error %d (%s)\n", impl_name, errno,
				strerror(errno));
		}
		return;
	}

	if (opts_size != sizeof(sock_opts)) {
		fprintf(stderr, "Warning: sock_opts size mismatch. Expected %zu, received %zu\n",
			sizeof(sock_opts), opts_size);
		opts_size = sizeof(sock_opts);
	}

	if (!field) {
		fprintf(stderr, "Warning: no socket opts field specified\n");
		return;
	} else if (strcmp(field, "enable_zerocopy_send_client") == 0) {
		sock_opts.enable_zerocopy_send_client = val;
	} else if (strcmp(field, "tls_version") == 0) {
		sock_opts.tls_version = val;
	} else if (strcmp(field, "ktls") == 0) {
		sock_opts.enable_ktls = val;
	} else if (strcmp(field, "psk_key") == 0) {
		if (!valstr) {
			fprintf(stderr, "No socket opts value specified\n");
			return;
		}
		sock_opts.psk_key = strdup(valstr);
		if (sock_opts.psk_key == NULL) {
			fprintf(stderr, "Failed to allocate psk_key in sock_impl\n");
			return;
		}
	} else if (strcmp(field, "psk_identity") == 0) {
		if (!valstr) {
			fprintf(stderr, "No socket opts value specified\n");
			return;
		}
		sock_opts.psk_identity = strdup(valstr);
		if (sock_opts.psk_identity == NULL) {
			fprintf(stderr, "Failed to allocate psk_identity in sock_impl\n");
			return;
		}
	} else if (strcmp(field, "zerocopy_threshold") == 0) {
		sock_opts.zerocopy_threshold = val;
	} else {
		fprintf(stderr, "Warning: invalid or unprocessed socket opts field: %s\n", field);
		return;
	}

	if (spdk_sock_impl_set_opts(impl_name, &sock_opts, opts_size)) {
		fprintf(stderr, "Failed to set %s: %d for sock impl %s : error %d (%s)\n", field, val, impl_name,
			errno, strerror(errno));
	}
}

static void
aio_setup_payload(struct perf_task *task, uint8_t pattern)
{
	uint32_t max_io_size_bytes;
	int rc;

  struct iovec *iov;

	task->iovs = calloc(1, sizeof(struct iovec));
	if (!task->iovs) {
		fprintf(stderr, "perf task failed to allocate iovs\n");
		exit(1);
	}
	task->iovcnt = 1;

  iov = &task->iovs[0];
	/* maximum extended lba format size from all active namespace,
	 * it's same with g_io_size_bytes for namespace without metadata.
	 */
	assert(g_enable_io_trace);
	max_io_size_bytes = g_max_trace_io_size * 512;
	rc = posix_memalign((void **)&iov->iov_base, 4096, max_io_size_bytes);
  iov->iov_len = max_io_size_bytes;
	if (rc != 0) {
		fprintf(stderr, "task->buf spdk_dma_zmalloc failed\n");
		exit(1);
	}
	memset(iov->iov_base, pattern, max_io_size_bytes);
}

static int
aio_submit(io_context_t aio_ctx, struct iocb *iocb, int fd, enum io_iocb_cmd cmd,
	   struct iovec *iov, uint64_t offset, uint64_t length, void *cb_ctx)
{
	iocb->aio_fildes = fd;
	iocb->aio_reqprio = 0;
	iocb->aio_lio_opcode = cmd;
	iocb->u.c.buf = iov->iov_base;
	iocb->u.c.nbytes = length;
	iocb->u.c.offset = offset;
	iocb->data = cb_ctx;

	if (io_submit(aio_ctx, 1, &iocb) < 0) {
		printf("io_submit");
		return -1;
	}

	return 0;
}

static int
aio_submit_io(struct perf_task_context *task_context, struct ns_worker_ctx *ns_ctx,
	       struct ns_entry *entry)
{
	uint64_t start_lba, lba_count;
	struct perf_task *task = task_context->task;

	start_lba = task->start_lba;
	lba_count = task->lba_count;

	task_context->lba = start_lba;
	task_context->io_size_blocks = lba_count;

	//printf("submit io %s, offset %lu length %lu\n", task->is_read ? "read" : "write", task_context->lba * 512, task_context->io_size_blocks * 512);
	

	if (task->is_read) {
		return aio_submit(ns_ctx->u.aio.ctx, &task->iocb, entry->u.aio.fd, IO_CMD_PREAD,
				  task->iovs, task_context->lba * 512, task_context->io_size_blocks * 512, task_context);
	} else {
		return aio_submit(ns_ctx->u.aio.ctx, &task->iocb, entry->u.aio.fd, IO_CMD_PWRITE,
				  task->iovs, task_context->lba * 512, task_context->io_size_blocks * 512, task_context);	
	}
}

static int64_t
aio_check_io(struct ns_worker_ctx *ns_ctx)
{
	int count, i;
	struct timespec timeout;

	timeout.tv_sec = 0;
	timeout.tv_nsec = 0;

	count = io_getevents(ns_ctx->u.aio.ctx, 1, g_queue_depth, ns_ctx->u.aio.events, &timeout);
	if (count < 0) {
		fprintf(stderr, "io_getevents error\n");
		exit(1);
	}

	for (i = 0; i < count; i++) {
		task_complete(ns_ctx->u.aio.events[i].data);
	}
	return count;
}

static void
aio_verify_io(struct perf_task *task, struct ns_entry *entry)
{
}

static int
aio_init_ns_worker_ctx(struct ns_worker_ctx *ns_ctx)
{
	ns_ctx->u.aio.events = calloc(g_queue_depth, sizeof(struct io_event));
	if (!ns_ctx->u.aio.events) {
		return -1;
	}
	ns_ctx->u.aio.ctx = 0;
	if (io_setup(g_queue_depth, &ns_ctx->u.aio.ctx) < 0) {
		free(ns_ctx->u.aio.events);
		perror("io_setup");
		return -1;
	}
	return 0;
}

static void
aio_cleanup_ns_worker_ctx(struct ns_worker_ctx *ns_ctx)
{
	// io_destroy(ns_ctx->u.aio.ctx);
	// free(ns_ctx->u.aio.events);
}

static const struct ns_fn_table aio_fn_table = {
	.setup_payload		= aio_setup_payload,
	.submit_io		= aio_submit_io,
	.check_io		= aio_check_io,
	.verify_io		= aio_verify_io,
	.init_ns_worker_ctx	= aio_init_ns_worker_ctx,
	.cleanup_ns_worker_ctx	= aio_cleanup_ns_worker_ctx,
};

static int
build_nvme_name(char *name, size_t length, struct spdk_nvme_ctrlr *ctrlr)
{
	const struct spdk_nvme_transport_id *trid;
	int res = 0;

	trid = spdk_nvme_ctrlr_get_transport_id(ctrlr);

	switch (trid->trtype) {
	case SPDK_NVME_TRANSPORT_PCIE:
		res = snprintf(name, length, "PCIE (%s)", trid->traddr);
		break;
	case SPDK_NVME_TRANSPORT_RDMA:
		res = snprintf(name, length, "RDMA (addr:%s subnqn:%s)", trid->traddr, trid->subnqn);
		break;
	case SPDK_NVME_TRANSPORT_TCP:
		res = snprintf(name, length, "TCP (addr:%s subnqn:%s)", trid->traddr, trid->subnqn);
		break;
	case SPDK_NVME_TRANSPORT_VFIOUSER:
		res = snprintf(name, length, "VFIOUSER (%s)", trid->traddr);
		break;
	case SPDK_NVME_TRANSPORT_CUSTOM:
		res = snprintf(name, length, "CUSTOM (%s)", trid->traddr);
		break;

	default:
		fprintf(stderr, "Unknown transport type %d\n", trid->trtype);
		break;
	}
	return res;
}

static void
build_nvme_ns_name(char *name, size_t length, struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid)
{
	int res = 0;

	res = build_nvme_name(name, length, ctrlr);
	if (res > 0) {
		snprintf(name + res, length - res, " NSID %u", nsid);
	}

}

static int
register_file(const char *path)
{
	struct ns_entry *entry;

	int flags, fd;
	uint64_t size;
	uint32_t blklen;

	flags = O_RDWR;
	
	// | O_DIRECT;

	fd = open(path, flags);
	if (fd < 0) {
		fprintf(stderr, "Could not open device %s: %s\n", path, strerror(errno));
		return -1;
	}

	size = spdk_fd_get_size(fd);
	if (size == 0) {
		fprintf(stderr, "Could not determine size of device %s\n", path);
		close(fd);
		return -1;
	}

	printf("Registering AIO device %s: size %" PRIu64 "Bytes\n", path, size);

	entry = malloc(sizeof(struct ns_entry));
	if (entry == NULL) {
		close(fd);
		perror("ns_entry malloc");
		return -1;
	}

	blklen = spdk_fd_get_blocklen(fd);
	if (blklen == 0) {
		fprintf(stderr, "Could not determine block size of device %s\n", path);
		close(fd);
		return -1;
	}

	entry->type = ENTRY_TYPE_AIO_FILE;
	entry->fn_table = &aio_fn_table;
	entry->u.aio.fd = fd;
	entry->size_in_ios = size / 512;
	entry->io_size_blocks = g_io_size_bytes / blklen;

	printf("Registering AIO device %s: entry->size_in_ios %" PRIu64 "Sector\n",
	       path, entry->size_in_ios);

	if (g_is_random) {
		entry->seed = rand();
		if (g_zipf_theta > 0) {
			entry->zipf = spdk_zipf_create(entry->size_in_ios, g_zipf_theta, 0);
		}
	}

	snprintf(entry->name, sizeof(entry->name), "%s", path);

	g_num_namespaces++;
	TAILQ_INSERT_TAIL(&g_namespaces, entry, link);

  return 0;
}

static int
register_files(int argc, char **argv)
{
	int i;

	/* Treat everything after the options as files for AIO/URING */
	for (i = g_file_optind; i < argc; i++) {
		printf("Registering file %s\n", argv[i]);
		if (register_file(argv[i]) != 0) {
			return 1;
		}
	}

	return 0;
}

static void
unregister_namespaces(void)
{
	struct ns_entry *entry, *tmp;

	TAILQ_FOREACH_SAFE(entry, &g_namespaces, link, tmp) {
		TAILQ_REMOVE(&g_namespaces, entry, link);
		spdk_zipf_free(&entry->zipf);
		if (g_use_uring) {
      close(entry->u.aio.fd);
		}
		free(entry);
	}
}

static void
enable_latency_tracking_complete(void *cb_arg, const struct spdk_nvme_cpl *cpl)
{
	if (spdk_nvme_cpl_is_error(cpl)) {
		printf("enable_latency_tracking_complete failed\n");
	}
	g_outstanding_commands--;
}

static void
set_latency_tracking_feature(struct spdk_nvme_ctrlr *ctrlr, bool enable)
{
	int res;
	union spdk_nvme_intel_feat_latency_tracking latency_tracking;

	if (enable) {
		latency_tracking.bits.enable = 0x01;
	} else {
		latency_tracking.bits.enable = 0x00;
	}

	res = spdk_nvme_ctrlr_cmd_set_feature(ctrlr, SPDK_NVME_INTEL_FEAT_LATENCY_TRACKING,
					      latency_tracking.raw, 0, NULL, 0, enable_latency_tracking_complete, NULL);
	if (res) {
		printf("fail to allocate nvme request.\n");
		return;
	}
	g_outstanding_commands++;

	while (g_outstanding_commands) {
		spdk_nvme_ctrlr_process_admin_completions(ctrlr);
	}
}

static inline void
submit_single_io(struct perf_task_context *task_context)
{
	// TODO: modify to use trace
	int			rc;
	struct perf_task *task = task_context->task;
	struct ns_worker_ctx	*ns_ctx = task->ns_ctx;
	struct ns_entry		*entry = ns_ctx->entry;

	assert(!ns_ctx->is_draining);

	assert(g_enable_io_trace);
	bool found = false;

	uint64_t start_lba, lba_count;
	bool is_read;

	while (ns_ctx->offset_in_ios < g_trace_line) {
		if (g_trace_entry[ns_ctx->offset_in_ios].pid % g_num_workers == ns_ctx->worker_id) {
			found = true;
			start_lba = g_trace_entry[ns_ctx->offset_in_ios].start_lba;
			lba_count = g_trace_entry[ns_ctx->offset_in_ios].lba_count;
			is_read = g_trace_entry[ns_ctx->offset_in_ios].is_read;
			ns_ctx->offset_in_ios++;
			break;
		}
		ns_ctx->offset_in_ios++;
	}

	if (!found) {
		ns_ctx->is_draining = true;
		return;
	}
	
	rc = clock_gettime(CLOCK_MONOTONIC, &task->submit_tsc);
	if (rc != 0) {
		perror("clock_gettime");
		exit(1);
	}
	task->is_read = is_read;
	task->start_lba = start_lba % entry->size_in_ios;
	task->lba_count = lba_count;
	task->lba_count = spdk_min(task->lba_count, entry->size_in_ios - task->start_lba);
	assert(task->start_lba + task->lba_count <= entry->size_in_ios);
	assert(task->lba_count > 0);

	// printf("submitting IO: start_lba %" PRIu64 ", lba_count %" PRIu64 "\n",
	//       task->start_lba, task->lba_count);

	rc = entry->fn_table->submit_io(task_context, ns_ctx, entry);

	if (spdk_unlikely(rc != 0)) {
		RATELIMIT_LOG("starting I/O failed\n");
		free(task);
	} else {
		ns_ctx->current_queue_depth++;
		ns_ctx->stats.io_submitted++;
	}
}

static inline void
task_complete(struct perf_task_context *task_context)
{
	struct perf_task *task = task_context->task;
	struct ns_worker_ctx	*ns_ctx;
	double		tsc_diff;
	struct ns_entry		*entry;
	int op = task->is_read ? PERF_IO_READ : PERF_IO_WRITE;

	ns_ctx = task->ns_ctx;
	entry = ns_ctx->entry;
	ns_ctx->current_queue_depth--;
	ns_ctx->stats.io_completed++;
	ns_ctx->stats.io_lba_completed += task->lba_count;
	tsc_diff = diff_time(task->submit_tsc);
	ns_ctx->stats.total_tsc += tsc_diff;
	if (spdk_unlikely(ns_ctx->stats.min_tsc > tsc_diff)) {
		ns_ctx->stats.min_tsc = tsc_diff;
	}
	if (spdk_unlikely(ns_ctx->stats.max_tsc < tsc_diff)) {
		ns_ctx->stats.max_tsc = tsc_diff;
	}

	if (spdk_unlikely(ns_ctx->stats.rw_max_tsc[op] < tsc_diff)) {
		ns_ctx->stats.rw_max_tsc[op] = tsc_diff;
	}
	if (spdk_unlikely(ns_ctx->stats.rw_min_tsc[op] > tsc_diff)) {
		ns_ctx->stats.rw_min_tsc[op] = tsc_diff;
	}
	ns_ctx->stats.rw_total_tsc[op] += tsc_diff;
	ns_ctx->stats.rw_io_completed[op]++;
	ns_ctx->stats.rw_io_lba_completed[op] += task->lba_count;

	if (spdk_unlikely(g_latency_sw_tracking_level > 0)) {
		spdk_histogram_data_tally(ns_ctx->histogram, tsc_diff);
	}

	if (spdk_unlikely(entry->md_size > 0)) {
		/* add application level verification for end-to-end data protection */
		entry->fn_table->verify_io(task, entry);
	}

	/*
	 * is_draining indicates when time has expired or io_submitted exceeded
	 * g_number_ios for the test run and we are just waiting for the previously
	 * submitted I/O to complete. In this case, do not submit a new I/O to
	 * replace the one just completed.
	 */
	if (spdk_unlikely(ns_ctx->is_draining)) {
    printf("Error: task_complete: ns_ctx->is_draining\n");
		/* TODO: memory leak */
		// free(task);
	} else {
		submit_single_io(task_context);
	}
}

static struct perf_task *
allocate_task(struct ns_worker_ctx *ns_ctx, int queue_depth)
{
	struct perf_task *task;

	task = calloc(1, sizeof(*task));
	if (task == NULL) {
		fprintf(stderr, "Out of memory allocating tasks\n");
		exit(1);
	}

	ns_ctx->entry->fn_table->setup_payload(task, queue_depth % 8 + 1);

	task->ns_ctx = ns_ctx;

	return task;
}

static struct perf_task_context *
allocate_task_cpl_context(struct perf_task *task)
{
	struct perf_task_context *task_cpl_context;

	task_cpl_context = calloc(1, sizeof(*task_cpl_context));
	if (task_cpl_context == NULL) {
		fprintf(stderr, "Out of memory allocating task completion context\n");
		exit(1);
	}

	task_cpl_context->task = task;

	return task_cpl_context;
}

static void
submit_io(struct ns_worker_ctx *ns_ctx, int queue_depth)
{
	struct perf_task *task;
	struct perf_task_context *task_cpl_context;

	while (queue_depth-- > 0) {
		task = allocate_task(ns_ctx, queue_depth);
		task_cpl_context = allocate_task_cpl_context(task);
		submit_single_io(task_cpl_context);
	}
}

static int
init_ns_worker_ctx(struct ns_worker_ctx *ns_ctx)
{
	return ns_ctx->entry->fn_table->init_ns_worker_ctx(ns_ctx);
}

static void
cleanup_ns_worker_ctx(struct ns_worker_ctx *ns_ctx)
{
	ns_ctx->entry->fn_table->cleanup_ns_worker_ctx(ns_ctx);
}

static void
perf_dump_transport_statistics(struct worker_thread *worker)
{
	struct ns_worker_ctx *ns_ctx;

	TAILQ_FOREACH(ns_ctx, &worker->ns_ctx, link) {
		if (ns_ctx->entry->fn_table->dump_transport_stats) {
			ns_ctx->entry->fn_table->dump_transport_stats(worker->lcore, ns_ctx);
		}
	}
}

static void *
work_fn(void *arg)
{
	struct timespec tsc_current;
	struct worker_thread *worker = (struct worker_thread *) arg;
	struct ns_worker_ctx *ns_ctx = NULL;
	uint32_t unfinished_ns_ctx;
	int rc;
	int64_t check_rc;
	uint64_t check_now;

	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET(worker->lcore, &mask);
	rc = pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask);
	if (rc != 0) {
		printf("pthread_setaffinity_np() failed for core %u\n", worker->lcore);
		exit(1);
	}

	/* Allocate queue pairs for each namespace. */
	TAILQ_FOREACH(ns_ctx, &worker->ns_ctx, link) {
		if (init_ns_worker_ctx(ns_ctx) != 0) {
			printf("ERROR: init_ns_worker_ctx() failed\n");
			/* Wait on barrier to avoid blocking of successful workers */
			pthread_barrier_wait(&g_worker_sync_barrier);
			return NULL;
		}
	}

	rc = pthread_barrier_wait(&g_worker_sync_barrier);
	if (rc != 0 && rc != PTHREAD_BARRIER_SERIAL_THREAD) {
		printf("ERROR: failed to wait on thread sync barrier\n");
		return NULL;
	}

	if (worker->lcore == g_main_core) {
		rc = clock_gettime(CLOCK_MONOTONIC, &g_tsc_start);
		if (rc != 0) {
			printf("ERROR: clock_gettime() failed\n");
			return NULL;
		}
	}

	rc = clock_gettime(CLOCK_MONOTONIC, &tsc_current);
	if (rc != 0) {
		printf("ERROR: clock_gettime() failed\n");
		return NULL;
	}

	printf("Starting workers on core %u\n", worker->lcore);

	/* Submit initial I/O for each namespace. */
	TAILQ_FOREACH(ns_ctx, &worker->ns_ctx, link) {
		submit_io(ns_ctx, g_queue_depth);
	}

	while (spdk_likely(!g_exit)) {
		bool all_draining = true;

		/*
		 * Check for completed I/O for each controller. A new
		 * I/O will be submitted in the io_complete callback
		 * to replace each I/O that is completed.
		 */
		TAILQ_FOREACH(ns_ctx, &worker->ns_ctx, link) {
			check_now = spdk_get_ticks();
			check_rc = ns_ctx->entry->fn_table->check_io(ns_ctx);

			if (check_rc > 0) {
				ns_ctx->stats.busy_tsc += check_now - ns_ctx->stats.last_tsc;
			} else {
				ns_ctx->stats.idle_tsc += check_now - ns_ctx->stats.last_tsc;
			}
			ns_ctx->stats.last_tsc = check_now;

			if (!ns_ctx->is_draining) {
				all_draining = false;
			}
		}

		if (spdk_unlikely(all_draining)) {
			break;
		}

		rc = clock_gettime(CLOCK_MONOTONIC, &tsc_current);
		if (rc != 0) {
			printf("ERROR: clock_gettime() failed\n");
			return NULL;
		}
	}

	/* Capture the actual elapsed time when we break out of the main loop. This will account
	 * for cases where we exit prematurely due to a signal. We only need to capture it on
	 * one core, so use the main core.
	 */

	if (g_dump_transport_stats) {
		pthread_mutex_lock(&g_stats_mutex);
		perf_dump_transport_statistics(worker);
		pthread_mutex_unlock(&g_stats_mutex);
	}

	/* drain the io of each ns_ctx in round robin to make the fairness */
	do {
		unfinished_ns_ctx = 0;
		TAILQ_FOREACH(ns_ctx, &worker->ns_ctx, link) {
			/* first time will enter into this if case */
			if (!ns_ctx->is_draining) {
				ns_ctx->is_draining = true;
			}

			if (ns_ctx->current_queue_depth > 0) {
				ns_ctx->entry->fn_table->check_io(ns_ctx);
				if (ns_ctx->current_queue_depth > 0) {
					unfinished_ns_ctx++;
				}
			}
		}
	} while (unfinished_ns_ctx > 0);

	printf("Begin to cleanup ns_ctx\n");

	TAILQ_FOREACH(ns_ctx, &worker->ns_ctx, link) {
		cleanup_ns_worker_ctx(ns_ctx);
	}

	return NULL;
}

static void
usage(char *program_name)
{
	printf("%s options", program_name);
	printf("\n");
	printf("\t[-b, --allowed-pci-addr <addr> allowed local PCIe device address]\n");
	printf("\t Example: -b 0000:d8:00.0 -b 0000:d9:00.0\n");
	printf("\t[-q, --io-depth <val> io depth]\n");
	printf("\t[-o, --io-size <val> io size in bytes]\n");
	printf("\t[-O, --io-unit-size io unit size in bytes (4-byte aligned) for SPDK driver. default: same as io size]\n");
	printf("\t[-P, --num-qpairs <val> number of io queues per namespace. default: 1]\n");
	printf("\t[-U, --num-unused-qpairs <val> number of unused io queues per controller. default: 0]\n");
	printf("\t[-w, --io-pattern <pattern> io pattern type, must be one of\n");
	printf("\t\t(read, write, randread, randwrite, rw, randrw)]\n");
	printf("\t[-M, --rwmixread <0-100> rwmixread (100 for reads, 0 for writes)]\n");
	printf("\t[-F, --zipf <theta> use zipf distribution for random I/O]\n");
	printf("\t[-L, --enable-sw-latency-tracking enable latency tracking via sw, default: disabled]\n");
	printf("\t\t-L for latency summary, -LL for detailed histogram\n");
	printf("\t[-l, --enable-ssd-latency-tracking enable latency tracking via ssd (if supported), default: disabled]\n");
	printf("\t[-t, --time <sec> time in seconds]\n");
	printf("\t[-a, --warmup-time <sec> warmup time in seconds]\n");
	printf("\t[-c, --core-mask <mask> core mask for I/O submission/completion.]\n");
	printf("\t\t(default: 1)\n");
	printf("\t[-d, --number-ios <val> number of I/O to perform per thread on each namespace. Note: this is additional exit criteria.]\n");
	printf("\t\t(default: 0 - unlimited)\n");
	printf("\t[-D, --disable-sq-cmb disable submission queue in controller memory buffer, default: enabled]\n");
	printf("\t[-H, --enable-tcp-hdgst enable header digest for TCP transport, default: disabled]\n");
	printf("\t[-I, --enable-tcp-ddgst enable data digest for TCP transport, default: disabled]\n");
	printf("\t[-N, --no-shst-notification no shutdown notification process for controllers, default: disabled]\n");
	printf("\t[-r, --transport <fmt> Transport ID for local PCIe NVMe or NVMeoF]\n");
	printf("\t Format: 'key:value [key:value] ...'\n");
	printf("\t Keys:\n");
	printf("\t  trtype      Transport type (e.g. PCIe, RDMA)\n");
	printf("\t  adrfam      Address family (e.g. IPv4, IPv6)\n");
	printf("\t  traddr      Transport address (e.g. 0000:04:00.0 for PCIe or 192.168.100.8 for RDMA)\n");
	printf("\t  trsvcid     Transport service identifier (e.g. 4420)\n");
	printf("\t  subnqn      Subsystem NQN (default: %s)\n", SPDK_NVMF_DISCOVERY_NQN);
	printf("\t  ns          NVMe namespace ID (all active namespaces are used by default)\n");
	printf("\t  hostnqn     Host NQN\n");
	printf("\t Example: -r 'trtype:PCIe traddr:0000:04:00.0' for PCIe or\n");
	printf("\t          -r 'trtype:RDMA adrfam:IPv4 traddr:192.168.100.8 trsvcid:4420' for NVMeoF\n");
	printf("\t Note: can be specified multiple times to test multiple disks/targets.\n");
	printf("\t[-e, --metadata <fmt> metadata configuration]\n");
	printf("\t Keys:\n");
	printf("\t  PRACT      Protection Information Action bit (PRACT=1 or PRACT=0)\n");
	printf("\t  PRCHK      Control of Protection Information Checking (PRCHK=GUARD|REFTAG|APPTAG)\n");
	printf("\t Example: -e 'PRACT=0,PRCHK=GUARD|REFTAG|APPTAG'\n");
	printf("\t          -e 'PRACT=1,PRCHK=GUARD'\n");
	printf("\t[-k, --keepalive <ms> keep alive timeout period in millisecond]\n");
	printf("\t[-s, --hugemem-size <MB> DPDK huge memory size in MB.]\n");
	printf("\t[-g, --mem-single-seg use single file descriptor for DPDK memory segments]\n");
	printf("\t[-C, --max-completion-per-poll <val> max completions per poll]\n");
	printf("\t\t(default: 0 - unlimited)\n");
	printf("\t[-i, --shmem-grp-id <id> shared memory group ID]\n");
	printf("\t[-Q, --skip-errors log I/O errors every N times (default: 1)]\n");
	printf("\t");
	spdk_log_usage(stdout, "-T");
	printf("\t[-V, --enable-vmd enable VMD enumeration]\n");
	printf("\t[-z, --disable-zcopy <impl> disable zero copy send for the given sock implementation. Default for posix impl]\n");
	printf("\t[-Z, --enable-zcopy <impl> enable zero copy send for the given sock implementation]\n");
	printf("\t[-A, --buffer-alignment IO buffer alignment. Must be power of 2 and not less than cache line (%u)]\n",
	       SPDK_CACHE_LINE_SIZE);
	printf("\t[-S, --default-sock-impl <impl> set the default sock impl, e.g. \"posix\"]\n");
	printf("\t[-m, --cpu-usage display real-time overall cpu usage on used cores]\n");
	printf("\t[-h, --enable-host memory buffer]\n");
#ifdef SPDK_CONFIG_URING
	printf("\t[-R, --enable-uring enable using liburing to drive kernel devices (Default: libaio)]\n");
#endif
#ifdef DEBUG
	printf("\t[-G, --enable-debug enable debug logging]\n");
#else
	printf("\t[-G, --enable-debug enable debug logging (flag disabled, must reconfigure with --enable-debug)]\n");
#endif
	printf("\t[--transport-stats dump transport statistics]\n");
	printf("\t[--iova-mode <mode> specify DPDK IOVA mode: va|pa]\n");
	printf("\t[--io-queue-size <val> size of NVMe IO queue. Default: maximum allowed by controller]\n");
	printf("\t[--disable-ktls disable Kernel TLS. Only valid for ssl impl. Default for ssl impl]\n");
	printf("\t[--enable-ktls enable Kernel TLS. Only valid for ssl impl]\n");
	printf("\t[--tls-version <val> TLS version to use. Only valid for ssl impl. Default: 0 (auto-negotiation)]\n");
	printf("\t[--psk-key <val> Default PSK KEY in hexadecimal digits, e.g. 1234567890ABCDEF (only applies when sock_impl == ssl)]\n");
	printf("\t[--psk-identity <val> Default PSK ID, e.g. psk.spdk.io (only applies when sock_impl == ssl)]\n");
	printf("\t[--zerocopy-threshold <val> data is sent with MSG_ZEROCOPY if size is greater than this val. Default: 0 to disable it]\n");
	printf("\t[--zerocopy-threshold-sock-impl <impl> specify the sock implementation to set zerocopy_threshold]\n");
	printf("\t[--transport-tos <val> specify the type of service for RDMA transport. Default: 0 (disabled)]\n");
}

static void
check_cutoff(void *ctx, uint64_t start, uint64_t end, uint64_t count,
	     uint64_t total, uint64_t so_far)
{
	double so_far_pct;
	double **cutoff = ctx;

	if (count == 0) {
		return;
	}

	so_far_pct = (double)so_far / total;
	while (so_far_pct >= **cutoff && **cutoff > 0) {
		printf("%9.5f%% : %9.3fus\n", **cutoff * 100, (double)end);
		(*cutoff)++;
	}
}

static void
print_bucket(void *ctx, uint64_t start, uint64_t end, uint64_t count,
	     uint64_t total, uint64_t so_far)
{
	double so_far_pct;

	if (count == 0) {
		return;
	}

	so_far_pct = (double)so_far * 100 / total;
	printf("%9.3f - %9.3f: %9.4f%%  (%9ju)\n",
	       (double)start,
	       (double)end,
	       so_far_pct, count);
}

static void
print_performance(void)
{
	uint64_t total_io_completed, total_io_lba_completed, total_io_tsc;
	double io_per_second, mb_per_second, average_latency, min_latency, max_latency;
	double sum_ave_latency, min_latency_so_far, max_latency_so_far;
	double total_io_per_second, total_mb_per_second;
	int ns_count;
	struct worker_thread	*worker;
	struct ns_worker_ctx	*ns_ctx;
	uint32_t max_strlen;
	const char *rw_op_name[2] = {"read", "write"};

	uint64_t total_rw_io_completed[2], total_rw_io_lba_completed[2], total_rw_io_tsc[2];
	double rw_io_per_second[2], rw_mb_per_second[2], rw_average_latency[2], rw_min_latency[2], rw_max_latency[2];
	double rw_sum_ave_latency[2], rw_min_latency_so_far[2], rw_max_latency_so_far[2];
	double total_rw_io_per_second[2], total_rw_mb_per_second[2];

	total_io_per_second = 0;
	total_mb_per_second = 0;
	total_io_completed = 0;
	total_io_lba_completed = 0;
	total_io_tsc = 0;
	min_latency_so_far = (double)UINT64_MAX;
	max_latency_so_far = 0;
	ns_count = 0;

	for (int i = 0; i < 2; i++) {
		total_rw_io_per_second[i] = 0;
		total_rw_mb_per_second[i] = 0;
		total_rw_io_completed[i] = 0;
		total_rw_io_lba_completed[i] = 0;
		total_rw_io_tsc[i] = 0;
		rw_min_latency_so_far[i] = (double)UINT64_MAX;
		rw_max_latency_so_far[i] = 0;
	}


	max_strlen = 0;
	TAILQ_FOREACH(worker, &g_workers, link) {
		TAILQ_FOREACH(ns_ctx, &worker->ns_ctx, link) {
			max_strlen = spdk_max(strlen(ns_ctx->entry->name), max_strlen);
		}
	}

	printf("========================================================\n");
	printf("%*s\n", max_strlen + 60, "Latency(us)");
	printf("%-*s: %10s %10s %10s %10s %10s\n",
	       max_strlen + 13, "Device Information", "IOPS", "MiB/s", "Average", "min", "max");

	TAILQ_FOREACH(worker, &g_workers, link) {
		TAILQ_FOREACH(ns_ctx, &worker->ns_ctx, link) {
			if (ns_ctx->stats.io_completed != 0) {
				io_per_second = (double)ns_ctx->stats.io_completed * 1000 * 1000 / g_elapsed_time_in_usec;
				mb_per_second = (double)ns_ctx->stats.io_lba_completed * 512 / (1024 * 1024) * 1000 * 1000 / g_elapsed_time_in_usec;
				average_latency = ((double)ns_ctx->stats.total_tsc / ns_ctx->stats.io_completed);
				min_latency = (double)ns_ctx->stats.min_tsc;
				if (min_latency < min_latency_so_far) {
					min_latency_so_far = min_latency;
				}

				max_latency = (double)ns_ctx->stats.max_tsc;
				if (max_latency > max_latency_so_far) {
					max_latency_so_far = max_latency;
				}

				printf("%-*.*s from core %2u: %10.2f %10.2f %10.2f %10.2f %10.2f\n",
				       max_strlen, max_strlen, ns_ctx->entry->name, worker->lcore,
				       io_per_second, mb_per_second,
				       average_latency, min_latency, max_latency);

				total_io_per_second += io_per_second;
				total_mb_per_second += mb_per_second;
				total_io_completed += ns_ctx->stats.io_completed;
				total_io_lba_completed += ns_ctx->stats.io_lba_completed;
				total_io_tsc += ns_ctx->stats.total_tsc;
				ns_count++;

				for (int op = 0; op < 2; op++) {
					if (ns_ctx->stats.rw_io_completed[op] != 0) {
						rw_io_per_second[op] = (double)ns_ctx->stats.rw_io_completed[op] * 1000 * 1000 /
									      g_elapsed_time_in_usec;
						rw_mb_per_second[op] = (double)ns_ctx->stats.rw_io_lba_completed[op] * 512 / (1024 * 1024) * 1000 * 1000 / g_elapsed_time_in_usec;
						rw_average_latency[op] = ((double)ns_ctx->stats.rw_total_tsc[op] / ns_ctx->stats.rw_io_completed[op]);
						rw_min_latency[op] = (double)ns_ctx->stats.rw_min_tsc[op];
						rw_max_latency[op] = (double)ns_ctx->stats.rw_max_tsc[op];
						if (rw_min_latency[op] < rw_min_latency_so_far[op]) {
							rw_min_latency_so_far[op] = rw_min_latency[op];
						}
						if (rw_max_latency[op] > rw_max_latency_so_far[op]) {
							rw_max_latency_so_far[op] = rw_max_latency[op];
						}

						total_rw_io_per_second[op] += rw_io_per_second[op];
						total_rw_mb_per_second[op] += rw_mb_per_second[op];
						total_rw_io_completed[op] += ns_ctx->stats.rw_io_completed[op];
						total_rw_io_lba_completed[op] += ns_ctx->stats.rw_io_lba_completed[op];
						total_rw_io_tsc[op] += ns_ctx->stats.rw_total_tsc[op];

						printf("%-*s: %10.2f %10.2f %10.2f %10.2f %10.2f\n",
				      max_strlen + 13, rw_op_name[op], rw_io_per_second[op], rw_mb_per_second[op],
				      rw_average_latency[op], rw_min_latency[op], rw_max_latency[op]);
					}
				}
			}
		}
	}

	if (ns_count != 0 && total_io_completed) {
		sum_ave_latency = ((double)total_io_tsc / total_io_completed);
		printf("========================================================\n");
		printf("%-*s: %10.2f %10.2f %10.2f %10.2f %10.2f\n",
		       max_strlen + 13, "Total", total_io_per_second, total_mb_per_second,
		       sum_ave_latency, min_latency_so_far, max_latency_so_far);
		printf("\n");
		for (int op = 0; op < 2; op++) {
			if (total_rw_io_completed[op] != 0) {
				rw_sum_ave_latency[op] = ((double)total_rw_io_tsc[op] / total_rw_io_completed[op]);
				printf("%-*s: %10.2f %10.2f %10.2f %10.2f %10.2f\n",
				       max_strlen + 13, rw_op_name[op], total_rw_io_per_second[op], total_rw_mb_per_second[op],
				       rw_sum_ave_latency[op], rw_min_latency_so_far[op], rw_max_latency_so_far[op]);
			}
		}
	}

	if (g_latency_sw_tracking_level == 0 || total_io_completed == 0) {
		return;
	}

	TAILQ_FOREACH(worker, &g_workers, link) {
		TAILQ_FOREACH(ns_ctx, &worker->ns_ctx, link) {
			const double *cutoff = g_latency_cutoffs;

			printf("Summary latency data for %-43.43s from core %u:\n", ns_ctx->entry->name, worker->lcore);
			printf("=================================================================================\n");

			spdk_histogram_data_iterate(ns_ctx->histogram, check_cutoff, &cutoff);

			printf("\n");
		}
	}

	if (g_latency_sw_tracking_level == 1) {
		return;
	}

	TAILQ_FOREACH(worker, &g_workers, link) {
		TAILQ_FOREACH(ns_ctx, &worker->ns_ctx, link) {
			printf("Latency histogram for %-43.43s from core %u:\n", ns_ctx->entry->name, worker->lcore);
			printf("==============================================================================\n");
			printf("       Range in us     Cumulative    IO count\n");

			spdk_histogram_data_iterate(ns_ctx->histogram, print_bucket, NULL);
			printf("\n");
		}
	}

}

static void
print_latency_page(struct ctrlr_entry *entry)
{
	int i;

	printf("\n");
	printf("%s\n", entry->name);
	printf("--------------------------------------------------------\n");

	for (i = 0; i < 32; i++) {
		if (entry->latency_page->buckets_32us[i]) {
			printf("Bucket %dus - %dus: %d\n", i * 32, (i + 1) * 32, entry->latency_page->buckets_32us[i]);
		}
	}
	for (i = 0; i < 31; i++) {
		if (entry->latency_page->buckets_1ms[i]) {
			printf("Bucket %dms - %dms: %d\n", i + 1, i + 2, entry->latency_page->buckets_1ms[i]);
		}
	}
	for (i = 0; i < 31; i++) {
		if (entry->latency_page->buckets_32ms[i])
			printf("Bucket %dms - %dms: %d\n", (i + 1) * 32, (i + 2) * 32,
			       entry->latency_page->buckets_32ms[i]);
	}
}

static void
print_latency_statistics(const char *op_name, enum spdk_nvme_intel_log_page log_page)
{
	struct ctrlr_entry	*ctrlr;

	printf("%s Latency Statistics:\n", op_name);
	printf("========================================================\n");
	TAILQ_FOREACH(ctrlr, &g_controllers, link) {
		if (spdk_nvme_ctrlr_is_log_page_supported(ctrlr->ctrlr, log_page)) {
			if (spdk_nvme_ctrlr_cmd_get_log_page(ctrlr->ctrlr, log_page, SPDK_NVME_GLOBAL_NS_TAG,
							     ctrlr->latency_page, sizeof(struct spdk_nvme_intel_rw_latency_page), 0,
							     enable_latency_tracking_complete,
							     NULL)) {
				printf("nvme_ctrlr_cmd_get_log_page() failed\n");
				exit(1);
			}

			g_outstanding_commands++;
		} else {
			printf("Controller %s: %s latency statistics not supported\n", ctrlr->name, op_name);
		}
	}

	while (g_outstanding_commands) {
		TAILQ_FOREACH(ctrlr, &g_controllers, link) {
			spdk_nvme_ctrlr_process_admin_completions(ctrlr->ctrlr);
		}
	}

	TAILQ_FOREACH(ctrlr, &g_controllers, link) {
		if (spdk_nvme_ctrlr_is_log_page_supported(ctrlr->ctrlr, log_page)) {
			print_latency_page(ctrlr);
		}
	}
	printf("\n");
}

static void
print_stats(void)
{
	print_performance();
	if (g_latency_ssd_tracking_enable) {
		if (g_rw_percentage != 0) {
			print_latency_statistics("Read", SPDK_NVME_INTEL_LOG_READ_CMD_LATENCY);
		}
		if (g_rw_percentage != 100) {
			print_latency_statistics("Write", SPDK_NVME_INTEL_LOG_WRITE_CMD_LATENCY);
		}
	}
}

static void
unregister_trids(void)
{
	struct trid_entry *trid_entry, *tmp;

	TAILQ_FOREACH_SAFE(trid_entry, &g_trid_list, tailq, tmp) {
		TAILQ_REMOVE(&g_trid_list, trid_entry, tailq);
		free(trid_entry);
	}
}

static int
add_trid(const char *trid_str)
{
	struct trid_entry *trid_entry;
	struct spdk_nvme_transport_id *trid;
	char *ns;
	char *hostnqn;

	trid_entry = calloc(1, sizeof(*trid_entry));
	if (trid_entry == NULL) {
		return -1;
	}

	trid = &trid_entry->trid;
	trid->trtype = SPDK_NVME_TRANSPORT_PCIE;
	snprintf(trid->subnqn, sizeof(trid->subnqn), "%s", SPDK_NVMF_DISCOVERY_NQN);

	if (spdk_nvme_transport_id_parse(trid, trid_str) != 0) {
		fprintf(stderr, "Invalid transport ID format '%s'\n", trid_str);
		free(trid_entry);
		return 1;
	}

	ns = strcasestr(trid_str, "ns:");
	if (ns) {
		char nsid_str[6]; /* 5 digits maximum in an nsid */
		int len;
		int nsid;

		ns += 3;

		len = strcspn(ns, " \t\n");
		if (len > 5) {
			fprintf(stderr, "NVMe namespace IDs must be 5 digits or less\n");
			free(trid_entry);
			return 1;
		}

		memcpy(nsid_str, ns, len);
		nsid_str[len] = '\0';

		nsid = spdk_strtol(nsid_str, 10);
		if (nsid <= 0 || nsid > 65535) {
			fprintf(stderr, "NVMe namespace IDs must be less than 65536 and greater than 0\n");
			free(trid_entry);
			return 1;
		}

		trid_entry->nsid = (uint16_t)nsid;
	}

	hostnqn = strcasestr(trid_str, "hostnqn:");
	if (hostnqn) {
		size_t len;

		hostnqn += strlen("hostnqn:");

		len = strcspn(hostnqn, " \t\n");
		if (len > (sizeof(trid_entry->hostnqn) - 1)) {
			fprintf(stderr, "Host NQN is too long\n");
			free(trid_entry);
			return 1;
		}

		memcpy(trid_entry->hostnqn, hostnqn, len);
		trid_entry->hostnqn[len] = '\0';
	}

	TAILQ_INSERT_TAIL(&g_trid_list, trid_entry, tailq);
	return 0;
}

static int
add_allowed_pci_device(const char *bdf_str, struct spdk_env_opts *env_opts)
{
	int rc;

	if (env_opts->num_pci_addr >= MAX_ALLOWED_PCI_DEVICE_NUM) {
		fprintf(stderr, "Currently we only support allowed PCI device num=%d\n",
			MAX_ALLOWED_PCI_DEVICE_NUM);
		return -1;
	}

	rc = spdk_pci_addr_parse(&env_opts->pci_allowed[env_opts->num_pci_addr], bdf_str);
	if (rc < 0) {
		fprintf(stderr, "Failed to parse the given bdf_str=%s\n", bdf_str);
		return -1;
	}

	env_opts->num_pci_addr++;
	return 0;
}

static size_t
parse_next_key(const char **str, char *key, char *val, size_t key_buf_size,
	       size_t val_buf_size)
{
	const char *sep;
	const char *separator = ", \t\n";
	size_t key_len, val_len;

	*str += strspn(*str, separator);

	sep = strchr(*str, '=');
	if (!sep) {
		fprintf(stderr, "Key without '=' separator\n");
		return 0;
	}

	key_len = sep - *str;
	if (key_len >= key_buf_size) {
		fprintf(stderr, "Key length %zu is greater than maximum allowed %zu\n",
			key_len, key_buf_size - 1);
		return 0;
	}

	memcpy(key, *str, key_len);
	key[key_len] = '\0';

	*str += key_len + 1;	/* Skip key */
	val_len = strcspn(*str, separator);
	if (val_len == 0) {
		fprintf(stderr, "Key without value\n");
		return 0;
	}

	if (val_len >= val_buf_size) {
		fprintf(stderr, "Value length %zu is greater than maximum allowed %zu\n",
			val_len, val_buf_size - 1);
		return 0;
	}

	memcpy(val, *str, val_len);
	val[val_len] = '\0';

	*str += val_len;

	return val_len;
}

static int
parse_metadata(const char *metacfg_str)
{
	const char *str;
	size_t val_len;
	char key[32];
	char val[1024];

	if (metacfg_str == NULL) {
		return -EINVAL;
	}

	str = metacfg_str;

	while (*str != '\0') {
		val_len = parse_next_key(&str, key, val, sizeof(key), sizeof(val));
		if (val_len == 0) {
			fprintf(stderr, "Failed to parse metadata\n");
			return -EINVAL;
		}

		if (strcmp(key, "PRACT") == 0) {
			if (*val == '1') {
				g_metacfg_pract_flag = SPDK_NVME_IO_FLAGS_PRACT;
			}
		} else if (strcmp(key, "PRCHK") == 0) {
			if (strstr(val, "GUARD") != NULL) {
				g_metacfg_prchk_flags |= SPDK_NVME_IO_FLAGS_PRCHK_GUARD;
			}
			if (strstr(val, "REFTAG") != NULL) {
				g_metacfg_prchk_flags |= SPDK_NVME_IO_FLAGS_PRCHK_REFTAG;
			}
			if (strstr(val, "APPTAG") != NULL) {
				g_metacfg_prchk_flags |= SPDK_NVME_IO_FLAGS_PRCHK_APPTAG;
			}
		} else {
			fprintf(stderr, "Unknown key '%s'\n", key);
		}
	}

	return 0;
}

#define PERF_GETOPT_SHORT "a:b:c:d:e:f:ghi:lmo:q:r:k:s:w:z:A:C:DF:GHILM:NO:P:Q:RS:T:U:VZ:"

static const struct option g_perf_cmdline_opts[] = {
#define PERF_WARMUP_TIME	'a'
	{"warmup-time",			required_argument,	NULL, PERF_WARMUP_TIME},
#define PERF_ALLOWED_PCI_ADDR	'b'
	{"allowed-pci-addr",			required_argument,	NULL, PERF_ALLOWED_PCI_ADDR},
#define PERF_CORE_MASK	'c'
	{"core-mask",			required_argument,	NULL, PERF_CORE_MASK},
#define PERF_IO_TRACE 'f'
	{"io-trace",			required_argument,	NULL, PERF_IO_TRACE},
#define PERF_METADATA	'e'
	{"metadata",			required_argument,	NULL, PERF_METADATA},
#define PERF_MEM_SINGL_SEG	'g'
	{"mem-single-seg", no_argument, NULL, PERF_MEM_SINGL_SEG},
#define PERF_ENABLE_HOST_MEM_BUF 'h'
	{"hmb",			no_argument,	NULL, PERF_ENABLE_HOST_MEM_BUF},
#define PERF_SHMEM_GROUP_ID	'i'
	{"shmem-grp-id",			required_argument,	NULL, PERF_SHMEM_GROUP_ID},
#define PERF_ENABLE_SSD_LATENCY_TRACING	'l'
	{"enable-ssd-latency-tracking", no_argument, NULL, PERF_ENABLE_SSD_LATENCY_TRACING},
#define PERF_CPU_USAGE	'm'
	{"cpu-usage", no_argument, NULL, PERF_CPU_USAGE},
#define PERF_IO_SIZE	'o'
	{"io-size",			required_argument,	NULL, PERF_IO_SIZE},
#define PERF_IO_DEPTH	'q'
	{"io-depth",			required_argument,	NULL, PERF_IO_DEPTH},
#define PERF_TRANSPORT	'r'
	{"transport",			required_argument,	NULL, PERF_TRANSPORT},
#define PERF_KEEPALIVE	'k'
	{"keepalive",			required_argument,	NULL, PERF_KEEPALIVE},
#define PERF_HUGEMEM_SIZE	's'
	{"hugemem-size",			required_argument,	NULL, PERF_HUGEMEM_SIZE},
#define PERF_NUMBER_IOS	'd'
	{"number-ios",			required_argument,	NULL, PERF_NUMBER_IOS},
#define PERF_IO_PATTERN	'w'
	{"io-pattern",			required_argument,	NULL, PERF_IO_PATTERN},
#define PERF_DISABLE_ZCOPY	'z'
	{"disable-zcopy",			required_argument,	NULL, PERF_DISABLE_ZCOPY},
#define PERF_BUFFER_ALIGNMENT	'A'
	{"buffer-alignment",			required_argument,	NULL, PERF_BUFFER_ALIGNMENT},
#define PERF_MAX_COMPLETIONS_PER_POLL	'C'
	{"max-completion-per-poll",			required_argument,	NULL, PERF_MAX_COMPLETIONS_PER_POLL},
#define PERF_DISABLE_SQ_CMB	'D'
	{"disable-sq-cmb",			no_argument,	NULL, PERF_DISABLE_SQ_CMB},
#define PERF_ZIPF		'F'
	{"zipf",				required_argument,	NULL, PERF_ZIPF},
#define PERF_ENABLE_DEBUG	'G'
	{"enable-debug",			no_argument,	NULL, PERF_ENABLE_DEBUG},
#define PERF_ENABLE_TCP_HDGST	'H'
	{"enable-tcp-hdgst",			no_argument,	NULL, PERF_ENABLE_TCP_HDGST},
#define PERF_ENABLE_TCP_DDGST	'I'
	{"enable-tcp-ddgst",			no_argument,	NULL, PERF_ENABLE_TCP_DDGST},
#define PERF_ENABLE_SW_LATENCY_TRACING	'L'
	{"enable-sw-latency-tracking", no_argument, NULL, PERF_ENABLE_SW_LATENCY_TRACING},
#define PERF_RW_MIXREAD	'M'
	{"rwmixread", required_argument, NULL, PERF_RW_MIXREAD},
#define PERF_NO_SHST_NOTIFICATION	'N'
	{"no-shst-notification", no_argument, NULL, PERF_NO_SHST_NOTIFICATION},
#define PERF_IO_UNIT_SIZE	'O'
	{"io-unit-size",			required_argument,	NULL, PERF_IO_UNIT_SIZE},
#define PERF_IO_QUEUES_PER_NS	'P'
	{"num-qpairs", required_argument, NULL, PERF_IO_QUEUES_PER_NS},
#define PERF_SKIP_ERRORS	'Q'
	{"skip-errors",			required_argument,	NULL, PERF_SKIP_ERRORS},
#define PERF_ENABLE_URING	'R'
	{"enable-uring", no_argument, NULL, PERF_ENABLE_URING},
#define PERF_DEFAULT_SOCK_IMPL	'S'
	{"default-sock-impl", required_argument, NULL, PERF_DEFAULT_SOCK_IMPL},
#define PERF_LOG_FLAG	'T'
	{"logflag", required_argument, NULL, PERF_LOG_FLAG},
#define PERF_NUM_UNUSED_IO_QPAIRS	'U'
	{"num-unused-qpairs", required_argument, NULL, PERF_NUM_UNUSED_IO_QPAIRS},
#define PERF_ENABLE_VMD	'V'
	{"enable-vmd", no_argument, NULL, PERF_ENABLE_VMD},
#define PERF_ENABLE_ZCOPY	'Z'
	{"enable-zcopy",			required_argument,	NULL, PERF_ENABLE_ZCOPY},
#define PERF_TRANSPORT_STATISTICS	257
	{"transport-stats", no_argument, NULL, PERF_TRANSPORT_STATISTICS},
#define PERF_IOVA_MODE		258
	{"iova-mode", required_argument, NULL, PERF_IOVA_MODE},
#define PERF_IO_QUEUE_SIZE	259
	{"io-queue-size", required_argument, NULL, PERF_IO_QUEUE_SIZE},
#define PERF_DISABLE_KTLS	260
	{"disable-ktls", no_argument, NULL, PERF_DISABLE_KTLS},
#define PERF_ENABLE_KTLS	261
	{"enable-ktls", no_argument, NULL, PERF_ENABLE_KTLS},
#define PERF_TLS_VERSION	262
	{"tls-version", required_argument, NULL, PERF_TLS_VERSION},
#define PERF_PSK_KEY		263
	{"psk-key", required_argument, NULL, PERF_PSK_KEY},
#define PERF_PSK_IDENTITY	264
	{"psk-identity ", required_argument, NULL, PERF_PSK_IDENTITY},
#define PERF_ZEROCOPY_THRESHOLD		265
	{"zerocopy-threshold", required_argument, NULL, PERF_ZEROCOPY_THRESHOLD},
#define PERF_SOCK_IMPL		266
	{"zerocopy-threshold-sock-impl", required_argument, NULL, PERF_SOCK_IMPL},
#define PERF_TRANSPORT_TOS		267
	{"transport-tos", required_argument, NULL, PERF_TRANSPORT_TOS},
	/* Should be the last element */
	{0, 0, 0, 0}
};

static int
parse_args(int argc, char **argv, struct spdk_env_opts *env_opts)
{
	int op, long_idx;
	long int val;
	long long int val2;
	int rc;
	char *endptr;
	bool ssl_used = false;
	char *sock_impl = "posix";

	while ((op = getopt_long(argc, argv, PERF_GETOPT_SHORT, g_perf_cmdline_opts, &long_idx)) != -1) {
		switch (op) {
		case PERF_WARMUP_TIME:
		case PERF_BUFFER_ALIGNMENT:
		case PERF_SHMEM_GROUP_ID:
		case PERF_MAX_COMPLETIONS_PER_POLL:
		case PERF_IO_QUEUES_PER_NS:
		case PERF_IO_SIZE:
		case PERF_IO_UNIT_SIZE:
		case PERF_IO_DEPTH:
		case PERF_KEEPALIVE:
		case PERF_HUGEMEM_SIZE:
		case PERF_RW_MIXREAD:
		case PERF_NUM_UNUSED_IO_QPAIRS:
		case PERF_SKIP_ERRORS:
		case PERF_IO_QUEUE_SIZE:
		case PERF_ZEROCOPY_THRESHOLD:
			val = spdk_strtol(optarg, 10);
			if (val < 0) {
				fprintf(stderr, "Converting a string to integer failed\n");
				return val;
			}
			switch (op) {
			case PERF_WARMUP_TIME:
				g_warmup_time_in_sec = val;
				break;
			case PERF_SHMEM_GROUP_ID:
				env_opts->shm_id = val;
				break;
			case PERF_MAX_COMPLETIONS_PER_POLL:
				g_max_completions = val;
				break;
			case PERF_IO_QUEUES_PER_NS:
				g_nr_io_queues_per_ns = val;
				break;
			case PERF_IO_SIZE:
				g_io_size_bytes = val;
				break;
			case PERF_IO_UNIT_SIZE:
				g_io_unit_size = val;
				break;
			case PERF_IO_DEPTH:
				g_queue_depth = val;
				break;
			case PERF_KEEPALIVE:
				g_keep_alive_timeout_in_ms = val;
				break;
			case PERF_HUGEMEM_SIZE:
				env_opts->mem_size = val;
				break;
			case PERF_RW_MIXREAD:
				g_rw_percentage = val;
				g_mix_specified = true;
				break;
			case PERF_SKIP_ERRORS:
				g_quiet_count = val;
				break;
			case PERF_NUM_UNUSED_IO_QPAIRS:
				g_nr_unused_io_queues = val;
				break;
			case PERF_BUFFER_ALIGNMENT:
				g_io_align = val;
				if (!spdk_u32_is_pow2(g_io_align) || g_io_align < SPDK_CACHE_LINE_SIZE) {
					fprintf(stderr, "Wrong alignment %u. Must be power of 2 and not less than cache lize (%u)\n",
						g_io_align, SPDK_CACHE_LINE_SIZE);
					usage(argv[0]);
					return 1;
				}
				g_io_align_specified = true;
				break;
			case PERF_IO_QUEUE_SIZE:
				g_io_queue_size = val;
				break;
			case PERF_ZEROCOPY_THRESHOLD:
				g_sock_zcopy_threshold = val;
			}
			break;
		case PERF_NUMBER_IOS:
			val2 = spdk_strtoll(optarg, 10);
			if (val2 < 0) {
				fprintf(stderr, "Converting a string to integer failed\n");
				return val2;
			}

			g_number_ios = (uint64_t)val2;
			break;
		case PERF_ZIPF:
			errno = 0;
			g_zipf_theta = strtod(optarg, &endptr);
			if (errno || optarg == endptr || g_zipf_theta < 0) {
				fprintf(stderr, "Illegal zipf theta value %s\n", optarg);
				return 1;
			}
			break;
		case PERF_ALLOWED_PCI_ADDR:
			if (add_allowed_pci_device(optarg, env_opts)) {
				usage(argv[0]);
				return 1;
			}
			break;
		case PERF_CORE_MASK:
			env_opts->core_mask = optarg;
			g_core_mask = spdk_strtol(optarg, 16);
			break;
		case PERF_IO_TRACE:
			g_enable_io_trace = true;
			g_io_trace_path = optarg;
			break;
		case PERF_METADATA:
			if (parse_metadata(optarg)) {
				usage(argv[0]);
				return 1;
			}
			break;
		case PERF_MEM_SINGL_SEG:
			env_opts->hugepage_single_segments = true;
			break;
		case PERF_ENABLE_SSD_LATENCY_TRACING:
			g_latency_ssd_tracking_enable = true;
			break;
		case PERF_CPU_USAGE:
			g_monitor_perf_cores = true;
			break;
		case PERF_TRANSPORT:
			if (add_trid(optarg)) {
				usage(argv[0]);
				return 1;
			}
			break;
		case PERF_IO_PATTERN:
			g_workload_type = optarg;
			break;
		case PERF_DISABLE_SQ_CMB:
			g_disable_sq_cmb = 1;
			break;
		case PERF_ENABLE_DEBUG:
#ifndef DEBUG
			fprintf(stderr, "%s must be configured with --enable-debug for -G flag\n",
				argv[0]);
			usage(argv[0]);
			return 1;
#else
			spdk_log_set_flag("nvme");
			spdk_log_set_print_level(SPDK_LOG_DEBUG);
			break;
#endif
		case PERF_ENABLE_HOST_MEM_BUF:
			g_host_mem_buf = 1;
			break;
		case PERF_ENABLE_TCP_HDGST:
			g_header_digest = 1;
			break;
		case PERF_ENABLE_TCP_DDGST:
			g_data_digest = 1;
			break;
		case PERF_ENABLE_SW_LATENCY_TRACING:
			g_latency_sw_tracking_level++;
			break;
		case PERF_NO_SHST_NOTIFICATION:
			g_no_shn_notification = true;
			break;
		case PERF_ENABLE_URING:
#ifndef SPDK_CONFIG_URING
			fprintf(stderr, "%s must be rebuilt with CONFIG_URING=y for -R flag.\n",
				argv[0]);
			usage(argv[0]);
			return 0;
#endif
			g_use_uring = true;
			break;
		case PERF_LOG_FLAG:
			rc = spdk_log_set_flag(optarg);
			if (rc < 0) {
				fprintf(stderr, "unknown flag\n");
				usage(argv[0]);
				exit(EXIT_FAILURE);
			}
#ifdef DEBUG
			spdk_log_set_print_level(SPDK_LOG_DEBUG);
#endif
			break;
		case PERF_ENABLE_VMD:
			g_vmd = true;
			break;
		case PERF_DISABLE_KTLS:
			ssl_used = true;
			perf_set_sock_opts("ssl", "ktls", 0, NULL);
			break;
		case PERF_ENABLE_KTLS:
			ssl_used = true;
			perf_set_sock_opts("ssl", "ktls", 1, NULL);
			break;
		case PERF_TLS_VERSION:
			ssl_used = true;
			val = spdk_strtol(optarg, 10);
			if (val < 0) {
				fprintf(stderr, "Illegal tls version value %s\n", optarg);
				return val;
			}
			perf_set_sock_opts("ssl", "tls_version", val, NULL);
			break;
		case PERF_PSK_KEY:
			ssl_used = true;
			perf_set_sock_opts("ssl", "psk_key", 0, optarg);
			break;
		case PERF_PSK_IDENTITY:
			ssl_used = true;
			perf_set_sock_opts("ssl", "psk_identity", 0, optarg);
			break;
		case PERF_DISABLE_ZCOPY:
			perf_set_sock_opts(optarg, "enable_zerocopy_send_client", 0, NULL);
			break;
		case PERF_ENABLE_ZCOPY:
			perf_set_sock_opts(optarg, "enable_zerocopy_send_client", 1, NULL);
			break;
		case PERF_DEFAULT_SOCK_IMPL:
			sock_impl = optarg;
			rc = spdk_sock_set_default_impl(optarg);
			if (rc) {
				fprintf(stderr, "Failed to set sock impl %s, err %d (%s)\n", optarg, errno, strerror(errno));
				return 1;
			}
			break;
		case PERF_TRANSPORT_STATISTICS:
			g_dump_transport_stats = true;
			break;
		case PERF_IOVA_MODE:
			env_opts->iova_mode = optarg;
			break;
		case PERF_SOCK_IMPL:
			g_sock_threshold_impl = optarg;
			break;
		case PERF_TRANSPORT_TOS:
			val = spdk_strtol(optarg, 10);
			if (val < 0) {
				fprintf(stderr, "Invalid TOS value\n");
				return 1;
			}
			g_transport_tos = val;
			break;
		default:
			usage(argv[0]);
			return 1;
		}
	}

	if (!g_nr_io_queues_per_ns) {
		usage(argv[0]);
		return 1;
	}

	if (!g_queue_depth) {
		fprintf(stderr, "missing -q (--io-depth) operand\n");
		usage(argv[0]);
		return 1;
	}
	if (!g_io_size_bytes) {
		fprintf(stderr, "missing -o (--io-size) operand\n");
		usage(argv[0]);
		return 1;
	}
	if (!g_io_unit_size || g_io_unit_size % 4) {
		fprintf(stderr, "io unit size can not be 0 or non 4-byte aligned\n");
		return 1;
	}
	if (!g_workload_type) {
		fprintf(stderr, "missing -w (--io-pattern) operand\n");
		usage(argv[0]);
		return 1;
	}
	if (!g_quiet_count) {
		fprintf(stderr, "-Q (--skip-errors) value must be greater than 0\n");
		usage(argv[0]);
		return 1;
	}

	if (strncmp(g_workload_type, "rand", 4) == 0) {
		g_is_random = 1;
		g_workload_type = &g_workload_type[4];
	}

	if (ssl_used && strncmp(sock_impl, "ssl", 3) != 0) {
		fprintf(stderr, "sock impl is not SSL but tried to use one of the SSL only options\n");
		usage(argv[0]);
		return 1;
	}


	if (strcmp(g_workload_type, "read") == 0 || strcmp(g_workload_type, "write") == 0) {
		g_rw_percentage = strcmp(g_workload_type, "read") == 0 ? 100 : 0;
		if (g_mix_specified) {
			fprintf(stderr, "Ignoring -M (--rwmixread) option... Please use -M option"
				" only when using rw or randrw.\n");
		}
	} else if (strcmp(g_workload_type, "rw") == 0) {
		if (g_rw_percentage < 0 || g_rw_percentage > 100) {
			fprintf(stderr,
				"-M (--rwmixread) must be specified to value from 0 to 100 "
				"for rw or randrw.\n");
			return 1;
		}
	} else {
		fprintf(stderr,
			"-w (--io-pattern) io pattern type must be one of\n"
			"(read, write, randread, randwrite, rw, randrw)\n");
		return 1;
	}

	if (g_sock_zcopy_threshold > 0) {
		if (!g_sock_threshold_impl) {
			fprintf(stderr,
				"--zerocopy-threshold must be set with sock implementation specified(--zerocopy-threshold-sock-impl <impl>)\n");
			return 1;
		}

		perf_set_sock_opts(g_sock_threshold_impl, "zerocopy_threshold", g_sock_zcopy_threshold, NULL);
	}

	if (g_number_ios && g_warmup_time_in_sec) {
		fprintf(stderr, "-d (--number-ios) with -a (--warmup-time) is not supported\n");
		return 1;
	}

	if (g_number_ios && g_number_ios < g_queue_depth) {
		fprintf(stderr, "-d (--number-ios) less than -q (--io-depth) is not supported\n");
		return 1;
	}

	if (TAILQ_EMPTY(&g_trid_list)) {
		/* If no transport IDs specified, default to enumerating all local PCIe devices */
		add_trid("trtype:PCIe");
	} else {
		struct trid_entry *trid_entry, *trid_entry_tmp;

		env_opts->no_pci = true;
		/* check whether there is local PCIe type */
		TAILQ_FOREACH_SAFE(trid_entry, &g_trid_list, tailq, trid_entry_tmp) {
			if (trid_entry->trid.trtype == SPDK_NVME_TRANSPORT_PCIE) {
				env_opts->no_pci = false;
				break;
			}
		}
	}

	g_file_optind = optind;

	return 0;
}

static int
register_workers(void)
{
	uint32_t i = 0;
	struct worker_thread *worker;
	int core_mask = g_core_mask;

	while (core_mask) {
		if (core_mask & 1) {
			worker = calloc(1, sizeof(*worker));
			if (worker == NULL) {
				fprintf(stderr, "Unable to allocate worker\n");
				return -1;
			}
			TAILQ_INIT(&worker->ns_ctx);
			worker->lcore = i;
			worker->id = g_num_workers;
			TAILQ_INSERT_TAIL(&g_workers, worker, link);
			g_num_workers++;
		}
		core_mask >>= 1;
		i++;
	}

	return 0;
}

static void
unregister_workers(void)
{
	struct worker_thread *worker, *tmp_worker;
	struct ns_worker_ctx *ns_ctx, *tmp_ns_ctx;

	/* Free namespace context and worker thread */
	TAILQ_FOREACH_SAFE(worker, &g_workers, link, tmp_worker) {
		TAILQ_REMOVE(&g_workers, worker, link);

		TAILQ_FOREACH_SAFE(ns_ctx, &worker->ns_ctx, link, tmp_ns_ctx) {
			TAILQ_REMOVE(&worker->ns_ctx, ns_ctx, link);
			spdk_histogram_data_free(ns_ctx->histogram);
			free(ns_ctx);
		}

		free(worker);
	}
}

static void
unregister_controllers(void)
{
	struct ctrlr_entry *entry, *tmp;
	struct spdk_nvme_detach_ctx *detach_ctx = NULL;

	TAILQ_FOREACH_SAFE(entry, &g_controllers, link, tmp) {
		TAILQ_REMOVE(&g_controllers, entry, link);

		spdk_dma_free(entry->latency_page);
		if (g_latency_ssd_tracking_enable &&
		    spdk_nvme_ctrlr_is_feature_supported(entry->ctrlr, SPDK_NVME_INTEL_FEAT_LATENCY_TRACKING)) {
			set_latency_tracking_feature(entry->ctrlr, false);
		}

		if (g_nr_unused_io_queues) {
			int i;

			for (i = 0; i < g_nr_unused_io_queues; i++) {
				spdk_nvme_ctrlr_free_io_qpair(entry->unused_qpairs[i]);
			}

			free(entry->unused_qpairs);
		}

		spdk_nvme_detach_async(entry->ctrlr, &detach_ctx);
		free(entry);
	}

	if (detach_ctx) {
		spdk_nvme_detach_poll(detach_ctx);
	}

	if (g_vmd) {
		spdk_vmd_fini();
	}
}

static int
allocate_ns_worker(struct ns_entry *entry, struct worker_thread *worker)
{
	struct ns_worker_ctx	*ns_ctx;

	ns_ctx = calloc(1, sizeof(struct ns_worker_ctx));
	if (!ns_ctx) {
		return -1;
	}

	printf("Associating %s with lcore %d\n", entry->name, worker->lcore);
	ns_ctx->stats.min_tsc = UINT64_MAX;
	ns_ctx->stats.rw_min_tsc[0] = ns_ctx->stats.rw_min_tsc[1] = UINT64_MAX;
	ns_ctx->entry = entry;
	ns_ctx->histogram = spdk_histogram_data_alloc();
	ns_ctx->worker_id = worker->id;
	TAILQ_INSERT_TAIL(&worker->ns_ctx, ns_ctx, link);

	return 0;
}

static int
associate_workers_with_ns(void)
{
	struct ns_entry		*entry = TAILQ_FIRST(&g_namespaces);
	struct worker_thread	*worker = TAILQ_FIRST(&g_workers);
	int			i, count;

	/* Each core contains single worker, and namespaces are associated as follows:
	 * 1) equal workers and namespaces - each worker associated with single namespace
	 * 2) more workers than namespaces - each namespace is associated with one or more workers
	 * 3) more namespaces than workers - each worker is associated with one or more namespaces
	 */
	count = g_num_namespaces > g_num_workers ? g_num_namespaces : g_num_workers;

	printf("Worker Count: %d\n", count);

	for (i = 0; i < count; i++) {
		if (entry == NULL) {
			break;
		}

		if (allocate_ns_worker(entry, worker) != 0) {
			return -1;
		}

		worker = TAILQ_NEXT(worker, link);
		if (worker == NULL) {
			worker = TAILQ_FIRST(&g_workers);
		}

		entry = TAILQ_NEXT(entry, link);
		if (entry == NULL) {
			entry = TAILQ_FIRST(&g_namespaces);
		}

	}

	return 0;
}

static void
sig_handler(int signo)
{
	g_exit = true;
}

static int
setup_sig_handlers(void)
{
	struct sigaction sigact = {};
	int rc;

	sigemptyset(&sigact.sa_mask);
	sigact.sa_handler = sig_handler;
	rc = sigaction(SIGINT, &sigact, NULL);
	if (rc < 0) {
		fprintf(stderr, "sigaction(SIGINT) failed, errno %d (%s)\n", errno, strerror(errno));
		return -1;
	}

	rc = sigaction(SIGTERM, &sigact, NULL);
	if (rc < 0) {
		fprintf(stderr, "sigaction(SIGTERM) failed, errno %d (%s)\n", errno, strerror(errno));
		return -1;
	}

	return 0;
}

int
main(int argc, char **argv)
{
	int rc;
	struct worker_thread *worker, *main_worker;
	struct spdk_env_opts opts;
	int core_mask = g_core_mask;
	pthread_t tids[26];
	int th_id = 0;

	/* Use the runtime PID to set the random seed */
	srand(getpid());

	spdk_env_opts_init(&opts);
	opts.name = "perf";
	opts.pci_allowed = g_allowed_pci_addr;
	rc = parse_args(argc, argv, &opts);
	if (rc != 0) {
		return rc;
	}
	/* Transport statistics are printed from each thread.
	 * To avoid mess in terminal, init and use mutex */
	rc = pthread_mutex_init(&g_stats_mutex, NULL);
	if (rc != 0) {
		fprintf(stderr, "Failed to init mutex\n");
		return -1;
	}

	if (g_enable_io_trace) {
		assert(g_io_trace_path != NULL);
		FILE* trace_fp = fopen(g_io_trace_path, "r");
		if (!trace_fp) {
			fprintf(stderr, "Unable to open trace file '%s': %s\n",
				g_io_trace_path, strerror(errno));
			assert(0);
			return -1;
		}

		char buffer[1024];
		g_trace_line = 0;

		while (fgets(buffer, sizeof(buffer), trace_fp)) {
			g_trace_line++;
		}

		rewind(trace_fp);

		g_trace_entry = calloc(g_trace_line, sizeof(*g_trace_entry));
		if (!g_trace_entry) {
			fprintf(stderr, "Unable to allocate trace buffer\n");
			assert(0);
		}

		printf("trace file '%s' contains %lu lines\n", g_io_trace_path, g_trace_line);

		uint64_t trace_ptr = 0;
		int pid = 0;
		uint64_t start_lba, lba_count;
		char rw_type[10];
		while (trace_ptr < g_trace_line) {
			char *rc_buffer = fgets(buffer, sizeof(buffer), trace_fp);
			assert(rc_buffer);
			struct trace_entry *entry = &g_trace_entry[trace_ptr];
			rc = sscanf(buffer, "%u %lu %lu %s",
				    &pid, &start_lba, &lba_count, rw_type);
			if (rc != 4) {
				printf("%s\n", buffer);
				fprintf(stderr, "Invalid trace file format\n");
				assert(0);
			}

			entry->pid = pid;
			entry->start_lba = start_lba;
			entry->lba_count = lba_count;

			g_max_trace_io_size = spdk_max(g_max_trace_io_size, lba_count);

			assert(strcmp(rw_type, "R") == 0 || strcmp(rw_type, "W") == 0);
			entry->is_read = (strcmp(rw_type, "R") == 0);
			
			trace_ptr++;
		}
	}

	rc = setup_sig_handlers();
	if (rc != 0) {
		rc = -1;
		goto cleanup;
	}

	if (register_workers() != 0) {
		rc = -1;
		goto cleanup;
	}

	if (register_files(argc, argv) != 0) {
		rc = -1;
		goto cleanup;
	}

	if (g_warn) {
		printf("WARNING: Some requested NVMe devices were skipped\n");
	}

	if (g_num_namespaces == 0) {
		fprintf(stderr, "No valid NVMe controllers or AIO or URING devices found\n");
		goto cleanup;
	}

	if (g_num_workers > 1 && g_quiet_count > 1) {
		fprintf(stderr, "Error message rate-limiting enabled across multiple threads.\n");
		fprintf(stderr, "Error suppression count may not be exact.\n");
	}

	if (associate_workers_with_ns() != 0) {
		rc = -1;
		goto cleanup;
	}

	rc = pthread_barrier_init(&g_worker_sync_barrier, NULL, g_num_workers);
	if (rc != 0) {
		fprintf(stderr, "Unable to initialize thread sync barrier\n");
		goto cleanup;
	}

	printf("Initialization complete. Launching workers.\n");

	/* Launch all of the secondary workers */

	if (!g_core_mask)	{
		printf("No core mask specified, using all available cores.\n");
		exit(1);
	}

	while (core_mask) {
		if (core_mask & 1) break;
		g_main_core++;
		core_mask >>= 1;
	}

	main_worker = NULL;
	TAILQ_FOREACH(worker, &g_workers, link) {
		if (worker->lcore != g_main_core) {
			rc = pthread_create(&tids[th_id], NULL, work_fn, worker);
			if (rc != 0) {
				fprintf(stderr, "Unable to create thread on core %d\n", worker->lcore);
				exit(1);
			}
			th_id++;
		} else {
			assert(main_worker == NULL);
			main_worker = worker;
		}
	}

	assert(main_worker != NULL);
	work_fn(main_worker);

	for (int i = 0; i < g_num_workers - 1; i++) {
		pthread_join(tids[i], NULL);
	}

	g_elapsed_time_in_usec = diff_time(g_tsc_start);

	printf("All workers completed. elapsed_time_in_usec %lu\n", g_elapsed_time_in_usec);

	print_stats();

	pthread_barrier_destroy(&g_worker_sync_barrier);

cleanup:

	unregister_trids();
	unregister_namespaces();
	unregister_controllers();
	unregister_workers();

	pthread_mutex_destroy(&g_stats_mutex);

	if (rc != 0) {
		fprintf(stderr, "%s: errors occurred\n", argv[0]);
	}

	return rc;
}
