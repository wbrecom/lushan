#ifndef HDICT_H
#define HDICT_H

#include <sys/queue.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>

#define LINE_SIZE 1024
#define BUF_SIZE 255

typedef struct {
	uint64_t key;
	uint64_t pos;
} idx_t;

typedef struct {
        uint32_t version;
        char label[21];
} meta_t;

typedef struct hdict_t hdict_t;
struct hdict_t {
	TAILQ_ENTRY(hdict_t) link;
	LIST_ENTRY(hdict_t) h_link;
	char *path;
	uint32_t idx_num;
	idx_t *idx;
	int fd;
	time_t open_time;
	uint32_t num_qry;
	uint32_t ref;
	uint32_t hdid;
        meta_t *hdict_meta;
};

TAILQ_HEAD(hdict_list_t, hdict_t);

#define HTAB_SIZE   1024
#define HASH(dict_id)  ((dict_id) % HTAB_SIZE)

typedef struct hdb_t hdb_t;
struct hdb_t {
	pthread_mutex_t mutex;
	struct hdict_list_t open_list;
	struct hdict_list_t close_list;
	int num_open;
	int num_close;
	LIST_HEAD(, hdict_t) htab[HTAB_SIZE];
};

#define EHDICT_OUT_OF_MEMERY	-1
#define EHDICT_BAD_FILE		-2

hdict_t* hdict_open(const char *path, int *hdict_errnop);

#define HDICT_VALUE_LENGTH_MAX 204800
int hdict_seek(hdict_t *hdict, uint64_t key, off_t *off, uint32_t *length);

int hdict_randomkey(hdict_t *hdict, uint64_t *key);

int hdict_read(hdict_t *hdict, char *buf, uint32_t length, off_t off);

void hdict_close(hdict_t *hdict);

void *hdb_mgr(void *arg);

int hdb_init(hdb_t *hdb);

int hdb_reopen(hdb_t *hdb, const char *hdict_path, uint32_t hdid);

int hdb_close(hdb_t *hdb, uint32_t hdid);

int hdb_info(hdb_t *hdb, char *buf, int size);

hdict_t *hdb_ref(hdb_t *hdb, uint32_t hdid);

int hdb_deref(hdb_t *hdb, hdict_t *hdict);

#endif
