#include "hdict.h"
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/queue.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <pthread.h>

#define LOCK(hdb)   pthread_mutex_lock(&(hdb)->mutex)
#define UNLK(hdb)   pthread_mutex_unlock(&(hdb)->mutex)

int get_hdict_meta(const char *path, meta_t *hdict_meta)
{
    FILE *fp = fopen(path, "r");
    if (fp == NULL) {
	return -1;
    }

    char line[LINE_SIZE];

    while (fgets(line, LINE_SIZE, fp)) {

	if (strncasecmp(line, "version", 7) == 0) {
		int i = 7;
		while((line[i] && line[i] != '\r' && line[i] != '\n') && 
			(line[i] == ' ' || line[i] == ':' || line[i] == '=')) i++;
		if (line[i] && line[i] != '\r' && line[i] != '\n')
			hdict_meta->version = atoi(line+i);
	} else if (strncasecmp(line, "label", 5) == 0) {
		int i = 5;
		while((line[i] && line[i] != '\r' && line[i] != '\n') && 
			(line[i] == ' ' || line[i] == ':' || line[i] == '=')) i++;
		int j = 0;
		while(line[i] && line[i] != '\r' && line[i] != '\n' && j < 20) {
			hdict_meta->label[j++] = line[i++];
		}
		hdict_meta->label[j] = '\0';
	}
    }

    fclose(fp);

    return 0;
}

hdict_t* hdict_open(const char *path, int *hdict_errnop)
{
	FILE *fp = NULL;
	char pathname[256];

	hdict_t *hdict = (hdict_t *)calloc(1, sizeof(hdict[0]));
	hdict->path = strdup(path);

	snprintf(pathname, sizeof(pathname), "%s/idx", hdict->path);

	struct stat st;
	if (stat(pathname, &st) == -1 ||
		(st.st_size % sizeof(hdict->idx[0])) != 0) {
		*hdict_errnop = EHDICT_BAD_FILE;
		goto error;
	}

	hdict->idx = (idx_t *)malloc(st.st_size);
	if (hdict->idx == NULL) {
		*hdict_errnop = EHDICT_OUT_OF_MEMERY;
		goto error;
	}

	if ((fp = fopen(pathname, "r")) == NULL) {
		*hdict_errnop = EHDICT_BAD_FILE;
		goto error;
	}

	hdict->idx_num = st.st_size / sizeof(hdict->idx[0]);
	if (fread(hdict->idx, sizeof(hdict->idx[0]), hdict->idx_num, fp) != hdict->idx_num) {
		*hdict_errnop = EHDICT_BAD_FILE;
		goto error;
	}

	fclose(fp);
	fp = NULL;

	snprintf(pathname, sizeof(pathname), "%s/dat", hdict->path);
	hdict->fd = open(pathname, O_RDWR);
	if (hdict->fd <= 0) {
		//*hdict_errnop = EHDICT_BAD_FILE;
		*hdict_errnop = EHDICT_OUT_OF_MEMERY;
		goto error;
	}
	hdict->open_time = time(NULL);

	// meta
	snprintf(pathname, sizeof(pathname), "%s/meta", hdict->path);

	meta_t *hdict_meta = (meta_t*)calloc(1, sizeof(meta_t));
	if (hdict_meta == NULL) {
	    *hdict_errnop = EHDICT_OUT_OF_MEMERY;
	    goto error;
	}

	get_hdict_meta(pathname, hdict_meta);
	hdict->hdict_meta = hdict_meta;

	return hdict;

error:
	if (fp) fclose(fp);
	if (hdict) hdict_close(hdict);
	return NULL;
}

int hdict_seek(hdict_t *hdict, uint64_t key, off_t *off, uint32_t *length)
{
	uint32_t low = 0;
	uint32_t high = hdict->idx_num;
	uint32_t mid;
	int hit = 0;
        int count = 0;
	while (low < high) {
                ++count;
		mid = (low + high) / 2;
		if (hdict->idx[mid].key > key) {
			high = mid;
		} else if (hdict->idx[mid].key < key) {
			low = mid + 1;
		} else {
			*off = (hdict->idx[mid].pos & 0xFFFFFFFFFF);
			*length = (hdict->idx[mid].pos >> 40);
			hit = 1;
			break;
		}
	}

	return hit;
}

int hdict_randomkey(hdict_t *hdict, uint64_t *key)
{
	if (hdict->idx_num == 0)
		return -1;
	int i = rand() % hdict->idx_num;
	*key = hdict->idx[i].key;
	return 0;
}

int hdict_read(hdict_t *hdict, char *buf, uint32_t length, off_t off)
{
	return pread(hdict->fd, buf, length, off);
}

void hdict_close(hdict_t *hdict)
{
	if (hdict->fd > 0) close(hdict->fd);
	if (hdict->idx) free(hdict->idx);
	if (hdict->hdict_meta) free(hdict->hdict_meta);
	free(hdict->path);
	free(hdict);
}

void *hdb_mgr(void *arg)
{
	hdb_t *hdb = (hdb_t *)arg;

	for (;;) {
		if (TAILQ_FIRST(&hdb->close_list)) {
			hdict_t *hdict, *next;
			hdict_t *hdicts[100];
			int i, k = 0;

			LOCK(hdb);
			for (hdict = TAILQ_FIRST(&hdb->close_list); hdict; hdict = next) {
				next = TAILQ_NEXT(hdict, link);
				if (hdict->ref == 0) {
					hdicts[k++] = hdict;
					TAILQ_REMOVE(&hdb->close_list, hdict, link);
					hdb->num_close--;
					if (k == 100)
						break;
				}
			}
			UNLK(hdb);

			for (i = 0; i < k; ++i) {
				hdict = hdicts[i];
				hdict_close(hdict);
			}
		}
		sleep(1);
	}
	return NULL;
}

int hdb_init(hdb_t *hdb)
{
	if (pthread_mutex_init(&hdb->mutex, NULL)) return -1;
	TAILQ_INIT(&hdb->open_list);
	TAILQ_INIT(&hdb->close_list);
	hdb->num_open = 0;
	hdb->num_close = 0;

	int i;
	LIST_HEAD(, hdict_t) htab[HTAB_SIZE];
	for (i = 0; i < HTAB_SIZE; i++) {
		LIST_INIT(htab + i);
	}
	return 0;
}

int hdb_reopen(hdb_t *hdb, const char *hdict_path, uint32_t hdid)
{
	hdict_t *hd, *next;
	uint32_t hash;
	char rpath[1024];
	realpath(hdict_path, rpath);

	int hdict_errno = 0;
	hdict_t *hdict = hdict_open(rpath, &hdict_errno);
	if (hdict == NULL) return hdict_errno;

	LOCK(hdb);
	for (hd = TAILQ_FIRST(&hdb->open_list); hd; hd = next) {
		next = TAILQ_NEXT(hd, link);
		if (hd->hdid == hdid) {
			LIST_REMOVE(hd, h_link);
			TAILQ_REMOVE(&hdb->open_list, hd, link);
			hdb->num_open--;
			TAILQ_INSERT_TAIL(&hdb->close_list, hd, link);
			hdb->num_close++;
			break;
		}
	}
	hdict->hdid = hdid;
	TAILQ_INSERT_TAIL(&hdb->open_list, hdict, link);
	hash = HASH(hdict->hdid);
	LIST_INSERT_HEAD(&hdb->htab[hash], hdict, h_link);
	hdb->num_open++;
	UNLK(hdb);

	return 0;
}

int hdb_close(hdb_t *hdb, uint32_t hdid)
{
	int found = 0;
	hdict_t *hd, *next;
	LOCK(hdb);
	for (hd = TAILQ_FIRST(&hdb->open_list); hd; hd = next) {
		next = TAILQ_NEXT(hd, link);
		if (hd->hdid == hdid) {
			LIST_REMOVE(hd, h_link);
			TAILQ_REMOVE(&hdb->open_list, hd, link);
			hdb->num_open--;
			TAILQ_INSERT_TAIL(&hdb->close_list, hd, link);
			hdb->num_close++;
			found = 1;
			break;
		}
	}
	UNLK(hdb);

	return found;
}

int hdb_info(hdb_t *hdb, char *buf, int size)
{
	int len = 0;
	len += snprintf(buf+len, size-len, "%2s %20s %5s %3s %9s %8s %13s %s\n", 
			"id", "label", "state", "ref", "num_qry", "idx_num", "open_time", "path");
	if (len < size) 
		len += snprintf(buf+len, size-len, "----------------------------------------------------------------\n");
	int pass, k;
	hdict_t *hdict;
	LOCK(hdb);
	for (pass = 0; pass < 2; ++pass) {
		const char *state;
		struct hdict_list_t *hlist;
		switch (pass) {
		case 0:
			state = "OPEN";
			hlist = &hdb->open_list;
			break;
		case 1:
			state = "CLOSE";
			hlist = &hdb->close_list;
			break;
		default:
			state = NULL;
			hlist = NULL;
		}

		k = 0;
		for (hdict = TAILQ_FIRST(hlist); hdict; hdict = TAILQ_NEXT(hdict, link)) {
			++k;
			if (len < size) {
				struct tm tm;
				localtime_r(&hdict->open_time, &tm);
				len += snprintf(buf+len, size-len, "%2d %20s %5s %2d %10d %8d %02d%02d%02d-%02d%02d%02d %s\n",
					hdict->hdid,
					hdict->hdict_meta->label,
					state,
					hdict->ref,
					hdict->num_qry,
					hdict->idx_num,
					tm.tm_year - 100, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec,
					hdict->path);
			}
		}
	}
	UNLK(hdb);
	return len;
}

hdict_t *hdb_ref(hdb_t *hdb, uint32_t hdid)
{
	hdict_t *hdict = NULL;
	hdict_t *hd;
	LOCK(hdb);
	uint32_t hash = HASH(hdid);
	for (hd = LIST_FIRST(&hdb->htab[hash]); hd; hd = LIST_NEXT(hd, h_link)) {
		if (hd->hdid == hdid) {
			hd->ref++;
			hdict = hd;
			break;
                }
	}
	UNLK(hdb);
	return hdict;
}

int hdb_deref(hdb_t *hdb, hdict_t *hdict)
{
	LOCK(hdb);
	hdict->ref--;
	UNLK(hdb);
	return 0;
}
