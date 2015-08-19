/* Copyright (c) 2015, WEIBO, Inc. */
/* All rights reserved. */

/* Redistribution and use in source and binary forms, with or without */
/* modification, are permitted provided that the following conditions are */
/* met: */

/*     * Redistributions of source code must retain the above copyright */
/* notice, this list of conditions and the following disclaimer. */

/*     * Redistributions in binary form must reproduce the above */
/* copyright notice, this list of conditions and the following disclaimer */
/* in the documentation and/or other materials provided with the */
/* distribution. */

/*     * Neither the name of the WEIBO nor the names of its */
/* contributors may be used to endorse or promote products derived from */
/* this software without specific prior written permission. */

/* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS */
/* "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT */
/* LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR */
/* A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT */
/* OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, */
/* SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT */
/* LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, */
/* DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY */
/* THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT */
/* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE */
/* OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. */

/*
 *  memcached - memory caching daemon
 *
 *       http://www.memcached.org/
 *
 *  Copyright 2003 Danga Interactive, Inc.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Anatoly Vorobey <mellon@pobox.com>
 *      Brad Fitzpatrick <brad@danga.com>
 */

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <signal.h>
#include <assert.h>
#include <event.h>
#include <errno.h>
#include <inttypes.h>


#include "hdict.h"

#define mytimesub(a, b, result)     do {            \
    (result)->tv_sec = (a)->tv_sec - (b)->tv_sec;       \
    (result)->tv_usec = (a)->tv_usec - (b)->tv_usec;    \
    if ((result)->tv_usec < 0) {                \
        --(result)->tv_sec;             \
        (result)->tv_usec += 1000000;           \
    }                           \
} while (0)

struct settings {
	int maxconns;
	int port;
	struct in_addr interf;
	int num_threads;
	int verbose;
	int timeout;
};

static struct settings settings;

static void settings_init(void) {
	settings.port = 9764;
	settings.interf.s_addr = htonl(INADDR_ANY);
	settings.maxconns = 8192;
	settings.num_threads = 4;
	settings.verbose = 0;
	settings.timeout = 0;
}

struct stats {
	uint32_t curr_conns;
	uint32_t conn_structs;
	uint32_t get_cmds;
	uint32_t get_hits;
	uint32_t get_misses;
	time_t started;
	uint32_t timeouts;
	uint32_t ialloc_failed;
};

static struct stats stats;
static pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;
#define STATS_LOCK()	pthread_mutex_lock(&stats_lock);
#define STATS_UNLOCK()	pthread_mutex_unlock(&stats_lock);

static void stats_init()
{
	stats.curr_conns = stats.conn_structs = 0;
	stats.get_cmds = stats.get_hits = stats.get_misses = 0;
	stats.ialloc_failed = 0;
	stats.started = time(NULL);
	return;
}

enum conn_states {
	conn_listening,  /* the socket which listens for connections */
	conn_read,       /* reading in a command line */
	conn_write,      /* writing out a simple response */
	conn_closing,    /* closing this connection */
};

typedef struct _conn {
	int sfd;
	int state;
	struct event event;
	short ev_flags;
	short which;   /* which events were just triggered */

	char *rbuf;    /* buffer to read commands into */
	char *rcurr;   /* but if we parsed some already, this is where we stopped */
	int rsize;     /* total allocated size of rbuf */
	int rbytes;    /* how much data, starting from rcur, do we have unparsed */

	char *wbuf;
	char *wcurr;
	int wsize;
	int wbytes;
	int write_and_go;

	char *cmd;
	struct timeval tv;
	struct _conn *next;
} conn;

struct conn_queue {
	conn *head;
	conn *tail;
	uint32_t length;
	int wakeup;
	pthread_mutex_t lock;
	pthread_cond_t  cond;
};

static struct conn_queue REQ;
static struct conn_queue RSP;
static pthread_mutex_t notify_lock = PTHREAD_MUTEX_INITIALIZER;
static int notify_receive_fd;
static int notify_send_fd;

static void cq_init(struct conn_queue *cq, int wakeup) {
	pthread_mutex_init(&cq->lock, NULL);
	pthread_cond_init(&cq->cond, NULL);
	cq->length = 0;
	cq->wakeup = wakeup;
	cq->head = NULL;
	cq->tail = NULL;
}

static void cq_push(struct conn_queue *cq, conn *item) {
	item->next = NULL;

	pthread_mutex_lock(&cq->lock);
	if (NULL == cq->tail)
		cq->head = item;
	else
		cq->tail->next = item;
	cq->tail = item;
	cq->length++;
	if (cq->wakeup) pthread_cond_signal(&cq->cond);
	pthread_mutex_unlock(&cq->lock);
}

static conn *cq_pop(struct conn_queue *cq) {
	conn *item;

	assert(cq->wakeup);
	pthread_mutex_lock(&cq->lock);
	while (NULL == cq->head)
		pthread_cond_wait(&cq->cond, &cq->lock);
	item = cq->head;
	cq->head = item->next;
	if (NULL == cq->head)
		cq->tail = NULL;
	cq->length--;
	pthread_mutex_unlock(&cq->lock);

	return item;
}

static conn *cq_peek(struct conn_queue *cq) {
	conn *item;

	pthread_mutex_lock(&cq->lock);
	item = cq->head;
	if (NULL != item) {
		cq->head = item->next;
		if (NULL == cq->head)
			cq->tail = NULL;
		cq->length--;
	}
	pthread_mutex_unlock(&cq->lock);

	return item;
}

static uint32_t cq_length(struct conn_queue *cq) {
	int length;
	pthread_mutex_lock(&cq->lock);
	length = cq->length;
	pthread_mutex_unlock(&cq->lock);
	return length;
}

void event_handler(const int fd, const short which, void *arg);
void out_string(conn *c, char *str);
conn *conn_new(int sfd, int init_state, int event_flags, struct event_base *base);

static conn **freeconns;
static int freetotal;
static int freecurr;

static void conn_init()
{
	freetotal = 200;
	freecurr = 0;
	freeconns = (conn **)malloc(sizeof (conn *)*freetotal);
	return;
}

#define DATA_BUFFER_SIZE 8192

static void conn_close(conn *c) {
	event_del(&c->event);

	if (settings.verbose > 1)
		fprintf(stderr, "<%d connection closed.\n", c->sfd);

	close(c->sfd);

	if (freecurr < freetotal) {
		freeconns[freecurr++] = c;
	} else {
		conn **new_freeconns = realloc(freeconns, sizeof(conn *)*freetotal*2);
		if (new_freeconns) {
			freetotal *= 2;
			freeconns = new_freeconns;
			freeconns[freecurr++] = c;
		} else {
			if (settings.verbose > 0)
				fprintf(stderr, "Couldn't realloc freeconns\n");
			free(c->rbuf);
			free(c->wbuf);
			free(c);

			STATS_LOCK();
			stats.conn_structs--;
			STATS_UNLOCK();
		}
	}

	STATS_LOCK();
	stats.curr_conns--;
	STATS_UNLOCK();
}

static int update_event(conn *c, const int new_flags) {
	assert(c != NULL);

	struct event_base *base = c->event.ev_base;
	if (c->ev_flags == new_flags)
		return 1;
	if (c->ev_flags != 0) {
		if (event_del(&c->event) == -1) return 0;
	}
	event_set(&c->event, c->sfd, new_flags, event_handler, (void *)c);
	event_base_set(base, &c->event);
	c->ev_flags = new_flags;
	if (event_add(&c->event, 0) == -1) return 0;
	return 1;
}

#define FORWARD_OUT 1
#define STAY_IN  2

static int process_command(conn *c, char *command) {

	if (strcmp(command, "quit") == 0) {
		c->state = conn_closing;
		return STAY_IN;
	} else if (strcmp(command, "stop") == 0) {
		exit(0);
	} else if (strcmp(command, "version") == 0) {
		// compat with old php Memcache client
		out_string(c, "VERSION 1.1.13");
		return STAY_IN;
	} else {
		if (event_del(&c->event) == -1) {
			out_string(c, "SERVER_ERROR can't forward");
			return STAY_IN;
		}
		gettimeofday(&c->tv, NULL);
		c->ev_flags = 0;
		c->cmd = command;
		cq_push(&REQ, c);
		return FORWARD_OUT;
	}
}

static int try_read_command(conn *c) {
	char *el, *cont;

	if (!c->rbytes)
		return 0;
	el = (char *)memchr(c->rcurr, '\n', c->rbytes);
	if (!el)
		return 0;
	cont = el + 1;
	if (el - c->rcurr > 1 && *(el - 1) == '\r') {
		el--;
	}
	*el = '\0'; 

	int res = process_command(c, c->rcurr);

	c->rbytes -= (cont - c->rcurr);
	c->rcurr = cont;

	return res;
}

void pack_string(conn *c, char *str) {
	int len;

	len = strlen(str);
	if (len + 2 > c->wsize) {
		/* ought to be always enough. just fail for simplicity */
		str = "SERVER_ERROR output line too long";
		len = strlen(str);
	}

	strcpy(c->wbuf, str);
	strcat(c->wbuf, "\r\n");
	c->wbytes = len + 2;

	return;
}

void out_string(conn *c, char *str) {
	pack_string(c, str);
	c->wcurr = c->wbuf;
	c->state = conn_write;
	c->write_and_go = conn_read;
	return;
}


static int try_read_network(conn *c) {
	int gotdata = 0;
	int res;

	if (c->rcurr != c->rbuf) {
		if (c->rbytes != 0) /* otherwise there's nothing to copy */
			memmove(c->rbuf, c->rcurr, c->rbytes);
		c->rcurr = c->rbuf;
	}

	while (1) {
		if (c->rbytes >= c->rsize) {
			char *new_rbuf = (char *)realloc(c->rbuf, c->rsize*2);
			if (!new_rbuf) {
				if (settings.verbose > 0)
					fprintf(stderr, "Couldn't realloc input buffer\n");
				c->rbytes = 0; /* ignore what we read */
				out_string(c, "SERVER_ERROR out of memory");
				c->write_and_go = conn_closing;
				return 1;
			}
			c->rcurr  = c->rbuf = new_rbuf;
			c->rsize *= 2;
		}
		int avail = c->rsize - c->rbytes;
		res = read(c->sfd, c->rbuf + c->rbytes, avail);
		if (res > 0) {
			gotdata = 1;
			c->rbytes += res;
			if (res == avail) {
				continue;
			} else {
				break;
			}
		}
		if (res == 0) {
			/* connection closed */
			c->state = conn_closing;
			return 1;
		}

		if (res == -1) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) break;
			else {
				c->state = conn_closing;
				return 1;
			}
		}
	}
	return gotdata;
}

static void drive_machine(conn *c) {
	int stop = 0;
	int sfd, flags = 1;
	socklen_t addrlen;
	struct sockaddr_in addr;
	conn *newc;
	int res;

	assert(c != NULL);

	while (!stop) {

		switch(c->state) {
		case conn_listening:
			addrlen = sizeof(addr);
			if ((sfd = accept(c->sfd, (struct sockaddr *)&addr, &addrlen)) == -1) {
				if (errno == EAGAIN || errno == EWOULDBLOCK) {
					/* these are transient, so don't log anything */
					stop = 1;
				} else if (errno == EMFILE) {
					if (settings.verbose > 0)
						fprintf(stderr, "Too many open connections\n");
					stop = 1;
				} else {
					perror("accept()");
					stop = 1;
				}
				break;
			}

			if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
				fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
				perror("setting O_NONBLOCK");
				close(sfd);
				break;
			}
			newc = conn_new(sfd, conn_read, EV_READ | EV_PERSIST, c->event.ev_base);
			if (!newc) {
				if (settings.verbose > 0)
					fprintf(stderr, "couldn't create new connection\n");
				close(sfd);
			}
			break;

		case conn_read:
			res = try_read_command(c);
			if (res == STAY_IN) {
				continue;
			} else if (res == FORWARD_OUT) {
				stop = 1;
				break;
			}
			if (try_read_network(c) != 0) {
				continue;
			}
			/* we have no command line and no data to read from network */
			if (!update_event(c, EV_READ | EV_PERSIST)) {
				if (settings.verbose > 0)
					fprintf(stderr, "Couldn't update event\n");
				c->state = conn_closing;
				break;
			}
			stop = 1;
			break;

		case conn_write:
			if (c->wbytes == 0) {
				c->state = c->write_and_go;
				break;
			}

			res = write(c->sfd, c->wcurr, c->wbytes);
			if (res > 0) {
				c->wcurr  += res;
				c->wbytes -= res;
				break;
			}
			if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
				if (!update_event(c, EV_WRITE | EV_PERSIST)) {
					if (settings.verbose > 0)
						fprintf(stderr, "Couldn't update event\n");
					c->state = conn_closing;
					break;
				}
				stop = 1;
				break;
			}
			c->state = conn_closing;
			break;

		case conn_closing:
			conn_close(c);
			stop = 1;
			break;
		}
  	}

	return;
}


void event_handler(const int fd, const short which, void *arg) {
	conn *c;

	c = (conn *)arg;
	assert(c != NULL);

	c->which = which;

	if (fd != c->sfd) {
		if (settings.verbose > 0)
			fprintf(stderr, "Catastrophic: event fd doesn't match conn fd!\n");
		conn_close(c);
		return;
	}

	drive_machine(c);

	return;
}

void notify_handler(const int fd, const short which, void *arg) {
	if (fd != notify_receive_fd) {
		if (settings.verbose > 0)
			fprintf(stderr, "Catastrophic: event fd doesn't match conn fd!\n");
		return;
	}
	char buf[1];
	if (read(fd, buf, 1) != 1) {
		if (settings.verbose > 0)
			fprintf(stderr, "Can't read from event pipe\n");
	}

	conn *c = cq_peek(&RSP);
	if (c != NULL) {
		c->wcurr = c->wbuf;
		c->state = conn_write;
		c->write_and_go = conn_read;

		drive_machine(c);
	}

	return;
}

conn *conn_new(int sfd, int init_state, int event_flags, struct event_base *base) {
	conn *c;

	if (freecurr > 0) {
		c = freeconns[--freecurr];
	} else {
		if (!(c = (conn *)malloc(sizeof(conn)))) {
			perror("malloc()");
			return 0;
		}

		c->rbuf = c->wbuf = 0;
		c->rbuf = (char *) malloc(DATA_BUFFER_SIZE);
		c->wbuf = (char *) malloc(DATA_BUFFER_SIZE);

		if (c->rbuf == 0 || c->wbuf == 0) {
			if (c->rbuf != 0) free(c->rbuf);
			if (c->wbuf != 0) free(c->wbuf);
			free(c);
			perror("malloc()");
			return 0;
		}
		c->rsize = c->wsize = DATA_BUFFER_SIZE;
		c->rcurr = c->rbuf;

		STATS_LOCK();
		stats.conn_structs++;
		STATS_UNLOCK();
	}

	if (settings.verbose > 1) {
		if (init_state == conn_listening)
			fprintf(stderr, "<%d server listening\n", sfd);
		else
			fprintf(stderr, "<%d new client connection\n", sfd);
	}

	c->sfd = sfd;
	c->state = init_state;
	c->rbytes = c->wbytes = 0;
	c->wcurr = c->wbuf;
	c->write_and_go = conn_read;
	event_set(&c->event, sfd, event_flags, event_handler, (void *)c);
	event_base_set(base, &c->event);
	c->ev_flags = event_flags;
	if (event_add(&c->event, 0) == -1) {
		if (freecurr < freetotal) {
			freeconns[freecurr++] = c;
		} else {
			free (c->rbuf);
			free (c->wbuf);
			free (c);

			STATS_LOCK();
			stats.conn_structs--;
			STATS_UNLOCK();
		}
		return 0;
	}

	STATS_LOCK();
	stats.curr_conns++;
	STATS_UNLOCK();
	return c;
}

static int l_socket = 0;

static int new_socket() {
	int sfd;
	int flags;

	if ((sfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		perror("socket()");
		return -1;
	}

	if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
		fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
		perror("setting O_NONBLOCK");
		close(sfd);
		return -1;
	}
	return sfd;
}

static int server_socket(const int port) {
	int sfd;
	struct linger ling = {0, 0};
	struct sockaddr_in addr;
	int flags = 1;

	if ((sfd = new_socket()) == -1) {
		return -1;
	}

	setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
	setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
	setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
	setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags));

	memset(&addr, 0, sizeof(addr));

	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	addr.sin_addr = settings.interf;
	if (bind(sfd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
		perror("bind()");
		close(sfd);
		return -1;
	}

	if (listen(sfd, 1024) == -1) {
		perror("listen()");
		close(sfd);
		return -1;
	}
	return sfd;
}

static void* worker(void *arg)
{
	pthread_detach(pthread_self());

	hdb_t *hdb = (hdb_t *)arg;
	hdict_t *hdict;
	conn *c;
	while(1) {
		c = cq_pop(&REQ);

		struct timeval now;
		gettimeofday(&now, NULL);

		struct timeval tv;
		mytimesub(&now, &c->tv, &tv);

		if (settings.timeout > 0 && tv.tv_sec * 1000 + tv.tv_usec/1000 > settings.timeout) {
			pack_string(c, "SERVER_ERROR timeout");
			STATS_LOCK();
			stats.timeouts++;
			STATS_UNLOCK();
		} else if (strcmp(c->cmd, "stats") == 0) {
			char temp[1024];
			pid_t pid = getpid();
			char *pos = temp;

			uint32_t length = cq_length(&REQ);
			STATS_LOCK();
			pos += sprintf(pos, "STAT pid %u\r\n", pid);
			pos += sprintf(pos, "STAT uptime %lld\r\n", (long long)stats.started);
			pos += sprintf(pos, "STAT curr_connections %u\r\n", stats.curr_conns - 1); /* ignore listening conn */
			pos += sprintf(pos, "STAT connection_structures %u\r\n", stats.conn_structs);
			pos += sprintf(pos, "STAT cmd_get %u\r\n", stats.get_cmds);
			pos += sprintf(pos, "STAT get_hits %u\r\n", stats.get_hits);
			pos += sprintf(pos, "STAT get_misses %u\r\n", stats.get_misses);
			pos += sprintf(pos, "STAT threads %u\r\n", settings.num_threads);
			pos += sprintf(pos, "STAT timeouts %u\r\n", stats.timeouts);
			pos += sprintf(pos, "STAT waiting_requests %u\r\n", length);
			pos += sprintf(pos, "STAT ialloc_failed %u\r\n", stats.ialloc_failed);
			pos += sprintf(pos, "END");
			STATS_UNLOCK();
			pack_string(c, temp);
		} else if (strcmp(c->cmd, "stats reset") == 0) {
			STATS_LOCK();
			stats.get_cmds = 0;
			stats.get_hits = 0;
			stats.get_misses = 0;
			stats.timeouts = 0;
			stats.ialloc_failed = 0;
			STATS_UNLOCK();
			pack_string(c, "RESET");
		} else if (strncmp(c->cmd, "open ", 5) == 0 ||
			strncmp(c->cmd, "reopen ", 7) == 0) {

			char path[256];
			uint32_t hdid;
			int res = sscanf(c->cmd, "%*s %255s %u\n", path, &hdid);
			if (res != 2 || strlen(path) == 0) {
				pack_string(c, "CLIENT_ERROR bad command line format");
			} else {
				int status = hdb_reopen(hdb, path, hdid);
				if (status == 0) {
					pack_string(c, "OPENED");
				} else {
					if (settings.verbose > 0)
						fprintf(stderr, "failed to open %s on %d, return %d\n", 
							path, hdid, status);
					if (status == EHDICT_OUT_OF_MEMERY) {
						STATS_LOCK();
						stats.ialloc_failed++;
						STATS_UNLOCK();
					}
					pack_string(c, "SERVER_ERROR open failed");
				}
			}
		} else if (strncmp(c->cmd, "close ", 6) == 0) {
			uint32_t hdid;
			hdid = atoi(c->cmd + 6);
			int res = hdb_close(hdb, hdid);
			if (res == 1) {
				pack_string(c, "CLOSED");
			} else {
				pack_string(c, "NOT_FOUND");
			}
		} else if (strncmp(c->cmd, "randomkey ", 10) == 0) {
			uint32_t hdid;
			hdid = atoi(c->cmd + 10);
			hdict = hdb_ref(hdb, hdid);
			if (hdict == NULL) {
				if (settings.verbose > 1)
					fprintf(stderr, "<Can't find hdb [%d]\n", hdid);
				pack_string(c, "NOT_FOUND");
			} else {
				uint64_t key;
				int res = hdict_randomkey(hdict, &key);
				if (res != 0) {
					pack_string(c, "EMPTY");
				} else {
					char temp[256];
					snprintf(temp, 256, "RANDOMKEY %"PRIu64"", key);
					pack_string(c, temp);
				}
				hdb_deref(hdb, hdict);
			}

		} else if (strncmp(c->cmd, "info", 4) == 0) {
			char temp[4096];
			hdb_info(hdb, temp, 4096);
			pack_string(c, temp);
		} else if (strncmp(c->cmd, "get ", 4) == 0) {
			char *start = c->cmd + 4;
			char token[251];
			int next;
			uint32_t hdid;
			uint64_t key;
			off_t off;
			uint32_t length;
			c->wbytes = 0;
			int res, nc;
			char *en_dash;
			while(sscanf(start, " %250s%n", token, &next) >= 1) {
				start += next;

				STATS_LOCK();
				stats.get_cmds++;
				STATS_UNLOCK();

				hdid = 0;
				if ((en_dash = strchr(token, '-')) == NULL) {
					key = strtoull(token, NULL, 10);
				} else {
					hdid = atoi(token);
					key = strtoull(en_dash+1, NULL, 10);
				}
				hdict = hdb_ref(hdb, hdid);
				if (hdict == NULL) {
					if (settings.verbose > 1)
						fprintf(stderr, "<Can't find hdb [%d]\n", hdid);
					STATS_LOCK();
					stats.get_misses++;
					STATS_UNLOCK();
					continue;
				}
				hdict->num_qry++;

				if (hdict_seek(hdict, key, &off, &length)) {
					STATS_LOCK();
					stats.get_hits++;
					STATS_UNLOCK();
					if (length < HDICT_VALUE_LENGTH_MAX) {
						if (c->wsize - c->wbytes < 512 + length) {
							int relloc_length = c->wsize * 2;
							if (relloc_length - c->wbytes < 512 + length) {
								relloc_length = 512 + length + c->wbytes;
								relloc_length = (relloc_length/DATA_BUFFER_SIZE + 1) * DATA_BUFFER_SIZE;
							}
							char *newbuf = (char *)realloc(c->wbuf, relloc_length);
							if (newbuf) {
								c->wbuf = newbuf;
								c->wsize = relloc_length;
							} else {
								if (settings.verbose > 0)
									fprintf(stderr, "Couldn't realloc output buffer\n");
								goto NEXT;
							}
						}

						nc = sprintf(c->wbuf + c->wbytes, "VALUE %s %u %u\r\n", token, 0, length);
						res = hdict_read(hdict, c->wbuf + c->wbytes + nc, length, off);
						if (res != length) {
							if (settings.verbose > 0)
								fprintf(stderr, "Failed to read from hdict\n");
							goto NEXT;
						}

						c->wbytes += nc;
						c->wbytes += length;
						c->wbytes += sprintf(c->wbuf + c->wbytes, "\r\n");
					}
				} else {
					STATS_LOCK();
					stats.get_misses++;
					STATS_UNLOCK();
				}
NEXT:
				hdb_deref(hdb, hdict);
			}
			c->wbytes += sprintf(c->wbuf + c->wbytes, "END\r\n");
		} else {
			pack_string(c, "ERROR");
		}

		cq_push(&RSP, c);
		pthread_mutex_lock(&notify_lock);
		if (write(notify_send_fd, "", 1) != 1) {
			perror("Writing to thread notify pipe");
		}
		pthread_mutex_unlock(&notify_lock);
	}

	return NULL;
}

static void usage(void) {
	printf("-p <num>      TCP port number to listen on (default: 9764)\n"
		"-l <ip_addr>  interface to listen on, default is INDRR_ANY\n"
		"-c <num>      max simultaneous connections (default: 1024)\n"
		"-d            run as a daemon\n"
		"-h            print this help and exit\n"
		"-v            verbose (print errors/warnings)\n"
		"-s            strict mode (exit while open hdb failed before startup)\n"
		"-t <num>      number of worker threads to use, default 4\n"
		"-T <num>      timeout in millisecond, 0 for none, default 0\n");
	return;
}

int main(int argc, char *argv[])
{
	int c;
	int daemonize = 0;
	int strict = 0;
	struct in_addr addr;

	char *init = NULL;

	settings_init();
	setbuf(stderr, NULL);

	while ((c = getopt(argc, argv, "p:c:hvdl:t:T:i:")) != -1) {
		switch (c) {
		case 'p':
			settings.port = atoi(optarg);
			break;
		case 'c':
			settings.maxconns = atoi(optarg);
			break;
		case 'h':
			usage();
			exit(0);
		case 'l':
			if (inet_pton(AF_INET, optarg, &addr) <= 0) {
				fprintf(stderr, "Illegal address: %s\n", optarg);
				return 1;
			} else {
				settings.interf = addr;
			}
			break;
		case 'd':
			daemonize = 1;
			break;
		case 'v':
			settings.verbose++;
			break;
		case 's':
			strict = 1;
			break;
		case 't':
			settings.num_threads = atoi(optarg);
			if (settings.num_threads == 0) {
				fprintf(stderr, "Number of threads must be greater than 0\n");
				return 1;
			}
			break;
		case 'T':
			settings.timeout = atoi(optarg);
			break;
		case 'i':
			init = strdup(optarg);
			break;
		default:
			fprintf(stderr, "Illegal argument \"%c\"\n", c);
			return 1;
		}
	}

	srand(time(NULL)^getpid());

	struct rlimit rlim;
	if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
		fprintf(stderr, "failed to getrlimit number of files\n");
		exit(1);
	} else {
		int maxfiles = settings.maxconns;
		if (rlim.rlim_cur < maxfiles)
			rlim.rlim_cur = maxfiles;
		if (rlim.rlim_max < rlim.rlim_cur)
			rlim.rlim_max = rlim.rlim_cur;
		if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
			fprintf(stderr, "failed to set rlimit for open files. Try running as root or requesting smaller maxconns value.\n");
			exit(1);
		}
	}

	if (daemonize) {
		int res;
		res = daemon(1, settings.verbose);
		if (res == -1) {
			fprintf(stderr, "failed to daemon() in order to daemonize\n");
			return 1;
		}
	}

	stats_init();
	conn_init();

	l_socket = server_socket(settings.port);
	if (l_socket == -1) {
		fprintf(stderr, "failed to listen\n");
		exit(1);
	}

	cq_init(&REQ, 1);
	cq_init(&RSP, 0);

	int fds[2];
	if (pipe(fds)) {
		fprintf(stderr, "can't create notify pipe\n");
		exit(1);
	}
	notify_receive_fd = fds[0];
	notify_send_fd = fds[1];

	hdb_t hdb;
	hdb_init(&hdb);

	int i;
	if (init) {
		FILE *fp;
		if ((fp = fopen(init, "r")) == NULL) {
			fprintf(stderr, "failed to open %s\n", init);
			exit(1);
		}

		char line[1024];
		int bad = 1;
		while(fgets(line, 1024, fp)) {
			i = strlen(line) - 1;
			while(i>=0 && (line[i]=='\r' || line[i]=='\n')) {
				line[i] = '\0';
				i--;
			}
			if (strncmp(line, "open ", 5) == 0) {
				char path[256];
				uint32_t hdid;
				int res = sscanf(line, "%*s %255s %u\n", path, &hdid);
				if (res != 2 || strlen(path) == 0) {
					fprintf(stderr, "illegal init command %s\n", line);
					exit(1);
				}
				int status = hdb_reopen(&hdb, path, hdid);
				if (status != 0) {
					fprintf(stderr, "failed to open %s on %d, return %d\n", path, hdid, status);
					if (strict)
						exit(1);
					else {
						if (status == EHDICT_OUT_OF_MEMERY) {
							stats.ialloc_failed++;
						}
					}

				}
			} else if (strcmp(line, "end") == 0) {
				bad = 0;
			}
		}
		fclose(fp);
		if (bad) {
			fprintf(stderr, "bad init command file %s, expect \"end\"\n", init);
			exit(1);
		}
	}

	pthread_t tid;
	pthread_create(&tid, NULL, hdb_mgr, &hdb);

	for (i = 0; i < settings.num_threads; i++) {
		pthread_create(&tid, NULL, worker, &hdb);
	}

	struct event_base *main_base = event_init();

	struct sigaction sa;
	sa.sa_handler = SIG_IGN;
	sa.sa_flags = 0;
	if (sigemptyset(&sa.sa_mask) == -1 ||
		sigaction(SIGPIPE, &sa, 0) == -1) {
		perror("failed to ignore SIGPIPE; sigaction");
		exit(1);
	}
	struct event notify_event;
	event_set(&notify_event, notify_receive_fd,
		EV_READ | EV_PERSIST, notify_handler, NULL);
	event_base_set(main_base, &notify_event);

	if (event_add(&notify_event, 0) == -1) {
		fprintf(stderr, "can't monitor libevent notify pipe\n");
		exit(1);
	}

	conn *listen_conn;
	if (!(listen_conn = conn_new(l_socket, conn_listening,
			EV_READ | EV_PERSIST, main_base))) {
		fprintf(stderr, "failed to create listening connection");
		exit(1);
	}
	event_base_loop(main_base, 0);

	exit(0);
}
