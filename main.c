#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <sys/time.h>
#include <unistd.h>
#include <err.h>
#include "utils.c"

#define DTYPE int
#define DEBUG(format, ...) printf("[ Thread: %lu ] :: "format"", pthread_self(), ##__VA_ARGS__)
#define DEBUG(format, ...) {}

/* Used to execute pthread function */
#define verify(func)                                                      \
    do {                                                                  \
        int e;                                                            \
        if ((e = func) != 0) {                                            \
            fprintf(stderr, "%s(%d) %s: %s\n", __FILE__, __LINE__, #func, \
                    strerror(e));                                         \
            exit(1);                                                      \
        }                                                                 \
    } while (0)



/* Qsort routine from Bentley & McIlroy's "Engineering a Sort Function" */
#define swapcode(TYPE, parmi, parmj, e_size) \
    {                                        \
        long i = (e_size) / sizeof(TYPE);    \
        TYPE *pi = (TYPE *) (parmi);         \
        TYPE *pj = (TYPE *) (parmj);         \
        do {                                 \
            TYPE t = *pi;                    \
            *pi++ = *pj;                     \
            *pj++ = t;                       \
        } while (--i > 0);                   \
    }

void myswap(void *x, void *y){
    /* todo: check data type; */ 
    DTYPE t = *(DTYPE *) x;
    *(DTYPE *) x = *(DTYPE *) y;
    *(DTYPE *) y = t;
}


#define cmpcode(x, y) (*(DTYPE *)x - *(DTYPE *)y)

int mycmp(void *a, void *b) {
    /* todo: check data type; */ 
    return cmpcode(a, b);
}

enum thread_status {
    ts_idle, /* Idle, waiting for jobs */
    ts_work, /* Working */
    ts_term  /* Asked to terminate. */
};


struct qsort {
    void *data;                 /* Base address of data to be sorted */
    int num_elem;               /* Number of elements in `data` */
     
    pthread_t tid;              /* Thread id */
    enum thread_status status;  /* Thread status */
    pthread_mutex_t lock_st;    /* Mutex lock for changing thread state */
    pthread_cond_t cond_st;     /* Condvar for signaling state change */
    struct commoninfo *common;      /* Shared data across threads */
};

/* Common data shared across threads */
struct commoninfo {
    size_t elem_size;
    int forkelem;         /* Minimum number of elements for creating a new thread */
    int nthreads;         /* Total number of threads in pool */
    int idlethreads;      /* Number of idle threads in pool */
    pthread_mutex_t lock_t; /* For thread status management */
    struct qsort *pool;  /* Thread pool */
};

/* 用來擋住 child thread 不要讓他自己工作 */
static int work_wait(struct qsort *work) {
    DEBUG("trying to get a lock...\n");
    verify(pthread_mutex_lock(&work->lock_st));
    DEBUG("hold the lock:%p\n", &work->lock_st);
    while (work->status == ts_idle) {
        DEBUG("release lock:%p and wait for condvar:%p\n", &work->lock_st, &work->cond_st);
        verify(pthread_cond_wait(&work->cond_st, &work->lock_st));
    }    
    int status = work->status;
    DEBUG("got released condvar:%p\n", &work->cond_st);
    verify(pthread_mutex_unlock(&work->lock_st));
    return status;
}

static void work_signal(struct qsort *work) {
    DEBUG("DEBUG\n");
    verify(pthread_mutex_lock(&work->lock_st));
    work->status = ts_work;
    verify(pthread_mutex_unlock(&work->lock_st));

    DEBUG("send signal to condvar:%p\n", &work->cond_st);
    verify(pthread_cond_signal(&work->cond_st));
    
}

/* 用來告知自己已經完成工作, */
static void work_done(struct commoninfo *common, struct qsort *work) {
    verify(pthread_mutex_lock(&common->lock_t));
    common->idlethreads++;
    verify(pthread_mutex_unlock(&common->lock_t));

    verify(pthread_mutex_lock(&work->lock_st));
    DEBUG("set status to idle\n");
    work->status = ts_idle;
    verify(pthread_mutex_unlock(&work->lock_st));
}


static inline char *find_median(char *a, char *b, char *c) {
    return mycmp(a, b) < 0  
                 ? ((mycmp(b, c)) < 0 ? b : (mycmp(a, c) < 0 ? c : a)) 
                 : ((mycmp(b, c)) < 0 ? b : (mycmp(a, c) < 0 ? a : c));
}

/* Allocate an idle thread from pool, change its state, lock its mutex, 
 * and decrease the number of idle threads. Return a pointer to its data area.
 * Return NULL if no thread is available.
 */
struct qsort *allocate_thread(struct commoninfo *common) {
    for (int i = 0; i < common->nthreads; i++) {
        DEBUG("try to allocate a thread\n");
        struct qsort *worker = &common->pool[i];
        if (pthread_mutex_trylock(&worker->lock_st) == 0) {
            if (common->pool[i].status == ts_idle) {
                // verify(pthread_mutex_lock(&common->lock_t));
                // common->idlethreads--;
                // verify(pthread_mutex_unlock(&common->lock_t));
                /* lock thread, make sure it would not execute before data is ready. */
                verify(pthread_mutex_unlock(&worker->lock_st));
                return (&common->pool[i]);
            }
            verify(pthread_mutex_unlock(&worker->lock_st));
        };
    }
    DEBUG("no available thread\n");
    return (NULL);
}

/* Quick sort algorithm */
void qsort_algo(void *arr, int num_elem, size_t elem_size, struct commoninfo *common) {
    char *darray = arr;
    int tmp;
    char *pi, *pj;

start:
    DEBUG("common:%p\n", common);
    /* qsort(3) */
    if (num_elem < 7) { /* switch to bubble sort */
        DEBUG("before bsort\n");
        for (pi = darray + (num_elem - 1) * elem_size; pi > darray; pi -= elem_size) {
            bool done = true;
            for (pj = darray + elem_size; pj <= pi; pj += elem_size) {
                if (mycmp(pj - elem_size, pj) > 0){
                    myswap(pj - elem_size, pj);
                    done = false;
                }
            }
            if (done) return;
        }
        return;
    }
    char *pmed = darray + (num_elem / 2) * elem_size;
    if (num_elem > 40) {
        int piece = num_elem / 9;
        char *pend = darray + elem_size * (num_elem - 1);
        char *tmpa = find_median(darray, darray + piece, darray + piece * 2);
        char *tmpb = find_median(pmed - piece, pmed, pmed + piece);
        char *tmpc = find_median(pend - piece * 2, pend - piece, pend);
        char *pmed = find_median(tmpa, tmpb, tmpc);
    }
    swapcode(DTYPE, darray, pmed, sizeof(DTYPE));

    int pl = 1; 
    int pr = num_elem - 1;

    while (true) {
        while (pl < pr && (tmp = mycmp(darray + pl * elem_size, darray)) <= 0) {
            pl++;
        };
        while (pl < pr && (tmp = mycmp(darray + pr * elem_size, darray)) > 0) {
            pr--;
        };   
        if (pl >= pr) break;
        swapcode(DTYPE, darray + pl * elem_size, darray + pr *elem_size, sizeof(DTYPE));
        pl++;
        pr--;
    };
    if (mycmp(darray + pl * elem_size, darray) < 0)
        pl++;
    swapcode(DTYPE, darray + (pl - 1) * elem_size, darray, sizeof(DTYPE));
    int nl = pl;
    int nr = num_elem - nl;

    /* try to launch new threads */
    if (nl > 1) {
        if (common && nl > common->forkelem) {
            struct qsort *subtask = allocate_thread(common);
            if (subtask) {
                DEBUG("got a idle thread: %lu\n", subtask->tid);
                subtask->data = darray;
                subtask->num_elem = nl;
                /* All data is ready, start qsort */
                verify(pthread_mutex_lock(&common->lock_t));
                common->idlethreads--;
                verify(pthread_mutex_unlock(&common->lock_t));
                work_signal(subtask);
                goto f2;
            }
        }
        DEBUG("recursive qsort_algo\n");
        qsort_algo(darray, nl, elem_size, common);
    }
f2:
    if (nr > 1) {
        darray = darray + nl * elem_size;
        num_elem = nr;
        DEBUG("goto start\n");
        goto start;
    }
}

/* Thread entry point */
void *qsort_thread(void *p) {
    DEBUG("Initialize...\n");
    struct qsort *qdata = p;
    struct commoninfo *common = qdata->common;
again:
    /* Wait for signal */
    if (work_wait(qdata) == ts_term)
        return NULL;
    DEBUG("start qsort_algo!\n");
    qsort_algo(qdata->data, qdata->num_elem, common->elem_size, common);
    DEBUG("finish qsort_algo\n");
    /* job finished, release thread */
    work_done(common, qdata);
    
    /* if there is no working thread(this is the last working thread), then terminate all threads */
    verify(pthread_mutex_lock(&common->lock_t));
    int idle_t = common->idlethreads;
    verify(pthread_mutex_unlock(&common->lock_t));
    
    if (idle_t == common->nthreads) {
        DEBUG("no working threads, terminate all threads\n");
        struct qsort *new_worker;
        for (int i = 0; i < common->nthreads; i++) {
            new_worker = &common->pool[i];
            if (new_worker == qdata) continue;
            verify(pthread_mutex_lock(&new_worker->lock_st));
            new_worker->status = ts_term;
            verify(pthread_cond_signal(&new_worker->cond_st));
            verify(pthread_mutex_unlock(&new_worker->lock_st));
        }
        // qdata->status = ts_term;
        return NULL;
    }
    DEBUG("goto again\n");
    goto again;
};

/* Initailize resources for multi-threading qsort */
void qsort_mt(void *darray, int num_elem, size_t elem_size, int fork_elem, int nthreads) {

    struct commoninfo common;
    struct qsort *worker;
    if (pthread_mutex_init(&common.lock_t, NULL) != 0){
        DEBUG("Cannot init mutex");
        exit(1);
    };
    if ((common.pool = calloc(nthreads, sizeof(struct qsort))) == NULL){
        DEBUG("Cannot init mutex");
        verify(pthread_mutex_destroy(&common.lock_t));
        exit(1);
    };
    for (int i = 0; i < nthreads; i++) {
        worker = &common.pool[i];
        if (pthread_mutex_init(&worker->lock_st, NULL) != 0) {
            DEBUG("unable to init mutex lock, exit");
            exit(1);
        };
        if (pthread_cond_init(&worker->cond_st, NULL) != 0) {
            DEBUG("unable to init condvar, exit");
            exit(1);
        };
        worker->status = ts_idle;
        worker->common = &common;
        if (pthread_create(&worker->tid, NULL, qsort_thread, worker) != 0) {
            DEBUG("unable to fork pthread, exit");
            exit(1);
        };
    };
    common.forkelem = fork_elem;
    common.idlethreads = common.nthreads = nthreads;
    common.elem_size = sizeof(DTYPE);
    // sleep(0.5);
    /* initialize the first work thread */
    worker = &common.pool[0];
    // verify(pthread_mutex_lock(&worker->lock_st));
    DEBUG("get the first worker\n");
    worker->data = darray;
    worker->num_elem = num_elem;
    
    verify(pthread_mutex_lock(&common.lock_t));
    common.idlethreads--;
    verify(pthread_mutex_unlock(&common.lock_t));

    work_signal(worker);
    
    /* start to qsort */
    DEBUG("release the condvar:%p\n", &worker->cond_st);
    verify(pthread_cond_signal(&worker->cond_st));
    // verify(pthread_mutex_unlock(&worker->lock_st));

    DEBUG("waiting all threads...\n");
    for (int i = 0; i < nthreads; i++) {
        worker = &common.pool[i];
        verify(pthread_join(worker->tid, NULL));
    };

    for (int i = 0; i < nthreads; i++) {
        worker = &common.pool[i];
        verify(pthread_mutex_destroy(&worker->lock_st));
        verify(pthread_cond_destroy(&worker->cond_st));
    };
    free(common.pool);

};

void usage(void) {
    fprintf(
        stderr,
        "usage: main [-tm] [-n elements] [-h threads] [-f fork_elements]\n"
        "\t-t\tPrint the execution time\n"
        "\t-m\tEnable multi-threading\n"
        "\t-n\tNumber of elements (Default is 100000)\n"
        "\t-h\tSpecify the number of threads in multi-threading mode (Default is 2)\n"
        "\t-f\tMinimum number of elements for each thread (Default is 100)\n"
    );
    exit(1);
}

int main(int argc, char *argv[]){
    int num_elem = 1000;
    bool opt_mt = false;
    bool opt_time = false;
    int nthreads = 2;
    int fork_elem = 100;
    int arg;
    char *endp;
    while ((arg = getopt(argc, argv, "n:h:f:tm")) != -1) {
        switch (arg) {
            case 't':
                opt_time = true; 
                break;
            case 'm':
                opt_mt = true; 
                break;
            case 'n':
                num_elem = (int) strtol(optarg, &endp, 10);
                if (num_elem <= 0 || *endp != '\0' ) {
                    warnx("Illegal number of option 'n' (got '%s')", optarg);
                    usage();
                }
                break;
            case 'f':
                fork_elem = (int) strtol(optarg, &endp, 10);
                if (fork_elem <= 0 || *endp != '\0' ) {
                    warnx("Illegal number of option 'f' (got '%s')", optarg);
                    usage();
                }
                break;
            case 'h':
                nthreads = (int) strtol(optarg, &endp, 10);
                if (nthreads <= 0 || nthreads > 8 || *endp != '\0' ) {
                    warnx("Illegal argument of option 'm' (got '%s')", optarg);
                    usage();
                }
                break;
            default:
                usage();
        }
    }
    // struct rusage ru;
    DTYPE *darray = malloc(sizeof(DTYPE) * num_elem);
    struct timeval start, end;
    for (int i = 0; i < num_elem; i++)
        darray[i] = rand() % num_elem;
    gettimeofday(&start, NULL);
    if (opt_mt && num_elem > fork_elem) {
        qsort_mt(darray, num_elem, sizeof(DTYPE), fork_elem, nthreads);  
    } else {
        qsort_algo(darray, num_elem, sizeof(DTYPE), NULL);
    }
    gettimeofday(&end, NULL);
    // getrusage(RUSAGE_SELF, &ru);
    if (opt_time)
        printf("%.3g\n", (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1e6);
    int res = check(darray, num_elem, sizeof(DTYPE));
    if (res != -1) {
        printf("sort error at index:%d (current is '%d' and next is '%d')\n", res, darray[res], darray[res+1]);
        exit(1);
    } else {
        printf("success\n");
    }
}
