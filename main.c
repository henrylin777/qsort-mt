#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <sys/time.h>
#include <unistd.h>
#include <err.h>


#define DTYPE int
#define DEBUG(format, ...) printf("[ %s:%d ] "format"", __FUNCTION__, __LINE__ , ##__VA_ARGS__)

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


static inline char *find_median(char *a, char *b, char *c) {
    return mycmp(a, b) < 0  
                 ? ((mycmp(b, c)) < 0 ? b : (mycmp(a, c) < 0 ? c : a)) 
                 : ((mycmp(b, c)) < 0 ? b : (mycmp(a, c) < 0 ? a : c));
}

void print_array(void *base, int nums) {
    printf("Array: [");
    for (char *i = (char *) base; i < (char *) base + nums * sizeof(DTYPE); i += sizeof(DTYPE)) {
        printf(" %d", *(DTYPE *)i);
    };
    printf(" ]\n");
}

int load_testcase(char **darray) {
    int buf[256];
    int data;
    int cnt = 0;
    FILE *ptr = fopen("testcase1.txt", "r");
    while(fscanf(ptr, "%d", &data) == 1) {
        buf[cnt] = data;
        cnt++;
    }
    DTYPE *arr = malloc(sizeof(DTYPE) * cnt);
    for (int i = 0; i < cnt; i++) {
        arr[i] = buf[i];
    };
    *darray = (char *)arr;
    return cnt;
}

/* Allocate an idle thread from pool, change its state, lock its mutex, 
 * and decrease the number of idle threads. Return a pointer to its data area.
 * Return NULL if no thread is available.
 */
struct qsort *allocate_thread(struct commoninfo *common) {
    verify(pthread_mutex_lock(&common->lock_t));
    for (int i = 0; i < common->nthreads; i++) {
        if (common->pool[i].status == ts_idle) {
            common->idlethreads--;
            common->pool[i].status = ts_work;
            /* lock thread, make sure it would not execute before data is ready. */
            verify(pthread_mutex_lock(&common->pool[i].lock_st));
            verify(pthread_mutex_unlock(&common->lock_t));
            return (&common->pool[i]);
        }
    }
    verify(pthread_mutex_unlock(&common->lock_t));
    return (NULL);
}

/* Quick sort algorithm */
void qsort_algo(void *arr, int num_elem, size_t elem_size, struct commoninfo *common) {
    char *darray = arr;
    int tmp;
    char *pi, *pj;

start:
    // printf("num of element: %d\n", num_elem);
    /* qsort(3) */
    if (num_elem < 7) { /* switch to bubble sort */
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
    // printf("debug\n");
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

    /* todo: what if pivot is the minimum？*/
    int nl = pl;
    int nr = num_elem - nl;

    /* try to launch new threads */
    if (common && nl > common->forkelem) {
        struct qsort *subtask = allocate_thread(common);
        if (subtask) {
            subtask->data = darray;
            subtask->num_elem = nl;
            /* All data is ready, start qsort */
            verify(pthread_cond_signal(&subtask->cond_st));
            verify(pthread_mutex_unlock(&subtask->lock_st));
        }
    } else if (nl > 1) {
        qsort_algo(darray, nl, elem_size, NULL);
    }
    if (nr > 1) {
        darray = darray + nl * elem_size;
        num_elem = nr;
        goto start;
    }
}

/* Thread entry point */
void *qsort_thread(void *p) {
    struct qsort *qdata = p;
    struct commoninfo *common = qdata->common;
again:
    /* Wait for signal */
    verify(pthread_mutex_lock(&qdata->lock_st));
    while (qdata->status == ts_idle)
        verify(pthread_cond_wait(&qdata->cond_st, &qdata->lock_st));
    verify(pthread_mutex_unlock(&qdata->lock_st));
    // todo: who change the status to ts_term ????
    if (qdata->status == ts_term)
        return NULL;
    
    // todo: needed?
    assert(qdata->status == ts_work);
    qsort_algo(qdata->data, qdata->num_elem, common->elem_size, common);

    /* finish the job, release the thread */
    verify(pthread_mutex_lock(&common->lock_t));
    qdata->status == ts_idle;
    common->idlethreads++;
    /* if there is no working thread(this is the last working thread), then terminate all threads */
    if (common->idlethreads == common->nthreads) {
        struct qsort *new_worker;
        for (int i = 0; i < common->nthreads; i++) {
            new_worker = &common->pool[i];
            if (new_worker == qdata)
                continue;
            verify(pthread_mutex_lock(&new_worker->lock_st));
            // todo: why doing this ???
            new_worker->status = ts_term;
            verify(pthread_cond_signal(&new_worker->cond_st));
            verify(pthread_mutex_unlock(&new_worker->lock_st));
        }
        verify(pthread_mutex_unlock(&common->lock_t));
        return NULL;
    }
    verify(pthread_mutex_unlock(&common->lock_t));
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

    /* initialize the first work thread */
    worker = &common.pool[0];
    verify(pthread_mutex_lock(&worker->lock_st));
    worker->data = darray;
    worker->num_elem = num_elem;
    worker->status = ts_work;
    common.idlethreads--;
    /* start to qsort */
    verify(pthread_cond_signal(&worker->cond_st));
    verify(pthread_mutex_unlock(&worker->lock_st));


    for (int i = 0; i < nthreads; i++) {
        worker = &common.pool[i];
        verify(pthread_join(worker->tid, NULL));
        verify(pthread_mutex_destroy(&worker->lock_st));
        verify(pthread_cond_destroy(&worker->cond_st));
    };
    free(common.pool);

};



void check(void *darray, int num_elem, size_t elem_size) {

    int idx = 0;
    DTYPE cur, next;
    while(idx < num_elem - 1) {
        cur = *((DTYPE *) darray + idx);
        next = *((DTYPE *) darray + idx + 1);
        if(cur > next){
            printf("Sort error at index:%d (data is '%d' and next is '%d')\n", idx, cur, next);
            return;
        }
        idx++;
    }
    printf("correct\n");
    return;
}

void usage(void) {
    fprintf(
        stderr,
        "usage: main [-n elements] [-m threads] [-f fork_elements]\n"
        "\t-m\tEnable multi-threading and specify the number of threads(Default is 2)\n"
        "\t-n\tNumber of elements (Default is 100000)\n"
        "\t-f\tMinimum number of elements for each thread (Default is 100)\n"
    );
    exit(1);
}

int main(int argc, char *argv[]){
    // char *darray;
    // int num_elem = load_testcase(&darray);
    // exit(1);
    int num_elem = 100000000;
    bool use_mt = false;
    int nthreads = 0;
    int fork_elem = 100;
    int arg;
    char *endp;
    while ((arg = getopt(argc, argv, "n:m:f:")) != -1) {
        switch (arg) {
            case 'm':
                use_mt = true; 
                nthreads = (int) strtol(optarg, &endp, 10);
                if (nthreads <= 0 || nthreads > 8 || *endp != '\0' ) {
                    warnx("Illegal argument of option 'm' (got '%s')", optarg);
                    usage();
                }
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
            default:
                usage();
        }
    }
    // struct rusage ru;
    DTYPE *darray = malloc(sizeof(DTYPE) * num_elem);
    for (int i = 0; i < num_elem; i++) {
        darray[i] = rand() % num_elem;
    }

    struct timeval start, end;
    gettimeofday(&start, NULL);
    if (use_mt && num_elem > fork_elem) {
        qsort_mt(darray, num_elem, sizeof(DTYPE), fork_elem, nthreads);  
    } else {
        qsort_algo(darray, num_elem, sizeof(DTYPE), NULL);
    }
    gettimeofday(&end, NULL);
    // getrusage(RUSAGE_SELF, &ru);
    check(darray, num_elem, sizeof(DTYPE));
    printf(
        "%.3g\n",
        (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1e6);
}
