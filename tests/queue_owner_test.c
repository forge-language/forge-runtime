#define _POSIX_C_SOURCE 200809L
#include "forge_runtime.h"
#include "forge/thread.h"
#include "work_queue.h"
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#define CHECK(x) do {if(!(x)){fprintf(stderr,"%s:%d: %s\n",__FILE__,__LINE__,#x);exit(1);}} while(0)
/* Registration happens before workers/producers start, and is immutable until
 * they join. Only real mutex events determine whether a caller owns the guard. */
static fr_mutex_t *owner, *private_locks[64];
static int private_count;
static _Thread_local int constructing;
static _Thread_local fr_mutex_t *held[16];
static _Thread_local int held_count;
static atomic_int owned_calls[6], inner_locks, malloc_failures;
static _Thread_local int fail_malloc;
fr_mutex_t *__real_fr_mutex_create(void);
fr_mutex_t *__wrap_fr_mutex_create(void) {
    fr_mutex_t *m=__real_fr_mutex_create();
    if(constructing&&m){
        if(!owner)owner=m;
        else {CHECK(private_count<64);private_locks[private_count++]=m;}
    }
    return m;
}
static int is_held(fr_mutex_t *m){for(int i=0;i<held_count;i++)if(held[i]==m)return 1;return 0;}
static void acquire(fr_mutex_t *m){CHECK(m&&!is_held(m)&&held_count<16);held[held_count++]=m;}
static void release(fr_mutex_t *m){
    for(int i=0;i<held_count;i++)if(held[i]==m){held[i]=held[--held_count];return;}
    CHECK(0);
}
void __real_fr_mutex_lock(fr_mutex_t *);
void __wrap_fr_mutex_lock(fr_mutex_t *m) {
    __real_fr_mutex_lock(m);if(!m)return;
    acquire(m);
    for(int i=0;i<private_count;i++)if(private_locks[i]==m)atomic_fetch_add(&inner_locks,1);
}
void __real_fr_mutex_unlock(fr_mutex_t *);
void __wrap_fr_mutex_unlock(fr_mutex_t *m){if(m)release(m);__real_fr_mutex_unlock(m);}
void __real_fr_cond_wait(fr_cond_t *,fr_mutex_t *);
void __wrap_fr_cond_wait(fr_cond_t *c,fr_mutex_t *m) {
    CHECK(is_held(m));release(m);__real_fr_cond_wait(c,m);acquire(m);
}
void *__real_malloc(size_t);
void *__wrap_malloc(size_t n) {
    if(fail_malloc){fail_malloc=0;atomic_fetch_add(&malloc_failures,1);return NULL;}
    return __real_malloc(n);
}
static void guard(fr_mutex_t *queue_lock,int operation) {
    int registered=0;
    for(int i=0;i<private_count;i++)if(private_locks[i]==queue_lock)registered=1;
    CHECK(registered&&is_held(owner)&&!is_held(queue_lock));
    atomic_fetch_add(&owned_calls[operation],1);
}
int __real_fr_run_queue_try_push_owned(fr_run_queue_t *,fr_coro_t *);
int __wrap_fr_run_queue_try_push_owned(fr_run_queue_t *q,fr_coro_t *c) {
    guard(q->lock,0);return __real_fr_run_queue_try_push_owned(q,c);
}
fr_coro_t *__real_fr_run_queue_pop_owned(fr_run_queue_t *);
fr_coro_t *__wrap_fr_run_queue_pop_owned(fr_run_queue_t *q) {
    guard(q->lock,1);return __real_fr_run_queue_pop_owned(q);
}
fr_coro_t *__real_fr_run_queue_steal_owned(fr_run_queue_t *);
fr_coro_t *__wrap_fr_run_queue_steal_owned(fr_run_queue_t *q) {
    guard(q->lock,2);return __real_fr_run_queue_steal_owned(q);
}
int __real_fr_native_queue_try_push_owned(fr_native_queue_t *,fr_native_fn,void *);
int __wrap_fr_native_queue_try_push_owned(fr_native_queue_t *q,fr_native_fn fn,void *arg) {
    guard(q->lock,3);return __real_fr_native_queue_try_push_owned(q,fn,arg);
}
fr_native_fn __real_fr_native_queue_pop_owned(fr_native_queue_t *,void **);
fr_native_fn __wrap_fr_native_queue_pop_owned(fr_native_queue_t *q,void **arg) {
    guard(q->lock,4);return __real_fr_native_queue_pop_owned(q,arg);
}
fr_native_fn __real_fr_native_queue_steal_owned(fr_native_queue_t *,void **);
fr_native_fn __wrap_fr_native_queue_steal_owned(fr_native_queue_t *q,void **arg) {
    guard(q->lock,5);return __real_fr_native_queue_steal_owned(q,arg);
}
/* Failed first worker makes owner-queue steals deterministic, for both kinds. */
static int skip_first, start_calls;
int __real_fr_thread_start(fr_thread_t **,fr_thread_fn,void *);
int __wrap_fr_thread_start(fr_thread_t **out,fr_thread_fn fn,void *arg) {
    if(skip_first&&start_calls++==0)return -1;
    return __real_fr_thread_start(out,fn,arg);
}
typedef struct {atomic_int seen;} item_t;
static atomic_int completed, callback_unlocked;
static fr_scheduler_t *scheduler;
static fr_process_t *process;
static void count(void *arg) {
    item_t *item=arg;CHECK(!is_held(owner));
    CHECK(atomic_fetch_add(&item->seen,1)==0);atomic_fetch_add(&completed,1);
    CHECK(fr_sched_pool_queued(scheduler)<10000);
    atomic_fetch_add(&callback_unlocked,1);
}
typedef struct {item_t *item;int remaining;} state_t;
static fr_coro_status_t step(fr_coro_t *c,void *arg) {
    state_t *s=arg;CHECK(!is_held(owner));
    if(s->remaining-- > 0)return fr_yield(c);
    count(s->item);return FR_CORO_DONE;
}
static void spawn(item_t *item,int yields) {
    state_t *state=malloc(sizeof(*state));CHECK(state);*state=(state_t){item,yields};
    CHECK(fr_coro_spawn(process,step,state,sizeof(*state)));
}
static void *publish(void *arg) {
    item_t *items=arg;
    for(int i=0;i<1000;i++){CHECK(fr_sched_pool_submit(scheduler,count,&items[2*i])==0);spawn(&items[2*i+1],i%100==0?2001:2);}
    return NULL;
}
static fr_scheduler_t *create(int workers) {
    owner=NULL;private_count=0;constructing=1;
    fr_scheduler_t *s=fr_scheduler_create(workers);constructing=0;
    CHECK(s&&owner&&private_count==2*workers);return s;
}
static void exercise(int workers) {
    item_t *items=calloc(4010,sizeof(*items));CHECK(items);
    for(int i=0;i<4010;i++)atomic_init(&items[i].seen,0);
    scheduler=create(workers);process=fr_process_create("owner-contract");CHECK(process);
    fr_scheduler_add_process(scheduler,process);
    atomic_store(&completed,0);atomic_store(&callback_unlocked,0);
    /* Publication allocation failures must not publish a node/count. */
    fail_malloc=1;CHECK(fr_sched_pool_submit(scheduler,count,&items[4000])==-1);
    CHECK(!fail_malloc&&fr_sched_pool_queued(scheduler)==0);
    state_t *rejected=malloc(sizeof(*rejected));CHECK(rejected);*rejected=(state_t){&items[4001],0};
    fail_malloc=1;CHECK(fr_coro_spawn(process,step,rejected,sizeof(*rejected)));
    CHECK(!fail_malloc); /* queue failure owns an inert terminal coroutine */
    for(int i=0;i<workers;i++)CHECK(fr_sched_pool_submit(scheduler,count,&items[4002+i])==0);
    spawn(&items[4009],0);
    /* Queue zero is nonempty; first failed worker forces both steal functions. */
    int run_steals=atomic_load(&owned_calls[2]), native_steals=atomic_load(&owned_calls[5]);
    skip_first=workers>1;start_calls=0;
    fr_scheduler_run(scheduler);skip_first=0;
    if(workers>1)CHECK(atomic_load(&owned_calls[2])>run_steals&&atomic_load(&owned_calls[5])>native_steals);
    for(int i=0;i<workers;i++)CHECK(atomic_load(&items[4002+i].seen)==1);
    CHECK(atomic_load(&items[4009].seen)==1);
    /* Stop/restart and concurrent producer/consumer phases use the same queues. */
    fr_scheduler_start(scheduler);
    fr_thread_t *a=NULL,*b=NULL;
    CHECK(fr_thread_start(&a,publish,items)==0);CHECK(fr_thread_start(&b,publish,items+2000)==0);
    CHECK(fr_thread_join(a)==0&&fr_thread_join(b)==0);
    fr_scheduler_run(scheduler);
    CHECK(atomic_load(&completed)==4001+workers&&atomic_load(&callback_unlocked)==4001+workers);
    for(int i=0;i<4000;i++)CHECK(atomic_load(&items[i].seen)==1);
    CHECK(!atomic_load(&items[4000].seen)&&!atomic_load(&items[4001].seen));
    CHECK(fr_sched_pool_queued(scheduler)==0&&atomic_load(&inner_locks)==0);
    fr_scheduler_destroy(scheduler);CHECK(held_count==0);free(items);
}
int main(void) {
    exercise(1);exercise(4);
    for(int i=0;i<6;i++)CHECK(atomic_load(&owned_calls[i])>0);
    CHECK(atomic_load(&inner_locks)==0);
    CHECK(atomic_load(&malloc_failures)==4);
    printf("owned queue calls:");for(int i=0;i<6;i++)printf(" %d",atomic_load(&owned_calls[i]));
    printf("; private mutex locks: %d\n",atomic_load(&inner_locks));return 0;
}
