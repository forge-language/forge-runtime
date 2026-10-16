#define _POSIX_C_SOURCE 200809L
#include "forge_runtime.h"
#include "forge/thread.h"
#include "work_queue.h"
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(c) do { if (!(c)) { fprintf(stderr,"%s:%d: %s\n",__FILE__,__LINE__,#c);exit(1); } } while (0)
enum { ROLE_NONE, ROLE_START, ROLE_STOP, ROLE_BATCH };
static _Thread_local int role, stop_unlocks;
static atomic_int start_waiting, start_returned, stop_released, allow_stop_return;
static atomic_int batch_waiting, batch_returned;
static atomic_int capture_malloc, tracked_frees;
static _Atomic(void *) tracked_allocation;
static int batch_limit = -1;
static _Thread_local int batch_pushes;

void *__real_malloc(size_t);
void __real_free(void *);
void *__wrap_malloc(size_t size) {
    void *p = __real_malloc(size);
    if (atomic_exchange(&capture_malloc, 0)) atomic_store(&tracked_allocation, p);
    return p;
}
void __wrap_free(void *p) {
    void *expected = p;
    if (p && atomic_compare_exchange_strong(&tracked_allocation, &expected, NULL))
        atomic_fetch_add(&tracked_frees, 1);
    __real_free(p);
}
void __real_fr_cond_wait(fr_cond_t *, fr_mutex_t *);
void __wrap_fr_cond_wait(fr_cond_t *c, fr_mutex_t *m) {
    if (role == ROLE_START) atomic_store(&start_waiting, 1);
    if (role == ROLE_BATCH) atomic_store(&batch_waiting, 1);
    __real_fr_cond_wait(c, m);
}
void __real_fr_mutex_unlock(fr_mutex_t *);
void __wrap_fr_mutex_unlock(fr_mutex_t *m) {
    __real_fr_mutex_unlock(m);
    /* Hold stop after its final unlock. A start can now finish; old stop's
     * formerly unlocked global CAS must not erase that new generation. */
    if (role == ROLE_STOP && ++stop_unlocks == 2) {
        atomic_store(&stop_released, 1);
        while (!atomic_load(&allow_stop_return)) fr_thread_yield();
    }
}
int __real_fr_native_queue_try_push_owned(fr_native_queue_t *, fr_native_fn, void *);
int __wrap_fr_native_queue_try_push_owned(fr_native_queue_t *q, fr_native_fn fn, void *arg) {
    if (role == ROLE_BATCH && batch_limit >= 0 && batch_pushes++ >= batch_limit) return 0;
    return __real_fr_native_queue_try_push_owned(q, fn, arg);
}

typedef struct { fr_mutex_t *lock; fr_cond_t *changed; int entered, release; } gate_t;
static gate_t make_gate(void) {
    gate_t g = {fr_mutex_create(), fr_cond_create(), 0, 0};
    CHECK(g.lock && g.changed); return g;
}
static void gated(void *arg) {
    gate_t *g = arg;fr_mutex_lock(g->lock);g->entered++;
    fr_cond_broadcast(g->changed);
    while (!g->release) fr_cond_wait(g->changed,g->lock);
    fr_mutex_unlock(g->lock);
}
static void wait_gate(gate_t *g,int count) {
    fr_mutex_lock(g->lock);
    while (g->entered < count) fr_cond_wait(g->changed,g->lock);
    fr_mutex_unlock(g->lock);
}
static void release_gate(gate_t *g) {
    fr_mutex_lock(g->lock);g->release=1;fr_cond_broadcast(g->changed);fr_mutex_unlock(g->lock);
}
static void free_gate(gate_t *g) {fr_cond_destroy(g->changed);fr_mutex_destroy(g->lock);}
static atomic_int callbacks;
static void counted(void *arg) {(void)arg;atomic_fetch_add(&callbacks,1);}
static int64_t owned(int64_t arg) {atomic_fetch_add(&callbacks,1);return arg;}
static void *ordinary_stop(void *arg) {fr_scheduler_stop(arg);return NULL;}
static void *held_stop(void *arg) {role=ROLE_STOP;stop_unlocks=0;fr_scheduler_stop(arg);role=ROLE_NONE;return NULL;}
static void *start(void *arg) {role=ROLE_START;fr_scheduler_start(arg);atomic_store(&start_returned,1);role=ROLE_NONE;return NULL;}

static void test_start_during_stop(void) {
    atomic_store(&start_waiting,0);atomic_store(&start_returned,0);
    atomic_store(&stop_released,0);atomic_store(&allow_stop_return,0);
    gate_t g=make_gate();fr_scheduler_t *s=fr_scheduler_create(1);CHECK(s);
    fr_scheduler_start(s);CHECK(fr_sched_pool_submit(s,gated,&g)==0);wait_gate(&g,1);
    fr_thread_t *stopping=NULL,*starting=NULL;
    CHECK(fr_thread_start(&stopping,held_stop,s)==0);
    while(fr_sched_pool_available())fr_thread_yield();
    CHECK(fr_thread_start(&starting,start,s)==0);
    while(!atomic_load(&start_waiting)) {CHECK(!atomic_load(&start_returned));fr_thread_yield();}
    CHECK(!fr_sched_pool_available());
    release_gate(&g);
    while(!atomic_load(&start_returned)||!atomic_load(&stop_released))fr_thread_yield();
    CHECK(fr_scheduler_global()==s && fr_sched_pool_available());
    atomic_store(&allow_stop_return,1);
    CHECK(fr_thread_join(stopping)==0);CHECK(fr_thread_join(starting)==0);
    CHECK(fr_scheduler_global()==s && fr_sched_pool_available());
    atomic_store(&callbacks,0);CHECK(fr_sched_pool_submit(s,counted,NULL)==0);
    fr_scheduler_run(s);CHECK(atomic_load(&callbacks)==1);
    fr_scheduler_destroy(s);free_gate(&g);
}

static void test_other_global_scheduler(void) {
    gate_t g=make_gate();fr_scheduler_t *a=fr_scheduler_create(1),*b=fr_scheduler_create(1);CHECK(a&&b);
    fr_scheduler_start(a);CHECK(fr_sched_pool_submit(a,gated,&g)==0);wait_gate(&g,1);
    fr_thread_t *t=NULL;CHECK(fr_thread_start(&t,ordinary_stop,a)==0);
    while(fr_sched_pool_available())fr_thread_yield();
    fr_scheduler_start(b);release_gate(&g);CHECK(fr_thread_join(t)==0);
    CHECK(fr_scheduler_global()==b && fr_sched_pool_available());
    fr_scheduler_destroy(a);CHECK(fr_scheduler_global()==b && fr_sched_pool_available());
    fr_scheduler_destroy(b);free_gate(&g);
}

static void track_next(void) {
    atomic_store(&tracked_allocation,NULL);atomic_store(&tracked_frees,0);atomic_store(&capture_malloc,1);
}
static void test_owned_and_borrowed_discard(void) {
    gate_t g=make_gate();fr_scheduler_t *s=fr_scheduler_create(1);CHECK(s);
    atomic_store(&callbacks,0);fr_scheduler_start(s);
    CHECK(fr_sched_pool_submit(s,gated,&g)==0);wait_gate(&g,1);
    track_next();CHECK(fr_sched_pool_spawn(owned,7)==0);
    CHECK(atomic_load(&tracked_allocation)!=NULL && atomic_load(&tracked_frees)==0);
    char borrowed[32];memset(borrowed,0x5a,sizeof(borrowed));
    CHECK(fr_sched_pool_submit(s,counted,borrowed)==0);
    fr_thread_t *t=NULL;CHECK(fr_thread_start(&t,ordinary_stop,s)==0);
    while(fr_sched_pool_available())fr_thread_yield();
    release_gate(&g);CHECK(fr_thread_join(t)==0);CHECK(fr_sched_pool_queued(s)==2);
    fr_scheduler_destroy(s);
    CHECK(atomic_load(&tracked_frees)==1 && atomic_load(&tracked_allocation)==NULL);
    CHECK(atomic_load(&callbacks)==0);
    for(size_t i=0;i<sizeof(borrowed);i++)CHECK((unsigned char)borrowed[i]==0x5a);
    free_gate(&g);
}

static void test_executed_context_not_discarded_twice(void) {
    fr_scheduler_t *s=fr_scheduler_create(1);CHECK(s);atomic_store(&callbacks,0);
    fr_scheduler_start(s);track_next();CHECK(fr_sched_pool_spawn(owned,9)==0);
    fr_scheduler_run(s);CHECK(atomic_load(&callbacks)==1 && atomic_load(&tracked_frees)==1);
    fr_scheduler_destroy(s);CHECK(atomic_load(&tracked_frees)==1);
}

static gate_t *index_gate;
static int64_t indexed(int64_t id,int64_t total) {
    CHECK(id>=0 && id<total);
    if(index_gate && id==0)gated(index_gate);
    atomic_fetch_add(&callbacks,1);return 0;
}
static void *batch(void *arg) {
    role=ROLE_BATCH;batch_pushes=0;
    fr_sched_pool_spawn_indexed(indexed,*(int *)arg);
    atomic_store(&batch_returned,1);role=ROLE_NONE;return NULL;
}
static void test_indexed_discard(int limit) {
    enum {WORKERS=4, REQUESTED=37};int requested=REQUESTED;
    atomic_store(&batch_waiting,0);atomic_store(&batch_returned,0);atomic_store(&callbacks,0);
    batch_limit=limit;gate_t g=make_gate();fr_scheduler_t *s=fr_scheduler_create(WORKERS);CHECK(s);
    for(int i=0;i<WORKERS;i++)CHECK(fr_sched_pool_submit(s,gated,&g)==0);
    fr_scheduler_start(s);wait_gate(&g,WORKERS);
    fr_thread_t *caller=NULL;CHECK(fr_thread_start(&caller,batch,&requested)==0);
    if(limit==0) {while(!atomic_load(&batch_returned))fr_thread_yield();}
    else {while(!atomic_load(&batch_waiting)) {CHECK(!atomic_load(&batch_returned));fr_thread_yield();}}
    size_t accepted=limit<0?REQUESTED:(size_t)limit;
    CHECK(fr_sched_pool_queued(s)==accepted);
    fr_thread_t *t=NULL;CHECK(fr_thread_start(&t,ordinary_stop,s)==0);
    while(fr_sched_pool_available())fr_thread_yield();
    release_gate(&g);CHECK(fr_thread_join(t)==0);
    if(accepted)CHECK(!atomic_load(&batch_returned));
    /* The cond-wait marker proves publication has ended. The waiter now uses
     * only batch state, so terminal disposal may release it without running fn. */
    fr_scheduler_destroy(s);CHECK(fr_thread_join(caller)==0);
    CHECK(atomic_load(&batch_returned) && atomic_load(&callbacks)==0);
    batch_limit=-1;free_gate(&g);
}

static void test_partly_executed_indexed_batch(void) {
    int requested=37;
    atomic_store(&batch_waiting,0);atomic_store(&batch_returned,0);atomic_store(&callbacks,0);
    gate_t g=make_gate();index_gate=&g;
    fr_scheduler_t *s=fr_scheduler_create(1);CHECK(s);fr_scheduler_start(s);
    fr_thread_t *caller=NULL;CHECK(fr_thread_start(&caller,batch,&requested)==0);
    wait_gate(&g,1);
    while(!atomic_load(&batch_waiting))fr_thread_yield();
    fr_thread_t *t=NULL;CHECK(fr_thread_start(&t,ordinary_stop,s)==0);
    while(fr_sched_pool_available())fr_thread_yield();
    release_gate(&g);CHECK(fr_thread_join(t)==0);
    CHECK(atomic_load(&callbacks)==1 && fr_sched_pool_queued(s)==36);
    CHECK(!atomic_load(&batch_returned));
    fr_scheduler_destroy(s);CHECK(fr_thread_join(caller)==0);
    CHECK(atomic_load(&batch_returned) && atomic_load(&callbacks)==1);
    index_gate=NULL;free_gate(&g);
}

int main(int argc,char **argv) {
    if(argc==2 && !strcmp(argv[1],"owned")) {
        test_owned_and_borrowed_discard();puts("owned cleanup passed");return 0;
    }
    for(int i=0;i<50;i++)test_start_during_stop();
    test_other_global_scheduler();
    test_owned_and_borrowed_discard();
    test_executed_context_not_discarded_twice();
    test_indexed_discard(-1);test_indexed_discard(5);test_indexed_discard(0);
    test_partly_executed_indexed_batch();
    puts("lifecycle tests passed");return 0;
}
