#define _GNU_SOURCE
#include "forge_runtime.h"
#include "forge/thread.h"
#include <pthread.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr,"%s:%d: %s\n",__FILE__,__LINE__,#x); exit(1); } } while (0)
typedef struct { void *p; size_t size; int live; } allocation_t;
typedef struct { void *p; int kind, valid; } primitive_t;
static allocation_t allocations[256];
static primitive_t primitives[256];
static int alloc_count, primitive_count, live, fd_live, owned_fd=-1;
static int capturing, allocation_calls, mutex_calls, cond_calls;
static int fail_allocation, fail_mutex, fail_cond, fail_epoll, injected;
static _Thread_local int observer;
void *__real_malloc(size_t);
void *__real_calloc(size_t,size_t);
void *__real_realloc(void *,size_t);
void __real_free(void *);
int __real_pthread_mutex_init(pthread_mutex_t *,const pthread_mutexattr_t *);
int __real_pthread_mutex_destroy(pthread_mutex_t *);
int __real_pthread_cond_init(pthread_cond_t *,const pthread_condattr_t *);
int __real_pthread_cond_destroy(pthread_cond_t *);
int __real_epoll_create1(int);
int __real_close(int);
static int find(void *p) {
    for(int i=alloc_count-1;i>=0;i--) if(allocations[i].live&&allocations[i].p==p)return i;
    return -1;
}
static void record(void *p,size_t bytes) {
    if(!p)return;
    CHECK(alloc_count<256);
    allocations[alloc_count++]=(allocation_t){p,bytes,1};live++;
}
static int allocation_fails(void) {
    if(observer&&capturing&&++allocation_calls==fail_allocation){injected++;return 1;}
    return 0;
}
void *__wrap_malloc(size_t bytes) {
    if(allocation_fails())return NULL;
    void *p=__real_malloc(bytes);if(observer&&capturing)record(p,bytes);return p;
}
void *__wrap_calloc(size_t n,size_t bytes) {
    if(allocation_fails())return NULL;
    void *p=__real_calloc(n,bytes);if(observer&&capturing)record(p,n*bytes);return p;
}
void __wrap_free(void *p) {
    if(observer&&p){
        int i=find(p);
        if(i>=0){
            uintptr_t start=(uintptr_t)p;
            for(int j=0;j<primitive_count;j++)
                CHECK(!primitives[j].valid||(uintptr_t)primitives[j].p<start||
                      (uintptr_t)primitives[j].p-start>=allocations[i].size);
            allocations[i].live=0;live--;
        }
    }
    __real_free(p);
}
static int register_primitive(void *p,int kind,int result) {
    if(observer&&capturing){
        CHECK(primitive_count<256);
        primitives[primitive_count++]=(primitive_t){p,kind,result==0};
    }
    return result;
}
int __wrap_pthread_mutex_init(pthread_mutex_t *p,const pthread_mutexattr_t *a) {
    int result;
    if(observer&&capturing&&++mutex_calls==fail_mutex){injected++;result=EAGAIN;}
    else result=__real_pthread_mutex_init(p,a);
    return register_primitive(p,1,result);
}
int __wrap_pthread_cond_init(pthread_cond_t *p,const pthread_condattr_t *a) {
    int result;
    if(observer&&capturing&&++cond_calls==fail_cond){injected++;result=EAGAIN;}
    else result=__real_pthread_cond_init(p,a);
    return register_primitive(p,2,result);
}
static void retire_primitive(void *p,int kind) {
    if(!observer)return;
    for(int i=primitive_count-1;i>=0;i--)if(primitives[i].p==p&&primitives[i].kind==kind){
        CHECK(primitives[i].valid);primitives[i].valid=0;return;
    }
}
int __wrap_pthread_mutex_destroy(pthread_mutex_t *p) {
    retire_primitive(p,1);
    int result=__real_pthread_mutex_destroy(p);CHECK(result==0);return result;
}
int __wrap_pthread_cond_destroy(pthread_cond_t *p) {
    retire_primitive(p,2);
    int result=__real_pthread_cond_destroy(p);CHECK(result==0);return result;
}
int __wrap_epoll_create1(int flags) {
    if(observer&&capturing&&fail_epoll){injected++;errno=EMFILE;return -1;}
    int fd=__real_epoll_create1(flags);
    if(observer&&capturing&&fd>=0){CHECK(!fd_live);owned_fd=fd;fd_live=1;}
    return fd;
}
int __wrap_close(int fd) {
    int tracked=observer&&fd_live&&fd==owned_fd;
    int result=__real_close(fd);
    if(tracked){CHECK(result==0);fd_live=0;owned_fd=-1;}
    return result;
}
static void reset(void) {
    CHECK(!live&&!fd_live);
    for(int i=0;i<primitive_count;i++)CHECK(!primitives[i].valid);
    memset(allocations,0,sizeof(allocations));memset(primitives,0,sizeof(primitives));
    alloc_count=primitive_count=allocation_calls=mutex_calls=cond_calls=0;
    fail_allocation=fail_mutex=fail_cond=fail_epoll=injected=0;
}
static fr_arena_t *sentinel;
static fr_arena_t *sentinel_provider(void){return sentinel;}
enum factory { MUTEX,COND,PROCESS,SUPERVISOR,SCHEDULER };
static void *create(enum factory f) {
    if(f==MUTEX)return fr_mutex_create();
    if(f==COND)return fr_cond_create();
    if(f==PROCESS)return fr_process_create("factory-process");
    if(f==SUPERVISOR)return fr_supervisor_create("factory-supervisor",FR_RESTART_ALL);
    return fr_scheduler_create(3);
}
static void destroy(enum factory f,void *p) {
    if(f==MUTEX)fr_mutex_destroy(p);
    else if(f==COND)fr_cond_destroy(p);
    else if(f==SCHEDULER)fr_scheduler_destroy(p);
    else fr_process_destroy(p);
}
static void one_failure(enum factory f,int allocation,int mutex,int cond,int epoll) {
    reset();fail_allocation=allocation;fail_mutex=mutex;fail_cond=cond;fail_epoll=epoll;
    if(f==SCHEDULER)fr_arena_set_coro_provider(sentinel_provider);
    capturing=1;void *p=create(f);capturing=0;
    CHECK(injected==1&&p==NULL&&live==0&&fd_live==0);
    for(int i=0;i<primitive_count;i++)CHECK(!primitives[i].valid);
    if(f==SCHEDULER)CHECK(fr_arena_tls()==sentinel);
}
static void sweep(enum factory f) {
    reset();capturing=1;void *p=create(f);capturing=0;CHECK(p);
    int a=allocation_calls,m=mutex_calls,c=cond_calls;
    destroy(f,p);CHECK(!live&&!fd_live);
    for(int i=1;i<=a;i++)one_failure(f,i,0,0,0);
    for(int i=1;i<=m;i++)one_failure(f,0,i,0,0);
    for(int i=1;i<=c;i++)one_failure(f,0,0,i,0);
    if(f==SCHEDULER)one_failure(f,0,0,0,1);
    reset();capturing=1;p=create(f);capturing=0;CHECK(p);destroy(f,p);CHECK(!live&&!fd_live);
}
/* Observe realloc arguments/results instead of the private supervisor layout. */
static int grow_calls, fail_grow;
static void *last_array, *old_array;
static size_t retry_bytes;
void *__wrap_realloc(void *p,size_t bytes) {
    if(observer&&fail_grow){
        grow_calls++;
        if(grow_calls==2){injected++;old_array=p;return NULL;}
        if(grow_calls==3){CHECK(p==old_array);retry_bytes=bytes;}
    }
    int old=observer?find(p):-1;
    void *next=__real_realloc(p,bytes);
    if(observer&&next){
        if(old>=0){allocations[old].live=0;live--;}
        if(capturing)record(next,bytes);
        last_array=next;
    }
    return next;
}
static void child_growth(void) {
    reset();capturing=1;
    fr_process_t *parent=fr_supervisor_create("parent",FR_RESTART_ALL);
    fr_process_t *a=fr_process_create("a"),*b=fr_process_create("b"),*c=fr_process_create("c");
    CHECK(parent&&a&&b&&c);fail_grow=1;grow_calls=0;
    fr_supervisor_add_child(parent,a);fr_supervisor_add_child(parent,b);fr_supervisor_add_child(parent,c);
    CHECK(injected==1&&grow_calls==3&&retry_bytes==2*sizeof(fr_process_t *));
    CHECK(((fr_process_t **)last_array)[0]==a&&((fr_process_t **)last_array)[1]==c);
    fail_grow=0;capturing=0;fr_process_destroy(parent);
    CHECK(!strcmp(fr_process_name(a),"a")&&!strcmp(fr_process_name(b),"b")&&!strcmp(fr_process_name(c),"c"));
    fr_process_destroy(a);fr_process_destroy(b);fr_process_destroy(c);CHECK(!live&&!fd_live);
}
static void count_job(void *p){(*(int *)p)++;}
static void known_inventory(void) {
    reset();capturing=1;fr_scheduler_t *s=fr_scheduler_create(2);capturing=0;CHECK(s);
    int done=0;CHECK(fr_sched_pool_submit(s,count_job,&done)==0);CHECK(fr_sched_pool_queued(s)==1);
    fr_scheduler_run(s);CHECK(done==1&&fr_sched_pool_queued(s)==0);fr_scheduler_destroy(s);CHECK(!live&&!fd_live);
}
static void foreign_owner(void) {
    reset();fr_scheduler_t *other=fr_scheduler_create(1);CHECK(other);fr_scheduler_start(other);
    fail_mutex=3;capturing=1;fr_scheduler_t *bad=fr_scheduler_create(3);capturing=0;
    CHECK(!bad&&injected==1&&!live&&!fd_live&&fr_scheduler_global()==other&&fr_sched_pool_available());
    int done=0;CHECK(fr_sched_pool_submit(other,count_job,&done)==0);fr_scheduler_run(other);CHECK(done==1);fr_scheduler_destroy(other);
}
int main(void) {
    observer=1;sentinel=fr_arena_create(128);CHECK(sentinel);
    for(enum factory f=MUTEX;f<=SCHEDULER;f++)sweep(f);
    child_growth();known_inventory();foreign_owner();reset();fr_arena_set_coro_provider(NULL);fr_arena_destroy(sentinel);
    return 0;
}
