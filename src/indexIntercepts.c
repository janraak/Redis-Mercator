#include "version.h"

#include "rxSuite.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define REDISMODULE_EXPERIMENTAL_API
#include <sched.h>
#include <signal.h>

#undef _GNU_SOURCE
#undef _DEFAULT_SOURCE
#undef _LARGEFILE_SOURCE
#undef LL_RAW
#include "server.h"
#include <ctype.h>
#include <locale.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
}
#endif

#include "indexIntercepts.h"

extern indexerThread index_info;
extern void *rxStashCommand2(SimpleQueue *ctx, const char *command, int argt, int argc, void **args);

extern struct redisServer server;

indexerCallBack *pre_op_indexer_callback = NULL;
indexerCallBack *post_op_indexer_callback = NULL;

static int __interceptors_installed = 0;
rax *InterceptedRedisCommandProc = NULL;

redisCommandProc **standard_command_procs = NULL;

void setCommandIntercept(client *c);
void setnxCommandIntercept(client *c);
void setexCommandIntercept(client *c);
void psetexCommandIntercept(client *c);
void appendCommandIntercept(client *c);
void delCommandIntercept(client *c);
void hsetCommandIntercept(client *c);
void hsetnxCommandIntercept(client *c);
void hmsetCommandIntercept(client *c);
void hdelCommandIntercept(client *c);
void msetCommandIntercept(client *c);
void msetnxCommandIntercept(client *c);
void moveCommandIntercept(client *c);
void renameCommandIntercept(client *c);
void renamenxCommandIntercept(client *c);
void getsetCommandIntercept(client *c);
void infoCommandIntercept(client *c);
void genericCommandIntercept(client *c);
void selectCommandIntercept(client *c);
void xaddCommandIntercept(client *c);
void xdelCommandIntercept(client *c);
void touchCommandIntercept(client *c);
void flushdbCommandIntercept(client *c);
void flushallCommandIntercept(client *c);

#if REDIS_VERSION_NUM < 0x00060200
#define INIT_CMD_STATS 0, 0
#else
#define INIT_CMD_STATS 0, 0, 0, 0
#endif

typedef struct interceptions
{
    struct __interceptions *next;
    struct redisCommandInterceptRule *rules;
    redisCommandProc *standard_command_procs;
    rax *index;
} interceptions;

struct redisCommandInterceptRule interceptorCommandTable[] = {
/* Note that we can't flag set as fast, since it may perform an
 * implicit DEL of a large key. */
#define SET_INTERCEPT 0
    {"set", setCommandIntercept, 0},
#define SETNX_INTERCEPT 1
    {"setnx", setCommandIntercept, 0},
#define SETEX_INTERCEPT 2
    {"setex", setexCommandIntercept, 0},
#define PSETEX_INTERCEPT 3
    {"psetex", psetexCommandIntercept, 0},
#define APPEND_INTERCEPT 4
    {"append", setCommandIntercept, 0},
#define DEL_INTERCEPT 5
    {"del", delCommandIntercept, 0},
#define HSET_INTERCEPT 6
    {"hset", hsetCommandIntercept, 0},
#define HSETNX_INTERCEPT 7
    {"hsetnx", hsetnxCommandIntercept, 0},
#define HMSET_INTERCEPT 8
    {"hmset", hsetCommandIntercept, 0},
#define HDEL_INTERCEPT 9
    {"hdel", hdelCommandIntercept, 0},
#define GETSET_INTERCEPT 10
    {"getset", getsetCommandIntercept, 0},
#define MSET_INTERCEPT 11
    {"mset", msetCommandIntercept, 0},
#define MSETNX_INTERCEPT 12
    {"msetnx", msetnxCommandIntercept, 0},
#define MOVE_INTERCEPT 13
    {"move", moveCommandIntercept, 0},
/* Like for SET, we can't mark rename as a fast command because
 * overwriting the target key may result in an implicit slow DEL. */
#define RENAME_INTERCEPT 14
    {"rename", renameCommandIntercept, 0},
#define RENAMENX_INTERCEPT 15
    {"renamenx", renamenxCommandIntercept, 0},
#define FLUSHALL_INTERCEPT 16
    {"flushdb", flushdbCommandIntercept, 0},
#define FLUSHDB_INTERCEPT 17
    {"flushall", flushallCommandIntercept, 0},
#define BGSAVE_INTERCEPT 18
    {"NObgsave", genericCommandIntercept, 0},
#define INFO_INTERCEPT 19
    {"noinfo", infoCommandIntercept, 0},
#define SELECT_INTERCEPT 20
    {"select", selectCommandIntercept, 0},
#define SWAPDB_INTERCEPT 21
    {"NOswapdb", genericCommandIntercept, 0},
#define XADD_INTERCEPT 22
    {"xadd", xaddCommandIntercept, 0},
#define XDEL_INTERCEPT 23
    {"xdel", xdelCommandIntercept, 0},
#define TOUCH_INTERCEPT 24
    {"NOtouch", touchCommandIntercept, 0}};

void *getIndexInterceptRules()
{
    return interceptorCommandTable;
}

void *getTriggerInterceptRules()
{
    return NULL;
}

void freeIndexingRequest(void *kfv)
{
    if (kfv == NULL)
        return;
    // TOD: Fix bug!!!
    // // Free key, fields and values
    // for (int j = 0; kfv[j] != NULL; j++)
    // {
    //     sdsfree(kfv[j]);
    // }
    // RedisModule_Free(kfv);
}

void freeCompletedRequests()
{
    while (canDequeueSimpleQueue(index_info.key_indexing_respone_queue))
    {
        sds *index_request = dequeueSimpleQueue(index_info.key_indexing_respone_queue);
        freeIndexingRequest(index_request);
    }
}

void enqueueWriteCommand(client *c)
{
    if (!__interceptors_installed)
        return;

    rxStashCommand2(index_info.key_indexing_request_queue, NULL, 2, c->argc, (void **)c->argv);
    freeCompletedRequests();
}

void setCommandIntercept(client *c)
{
    // SET key value

    // rxServerLog(rxLL_NOTICE, "Intercepted %s for %s with %s", (char *)c->argv[0]->ptr, (char *)c->argv[1]->ptr, (char *)c->argv[2]->ptr);
    index_info.set_tally++;
    redisCommandProc *standard_command_proc = standard_command_procs[SET_INTERCEPT];
    enqueueWriteCommand(c);
    standard_command_proc(c);
}

void hsetCommandIntercept(client *c)
{
    index_info.hset_tally++;
    redisCommandProc *standard_command_proc = standard_command_procs[HSET_INTERCEPT];
    enqueueWriteCommand(c);
    standard_command_proc(c);
}

void setnxCommandIntercept(client *c)
{
    redisCommandProc *standard_command_proc = standard_command_procs[SETNX_INTERCEPT];
    enqueueWriteCommand(c);
    standard_command_proc(c);
}

void setexCommandIntercept(client *c)
{
    redisCommandProc *standard_command_proc = standard_command_procs[SETEX_INTERCEPT];
    enqueueWriteCommand(c);
    standard_command_proc(c);
}

void psetexCommandIntercept(client *c)
{
    rxServerLog(LL_NOTICE, "PSETEX_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[PSETEX_INTERCEPT];
    enqueueWriteCommand(c);
    standard_command_proc(c);
}

void appendCommandIntercept(client *c)
{
    redisCommandProc *standard_command_proc = standard_command_procs[APPEND_INTERCEPT];
    enqueueWriteCommand(c);
    standard_command_proc(c);
}

void delCommandIntercept(client *c)
{
    redisCommandProc *standard_command_proc = standard_command_procs[DEL_INTERCEPT];
    enqueueWriteCommand(c);
    standard_command_proc(c);
}

void hsetnxCommandIntercept(client *c)
{
    redisCommandProc *standard_command_proc = standard_command_procs[HSETNX_INTERCEPT];
    enqueueWriteCommand(c);
    standard_command_proc(c);
}

void hdelCommandIntercept(client *c)
{
    redisCommandProc *standard_command_proc = standard_command_procs[HDEL_INTERCEPT];
    enqueueWriteCommand(c);
    standard_command_proc(c);
}

void getsetCommandIntercept(client *c)
{
    redisCommandProc *standard_command_proc = standard_command_procs[GETSET_INTERCEPT];
    enqueueWriteCommand(c);
    standard_command_proc(c);
}

void msetCommandIntercept(client *c)
{
    rxServerLog(LL_NOTICE, "MSET_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[MSET_INTERCEPT];
    enqueueWriteCommand(c);
    standard_command_proc(c);
}

void msetnxCommandIntercept(client *c)
{
    rxServerLog(LL_NOTICE, "MSETNX_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[MSETNX_INTERCEPT];
    enqueueWriteCommand(c);
    standard_command_proc(c);
}

void moveCommandIntercept(client *c)
{
    rxServerLog(LL_NOTICE, "MOVE_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[MOVE_INTERCEPT];
    standard_command_proc(c);
}

void renameCommandIntercept(client *c)
{
    rxServerLog(LL_NOTICE, "RENAME_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[RENAME_INTERCEPT];
    standard_command_proc(c);
}

void renamenxCommandIntercept(client *c)
{
    rxServerLog(LL_NOTICE, "RENAMENX_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[RENAMENX_INTERCEPT];
    standard_command_proc(c);
}

void selectCommandIntercept(client *c)
{
    redisNodeInfo *index_config = rxIndexNode();
    index_config->database_id = atoi(c->argv[1]->ptr);
    redisCommandProc *standard_command_proc = standard_command_procs[SELECT_INTERCEPT];
    standard_command_proc(c);
}

void xaddCommandIntercept(client *c)
{
    redisCommandProc *standard_command_proc = standard_command_procs[XADD_INTERCEPT];
    enqueueWriteCommand(c);
    standard_command_proc(c);
}

void xdelCommandIntercept(client *c)
{
    redisCommandProc *standard_command_proc = standard_command_procs[DEL_INTERCEPT];
    enqueueWriteCommand(c);
    standard_command_proc(c);
}

void touchCommandIntercept(client *c)
{
    redisCommandProc *standard_command_proc = standard_command_procs[TOUCH_INTERCEPT];
    standard_command_proc(c);
}

void genericCommandIntercept(client *c)
{
    rxServerLog(LL_NOTICE, "%s_INTERCEPT", (char *)c->argv[0]->ptr);

    struct redisCommand *cmd = lookupCommandByCString((char *)c->argv[0]->ptr);
                #if REDIS_VERSION_NUM > 0x00060000
    redisCommandProc *standard_command_proc = standard_command_procs[cmd->id];
    #else
    redisCommandProc *standard_command_proc = raxFind(InterceptedRedisCommandProc, (UCHAR *)c->argv[0]->ptr, strlen((char *)c->argv[0]->ptr));
    #endif
    standard_command_proc(c);

    sds *redis_request = rxMemAlloc(sizeof(sds) * (c->argc + 1));
    int j = 0;
    for (; j < c->argc; ++j)
    {
        redis_request[j] = sdsdup(c->argv[j]->ptr);
    }
    enqueueSimpleQueue(index_info.index_update_request_queue, redis_request);
}

void infoCommandIntercept(client *c)
{
    rxServerLog(LL_DEBUG, "INFO_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[INFO_INTERCEPT];
    standard_command_proc(c);
}

void flushdbCommandIntercept(client *c)
{
    if (pre_op_indexer_callback != NULL)
        pre_op_indexer_callback(c);
    redisCommandProc *standard_command_proc = standard_command_procs[FLUSHDB_INTERCEPT];
    standard_command_proc(c);
    if (post_op_indexer_callback != NULL)
        post_op_indexer_callback(c);
}

void flushallCommandIntercept(client *c)
{
    if (pre_op_indexer_callback != NULL)
        pre_op_indexer_callback(c);
    redisCommandProc *standard_command_proc = standard_command_procs[FLUSHALL_INTERCEPT];
    rxServerLog(LL_NOTICE, "About to flush all data");
    standard_command_proc(c);
    rxServerLog(LL_NOTICE, "All data flushed");
    if (post_op_indexer_callback != NULL)
        post_op_indexer_callback(c);
}

static int no_of_intercepts = 0;
void installIndexerInterceptors(indexerCallBack *pre_op, indexerCallBack *post_op)
{
    __interceptors_installed = 1;
    if (standard_command_procs != NULL)
        return;
    if (InterceptedRedisCommandProc == NULL)
        InterceptedRedisCommandProc = raxNew();
    pre_op_indexer_callback = pre_op;
    post_op_indexer_callback = post_op;

    no_of_intercepts = sizeof(interceptorCommandTable) / sizeof(struct redisCommandInterceptRule);
    standard_command_procs = rxMemAlloc(sizeof(redisCommandProc *) * no_of_intercepts);
    for (int j = 0; j < no_of_intercepts; ++j)
    {
        struct redisCommand *cmd = lookupCommandByCString(interceptorCommandTable[j].name);
        if (cmd)
        {
            if (cmd->proc != (void (*)(client *))interceptorCommandTable[j].proc)
            {
#if REDIS_VERSION_NUM > 0x00060000
                interceptorCommandTable[j].id = cmd->id;
#endif
                raxInsert(InterceptedRedisCommandProc, (UCHAR *)interceptorCommandTable[j].name, strlen(interceptorCommandTable[j].name), cmd->proc, NULL);
                standard_command_procs[j] = cmd->proc;
                cmd->proc = (void (*)(client *))interceptorCommandTable[j].proc;
            }
        }
    }
}

void uninstallIndexerInterceptors()
{
    __interceptors_installed = 0;
    return;
    if (standard_command_procs == NULL)
        return;
    // Restore original command processors
    for (int j = 0; j < no_of_intercepts; ++j)
    {
        struct redisCommand *cmd = lookupCommandByCString(interceptorCommandTable[j].name);
        if (cmd)
        {
            raxRemove(InterceptedRedisCommandProc, (UCHAR *)interceptorCommandTable[j].name, strlen(interceptorCommandTable[j].name), NULL);
            cmd->proc = standard_command_procs[j];
#if REDIS_VERSION_NUM > 0x00060000
            standard_command_procs[cmd->id] = NULL;
#endif
        }
    }
    rxMemFree(standard_command_procs);
    standard_command_procs = NULL;
}