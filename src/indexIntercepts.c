#include "indexIntercepts.h"
#include "rxSuite.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define REDISMODULE_EXPERIMENTAL_API
#include "/usr/include/arm-linux-gnueabihf/bits/types/siginfo_t.h"
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

extern indexerThread index_info;
extern void *rxStashCommand2(SimpleQueue *ctx, const char *command, int argt, int argc, void **args);

extern struct redisServer server;

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

struct redisCommand interceptorCommandTable[] = {
/* Note that we can't flag set as fast, since it may perform an
 * implicit DEL of a large key. */
#define SET_INTERCEPT 0
    {"set", setCommandIntercept, -3, "write use-memory @string", 0, NULL, 1, 1, 1, 0, 0, 0},
#define SETNX_INTERCEPT 1
    {"setnx", setnxCommandIntercept, 3, "write use-memory fast @string", 0, NULL, 1, 1, 1, 0, 0, 0},
#define SETEX_INTERCEPT 2
    {"setex", setexCommandIntercept, 4, "write use-memory @string", 0, NULL, 1, 1, 1, 0, 0, 0},
#define PSETEX_INTERCEPT 3
    {"psetex", psetexCommandIntercept, 4, "write use-memory @string", 0, NULL, 1, 1, 1, 0, 0, 0},
#define APPEND_INTERCEPT 4
    {"append", appendCommandIntercept, 3, "write use-memory fast @string", 0, NULL, 1, 1, 1, 0, 0, 0},
#define DEL_INTERCEPT 5
    {"del", delCommandIntercept, -2, "write @keyspace", 0, NULL, 1, -1, 1, 0, 0, 0},
#define HSET_INTERCEPT 6
    {"hset", hsetCommandIntercept, -4, "write use-memory fast @hash", 0, NULL, 1, 1, 1, 0, 0, 0},
#define HSETNX_INTERCEPT 7
    {"hsetnx", hsetnxCommandIntercept, 4, "write use-memory fast @hash", 0, NULL, 1, 1, 1, 0, 0, 0},
#define HMSET_INTERCEPT 8
    {"hmset", hsetCommandIntercept, -4, "write use-memory fast @hash", 0, NULL, 1, 1, 1, 0, 0, 0},
#define HDEL_INTERCEPT 9
    {"hdel", hdelCommandIntercept, -3, "write fast @hash", 0, NULL, 1, 1, 1, 0, 0, 0},
#define GETSET_INTERCEPT 10
    {"getset", getsetCommandIntercept, 3, "write use-memory fast @string", 0, NULL, 1, 1, 1, 0, 0, 0},
#define MSET_INTERCEPT 11
    {"mset", msetCommandIntercept, -3, "write use-memory @string", 0, NULL, 1, -1, 2, 0, 0, 0},
#define MSETNX_INTERCEPT 12
    {"msetnx", msetnxCommandIntercept, -3, "write use-memory @string", 0, NULL, 1, -1, 2, 0, 0, 0},
#define MOVE_INTERCEPT 13
    {"move", moveCommandIntercept, 3, "write fast @keyspace", 0, NULL, 1, 1, 1, 0, 0, 0},
/* Like for SET, we can't mark rename as a fast command because
 * overwriting the target key may result in an implicit slow DEL. */
#define RENAME_INTERCEPT 14
    {"rename", renameCommandIntercept, 3, "write @keyspace", 0, NULL, 1, 2, 1, 0, 0, 0},
#define RENAMENX_INTERCEPT 15
    {"renamenx", renamenxCommandIntercept, 3, "write fast @keyspace", 0, NULL, 1, 2, 1, 0, 0, 0},
#define FLUSHALL_INTERCEPT 16
    {"NOflushdb", genericCommandIntercept, -1, "write @keyspace @dangerous", 0, NULL, 0, 0, 0, 0, 0, 0},
#define FLUSHDB_INTERCEPT 17
    {"NOflushall", genericCommandIntercept, -1, "write @keyspace @dangerous", 0, NULL, 0, 0, 0, 0, 0, 0},
#define BGSAVE_INTERCEPT 18
    {"NObgsave", genericCommandIntercept, -1, "write @keyspace @dangerous", 0, NULL, 0, 0, 0, 0, 0, 0},
#define INFO_INTERCEPT 19
    {"info", infoCommandIntercept, -1, "readonly", 0, NULL, 0, 0, 0, 0, 0, 0},
#define SELECT_INTERCEPT 20
    {"select", selectCommandIntercept, -1, "readonly", 0, NULL, 0, 0, 0, 0, 0, 0},
#define SWAPDB_INTERCEPT 21
    {"NOswapdb", genericCommandIntercept, -1, "readonly", 0, NULL, 0, 0, 0, 0, 0, 0},
#define XADD_INTERCEPT 22
    {"xadd", xaddCommandIntercept, -1, "write", 0, NULL, 0, 0, 0, 0, 0, 0},
#define XDEL_INTERCEPT 23
    {"xdel", xdelCommandIntercept, -2, "write", 0, NULL, 1, -1, 1, 0, 0, 0},
#define TOUCH_INTERCEPT 24
    {"NOtouch", touchCommandIntercept, -2, "write", 0, NULL, 1, -1, 1, 0, 0, 0}};

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
        // /**/ printf("x%x #3 key_indexing_respone_queue\n", (POINTER)index_request);
        freeIndexingRequest(index_request);
    }
}

void enqueueSetCommand(client *c)
{
    rxStashCommand2(index_info.key_indexing_request_queue, NULL, 2, c->argc, (void **)c->argv);
    freeCompletedRequests();
}
void setCommandIntercept(client *c)
{
    // SET key value

    index_info.set_tally++;
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[SET_INTERCEPT].id];
    enqueueSetCommand(c);
    standard_command_proc(c);
}

void hsetCommandIntercept(client *c)
{
    index_info.hset_tally++;
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[HSET_INTERCEPT].id];
    enqueueSetCommand(c);
    standard_command_proc(c);
}

void setnxCommandIntercept(client *c)
{
    serverLog(LL_NOTICE, "SETNX_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[SETNX_INTERCEPT].id];
    standard_command_proc(c);
}

void setexCommandIntercept(client *c)
{
    serverLog(LL_NOTICE, "SETEX_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[SETEX_INTERCEPT].id];
    standard_command_proc(c);
}

void psetexCommandIntercept(client *c)
{
    serverLog(LL_NOTICE, "PSETEX_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[PSETEX_INTERCEPT].id];
    standard_command_proc(c);
}

void appendCommandIntercept(client *c)
{
    serverLog(LL_NOTICE, "APPEND_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[APPEND_INTERCEPT].id];
    standard_command_proc(c);
}

void delCommandIntercept(client *c)
{
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[DEL_INTERCEPT].id];
    enqueueSetCommand(c);
    standard_command_proc(c);
}

void hsetnxCommandIntercept(client *c)
{
    serverLog(LL_NOTICE, "HSETNX_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[HSETNX_INTERCEPT].id];
    standard_command_proc(c);
}

void hdelCommandIntercept(client *c)
{
    serverLog(LL_NOTICE, "HDEL_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[HDEL_INTERCEPT].id];
    standard_command_proc(c);
}

void getsetCommandIntercept(client *c)
{
    serverLog(LL_NOTICE, "GETSET_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[GETSET_INTERCEPT].id];
    standard_command_proc(c);
}

void msetCommandIntercept(client *c)
{
    serverLog(LL_NOTICE, "MSET_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[MSET_INTERCEPT].id];
    standard_command_proc(c);
}

void msetnxCommandIntercept(client *c)
{
    serverLog(LL_NOTICE, "MSETNX_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[MSETNX_INTERCEPT].id];
    standard_command_proc(c);
}

void moveCommandIntercept(client *c)
{
    serverLog(LL_NOTICE, "MOVE_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[MOVE_INTERCEPT].id];
    standard_command_proc(c);
}

void renameCommandIntercept(client *c)
{
    serverLog(LL_NOTICE, "RENAME_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[RENAME_INTERCEPT].id];
    standard_command_proc(c);
}

void renamenxCommandIntercept(client *c)
{
    serverLog(LL_NOTICE, "RENAMENX_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[RENAMENX_INTERCEPT].id];
    standard_command_proc(c);
}

void selectCommandIntercept(client *c)
{
    redisNodeInfo *index_config = rxIndexNode();
    index_config->database_id = atoi(c->argv[1]->ptr);
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[SELECT_INTERCEPT].id];
    standard_command_proc(c);
}

void xaddCommandIntercept(client *c)
{
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[XADD_INTERCEPT].id];
    enqueueSetCommand(c);
    standard_command_proc(c);
}

void xdelCommandIntercept(client *c)
{
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[DEL_INTERCEPT].id];
    enqueueSetCommand(c);
    standard_command_proc(c);
}

void touchCommandIntercept(client *c)
{
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[TOUCH_INTERCEPT].id];
    standard_command_proc(c);
}

void genericCommandIntercept(client *c)
{
    serverLog(LL_NOTICE, "%s_INTERCEPT", (char *)c->argv[0]->ptr);

    struct redisCommand *cmd = lookupCommandByCString((char *)c->argv[0]->ptr);
    redisCommandProc *standard_command_proc = standard_command_procs[cmd->id];
    standard_command_proc(c);

    sds *redis_request = zmalloc(sizeof(sds) * (c->argc + 1));
    int j = 0;
    for (; j < c->argc; ++j)
    {
        redis_request[j] = sdsdup(c->argv[j]->ptr);
    }
    enqueueSimpleQueue(index_info.index_update_request_queue, redis_request);
}

void infoCommandIntercept(client *c)
{
    serverLog(LL_NOTICE, "INFO_INTERCEPT");
    redisCommandProc *standard_command_proc = standard_command_procs[interceptorCommandTable[INFO_INTERCEPT].id];
    standard_command_proc(c);
}

void installIndexerInterceptors()
{
    standard_command_procs = zmalloc(sizeof(redisCommandProc *) * dictSize(server.commands));
    for (unsigned int j = 0; j < sizeof(interceptorCommandTable) / sizeof(struct redisCommand); ++j)
    {
        struct redisCommand *cmd = lookupCommandByCString(interceptorCommandTable[j].name);
        if (cmd)
        {
            interceptorCommandTable[j].id = cmd->id;
            standard_command_procs[cmd->id] = cmd->proc;
            cmd->proc = interceptorCommandTable[j].proc;
        }
    }
}

void uninstallIndexerInterceptors()
{
    // Restore original command processors
    for (unsigned int j = 0; j < sizeof(interceptorCommandTable) / sizeof(struct redisCommand); ++j)
    {
        struct redisCommand *cmd = lookupCommandByCString(interceptorCommandTable[j].name);
        if (cmd)
        {
            cmd->proc = standard_command_procs[cmd->id];
            standard_command_procs[cmd->id] = NULL;
        }
    }
    zfree(standard_command_procs);
}