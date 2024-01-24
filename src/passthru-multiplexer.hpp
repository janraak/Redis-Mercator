#ifndef __Passthru_multiplexer_H__
#define __Passthru_multiplexer_H__

#include "command-multiplexer.hpp"

#ifdef __cplusplus
extern "C"
{
#endif

#include "../../deps/hiredis/hiredis.h"
#include "string.h"
#include "sdsWrapper.h"
#include <pthread.h>
#include "rxSuite.h"
#include "rxSuiteHelpers.h"
#define REDISMODULE_EXPERIMENTAL_API
#include "../../src/redismodule.h"

#ifdef __cplusplus
}
#endif
#include "client-pool.hpp"
#include "graphstackentry.hpp"
#include "simpleQueue.hpp"

static void *execPassthruThread(void *ptr);

class PassthruMultiplexer : public Multiplexer
{
public:
    enum states
    {
        PASSTHRU_WAITING,
        PASSTHRU_STARTED,
        PASSTHRU_READY,
        PASSTHRU_RESPONDED,
        PASSTHRU_RELEASED,
        PASSTHRU_DONE
    } pstate;
    const char *stash;
    int argc;
    pthread_t passthruThreadHandle;

    redisContext *index_context;
    RedisModuleCallReply *reply;
    PassthruMultiplexer(RedisModuleString **argv, int argc)
        : Multiplexer()
    {
        redisNodeInfo *indexConfig = rxIndexNode();
        this->index_context = RedisClientPool<redisContext>::Acquire(indexConfig->host_reference, "_CLIENT", "RxDescribePassthruMultiplexer");
        this->reply = NULL;
        this->pstate = PASSTHRU_WAITING;
        this->stash = rxStringBuildRedisCommand(argc, (rxRedisModuleString **)argv);
        if (pthread_create(&this->passthruThreadHandle, NULL, execPassthruThread, this))
        {
            fprintf(stderr, "FATAL: Failed to start indexer thread\n");
            exit(1);
        }
    }

    ~PassthruMultiplexer()
    {
        RedisClientPool<redisContext>::Release(index_context, "RxDescribePassthruMultiplexer");
        rxMemFree((void *)this->stash);
    }

    void Started()
    {
        this->pstate = PASSTHRU_STARTED;
    }

    void Ready()
    {
        this->pstate = PASSTHRU_READY;
    }

    bool IsResponseWritten()
    {
        return this->pstate == PASSTHRU_RESPONDED;
    }

    int Execute()
    {
        if (this->pstate == PASSTHRU_READY)
        {
            return -1;
        }
        return 1;
    }

    int Write(RedisModuleCtx *ctx)
    {
        if (this->reply)
        {
            RedisModule_ReplyWithCallReply(ctx, this->reply);
        }
        else
            RedisModule_ReplyWithSimpleString(ctx, this->index_context->errstr);
        this->pstate = PASSTHRU_RESPONDED;
        while (this->pstate != PASSTHRU_RELEASED)
        {
            sched_yield();
        }
        this->pstate = PASSTHRU_DONE;
        return 1;
    }

    int ReleaseResponse()
    {
        if (this->reply)
        {
            freeReplyObject(this->reply);
        }
        return this->pstate = PASSTHRU_RELEASED;
    }

    int Done()
    {
        return this->pstate == PASSTHRU_READY ? 1 : 0;
    }
};

static void *execPassthruThread(void *ptr)
{
    auto *passthru_thread = (PassthruMultiplexer *)ptr;
    rxServerLog(rxLL_NOTICE, "PassThru async started");
    passthru_thread->Started();

    rxString cmd = hi_sdsnewlen(passthru_thread->stash, strlen(passthru_thread->stash));
    passthru_thread->reply = (RedisModuleCallReply *)redisCommand(
        passthru_thread->index_context,
        cmd,
        NULL);

    passthru_thread->Ready();
    while (!passthru_thread->IsResponseWritten())
    {
        sched_yield();
    }
    // Keep the response until written
    passthru_thread->ReleaseResponse();
    rxServerLog(rxLL_NOTICE, "PassThru async stopped");
    return NULL;
}

void *PassthruAsync_Go(void *privData)
{
    auto *multiplexer = (PassthruMultiplexer *)privData;
    multiplexer->state = Multiplexer::running;

    char cmd[4096];
    for (int n = 0; n < multiplexer->argc; ++n)
    {
        size_t len;
        const char *argS = RedisModule_StringPtrLen(multiplexer->argv[n], &len);
        strcat(cmd, argS);
        strcat(cmd, " ");
    }
    auto index = rxIndexNode();
    auto *index_node = RedisClientPool<redisContext>::Acquire(index->host_reference, "_CLIENT", "Invoke_IndexStore");

    multiplexer->reply = (RedisModuleCallReply *)redisCommand(
        index_node,
        cmd,
        NULL);
    RedisClientPool<redisContext>::Release(index_node, "Invoke_IndexStore");

    multiplexer->state = Multiplexer::done;
    return NULL;
}

#endif