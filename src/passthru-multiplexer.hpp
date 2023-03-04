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
    } state;
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
        this->state = PASSTHRU_WAITING;
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
        this->state = PASSTHRU_STARTED;
    }

    void Ready()
    {
        this->state = PASSTHRU_READY;
    }

    bool IsResponseWritten()
    {
        return this->state == PASSTHRU_RESPONDED;
    }

    int Execute()
    {
        if (this->state == PASSTHRU_READY)
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
        this->state = PASSTHRU_RESPONDED;
        while (this->state != PASSTHRU_RELEASED)
        {
            sched_yield();
        }
        this->state = PASSTHRU_DONE;
        return 1;
    }

    int ReleaseResponse()
    {
        if (this->reply)
        {
            freeReplyObject(this->reply);
        }
        return this->state = PASSTHRU_RELEASED;
    }

    int Done()
    {
        return this->state == PASSTHRU_READY ? 1 : 0;
    }
};

static void *execPassthruThread(void *ptr)
{
    auto *passthru_thread = (PassthruMultiplexer *)ptr;
    rxServerLog(rxLL_NOTICE, "PassThru async started");
    passthru_thread->Started();

    passthru_thread->reply = (RedisModuleCallReply *)redisCommand(
        passthru_thread->index_context,
        passthru_thread->stash,
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

#endif