#ifndef __Passthru_multiplexer_H__
#define __Passthru_multiplexer_H__

#include "command-multiplexer.hpp"

#ifdef __cplusplus
extern "C"
{
#endif

#include "../../deps/hiredis/hiredis.h"
#include "rxSuite.h"
#include "rxSuiteHelpers.h"
#include "sdsWrapper.h"
#include "string.h"
#include <pthread.h>
#define REDISMODULE_EXPERIMENTAL_API
#include "../../src/redismodule.h"

#ifdef __cplusplus
}
#endif
#include "client-pool.hpp"
#include "graphstackentry.hpp"
#include "simpleQueue.hpp"

class PassthruMultiplexer : public Multiplexer
{
public:
    const char *stash;
    int argc;
    pthread_t passthruThreadHandle;
    redisContext *index_node = NULL;

    redisContext *index_context;
    RedisModuleCallReply *reply = NULL;
    PassthruMultiplexer(RedisModuleString **argv, int argc)
        : Multiplexer(argv, argc)
    {
        redisNodeInfo *indexConfig = rxIndexNode();
        this->index_context = RedisClientPool<redisContext>::Acquire(indexConfig->host_reference, "_CLIENT", "RxDescribePassthruMultiplexer");
        this->stash = rxStringBuildRedisCommand(argc, (rxRedisModuleString **)argv);
    }

    ~PassthruMultiplexer()
    {
        RedisClientPool<redisContext>::Release(index_context, "RxDescribePassthruMultiplexer");
        rxMemFree((void *)this->stash);
    }

    int Execute()
    {
        if (this->reply)
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
            freeReplyObject(this->reply);
            if (this->index_node)
                RedisClientPool<redisContext>::Release(this->index_node, "Invoke_IndexStore");
        }
        else
            RedisModule_ReplyWithSimpleString(ctx, this->index_context->errstr);
        return 1;
    }

    int Done()
    {

        return 1;
    }
};

int rc = 0;
void *PassthruAsync_Go(void *privData)
{
    auto *multiplexer = (PassthruMultiplexer *)privData;
    multiplexer->state = Multiplexer::running;

    auto index = rxIndexNode();
    multiplexer->index_node = RedisClientPool<redisContext>::Acquire(index->host_reference, "_CLIENT", "Invoke_IndexStore");

    multiplexer->reply = (RedisModuleCallReply *)redisCommand(
        multiplexer->index_node,
        multiplexer->stash,
        NULL);

    multiplexer->state = Multiplexer::done;
    //pthread_exit(&rc);
    return NULL;
}

#endif