#ifndef __COMMAND_multiplexer_H__
#define __COMMAND_multiplexer_H__

extern "C"
{
#include <pthread.h>
}

typedef void *(*runner)(void *);
class Multiplexer_Async_State
{
public:
    enum state
    {
        not_started,
        running,
        cleanup,
        done
    } state;
    const char *name;
    pthread_t *thread;

    Multiplexer_Async_State()
    {
        this->state = not_started;
        this->name = NULL;
        this->thread = NULL;
    }
};
class Multiplexer
{
private:
    long long cron_id;

public:
    RedisModuleString **argv;
    int argc;
    const char *result_text;

    // const char *name;
    pthread_t multiplexer_thread;
    enum state
    {
        undefined,
        not_started,
        running,
        cleanup,
        done
    } state;

    RedisModuleBlockedClient *bc;
    RedisModuleCtx *ctx;
    long long interval_ms;
    long long delay_ms;

    long long hits;
    long long misses;
    long long ignored;

    Multiplexer();
    Multiplexer(RedisModuleString **argv, int argc);
    virtual ~Multiplexer();
    virtual long long Timeout();
    int Start(RedisModuleCtx *ctx);
    int Async(RedisModuleCtx *ctx, runner handler);
    // static int Go(void *privData);
    virtual int Execute() = 0;
    virtual int Write(RedisModuleCtx *ctx) = 0;
    virtual int Done() = 0;
};

#ifdef __cplusplus
extern "C"
{
#endif
    extern long long ustime();

#include "rxSuiteHelpers.h"

    int multiplexer_Cron(struct aeEventLoop *eventLoop, long long id, void *clientData)
    {
        auto *multiplexer = (Multiplexer *)clientData;
        rxUNUSED(eventLoop);
        rxUNUSED(id);
        long long start = ustime();

        while (multiplexer->Execute() >= 0)
        {
            // Check in loop! Slot time exhausted?
            if (ustime() - start >= multiplexer->interval_ms * 1000)
            {
                // Yield when slot time over
                return multiplexer->delay_ms;
            }
        }
        // When done
        multiplexer->Done();
        RedisModule_UnblockClient(multiplexer->bc, clientData);
        return -1;
    }

    int multiplexer_async_Cron(struct aeEventLoop *, long long, void *clientData)
    {
        auto *multiplexer = (Multiplexer *)clientData;

        switch (multiplexer->state)
        {
        case Multiplexer::not_started:
        case Multiplexer::running:
        case Multiplexer::cleanup:
        case Multiplexer::undefined:
            return multiplexer->delay_ms;
        case Multiplexer::done:
        default:
            multiplexer->Done();
            RedisModule_UnblockClient(multiplexer->bc, clientData);
            return -1;
        }
    }

    /* Reply callback command multiplexer */
    int multiplexer_Continuation_Handler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
    {
        REDISMODULE_NOT_USED(argv);
        REDISMODULE_NOT_USED(argc);
        auto *bci = (Multiplexer *)RedisModule_GetBlockedClientPrivateData(ctx);
        bci->Write(ctx);
        return REDIS_OK;
    }

    /* Timeout callback command Multiplexer */
    int multiplexer_Timeout(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
    {
        REDISMODULE_NOT_USED(argv);
        REDISMODULE_NOT_USED(argc);
        return RedisModule_ReplyWithSimpleString(ctx, "Request timedout");
    }

    /* Private data freeing callback for command multiplexer. */
    void multiplexer_FreeData(RedisModuleCtx *ctx, void *privdata)
    {
        REDISMODULE_NOT_USED(privdata);
        auto *bci = (Multiplexer *)RedisModule_GetBlockedClientPrivateData(ctx);
        delete bci;
    }

#ifdef __cplusplus
}
#endif

Multiplexer::Multiplexer()
{
    this->cron_id = -1;
    this->interval_ms = 50; // Max lot time
    this->delay_ms = 25;    // Min time between slots
    this->hits = 0;
    this->misses = 0;
    this->ignored = 0;
    this->argv = NULL;
    this->argc = 0;
    this->multiplexer_thread = 0;
    this->state = undefined;
    this->result_text = NULL;
};

Multiplexer::Multiplexer(RedisModuleString **argv, int argc)
    : Multiplexer()
{
    this->argv = argv;
    this->argc = argc;
}

Multiplexer::~Multiplexer(){};

long long Multiplexer::Timeout()
{
    return 60000;
}

int Multiplexer::Start(RedisModuleCtx *ctx)
{
    this->bc = RedisModule_BlockClient(ctx, multiplexer_Continuation_Handler, multiplexer_Timeout, multiplexer_FreeData, this->Timeout());
    this->cron_id = rxCreateTimeEvent(1, (aeTimeProc *)multiplexer_Cron, this, NULL);
    return 1;
};

int Multiplexer::Async(RedisModuleCtx *ctx, runner handler)
{
    this->bc = RedisModule_BlockClient(ctx, multiplexer_Continuation_Handler, multiplexer_Timeout, multiplexer_FreeData, this->Timeout());
    this->delay_ms = 25;
    this->cron_id = rxCreateTimeEvent(1, (aeTimeProc *)multiplexer_async_Cron, this, NULL);

    this->state = not_started;
    this->ctx = ctx;
    pthread_create(&this->multiplexer_thread, NULL, handler, this);
    pthread_setname_np(this->multiplexer_thread, "ASYNC");
    return 1;
};

// int Multiplexer::Go(void *privData){
//     return C_OK;
// };

int Multiplexer::Execute()
{
    return -1;
};

int Multiplexer::Write(RedisModuleCtx *ctx)
{
    if (this->result_text)
    {
        RedisModule_ReplyWithSimpleString(ctx, this->result_text);
    }
    return -1;
};

int Multiplexer::Done()
{
    return -1;
};

#endif