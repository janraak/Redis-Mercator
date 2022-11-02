#ifndef __COMMAND_multiplexer_H__
#define __COMMAND_multiplexer_H__


class Multiplexer
{
private:
    long long cron_id;

public:
    RedisModuleBlockedClient *bc;
    int argc;
    long long interval_ms;
    long long delay_ms;

    long long hits;
    long long misses;
    long long ignored;

    Multiplexer();
    virtual ~Multiplexer();
    virtual long long Timeout();
    int Start(RedisModuleCtx *ctx);
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

Multiplexer::Multiplexer(){        
        this->cron_id = -1;
        this->interval_ms = 50; // Max lot time
        this->delay_ms = 25;    // Min time between slots
        this->hits = 0;
        this->misses = 0;
        this->ignored = 0;
    };

    Multiplexer::~Multiplexer(){};

    long long Multiplexer::Timeout(){
        return 60000;
    }

    int Multiplexer::Start(RedisModuleCtx *ctx)
    {
        this->bc = RedisModule_BlockClient(ctx, multiplexer_Continuation_Handler, multiplexer_Timeout, multiplexer_FreeData, this->Timeout());
        this->cron_id = rxCreateTimeEvent(1, (aeTimeProc *)multiplexer_Cron, this, NULL);
        return 1;
    };

     int Multiplexer::Execute(){
        return -1;
    };


     int Multiplexer::Write(RedisModuleCtx *ctx){
         rxUNUSED(ctx);
         return -1;
    };

     int Multiplexer::Done(){
        return -1;
    };

#endif