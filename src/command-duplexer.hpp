#ifndef __COMMAND_DUPLEXER_H__
#define __COMMAND_DUPLEXER_H__


class Duplexer
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

    Duplexer();
    virtual ~Duplexer();
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

int Duplexer_Cron(struct aeEventLoop *eventLoop, long long id, void *clientData)
{
    auto *duplexer = (Duplexer *)clientData;
    rxUNUSED(eventLoop);
    rxUNUSED(id);
    long long start = ustime();

    while (duplexer->Execute() >= 0)
    {
        // Check in loop! Slot time exhausted?
        if (ustime() - start >= duplexer->interval_ms * 1000)
        {
            // Yield when slot time over
            return duplexer->delay_ms;
        }
    }
    // When done
    duplexer->Done();
    RedisModule_UnblockClient(duplexer->bc, clientData);
    return -1;
}

/* Reply callback command duplexer */
int Duplexer_Continuation_Handler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    auto *bci = (Duplexer *)RedisModule_GetBlockedClientPrivateData(ctx);
    bci->Write(ctx);
    return REDIS_OK;
}

/* Timeout callback command duplexer */
int Duplexer_Timeout(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    return RedisModule_ReplyWithSimpleString(ctx, "Request timedout");
}

/* Private data freeing callback for command duplexer. */
void Duplexer_FreeData(RedisModuleCtx *ctx, void *privdata)
{
    REDISMODULE_NOT_USED(privdata);
    auto *bci = (Duplexer *)RedisModule_GetBlockedClientPrivateData(ctx);
    delete bci;
}

#ifdef __cplusplus
}
#endif

Duplexer::Duplexer(){        
        this->cron_id = -1;
        this->interval_ms = 50; // Max lot time
        this->delay_ms = 25;    // Min time between slots
        this->hits = 0;
        this->misses = 0;
        this->ignored = 0;
    };

    Duplexer::~Duplexer(){};

    long long Duplexer::Timeout(){
        return 60000;
    }

    int Duplexer::Start(RedisModuleCtx *ctx)
    {
        this->bc = RedisModule_BlockClient(ctx, Duplexer_Continuation_Handler, Duplexer_Timeout, Duplexer_FreeData, this->Timeout());
        this->cron_id = rxCreateTimeEvent(1, (aeTimeProc *)Duplexer_Cron, this, NULL);
        return 1;
    };

     int Duplexer::Execute(){
        return -1;
    };


     int Duplexer::Write(RedisModuleCtx *ctx){
         rxUNUSED(ctx);
         return -1;
    };

     int Duplexer::Done(){
        return -1;
    };

#endif