#ifndef __RXQUERY_multiplexer_H__
#define __RXQUERY_multiplexer_H__

#define VALUE_ONLY 2
#define FIELD_AND_VALUE_ONLY 3
#define FIELD_OP_VALUE 4

#include "client-pool.hpp"
#include "command-multiplexer.hpp"
#include "simpleQueue.hpp"
#include "graphstackentry.hpp"
#include "sjiboleth.hpp"

using std::string;
#include "sjiboleth-fablok.hpp"
#include "sjiboleth.h"

#ifdef __cplusplus
extern "C"
{
#endif

// #include "string.h"

#ifdef __cplusplus
}
#endif
const char *AS_ARG = "AS";
const char *DEBUG_ARG = "DEBUG";
const char *RESET_ARG = "RESET";
const char *RANKED_ARG = "RANKED";
const char *OVER_ARG = "OVER";
const char *BELOW_ARG = "UNDER";
const char *CLEAR_ARG = "RESET";
const char *GREMLIN_PREFX = "g:";
const char *GREMLIN_PREFIX_ALT = "g.";
const char *GREMLIN_DIALECT = "gremlin";
const char *QUERY_DIALECT = "query";

long long max_idle_seconds = 60;
long long execute_command_cron_tally = 0;
long long execute_command_cron_id = -1;
long long execute_command_interval_ms = 50;
long long execute_command_delay_ms = 50;

int Execute_Command_Cron(struct aeEventLoop *, long long, void * /*clientData*/)
{
    // auto *queue = (SimpleQueue *)clientData;
    // long long start = ustime();

    // while (execute_command_cron_tally == 0 || queue->QueueLength() > 0 || queue->response_queue->QueueLength() > 0)
    // {
    //     rxString *request = queue->Dequeue();
    //     if (request == NULL)
    //     {
    //         return execute_command_delay_ms;
    //     }
    //     void **stash = (void **)request;
    //     ExecuteRedisCommand(queue, stash);

    //     // Check in loop! Slot time exhausted?
    //     if (ustime() - start >= execute_command_interval_ms * 1000)
    //     {
    //         // Yield when slot time over
    //         return execute_command_delay_ms;
    //     }
    // }
    // When done
    return -1;
}

static void *execQueryThread(void *ptr)
{
    redisNodeInfo *index_config = rxIndexNode();
    SimpleQueue *command_reponse_queue = new SimpleQueue("execQueryThreadRESP");
    SimpleQueue *command_request_queue = new SimpleQueue("execQueryThreadREQ", command_reponse_queue);
    execute_command_cron_id = rxCreateTimeEvent(1, (aeTimeProc *)Execute_Command_Cron, command_request_queue, NULL);
    SimpleQueue *control_query_request_queue = (SimpleQueue *)ptr;
    // indexer_set_thread_title("rxQuery async loader");
    rxServerLog(rxLL_NOTICE, "rxQuery async loader started\n");
    control_query_request_queue->Started();

    // long long start = ustime();
    void *load_entry = control_query_request_queue->Dequeue();
    // start = ustime();
    while (load_entry == NULL)
    {
        sched_yield();
        load_entry = control_query_request_queue->Dequeue();
    }
    if (load_entry != NULL)
    {
        GET_ARGUMENTS_FROM_STASH(load_entry);
        // rxString target_setname = rxStringEmpty();
        rxString query = rxStringEmpty();

        char sep[2] = {0x00, 0x00};
        for (int j = 1; j < argc; ++j)
        {
            rxString q = (rxString)rxGetContainedObject(argv[j]);
            if (rxStringMatchLen(q, strlen(AS_ARG), AS_ARG, strlen(AS_ARG), 1) && strlen(q) == strlen(AS_ARG))
            {
                ++j;
                // q = (rxString)rxGetContainedObject(argv[j]);
                // target_setname = q;
            }
            else if (rxStringMatchLen(q, strlen(RESET_ARG), RESET_ARG, strlen(RESET_ARG), 1) && strlen(q) == strlen(RESET_ARG))
            {
                FaBlok::ClearCache();
            }
            else
            {
                query = rxStringFormat("%s%s%s", query, sep, q);
                sep[0] = ' ';
            }
        }
    
        Sjiboleth *parser;
        if (
            rxStringMatchLen(query, 2, GREMLIN_PREFX, strlen(GREMLIN_PREFX), 1) ||
            rxStringMatchLen(query, 2, GREMLIN_PREFIX_ALT, strlen(GREMLIN_PREFIX_ALT), 1))
            parser = new GremlinDialect();
        else
            parser = new QueryDialect();

        auto *parsed_query = parser->Parse(query);

        auto *e = new SilNikParowy_Kontekst(index_config, NULL);
        rax *r = e->Execute(parsed_query);
        // if (r)
        // {
        //     WriteResults(r, ctx, 0, NULL);
        //     if (e->CanDeleteResult())
        //         FreeResults(r);
        // }
        // else
        //     RedisModule_ReplyWithSimpleString(ctx, "No results!");
        e->Reset();

        delete parsed_query;
        delete parser;
        void *rr = rxCreateObject(rxOBJ_RAX, r);
        rxStashCommand2(control_query_request_queue->response_queue, "$RESPONSE$", 2, 1, &rr);

        // free stashed redis command on same thread as allocated
        void *stash = command_reponse_queue->Dequeue();
        while (stash != NULL)
        {
            rxServerLogHexDump(rxLL_NOTICE, stash, 128/*rxMemAllocSize(stash)*/, "execQueryThread %s %p DEQUEUE", command_reponse_queue->name, stash);
            FreeStash(stash);
            stash = command_reponse_queue->Dequeue();
        }

        // break;
    }

    // if (ustime() - start >= max_idle_seconds * 1000)
    // {
    //     // Yield when slot time over
    //     break;
    // }
    control_query_request_queue->Stopped();
    rxServerLog(rxLL_NOTICE, "rxQuery async loader stopped\n");
    while ((command_reponse_queue->QueueLength() + command_request_queue->QueueLength()) > 0)
    {
        // free stashed redis command on same thread as allocated
        void *stash2 = command_reponse_queue->Dequeue();
        while (stash2 != NULL)
        {
            FreeStash(stash2);
            stash2 = command_reponse_queue->Dequeue();
        }
    }
    rxServerLog(rxLL_NOTICE, "rxQuery async redis commands stopped\n");
    return NULL;
}

class RxQueryMultiplexer : public Multiplexer
{
public:
    SimpleQueue *control_q_request;
    SimpleQueue *control_q_response;
    redisNodeInfo *index_config;

    RxQueryMultiplexer(void * query, redisNodeInfo *index_config)
        : Multiplexer()
    {
        this->control_q_response = new SimpleQueue("RxQueryMultiplexerRESP");
        this->control_q_request = new SimpleQueue("RxQueryMultiplexerREQ", (void *)execQueryThread, 1, this->control_q_response);
        this->index_config = index_config;
        this->control_q_request->Enqueue(query);
    }

    ~RxQueryMultiplexer()
    {
        this->control_q_response->Release();
        this->control_q_request->Release();

        delete this->control_q_request;
        delete this->control_q_response;
    }

    long long Timeout()
    {
        return 0;
    }

    int Execute()
    {
        void *response = this->control_q_response->Dequeue();
        if (response != NULL)
        {
            GET_ARGUMENTS_FROM_STASH(response);
            if(argc >= 2)
                if (rxGetObjectType(argv[1]) == rxOBJ_RAX)
                    raxFree((rax *)rxGetContainedObject(argv[1]));
            FreeStash(response);
            return -1;
        }
        return 1;
    }

    int Write(RedisModuleCtx *ctx)
    {
        RedisModule_ReplyWithSimpleString(ctx, "OK");
        return 1;
    }

    int Done()
    {
        return 1;
    }
};
#endif