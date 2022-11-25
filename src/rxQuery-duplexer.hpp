#ifndef __RXQUERY_DUPLEXER_H__
#define __RXQUERY_DUPLEXER_H__

#define VALUE_ONLY 2
#define FIELD_AND_VALUE_ONLY 3
#define FIELD_OP_VALUE 4

#include "client-pool.hpp"
#include "command-duplexer.hpp"
#include "simpleQueue.hpp"
#include "graphstackentry.hpp"
#include "sjiboleth.hpp"

using std::string;
#include "sjiboleth.h"
#include "sjiboleth-fablok.hpp"


// extern void ExecuteRedisCommand(SimpleQueue *ctx, void *stash);

// typedef int comparisonProc(char *l, int ll, char *r);

#ifdef __cplusplus
extern "C"
{
#endif

// #include "string.h"
#include "util.h"
// #include "sdsWrapper.h"

#ifdef __cplusplus
}
#endif
const char *AS_ARG = "AS";
const char *DEBUG_ARG = "DEBUG";
const char *RESET_ARG = "RESET";
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

int Execute_Command_Cron(struct aeEventLoop *, long long, void */*clientData*/)
{
    // auto *queue = (SimpleQueue *)clientData;
    // long long start = ustime();

    // while (execute_command_cron_tally == 0 || queue->QueueLength() > 0 || queue->response_queue->QueueLength() > 0)
    // {
    //     sds *request = queue->Dequeue();
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
    SimpleQueue *command_reponse_queue = new SimpleQueue();
    SimpleQueue *command_request_queue = new SimpleQueue(command_reponse_queue);
    execute_command_cron_id = rxCreateTimeEvent(1, (aeTimeProc *)Execute_Command_Cron, command_request_queue, NULL);
    SimpleQueue *control_query_request_queue = (SimpleQueue *)ptr;
    // indexer_set_thread_title("rxQuery async loader");
    printf("rxQuery async loader started\n");
    control_query_request_queue->Started();

    // long long start = ustime();
    sds *load_entry = control_query_request_queue->Dequeue();
    // start = ustime();
    while (load_entry == NULL)
    {
        sched_yield();
        load_entry = control_query_request_queue->Dequeue();
    }
    if (load_entry != NULL)
    {
        sds query = load_entry[1];
        Sjiboleth *parser;
        if (
            stringmatchlen(query, 2, GREMLIN_PREFX, strlen(GREMLIN_PREFX), 1) ||
            stringmatchlen(query, 2, GREMLIN_PREFIX_ALT, strlen(GREMLIN_PREFIX_ALT), 1))
            parser = new GremlinDialect();
        else
            parser = new QueryDialect();

        auto *parsed_query = parser->Parse(load_entry[1]);

        auto *e = new SilNikParowy_Kontekst((char *)"192.168.1.180", 6379, NULL);
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
        load_entry[2] = (char *)r;
        control_query_request_queue->response_queue->Enqueue(load_entry);

        // free stashed redis command on same thread as allocated
        sds *stash = command_reponse_queue->Dequeue();
        while (stash != NULL)
        {
            rxMemFree(stash);
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
    printf("rxQuery async loader stopped\n");
    while ((command_reponse_queue->QueueLength() + command_request_queue->QueueLength()) > 0)
    {
        // free stashed redis command on same thread as allocated
        sds *stash2 = command_reponse_queue->Dequeue();
        while (stash2 != NULL)
        {
            rxMemFree(stash2);
            stash2 = command_reponse_queue->Dequeue();
        }
    }
    printf("rxQuery async redis commands stopped\n");
    return NULL;
}

class RxQueryDuplexer : public Duplexer
{
public:
    SimpleQueue *control_q_request;
    SimpleQueue *control_q_response;

    RxQueryDuplexer(sds command, sds query)
        : Duplexer()
    {
        this->control_q_response = new SimpleQueue();
        this->control_q_request = new SimpleQueue((void *)execQueryThread, 1, this->control_q_response);
        sds *load_entry = new sds[3];
        load_entry[0] = command;
        load_entry[1] = query;
        load_entry[2] = NULL;
        this->control_q_request->Enqueue(load_entry);
    }

    ~RxQueryDuplexer()
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
        sds *response = this->control_q_response->Dequeue();
        if (response != NULL)
        {
            // sdsfree(response[0]);
            raxFree((rax *)response[2]);
            delete response;
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