#ifndef __RXGRAPHLOAD_multiplexer_H__
#define __RXGRAPHLOAD_multiplexer_H__

#define VALUE_ONLY 2
#define FIELD_AND_VALUE_ONLY 3
#define FIELD_OP_VALUE 4

#include "command-multiplexer.hpp"
#include "simpleQueue.hpp"
#include "sjiboleth.hpp"
#include "client-pool.hpp"
#include "graphstackentry.hpp"

extern     void ExecuteRedisCommand(SimpleQueue *ctx, void *stash, const char *address, int port);

typedef int comparisonProc(char *l, int ll, char *r);

#ifdef __cplusplus
extern "C"
{
#endif

#include "string.h"
#include "sdsWrapper.h"
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>

#ifdef __cplusplus
}
#endif


rxString readFileIntoSds(const string &path)
{
    struct stat sb
    {
    };
    string res;

    FILE *input_file = fopen(path.c_str(), "r");
    if (input_file == nullptr)
    {
        perror("fopen");
        return rxStringEmpty();
    }

    stat(path.c_str(), &sb);
    res.resize(sb.st_size);
    fread(const_cast<char *>(res.data()), sb.st_size, 1, input_file);
    fclose(input_file);

    return rxStringNew(res.c_str());
}

class GraphParser : public JsonDialect
{
public:
    GraphParser()
        : JsonDialect()
    {
        this->object_and_array_controls = true;
        this->RegisterDefaultSyntax();
    }

    bool RegisterDefaultSyntax()
    {
        this->DeregisterSyntax("!!!,");
        this->DeregisterSyntax(",");
        this->DeregisterSyntax(":");
        this->RegisterSyntax(",", 5, 0, 0, NULL);
        this->RegisterSyntax(":", 30, 0, 0, NULL);
        return true;
    }
};
static void Load(ParsedExpression *rpn, SimpleQueue *ctx)
{
    ParserToken *t3 = NULL;
    rpn->RPN()->StartHead();
    GraphStackEntry *se;
    auto *graph_stack = new GraphStack<GraphStackEntry>();
    // int tally = 0;
    while ((t3 = rpn->RPN()->Next()) != NULL)
    {
        // if(tally++ >= 10000)
        //     break;
        // printf("Stack: %d ParserToken: %d %d %s\n", graph_stack->Size(), item++, t3->TokenType(), t3->TokenAsSds());
        switch (t3->TokenType())
        {
        case _literal: // A string token
        case _key_set: // A string token
        case _operand: // A string token
        {
            // printf("-->ITEM %d --> ", item);
            se = new GraphStackEntry(t3->TokenAsSds(), graph_stack->Peek());
            se->ctx = ctx;
            graph_stack->Push(se);
            break;
        }
        case _immediate_operator: // An operator
        case _operator:           // An operator
        {
            if (strcmp(t3->TokenAsSds(), COMMA) == 0)
                break;
            if (strcmp(t3->TokenAsSds(), COLON) == 0)
            {
                try{
                // printf("-->ITEM %d --> ", item);
                GraphStackEntry *v = graph_stack->Pop();
                GraphStackEntry *k = graph_stack->Pop();
                if (k->token_value)
                {
                    GraphStackEntry *d = graph_stack->Pop();
                    d->Set(k, v);
                    free(v);
                    free(k);
                    graph_stack->Push(d);
                }
                else
                {
                    k->Set(v);
                    graph_stack->Push(k);
                }
                }
                catch(...){
                    printf("graphdb load exception\n");
                }
            }
            break;
        }
        case _open_bracket: // Open Bracket
        {
            if (strcmp(t3->TokenAsSds(), BEGIN_ARRAY) == 0)
                continue;
            se = new GraphStackEntry(ctx, graph_stack->Peek());
            graph_stack->Push(se);
            break;
        }
        case _close_bracket: // Close Bracket
        {
            if (strcmp(t3->TokenAsSds(), END_ARRAY) == 0)
                continue;
            se = graph_stack->Pop();
            // check edge/vertex vs graph!!!
            if (se->Contains(SUBJECT) && se->Contains(OBJECT) && se->Contains(PREDICATE))
            {
                se->Dump("Edge");
                se->Persist(ctx);
            }
            else
            {
                se->Dump("Vertice");
                se->Persist(ctx);
            }
            break;
        }
        }
    }
    free(graph_stack);
}

long long max_idle_seconds = 60;
long long execute_command_cron_tally = 0;
long long execute_command_cron_id = -1;
long long execute_command_interval_ms = 50;
long long execute_command_delay_ms = 50;

int Execute_Command_Cron(struct aeEventLoop *, long long, void *clientData)
{
    redisNodeInfo *data_config = rxDataNode();
    auto *queue = (SimpleQueue *)clientData;
    long long start = ustime();

    while (execute_command_cron_tally == 0 
            || queue->QueueLength() > 0 
            || queue->response_queue->QueueLength() > 0
            )
    {
        void *request = queue->Dequeue();
        if (request == NULL)
        {
            return execute_command_delay_ms;
        }
        ExecuteRedisCommand(queue, request, data_config->host_reference);

        // Check in loop! Slot time exhausted?
        if (ustime() - start >= execute_command_interval_ms * 1000)
        {
            // Yield when slot time over
            return execute_command_delay_ms;
        }
    }
    // When done
    return -1;
}

static void *execLoadThread(void *ptr)
{
    SimpleQueue *command_reponse_queue =  new SimpleQueue();
    SimpleQueue *command_request_queue =  new SimpleQueue(command_reponse_queue);
    execute_command_cron_id =  rxCreateTimeEvent(1, (aeTimeProc *)Execute_Command_Cron, command_request_queue, NULL);
    SimpleQueue *loader_queue = (SimpleQueue *)ptr;
    // indexer_set_thread_title("rxGraphDb async loader");
    printf("rxGraphDb async loader started\n");
    loader_queue->Started();

    // long long start = ustime();
    void *load_entry = loader_queue->Dequeue();
    // start = ustime();
    while (load_entry == NULL)
    {
        sched_yield();
        load_entry = loader_queue->Dequeue();
    }
    if (load_entry != NULL)
    {
        // TODO
        GET_ARGUMENTS_FROM_STASH(load_entry);
        auto *parser = new GraphParser();
        auto *parsed_json = parser->Parse((rxString)rxGetContainedObject(argv[1]));
        auto *sub = parsed_json;
        while (sub != NULL)
        {
            // printf("%s\n", sub->ToString());
            Load(sub, command_request_queue);
            sub = sub->Next();
        }
        delete parsed_json;
        delete parser;
        loader_queue->response_queue->Enqueue(load_entry);

    // free stashed redis command on same thread as allocated
    void *stash = command_reponse_queue->Dequeue();
    while (stash != NULL)
    {
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
    loader_queue->Stopped();
    printf("rxGraphDb async loader stopped\n");
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
    printf("rxGraphDb async redis commands stopped\n");
    return NULL;
}

class RxGraphLoadMultiplexer : public Multiplexer
{
public:
    SimpleQueue *request;
    SimpleQueue *response;

    RxGraphLoadMultiplexer(RedisModuleString **argv, int argc)
        : Multiplexer()
    {
        this->response = new SimpleQueue();
        this->request = new SimpleQueue((void *)execLoadThread, 1, this->response);
        size_t len;
        const char *argS = RedisModule_StringPtrLen(argv[1], &len);
        rxString arg = rxStringNew(argS);
        rxStringToUpper(arg);
        rxString keyword = rxStringNew("FILE");
        // TODO fixed bugs from use of stashes!!!!!!
        if (argc == 3 && strcmp(keyword, arg) == 0)
        {
            const char *pathS = RedisModule_StringPtrLen(argv[2], &len);
            rxString path = rxStringNew(pathS);
            rxString graph = readFileIntoSds(path);
            rxStashCommand(this->request, "", 1, graph);
            rxStringFree(arg);
        }
        else
        {
            rxStashCommand(this->request, "", 1, arg);
        }
    }

    ~RxGraphLoadMultiplexer()
    {
        this->response->Release();
        this->request->Release();

        delete this->request;
        delete this->response;
    }

    long long Timeout()
    {
        return 0;
    }

    int Execute()
    {
        void *response = this->response->Dequeue();
        if (response != NULL)
        {
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