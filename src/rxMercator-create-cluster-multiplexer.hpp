#ifndef __RXTEXTLOAD_multiplexer_H__
#define __RXTEXTLOAD_multiplexer_H__

#define VALUE_ONLY 2
#define FIELD_AND_VALUE_ONLY 3
#define FIELD_OP_VALUE 4

#include "client-pool.hpp"
#include "command-multiplexer.hpp"
#include "graphstackentry.hpp"

#ifdef __cplusplus
extern "C"
{
#endif

#include "sdsWrapper.h"
#include "string.h"
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>

#ifdef __cplusplus
}
#endif

extern int Execute_Command_Cron(struct aeEventLoop *, long long, void * /*clientData*/);
long long execute_textload_command_cron_id = -1;

// rxGraphLoadMultiplexer cron is used to execute redis commands on the main thread
/*
   Asynchronous execution of the load.text command

   Syntax:

            LOAD.TEXT [TAB_SEPAPERATED | COMMA_SEPARATED | SEMICOLON_SEPARATED ] { AS_JSON | AS_HASH | AS_TEXT } %TEXT_BLOCK%

            TAB_SEPARATED: The column values are separated by tabs ('\t').
            COMMA_SEPARATED:  The column values are separated by commas (',').
            SEMICOLON_SEPARATED:  The column values are separated by semicolons (';').
            AS_JSON: Each row is transformed in a JSON object and stored in a STRING key type.
            AS_HASH:  Each row is a hash key, all colums are stored as fields of the hash type.
            AS_TEXT:  Each row is transformed in a composite string object and stored in a STRING key type.
            %TEXT_BLOCK%: The text table, consisting of rows and columns. The first row contains the column names. = 1

            The first column is taken as row key.


*/
#define TAB_SEPAPERATED "TAB_SEPARATED"
#define COMMA_SEPARATED "COMMA_SEPARATED"
#define SEMICOLON_SEPARATED "SEMICOLON_SEPARATED"
#define AS_JSON "AS_JSON"
#define AS_HASH "AS_HASH"
#define FROM_FILE "FROM"
#define AS_TEXT "AS_TEXT"
#define AS_STRING "AS_STRING"
#define USE_CRLF "CRLF"
#define USE_BAR "BAR"

static void *execCreateClusterThread(void *ptr)
{
    auto *command_reponse_queue = new SimpleQueue("xMakeCRESP");
    auto *command_request_queue = new SimpleQueue("xMakeCREQ", command_reponse_queue);
    execute_textload_command_cron_id = rxCreateTimeEvent(1, (aeTimeProc *)Execute_Command_Cron, command_request_queue, NULL); //????

    auto *creator_queue = (SimpleQueue *)ptr;
    rxServerLog(rxLL_NOTICE, "rxMercator.Create.Cluster async started");
    creator_queue->Started();

    // long long start = ustime();
    void *load_entry = creator_queue->Dequeue();
    // start = ustime();
    while (load_entry == NULL)
    {
        sched_yield();
        load_entry = creator_queue->Dequeue();
    }
    if (load_entry != NULL)
    {
        GET_ARGUMENTS_FROM_STASH(load_entry);

        int replication_requested = 0;
        int clustering_requested = 0;
        int controller_requested = 0;
        char *controller_path = 0;
        int start_requested = 0;
        rxString c_ip = rxStringEmpty();
        size_t arg_len;
        for (int j = 1; j < argc; ++j)
        {
            const char *q = (const char *)rxGetContainedObject(argv[j]);
            if (rxStringMatch(q, REPLICATION_ARG, MATCH_IGNORE_CASE) && strlen(q) == strlen(REPLICATION_ARG))
                replication_requested = 1;
            else if (rxStringMatch(q, CLUSTERING_ARG, MATCH_IGNORE_CASE) && strlen(q) == strlen(CLUSTERING_ARG))
                clustering_requested = 2;
            else if (rxStringMatch(q, CONTROLLER_ARG, MATCH_IGNORE_CASE) && strlen(q) == strlen(CONTROLLER_ARG))
            {
                controller_requested = 4;
                controller_path = (char *)rxGetContainedObject(argv[j + 1]);
                ++j;
            }
            else if (rxStringMatch(q, START_ARG, MATCH_IGNORE_CASE) && strlen(q) == strlen(START_ARG))
                start_requested = 4;
            else
                c_ip = rxStringNew(q);
        }
        rxString sha1 = getSha1("sii", c_ip, replication_requested, clustering_requested);
        rxString cluster_key = rxStringFormat("__MERCATOR__CLUSTER__%s", sha1);

        // free stashed redis command on same thread as allocated
        void *stash = command_reponse_queue->Dequeue();
        while (stash != NULL)
        {
            FreeStash(stash);
            stash = command_reponse_queue->Dequeue();
        }
    }
    creator_queue->Stopped();
    rxServerLog(rxLL_NOTICE, "rxMercator.Create.Cluster async stopped");
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

    creator_queue->response_queue->Enqueue(load_entry);
    rxDeleteTimeEvent(execute_textload_command_cron_id);
    rxServerLog(rxLL_NOTICE, "rxMercator.Create.Cluster async redis commands stopped");
    return NULL;
}

class rxCreateClusterMultiplexer : public Multiplexer
{
public:
    RedisModuleString **argv;
    int argc;
    rxCreateClusterMultiplexer(RedisModuleString **argv, int argc)
        : Multiplexer()
    {
        this->argv = argv;
        this->argc = argc;
    }

    ~rxCreateClusterMultiplexer()
    {
    }

    long long Timeout()
    {
        return 0;
    }

    int Execute()
    {
        void *response = this->threading_response->Dequeue();
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