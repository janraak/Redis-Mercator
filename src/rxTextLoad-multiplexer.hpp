#ifndef __RXTEXTLOAD_multiplexer_H__
#define __RXTEXTLOAD_multiplexer_H__

#define VALUE_ONLY 2
#define FIELD_AND_VALUE_ONLY 3
#define FIELD_OP_VALUE 4

#include "command-multiplexer.hpp"
#include "simpleQueue.hpp"
#include "sjiboleth.hpp"
#include "client-pool.hpp"
#include "graphstackentry.hpp"

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
#define TAB_SEPAPERATED "TAB_SEPAPERATED"
#define COMMA_SEPARATED "COMMA_SEPARATED"
#define SEMICOLON_SEPARATED "SEMICOLON_SEPARATED"
#define AS_JSON "AS_JSON"
#define AS_HASH "AS_HASH"
#define AS_TEXT "AS_TEXT"
#define AS_STRING "AS_STRING"
#define USE_CRLF "CRLF"
#define USE_BAR "BAR"

// TODO Assemble hdr array!
static int getColumnCount(char *start, char *end, char *column_separator)
{
    int column_count = 0;

    while (start < end)
    {
        char *tab = strstr(start, column_separator);
        if (tab && tab < end)
        {
            ++column_count;
            start = tab + 1;
        }
        else
            return column_count + 1;
    }
    return column_count;
}

typedef void *rowExtractionProc(SimpleQueue *req_q, char *row_start, char *row_end, char *hdr_start, char *hdr_end, char *column_separator);

static rxString extractRowForStringKey(char *row_start, char *row_end, char *hdr_start, char *hdr_end, char *column_separator, const char *format)
{
    rxString value = rxStringEmpty();
    bool done = false;
    char *f_tab = NULL;
    char *v_tab = NULL;
    while (done == false)
    {
        if (hdr_start != NULL)
        {
            f_tab = strstr(hdr_start, column_separator);
            if (f_tab > hdr_end)
            {
                f_tab = hdr_end;
                done = true;
            }
            else if (f_tab == NULL)
            {
                f_tab = strchr(hdr_start, 0x00);
                done = true;
            }
        }
        v_tab = strstr(row_start, column_separator);
        if (v_tab > row_end)
        {
            v_tab = row_end;
            done = true;
        }
        else if (v_tab == NULL)
        {
            v_tab = strchr(row_start, 0x00);
            done = true;
        }
        rxString f = (hdr_start != NULL) ? rxStringNewLen(hdr_start, f_tab - hdr_start) : rxStringEmpty();
        int enclosure = 0;
        if(*row_start == '"' && *(v_tab - 1) == '"'){
            enclosure = 1;

        }
        rxString v = rxStringNewLen(row_start + enclosure, (v_tab - enclosure) - (row_start+enclosure));

        if (hdr_start == NULL)
            value = rxStringFormat(format, value, v);
        else if (strlen(f) > 0 && strlen(v) > 0)
            value = rxStringFormat(format, value, f, v);
        else if (strlen(v) > 0)
            value = rxStringFormat(format, value, "*", v);
        rxStringFree(f);
        rxStringFree(v);
        if (hdr_start != NULL)
            hdr_start = f_tab + strlen(column_separator);
        row_start = v_tab + strlen(column_separator);
    }
    return value;
}

static void *extractRowAsJson(SimpleQueue *req_q, char *row_start, char *row_end, char *hdr_start, char *hdr_end, char *column_separator)
{
    char *f_tab = strstr(hdr_start, column_separator);
    char *v_tab = strstr(row_start, column_separator);
    if (v_tab == NULL)
        return NULL;
    rxString key = rxStringNewLen(row_start, v_tab - row_start);
    if (strlen(key) == 0)
        return NULL;

    hdr_start = f_tab + strlen(column_separator);
    row_start = v_tab + strlen(column_separator);
    rxString value = extractRowForStringKey(row_start, row_end, hdr_start, hdr_end, column_separator, "%s\"%s\":\"%s\",");
    value = rxStringFormat("{%s}", value);
    if (value != NULL)
    {
        void *args[] = {(void *)key, (void *)value};
        rxStashCommand2(req_q, "SETNX", STASH_STRING, 2, args);
        rxStringFree(value);
    }
    rxStringFree(key);
    return NULL;
}

static void *extractRowAsHash(SimpleQueue *req_q, char *row_start, char *row_end, char *hdr_start, char *hdr_end, char *column_separator)
{

    char *f_tab = strstr(hdr_start, column_separator);
    char *v_tab = strstr(row_start, column_separator);
    if (v_tab == NULL)
        return NULL;
    rxString key = rxStringNewLen(row_start, v_tab - row_start);
    if (strlen(key) == 0)
        return NULL;

    hdr_start = f_tab + strlen(column_separator);
    row_start = v_tab + strlen(column_separator);
    bool done = false;
    while (done == false)
    {
        f_tab = strstr(hdr_start, column_separator);
        if (f_tab > hdr_end)
        {
            f_tab = hdr_end;
            done = true;
        }
        else if (f_tab == NULL)
        {
            f_tab = strchr(hdr_start, 0x00);
            done = true;
        }
        v_tab = strstr(row_start, column_separator);
        if (v_tab > row_end)
        {
            v_tab = row_end;
            done = true;
        }
        else if (v_tab == NULL)
        {
            v_tab = strchr(row_start, 0x00);
            done = true;
        }
        rxString f = rxStringNewLen(hdr_start, f_tab - hdr_start);
        int enclosure = 0;
        if(*row_start == '"' && *(v_tab - 1) == '"'){
            enclosure = 1;

        }
        rxString v = rxStringNewLen(row_start + enclosure, (v_tab - enclosure) - (row_start+enclosure));

        if (strlen(f) == 0)
            f = rxStringNew("any");

        if (strlen(v) > 0)
        {
            void *args[] = {(void *)key, (void *)f, (void *)v};
            rxStashCommand2(req_q, WREDIS_CMD_HSET, STASH_STRING, 3, args);
        }

        rxStringFree(f);
        rxStringFree(v);
        hdr_start = f_tab + strlen(column_separator);
        row_start = v_tab + strlen(column_separator);
    }
    rxStringFree(key);
    return NULL;
}

/*
    Use column 1 as key
    Join column 2...n as <field>:<value>;
*/
static void *extractRowAsText(SimpleQueue *req_q, char *row_start, char *row_end, char *hdr_start, char *hdr_end, char *column_separator)
{
    char *f_tab = strstr(hdr_start, column_separator);
    char *v_tab = strstr(row_start, column_separator);
    if (v_tab == NULL)
        return NULL;
    rxString key = rxStringNewLen(row_start, v_tab - row_start);
    if (strlen(key) == 0)
        return NULL;

    hdr_start = f_tab + strlen(column_separator);
    row_start = v_tab + strlen(column_separator);
    rxString value = extractRowForStringKey(row_start, row_end, hdr_start, hdr_end, column_separator, "%s%s:%s;");
    if (value != NULL)
    {
        void *args[] = {(void *)key, (void *)value};
        rxStashCommand2(req_q, "SETNX", STASH_STRING, 2, args);
        rxStringFree(value);
    }
    rxStringFree(key);
    return NULL;
}

static void *extractRowAsString(SimpleQueue *req_q, char *row_start, char *row_end, char *hdr_start, char *hdr_end, char *column_separator)
{
    char *f_tab = strstr(hdr_start, column_separator);
    char *v_tab = strstr(row_start, column_separator);
    if (v_tab == NULL)
        return NULL;
    rxString key = rxStringNewLen(row_start, v_tab - row_start);
    if (strlen(key) == 0)
        return NULL;

    hdr_start = f_tab + strlen(column_separator);
    row_start = v_tab + strlen(column_separator);
    rxString value = extractRowForStringKey(row_start, row_end, NULL, hdr_end, column_separator, "%s%s ");
    if (value != NULL)
    {
        void *args[] = {(void *)key, (void *)value};
        rxStashCommand2(req_q, "SETNX", STASH_STRING, 2, args);
        rxStringFree(value);
    }
    rxStringFree(key);
    return NULL;
}

static void *execTextLoadThread(void *ptr)
{
    SimpleQueue *command_reponse_queue = new SimpleQueue("execTextLoadThreadRESP");
    SimpleQueue *command_request_queue = new SimpleQueue("execTextLoadThreadREQ", command_reponse_queue);
    execute_textload_command_cron_id = rxCreateTimeEvent(1, (aeTimeProc *)Execute_Command_Cron, command_request_queue, NULL);

    // // TODO: Fix potential memory release problem!
    // SimpleQueue *command_request_queue = (SimpleQueue *)rxGetCronCommandQueue();
    // SimpleQueue *command_reponse_queue =  command_request_queue->response_queue;

    SimpleQueue *loader_queue = (SimpleQueue *)ptr;
    // indexer_set_thread_title("rxGraphDb.Load.Text async loader");
    rxServerLog(rxLL_NOTICE, "rxGraphDb.Load.Text async loader started");
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
        GET_ARGUMENTS_FROM_STASH(load_entry);

        char *column_separator = (char *)"\t";
        char *row_separator = (char *)"\n";
        rowExtractionProc *rowExtractor = (rowExtractionProc *)extractRowAsHash;
        char *tlob = NULL;

        for (int a = 1; a < argc; ++a)
        {
            const char *argS = (const char *)rxGetContainedObject(argv[a]);
            if (rxStringMatch(argS, TAB_SEPAPERATED, MATCH_IGNORE_CASE))
                column_separator = (char *)"\t";
            else if (rxStringMatch(argS, COMMA_SEPARATED, MATCH_IGNORE_CASE))
                column_separator = (char *)",";
            else if (rxStringMatch(argS, SEMICOLON_SEPARATED, MATCH_IGNORE_CASE))
                column_separator = (char *)";";
            else if (rxStringMatch(argS, AS_JSON, MATCH_IGNORE_CASE))
                rowExtractor = (rowExtractionProc *)extractRowAsJson;
            else if (rxStringMatch(argS, AS_HASH, MATCH_IGNORE_CASE))
                rowExtractor = (rowExtractionProc *)extractRowAsHash;
            else if (rxStringMatch(argS, AS_TEXT, MATCH_IGNORE_CASE))
                rowExtractor = (rowExtractionProc *)extractRowAsText;
            else if (rxStringMatch(argS, AS_STRING, MATCH_IGNORE_CASE))
                rowExtractor = (rowExtractionProc *)extractRowAsString;
            else if (rxStringMatch(argS, USE_CRLF, MATCH_IGNORE_CASE))
                row_separator = (char *)"\r\n";
            else if (rxStringMatch(argS, USE_BAR, MATCH_IGNORE_CASE))
                row_separator = (char *)"|";
            else
                tlob = (char *)argS;
        }

        char *hdr_start = tlob;
        char *hdr_end = strstr(tlob, row_separator);
        if (hdr_end == NULL)
        {
            return NULL;
        }
        int column_count = getColumnCount(hdr_start, hdr_end, column_separator);

        rxUNUSED(column_count);
        tlob = hdr_end + strlen(row_separator);
        while (*tlob)
        {
            char *row_end = strstr(tlob, row_separator);
            if (row_end == NULL)
                row_end = strchr(tlob, 0x00);
            (rowExtractor)(command_request_queue, tlob, row_end, hdr_start, hdr_end, column_separator);
            if (*row_end == 0x00)
                break;
            tlob = row_end + strlen(row_separator);
        }

        loader_queue->response_queue->Enqueue(load_entry);

        // free stashed redis command on same thread as allocated
        void *stash = command_reponse_queue->Dequeue();
        while (stash != NULL)
        {
            FreeStash(stash);
            stash = command_reponse_queue->Dequeue();
        }
    }
    loader_queue->Stopped();
    rxServerLog(rxLL_NOTICE, "rxGraphDb.Load.Text async loader stopped");
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
    rxDeleteTimeEvent(execute_textload_command_cron_id);
    rxServerLog(rxLL_NOTICE, "rxGraphDb.Load.Text.Load.Text async redis commands stopped");
    return NULL;
}

class RxTextLoadMultiplexer : public Multiplexer
{
public:
    SimpleQueue *request;
    SimpleQueue *response;

    RxTextLoadMultiplexer(RedisModuleString **argv, int argc)
        : Multiplexer()
    {
        this->response = new SimpleQueue("RxTextLoadMultiplexerRESP");
        this->request = new SimpleQueue("RxTextLoadMultiplexerREQ", (void *)execTextLoadThread, 1, this->response);
        rxStashCommand2(this->request, "", 2, argc, (void **)argv);
    }

    ~RxTextLoadMultiplexer()
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