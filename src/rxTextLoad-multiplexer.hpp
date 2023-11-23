#ifndef __RXTEXTLOAD_multiplexer_H__
#define __RXTEXTLOAD_multiplexer_H__

#define VALUE_ONLY 2
#define FIELD_AND_VALUE_ONLY 3
#define FIELD_OP_VALUE 4

#include "client-pool.hpp"
#include "command-multiplexer.hpp"
#include "graphstackentry.hpp"
#include "simpleQueue.hpp"
#include "sjiboleth.hpp"
#define GetCurrentDir getcwd

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

long long execute_textload_command_cron_id = -1;

// rxGraphLoadMultiplexer cron is used to execute redis commands on the main thread
/*
   Asynchronous execution of the load.text command

   Syntax:

            LOAD.TEXT [TAB_SEPAPERATED | COMMA_SEPARATED | SEMICOLON_SEPARATED ] { AS_JSON | AS_HASH | AS_TEXT } {KEY %column}  {TYPE %column} %TEXT_BLOCK%

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
#define KEY_COLUMN "KEY"
#define TYPE_COLUMN "TYPE"

/* Backport from Redis 7*/
/* Modify the buffer replacing all occurrences of chars from the 'from'
 * set with the corresponding char in the 'to' set. Always returns s.
 */
static char *rxStringLenMapCharsCopy(char *s, size_t len, const char *from, const char *to, size_t setlen) {
    for (size_t j = 0; j < len; j++) {
        for (size_t i = 0; i < setlen; i++) {
            if (s[j] == from[i]) {
                s[j] = to[i];
                break;
            }
        }
    }
    return s;
}

// TODO Assemble hdr array!
static int getColumnCount(char *start, char *end, char *column_separator, char *key_column, int *key_column_no, char *type_column, int *type_column_no)
{
    int column_count = 0;
    char *prev_tab = start;
    while (start < end)
    {
        char *tab = strstr(start, column_separator);
        if (tab && tab < end)
        {
            if (key_column && rxStringMatchLen(key_column, strlen(key_column), prev_tab, tab - prev_tab, MATCH_IGNORE_CASE))
                *key_column_no = column_count;
            if (type_column && rxStringMatchLen(type_column, strlen(type_column), prev_tab, tab - prev_tab, MATCH_IGNORE_CASE))
                *type_column_no = column_count;
            ++column_count;
            start = tab + 1;
        }
        else
            return column_count + 1;
        prev_tab = tab + 1;
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
        if (*row_start == '"' && *(v_tab - 1) == '"')
        {
            enclosure = 1;
        }
        rxString v = rxStringNewLen(row_start + enclosure, (v_tab - enclosure) - (row_start + enclosure));

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

static void *extractRowAsJson(SimpleQueue *, char *row_start, char *row_end, char *hdr_start, char *hdr_end, char *column_separator)
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
    int l = strlen(value);
    *((char *)value + l - 1) = 0x00;
    value = rxStringFormat("{%s}", value);
    if (value != NULL)
    {
        redisNodeInfo *data_config = rxDataNode();
        auto *client = RedisClientPool<redisContext>::Acquire(data_config->host_reference, "_CLIENT", "extractRowAsString");
        auto *rcc = (redisReply *)redisCommand(client, "SET %s %s", key, value);
        if (rcc != NULL)
            freeReplyObject(rcc);
        RedisClientPool<redisContext>::Release(client, "extractRowAsString");
    }
    rxStringFree(key);
    return NULL;
}

static void *extractRowAsHash(SimpleQueue *, char *row_start, char *row_end, char *hdr_start, char *hdr_end, char *column_separator)
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
        if (*row_start == '"' && *(v_tab - 1) == '"')
        {
            enclosure = 1;
        }
        int tailing_spaces = 0;
        char *tail = v_tab - 1;
        while (*tail == ' ')
        {
            tail--;
            tailing_spaces++;
        }

        rxString v = rxStringNewLen(row_start + enclosure, (v_tab - enclosure - tailing_spaces) - (row_start + enclosure));

        if (strlen(f) == 0)
            f = (char *)"any";

        if (strlen(v) > 0)
        {
            redisNodeInfo *data_config = rxDataNode();
            auto *client = RedisClientPool<redisContext>::Acquire(data_config->host_reference, "_CLIENT", "extractRowAsString");
            auto *rcc = (redisReply *)redisCommand(client, "HSET %s %s %s", key, f, v);
            if (rcc != NULL)
                freeReplyObject(rcc);
            if (rxStringMatch(TYPE_COLUMN, f, MATCH_IGNORE_CASE))
            {
                auto tkey = rxStringFormat("`%s", v);
                rcc = (redisReply *)redisCommand(client, "SADD %s %s", tkey, key);
                if (rcc != NULL)
                    freeReplyObject(rcc);
            }
            RedisClientPool<redisContext>::Release(client, "extractRowAsString");
        }

        if (f != (char *)"any")
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
static void *extractRowAsText(SimpleQueue *, char *row_start, char *row_end, char *hdr_start, char *hdr_end, char *column_separator)
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
        redisNodeInfo *data_config = rxDataNode();
        auto *client = RedisClientPool<redisContext>::Acquire(data_config->host_reference, "_CLIENT", "extractRowAsString");
        auto *rcc = (redisReply *)redisCommand(client, "SET %s %s", key, value);
        if (rcc != NULL)
            freeReplyObject(rcc);
        RedisClientPool<redisContext>::Release(client, "extractRowAsString");
    }
    rxStringFree(key);
    return NULL;
}

static void *extractRowAsString(SimpleQueue *, char *row_start, char *row_end, char *hdr_start, char *hdr_end, char *column_separator)
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
        redisNodeInfo *data_config = rxDataNode();
        auto *client = RedisClientPool<redisContext>::Acquire(data_config->host_reference, "_CLIENT", "extractRowAsString");
        auto *rcc = (redisReply *)redisCommand(client, "SET %s %s", key, value);
        if (rcc != NULL)
            freeReplyObject(rcc);
        RedisClientPool<redisContext>::Release(client, "extractRowAsString");

        rxStringFree(value);
    }
    rxStringFree(key);
    return NULL;
}
extern rxString readFileIntoSds(const string &path);

static void *execTextLoadThread(void *ptr)
{
    auto *command_reponse_queue = new SimpleQueue("xTxtRESP");
    auto *command_request_queue = new SimpleQueue("xTxtREQ", command_reponse_queue);
    execute_textload_command_cron_id = rxCreateTimeEvent(1, (aeTimeProc *)Execute_Command_Cron, command_request_queue, NULL);

    // // TODO: Fix potential memory release problem!
    // SimpleQueue *command_request_queue = (SimpleQueue *)rxGetCronCommandQueue();
    // SimpleQueue *command_reponse_queue =  command_request_queue->response_queue;

    auto *loader_queue = (SimpleQueue *)ptr;
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
        rxSetIndexScoringMethodFromString("default");

        char *column_separator = (char *)"\t";
        char *row_separator = (char *)"\n";
        rowExtractionProc *rowExtractor = (rowExtractionProc *)extractRowAsHash;
        char *tlob = NULL;
        char *loaded_file = NULL;
        char *key_column = NULL;
        int key_column_no = -1;
        char *type_column = NULL;
        int type_column_no = -1;

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
            else if (rxStringMatch(argS, FROM_FILE, MATCH_IGNORE_CASE))
            {
                ++a;
                argS = (const char *)rxGetContainedObject(argv[a]);

                auto *config = getRxSuite();
                const char *path = rxStringFormat("%s/%s", config->wget_root, argS);
                loaded_file = (char *)readFileIntoSds(path);
                if (!loaded_file)
                {
                    path = rxStringFormat("%s/data/%s", config->wget_root, argS);
                    loaded_file = (char *)readFileIntoSds(path);
                }
            }
            else if (rxStringMatch(argS, "INDEX_SCORING", MATCH_IGNORE_CASE))
            {
                ++a;
                argS = (const char *)rxGetContainedObject(argv[a]);
                rxSetIndexScoringMethodFromString(argS);
            }
            else if (rxStringMatch(argS, KEY_COLUMN, MATCH_IGNORE_CASE))
            {
                ++a;
                key_column = (char *)rxGetContainedObject(argv[a]);
            }
            else if (rxStringMatch(argS, TYPE_COLUMN, MATCH_IGNORE_CASE))
            {
                ++a;
                type_column = (char *)rxGetContainedObject(argv[a]);
            }
            else
                tlob = (char *)argS;
        }

        if (loaded_file)
            tlob = loaded_file;

        char *hdr_start = tlob;
        char *hdr_end = strstr(tlob, row_separator);
        if (hdr_end == NULL)
        {
            return NULL;
        }
        rxStringLenMapCharsCopy(hdr_start, hdr_end - hdr_start, " ()./+-*%", "_________", 9);
        int column_count = getColumnCount(hdr_start, hdr_end, column_separator, key_column, &key_column_no, type_column, &type_column_no);

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

    loader_queue->response_queue->Enqueue(load_entry);
    rxDeleteTimeEvent(execute_textload_command_cron_id);
    rxServerLog(rxLL_NOTICE, "rxGraphDb.Load.Text.Load.Text async redis commands stopped");
    return NULL;
}

class RxTextLoadMultiplexer : public Multiplexer
{
public:
    SimpleQueue *threading_request;
    SimpleQueue *threading_response;

    RxTextLoadMultiplexer(RedisModuleString **argv, int argc)
        : Multiplexer(argv, argc)
    {
        this->threading_response = new SimpleQueue("TxtLdMuxRESP");
        this->threading_request = new SimpleQueue("TxtLdMuxREQ", (void *)execTextLoadThread, 1, this->threading_response);
        rxStashCommand2(this->threading_request, "", 2, argc, (void **)argv);
    }

    ~RxTextLoadMultiplexer()
    {
        this->threading_response->Release();
        this->threading_request->Release();

        delete this->threading_request;
        delete this->threading_response;
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