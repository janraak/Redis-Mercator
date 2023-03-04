#ifndef __RXWget_multiplexer_H__
#define __RXWget_multiplexer_H__

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

long long execute_Wget_command_cron_id = -1;

// rxGraphLoadMultiplexer cron is used to execute redis commands on the main thread
/*
   Asynchronous execution of the load.text command

   Syntax:

            G.WGET %url%

        The named resource is download to the data folder.
        The filename is returned.

*/

static void *execWgetThread(void *ptr);

class RxWgetMultiplexer : public Multiplexer
{
public:
    const char *url;
    bool done;
    pthread_t thread_id;
    RxWgetMultiplexer(RedisModuleString **argv, int)
        : Multiplexer()
    {
        url = (const char *)rxGetContainedObject(argv[1]);
        this->done = false;
        if (pthread_create(&this->thread_id, NULL, execWgetThread, this))
        {
            fprintf(stderr, "FATAL: Failed to start wget thread\n");
            exit(1);
        }
    }

    ~RxWgetMultiplexer()
    {
    }

    long long Timeout()
    {
        return 0;
    }

    int Execute()
    {
        if (this->done)
        {
            return -1;
        }
        return 1;
    }

    int Write(RedisModuleCtx *ctx)
    {
        char *fn = (char *)this->url + strlen(this->url) - 1;
        while (fn >= this->url && *fn != '/')
            fn--;
        ++fn;
        RedisModule_ReplyWithSimpleString(ctx, fn);
        return 1;
    }

    int Done()
    {
        return this->done;
    }
};

static void *execWgetThread(void *ptr)
{
    auto *multiplexer = (RxWgetMultiplexer *)ptr;

    char cwd[FILENAME_MAX]; // create string buffer to hold path
    GetCurrentDir(cwd, FILENAME_MAX);
    rxString command = rxStringFormat("wget  --timestamping  -P %s/data %s", cwd, multiplexer->url);
    system(command);
    multiplexer->done = true;
    return NULL;
}

#endif