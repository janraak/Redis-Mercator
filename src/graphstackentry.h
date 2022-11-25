#ifndef __GRAPHSTACKENTRY_H__
#define __GRAPHSTACKENTRY_H__


#include <cstdarg>
#include <typeinfo>

#define WRAPPER
#include "simpleQueue.hpp"

#ifdef __cplusplus
extern "C"
{
#endif
#include "adlist.h"
#include "sdsWrapper.h"
#include <stddef.h>
#include "../../deps/hiredis/hiredis.h"

    void *rxStashCommand(SimpleQueue *ctx, const char *command, int argc, ...);
    void *rxStashCommand2(SimpleQueue *ctx, const char *command, int argt, int argc, void **args);
    void ExecuteRedisCommand(SimpleQueue *ctx, void *stash, const char *host_reference);
    void *rxSdsStashCommand(SimpleQueue *ctx, const char *command, int argc, ...);
    void ExecuteRedisCommandOnContext(SimpleQueue *ctx, void *stash, redisContext *redis);
    void FreeStash(void *stash);

#ifdef __cplusplus
}
#endif

#endif
