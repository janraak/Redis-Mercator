#ifndef __CLIENTPOOL_H__
#define __CLIENTPOOL_H__

#ifdef __cplusplus
extern "C"
{
#endif
#include <stddef.h>
#include <stdint.h>
#include "rxSuite.h"

#include "rax.h"
#include "../../deps/hiredis/hiredis.h"

#ifdef __cplusplus
}
#endif

#include "graphstack.hpp"

class RedisClientPool
{
public:
    static rax *Registry;
    static rax *Lookup;

    sds host;
    int port;
    int grow_by;

    GraphStack<redisContext> in_use;
    GraphStack<redisContext> free;

    RedisClientPool(sds host, int port, int initial_number_of_connections, int extra_number_of_connections);

    int Grow();

    static redisContext *Acquire(sds host, int port);
    static redisContext *Acquire(const char *host, int port);

    static void Release(redisContext *client);
};

#endif