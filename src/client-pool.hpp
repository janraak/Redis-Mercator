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
#include "rxSuiteHelpers.h"
#ifdef __cplusplus
}
#endif

#include "graphstack.hpp"

template <typename T>
class RedisClientPool
{
public:
      static  rax *Get_ClientPool_Registry();
public:
    static rax *HostRegistry;
    static rax *Lookup;

    const char *host_reference;
    const char *host;
    int port;
    int grow_by;

    GraphStack<T> in_use;
    GraphStack<T> free;

    RedisClientPool();
    void Init(const char *host_reference, const char *host, int port, int initial_number_of_connections, int extra_number_of_connections);
    static RedisClientPool<T> *New(const char *address, int initial_number_of_connections, int extra_number_of_connections);
    static void Free(RedisClientPool<T> *connector);
    T *NewInstance();
    int Grow();

    static T *Acquire(const char *host_reference, const char *suffix, const char */*caller*/);

    static void Release(T *client, const char */*caller*/);
};

#endif