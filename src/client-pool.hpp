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
      static  rax *Get_Thread_Registry();
public:
    static rax *Registry;
    static rax *Lookup;

    sds host;
    int port;
    int grow_by;

    GraphStack<T> in_use;
    GraphStack<T> free;

    RedisClientPool(sds host, int port, int initial_number_of_connections, int extra_number_of_connections);

    T *NewInstance();
    int Grow();

    static T *Acquire(sds host, int port);
    static T *Acquire(const char *host, int port);

    static void Release(T *client);
};

#endif