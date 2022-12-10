/*
 * See: Template specialization [https://en.cppreference.com/w/cpp/language/template_specialization]
 */ 
#include "client-pool.hpp"
#ifdef __cplusplus
extern "C"
{
#endif
#include <pthread.h>
#include <string.h>
#include "stdlib.h"
#include "sdsWrapper.h"
#ifdef __cplusplus
}
#endif

struct client;

template <typename T>
rax *RedisClientPool<T>::Get_Thread_Registry()
{
    pthread_t id = pthread_self();
    rax *local_registry = (rax *)raxFind(RedisClientPool<T>::Registry, (UCHAR *)&id, sizeof(id));
    if (local_registry == raxNotFound)
    {
        local_registry = raxNew();
        raxInsert(RedisClientPool<T>::Registry, (UCHAR *)&id, sizeof(id), local_registry, NULL);
    }
    return local_registry;
}

template <typename T>
RedisClientPool<T>::RedisClientPool()
{
    this->host_reference = NULL;
    this->host = NULL;
    this->port = 0;
    grow_by = 1;
}

template <typename T>
void RedisClientPool<T>::Init(const char *host_reference, const char *host, int port, int initial_number_of_connections, int extra_number_of_connections)
{
    this->free.Init();
    this->in_use.Init();
    this->host_reference = host_reference;
    this->host = host;
    this->port = port;

    grow_by = initial_number_of_connections;
    this->Grow();
    grow_by = extra_number_of_connections;
}

template <typename T>
RedisClientPool<T> *RedisClientPool<T>::New(const char *address, int initial_number_of_connections, int extra_number_of_connections)
{
    RedisClientPool<T> *connector = NULL;

    int l = strlen(address);
    const char *colon = strstr(address, ":");
    void *connectorSpace = rxMemAlloc(sizeof(RedisClientPool<T>) + 2 * l + 1);
    memset(connectorSpace, 0xff, sizeof(RedisClientPool<T>) + 2 * l + 1);
    char *r = (char *)connectorSpace + sizeof(RedisClientPool<T>);
    char *h = r + l + 1;
    strncpy(r, address, l);
    r[l] = 0x00;
    strncpy(h, address, colon - address);
    h[colon - address] = 0x00;
    connector = ((RedisClientPool<T> *)connectorSpace);
    connector->Init(r, h, atoi(colon + 1), initial_number_of_connections, extra_number_of_connections);
    return connector;
}

template <typename T>
void RedisClientPool<T>::Free(RedisClientPool *connector){
    rxMemFree(connector);
}

template <typename T>
T *RedisClientPool<T>::NewInstance()
{
    return NULL;
}

template <>
redisContext *RedisClientPool<redisContext>::NewInstance()
{
    auto *c = redisConnect(this->host, this->port);
    if (c == NULL || c->err)
    {
        if (c)
        {
            rxString e = rxStringFormat("Connection error: %s\n", c->errstr);
            redisFree(c);
            rxStringFree(e);
            return NULL;
        }
        else
        {
            rxString e = rxStringFormat("Connection error: can't allocate lzf context\n");
            rxStringFree(e);
            return NULL;
        }
        return NULL;
    }
    return c;
}

template <>
struct client *RedisClientPool<struct client>::NewInstance()
{
    return (struct client *)rxCreateAOFClient();
}

template <typename T>
int RedisClientPool<T>::Grow()
{
    int tally_acquired = 0;
    for (int n = 0; n < this->grow_by; ++n)
    {
        auto *client = this->NewInstance();
        if (client == NULL)
        {
            return tally_acquired;
        }
        this->free.Push((T *)client);
        tally_acquired++;
    }
    return tally_acquired;
}

template <typename T>
T *RedisClientPool<T>::Acquire(const char *address)
{
    auto *pool = (RedisClientPool *)raxFind(RedisClientPool<T>::Get_Thread_Registry(), (UCHAR *)address, strlen(address));
    if (pool == raxNotFound)
    {
        pool = RedisClientPool::New(address, 4, 4);
        void *old;
        raxInsert(RedisClientPool<T>::Get_Thread_Registry(), (UCHAR *)address, strlen(address), pool, &old);
    }
    if (!pool->free.HasEntries())
    {
        pool->Grow();
    }
    if (!pool->free.HasEntries())
        return NULL;
    auto *client = pool->free.Pop();
    pool->in_use.Push(client);
    void *old;
    raxInsert(RedisClientPool<T>::Lookup, (UCHAR *)client, sizeof(client), pool, &old);
    return client;
}

template <typename T>
void RedisClientPool<T>::Release(T *client)
{
    auto *pool = (RedisClientPool<T> *)raxFind(RedisClientPool::Lookup, (UCHAR *)client, sizeof(client));
    if (pool == raxNotFound)
    {
        printf("RedisClientPool unregisted redis client: 0x%lx\n", (POINTER)client);
    }
    pool->in_use.Remove(client);
    pool->free.Push(client);
}

template <typename T>
rax *RedisClientPool<T>::Registry = raxNew();
template <typename T>
rax *RedisClientPool<T>::Lookup = raxNew();

template class RedisClientPool<redisContext>;
// template class RedisClientPool<void>;
template class RedisClientPool<struct client>;
