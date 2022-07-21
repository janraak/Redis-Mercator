/*
 * See: Template specialization [https://en.cppreference.com/w/cpp/language/template_specialization]
 */ 
#include "client-pool.hpp"
#ifdef __cplusplus
extern "C"
{
#endif
#include <pthread.h>

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
RedisClientPool<T>::RedisClientPool(sds host, int port, int initial_number_of_connections, int extra_number_of_connections)
{
    this->host = host;
    this->port = port;
    grow_by = initial_number_of_connections;
    this->Grow();
    grow_by = extra_number_of_connections;
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
            sds e = sdscatprintf(sdsempty(), "Connection error: %s\n", c->errstr);
            redisFree(c);
            sdsfree(e);
            return NULL;
        }
        else
        {
            sds e = sdscatprintf(sdsempty(), "Connection error: can't allocate lzf context\n");
            sdsfree(e);
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
T *RedisClientPool<T>::Acquire(sds host, int port)
{
    sds address = sdscatfmt(sdsempty(), "%s:%i", host, port);
    auto *pool = (RedisClientPool *)raxFind(RedisClientPool<T>::Get_Thread_Registry(), (UCHAR *)address, sdslen(address));
    if (pool == raxNotFound)
    {
        pool = new RedisClientPool(host, port, 1, 1);
        void *old;
        raxInsert(RedisClientPool<T>::Get_Thread_Registry(), (UCHAR *)address, sdslen(address), pool, &old);
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
T *RedisClientPool<T>::Acquire(const char *host, int port)
{
    if(host == NULL)
        return NULL;
    sds address = sdsnew(host);
    T *ctx = RedisClientPool<T>::Acquire(address, port);
    sdsfree(address);
    return ctx;
}

template <typename T>
void RedisClientPool<T>::Release(T *client)
{
    auto *pool = (RedisClientPool<T> *)raxFind(RedisClientPool::Lookup, (UCHAR *)client, sizeof(client));
    if (pool == raxNotFound)
    {
        printf("RedisClientPool unregisted redis client: 0x%x\n", (POINTER)client);
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
