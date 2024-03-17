/*
 * See: Template specialization [https://en.cppreference.com/w/cpp/language/template_specialization]
 */ 
#include <typeinfo>
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
#include "tls.hpp"
#include "rxSuiteHelpers.h"

struct client;
thread_local  rax *RedisClientPoolRegistry = NULL;

void *_allocateRax(void *){
    return raxNew();
}

template <typename T>
rax *RedisClientPool<T>::Get_ClientPool_Registry()
{
    char poolname[256] = "RedisClientPool:";
    snprintf((char *)&poolname, sizeof(poolname), "RedisClientPool:%d", 0 * (int)gettid());
    rax *registry = tls_get<rax *>((const char *)&poolname, _allocateRax, NULL);
    return registry;
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
    // char *cname = typeid(his).name();
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

    int l = strlen(address) + 1;
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
    auto c = redisConnect(this->host, this->port);
    if (c == NULL || c->err)
    {
        if (c)
        {
            rxServerLog(rxLL_NOTICE, "# RedisClientPool<redisContext>::NewInstance() # host:%s Connection error: %s", this->host_reference, c->errstr);
            redisFree(c);
            return NULL;
        }
        else
        {
            rxServerLog(rxLL_NOTICE, "# RedisClientPool<redisContext>::NewInstance() # host:%s Connection error", this->host_reference);
            return NULL;
        }
        return NULL;
    }
    return c;
}

template <>
redisAsyncContext *RedisClientPool<redisAsyncContext>::NewInstance()
{
    auto *c = redisAsyncConnect(this->host, this->port);
    if (c == NULL || c->err)
    {
        if (c)
        {
            rxServerLog(rxLL_NOTICE, "# RedisClientPool<redisAsyncContext>::NewInstance() # host:%s Connection error: %s", this->host_reference, c->errstr);
            redisAsyncFree(c);
            return NULL;
        }
        else
        {
            rxServerLog(rxLL_NOTICE, "# RedisClientPool<redisAsyncContext>::NewInstance() # host:%s Connection error", this->host_reference);
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
        auto *client = RedisClientPool<T>::NewInstance();
        if (client == NULL)
        {
            rxServerLog(rxLL_NOTICE, "::Grow %s free:%d inuse:%ld UNABLE TO GROW", this->host_reference, this->free.Size(), raxSize(Get_ClientPool_Registry()));
            return tally_acquired;
        }
        this->free.Push((T *)client);
        tally_acquired++;
    }
    if((this->free.Size(), raxSize(Get_ClientPool_Registry())) > 64)
        rxServerLog(rxLL_NOTICE, "::Grow %s free:%d inuse:%ld TOO BIG", this->host_reference, this->free.Size(), raxSize(Get_ClientPool_Registry()));
    //rxServerLog(rxLL_NOTICE, "::Grow %s free:%d inuse:%ld", this->host_reference, this->free.Size(), raxSize(Get_ClientPool_Registry()));
    return tally_acquired;
}

template <typename T>
T *RedisClientPool<T>::Acquire(const char *address, const char *suffix, const char */*caller*/)
{
    char reference[256];
    strcpy(reference,address);
    strcat(reference, suffix);
    auto *pool = (RedisClientPool *)raxFind(Get_ClientPool_Registry(), (UCHAR *)reference, strlen(reference));
    if (pool == raxNotFound)
    {
        pool = RedisClientPool<T>::New(reference, 4, 4);
        void *old;
        raxInsert(Get_ClientPool_Registry(), (UCHAR *)reference, strlen(reference), pool, &old);
    }
    if (!pool->free.HasEntries())
    {
        pool->Grow();
    }
    if (!pool->free.HasEntries())
        return NULL;
    auto *client = pool->free.Pop();
    // pool->in_use.Push(client);
    void *old;
    // Maintain client to pool!!
    raxInsert(Get_ClientPool_Registry(), (UCHAR *)client, sizeof(client), pool, &old);
    //rxServerLog(rxLL_NOTICE, "::Acquire %s->%p free:%d inuse:%ld by:%s", reference, client, pool->free.Size(), raxSize(Get_ClientPool_Registry()), caller);
    return client;
}

template <typename T>
void RedisClientPool<T>::Purge(const char *address, const char *suffix)
{
    char reference[256];
    strcpy(reference, address);
    strcat(reference, suffix);
    auto pool = (RedisClientPool *)raxFind(Get_ClientPool_Registry(), (UCHAR *)reference, strlen(reference));
    if (pool == raxNotFound)
        return;
    pool->free.StartHead();
    while (pool->free.HasEntries())
    {
        T *c = pool->free.Pop();
        RedisClientPool<T>::Disconnect(c);
    }
    // pool->in_use.StartHead();
    // while (pool->in_use.HasEntries())
    // {
    //     T *c = pool->in_use.Pop();
    //     Disconnect(c);
    // }
}

template<> void RedisClientPool<redisContext>::Disconnect(redisContext *c){
    redisFree(c);

}

template<> void RedisClientPool<redisAsyncContext>::Disconnect(redisAsyncContext *c){
    redisAsyncFree(c);
}


template<> void RedisClientPool<struct client>::Disconnect(struct client *){
    
}

template <typename T>
void RedisClientPool<T>::Release(T *client, const char *caller)
{
    // find pool for client
    auto ppool = Get_ClientPool_Registry();
    RedisClientPool<T> *pool = NULL;
    raxRemove(ppool, (UCHAR *)client, sizeof(client), (void**)&pool);
    if (pool == NULL || pool == raxNotFound)
    {
        rxServerLog(rxLL_NOTICE, "RedisClientPool unregisted redis client %p caller: %s\n", client, caller);
        RedisClientPool<T>::Disconnect(client);
    }
    else
    {
        // rxServerLog(rxLL_NOTICE, "::Release %s<-%p free:%d inuse:%d B by:%s", pool->host_reference, client, pool->free.Size(), raxSize(Get_ClientPool_Registry()), caller);
       RedisClientPool<T>::Disconnect(client);

        // pool->free.Push(client);
    }
    // rxServerLog(rxLL_NOTICE, "::Release %s<-%p free:%d inuse:%ld A by:%s", pool->host_reference, client, pool->free.Size(), raxSize(Get_ClientPool_Registry()), caller);
}

template <typename T>
rax *RedisClientPool<T>::HostRegistry = raxNew();
template <typename T>
rax *RedisClientPool<T>::Lookup = raxNew();

template class RedisClientPool<redisContext>;
template class RedisClientPool<struct client>;

template class RedisClientPool<redisAsyncContext>;

extern "C" void    ExecuteOnFake(const char *commandName, int argc, void **argv){
    client *c = (struct client *)RedisClientPool<client>::Acquire("THIS", "_FAKE", "ExecuteRedisCommand cmd");
    rxAllocateClientArgs(c, argv, 3);
    void *command_definition = rxLookupCommand(commandName);
    if(command_definition)
        rxClientExecute(c, command_definition);
    else
        rxServerLog(rxLL_WARNING, "Unknown command %s,  arg count: %d", commandName, argc);
    RedisClientPool<struct client>::Release(c, "ExecuteRedisCommand cmd");
}

char *extractStringFromRedisReply(redisReply *r, const char *field)
{
    if (r && r->type == REDIS_REPLY_ARRAY)
    {
        for (size_t n = 0; n < r->elements; n += 2)
        {
            if (rxStringMatch(field, (const char *)r->element[n]->str, 1))
            {
                return r->element[n + 1]->str;
            }
        }
    }
    return NULL;
}

redisReply *extractGroupFromRedisReply(redisReply *r, const char *field)
{
    if (r && r->type == REDIS_REPLY_ARRAY)
    {
        for (size_t n = 0; n < r->elements; n += 2)
        {
            if (rxStringMatch(field, (const char *)r->element[n]->str, 1))
            {
                return r->element[n + 1];
            }
        }
    }
    return NULL;
}

redisReply *extractGroupFromRedisReplyByIndex(redisReply *r, size_t index)
{
    if (r && (r->type == REDIS_REPLY_ARRAY) && (r->elements >= index))
    {
                return r->element[index];
    }
    return NULL;
}


extern "C" void forwardTriggeredKey(void *key){
    redisNodeInfo *data_config = rxDataNode();

    void *argv[2] = {rxCreateStringObject("RXTRIGGER", 9), key};
    auto *c = (struct client *)RedisClientPool<struct client>::Acquire(data_config->host_reference, "_FWDR", "Trigger Rules");
    rxSetDatabase(c, 0);
    rxAllocateClientArgs(c, argv, 2);
    void *command_definition = rxLookupCommand("RXTRIGGER");
    if (command_definition)
        rxClientExecute(c, command_definition);
    rxFreeStringObject(argv[0]);
    RedisClientPool<struct client>::Release(c, "Trigger Rules");
}


template<> void GraphStack<redisContext>::PopAndDeleteValue(){
    /*redisContext *e = */this->Pop();
    // RedisClientPool<redisContext>::Disconnect(e);
}


template<> void GraphStack<redisAsyncContext>::PopAndDeleteValue(){
    /*redisAsyncContext *e = */this->Pop();
    // RedisClientPool<redisAsyncContext>::Disconnect(e);
}



template<> void GraphStack<struct client>::PopAndDeleteValue(){
    /*struct client *e = */this->Pop();
    // RedisClientPool<struct client>::Disconnect(e);
}
