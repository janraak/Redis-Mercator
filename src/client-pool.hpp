#ifndef __CLIENTPOOL_H__
#define __CLIENTPOOL_H__

#ifdef __cplusplus
extern "C"
{
#endif

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

    RedisClientPool(sds host, int port, int initial_number_of_connections, int extra_number_of_connections){
        this->host = host;
        this->port = port;
        grow_by = initial_number_of_connections;
        this->Grow();
        grow_by = extra_number_of_connections;
    }

    int Grow(){
        int tally_acquired = 0;
        for (int n = 0; n < this->grow_by; ++n)
        {
            auto *client = redisConnect(this->host, this->port);
            if (client == NULL || client->err)
            {
                if (client)
                {
                    sds e = sdscatprintf(sdsempty(), "Connection error: %s\n", client->errstr);
                    redisFree(client);
                    sdsfree(e);
                    return tally_acquired;
                }
                else
                {
                    sds e = sdscatprintf(sdsempty(), "Connection error: can't allocate lzf context\n");
                    sdsfree(e);
                    return tally_acquired;
                }
                return tally_acquired;
            }
            this->free.Push(client);
            tally_acquired++;
        }
        return tally_acquired;
    }

    static redisContext* Acquire(sds host, int port){
        sds address = sdscatfmt(sdsempty(), "%s:%i", host, port);
        auto *pool = (RedisClientPool *)raxFind(RedisClientPool::Registry, (UCHAR *)address, sdslen(address));
        if (pool == raxNotFound)
        {
            pool = new RedisClientPool(host, port, 1, 1);
            void *old;
            raxInsert(RedisClientPool::Registry, (UCHAR *)address, sdslen(address), pool, &old);
        }
        if(!pool->free.HasEntries()){
            pool->Grow();
        }
        if(!pool->free.HasEntries())
            return NULL;
        auto *client = pool->free.Pop();
        pool->in_use.Push(client);
        void *old;
        raxInsert(RedisClientPool::Lookup, (UCHAR *)client, sizeof(client), pool, &old);
        return client;
    }

    static void Release(redisContext* client){
        auto *pool = (RedisClientPool *)raxFind(RedisClientPool::Lookup, (UCHAR *)client, sizeof(client));
        if(pool == raxNotFound){
            printf("RedisClientPool unregisted redis client: 0x%x\n", (POINTER)client);
        }
        pool->in_use.Remove(client);
        pool->free.Push(client);
    }
};

rax *RedisClientPool::Registry = raxNew();
rax *RedisClientPool::Lookup = raxNew();
#endif