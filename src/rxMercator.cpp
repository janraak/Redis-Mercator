/* rxMercator -- Mercator Session Manager
 *
 * This module provides a session manager to cdfeate/start/stop/destroy demon clusters.
 *
 * -----------------------------------------------------------------------------
 *
 * Copyright (c) 2022, Jan Raak <jan dotraak dot nl at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#include "rxMercator.hpp"
#include "rxMercatorMonitor.hpp"

#include <fstream>
#include <iostream>
#include <string>

#include <array>
#include <cstdio>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

using namespace std;

#ifdef __cplusplus
extern "C"
{
#endif
#include "sdsWrapper.h"
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <version.h>

    /* Utils */
    long long ustime(void);
    long long mstime(void);

#include <unistd.h>
#define GetCurrentDir getcwd

#undef RXSUITE_SIMPLE
#include "rxSuite.h"
#include "rxSuiteHelpers.h"
#include "sdsWrapper.h"

#include "sha1.h"

    int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
#ifdef __cplusplus
}
#endif
#include <cstdarg>

/*
 * This module execute rx mercator commands
 */
#ifdef __cplusplus
extern "C"
{
#endif
#include "../../deps/hiredis/hiredis.h"
#ifdef __cplusplus
}
#endif

#include "client-pool.hpp"
#include "command-multiplexer.hpp"

typedef int (*tClusterFunction)(RedisModuleCtx *, redisContext *, char *, const char *, const char *, void *);

const char *CLUSTERING_ARG = "CLUSTERED";
const char *REPLICATION_ARG = "REPLICATED";
const char *CONTROLLER_ARG = "ZONE";
const char *START_ARG = "START";

const char *ADDRESS_FIELD = "address";
const char *PORTS_FIELD = "port";
const char *PORT_FIELD = "port";
const char *HMSET_COMMAND = "HMSET";
const char *OWNER_FIELD = "owner";
const char *SERVER_FIELD = "server";
const char *MAXMEM_FIELD = "maxmem";
const char *MAXMEM_VALUE = "1MB";
const char *ROLE_FIELD = "role";
const char *DATA_ROLE_VALUE = "data";
const char *CONTROLLER_ROLE_VALUE = "controller";
const char *ORDER_FIELD = "order";
const char *SHARD_FIELD = "shard";
const char *INDEX_FIELD = "index";
const char *PRIMARY_FIELD = "primary";

const char *HELP_STRING = "Mercator Demo Cluster Manager Commands:\n"
                          "\n"
                          " mercator.create.cluster caller %c_ip% [ clustered ] [ replicated ]\n"
                          " mercator.create.cluster caller %c_ip% [ clustered ] [ replicated ] [ serverless %keys% %avg_size% ]\n"
                          "\nAllows sharing of servers and instances, the will be created in terms of database rather than nodes"
                          "\n"
                          "\n"
                          " mercator.destroy.cluster %cluster_id%\n"
                          " mercator.start.cluster %cluster_id%\n"
                          " mercator.stop.cluster %cluster_id%\n"
                          " mercator.kill.cluster %cluster_id%\n"
                          " mercator.snapshot.cluster %cluster_id%\n"
                          " mercator.cluster.info %cluster_id%\n"
                          "\n"
                          " mercator.add.server {%name% %ip% %mem %ports% [%dbs% ]} ...\n"
                          " mercator.remove.server %name% ...\n"
                          "\n";

rxString getSha1(const char *codes, ...)
{
    int argc = strlen(codes);
    std::va_list args;
    va_start(args, argc);

    SHA1_CTX ctx;
    unsigned char hash[20];
    int i;

    SHA1Init(&ctx);
    for (; *codes; ++codes)
    {
        switch (*codes)
        {
        case 's':
        {
            rxString s = va_arg(args, rxString);
            SHA1Update(&ctx, (const unsigned char *)s, strlen(s));
        }
        break;
        case 'i':
        {
            int i = va_arg(args, int);
            SHA1Update(&ctx, (const unsigned char *)&i, sizeof(int));
        }
        break;
        case 'f':
        {
            int f = va_arg(args, double);
            SHA1Update(&ctx, (const unsigned char *)&f, sizeof(double));
        }
        break;
        }
    }
    SHA1Final(hash, &ctx);

    char *sha1 = (char *)rxMemAlloc(41);
    char *filler = sha1;
    for (i = 0; i < 20; i++, filler += 2)
    {
        sprintf(filler, "%02x", hash[i]);
    }
    *filler = 0x00;
    return rxStringNew(sha1);
}

void freeSha1(rxString sha1)
{
    rxStringFree(sha1);
}
class CreateClusterAsync : public Multiplexer
{
public:
    CreateClusterAsync()
        : Multiplexer()
    {
    }

    CreateClusterAsync(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
        : Multiplexer(argv, argc)
    {
        this->ctx = ctx;
    }

    int Write(RedisModuleCtx *ctx)
    {
        if (this->result_text)
        {
            RedisModule_ReplyWithStringBuffer(ctx, this->result_text, strlen(this->result_text));
        }
        else
            RedisModule_ReplyWithNull(ctx);
        return -1;
    }

    int Done()
    {
        return -1;
    }

    int Execute()
    {
        return -1;
    };

    void *StopThread()
    {
        this->state = Multiplexer::done;
        return NULL;
    }
};

int start_cluster(RedisModuleCtx *ctx, char *sha1);

redisReply *FindInstanceGroup(const char *sha1, char *fields = NULL);
void LoanInstancesToController(redisContext *sub_controller, int no_of_instances);
void *PropagateCommandToAllSubordinateControllers(CreateClusterAsync *multiplexer);

#define LOCAL_STANDARD 0
#define LOCAL_FREE_CMD 1
#define LOCAL_NO_RESPONSE 2

redisReply *ExecuteLocal(const char *cmd, int options)
{
    rxServerLogRaw(rxLL_NOTICE, cmd);
    if (strstr(cmd, "break"))
        rxServerLogRaw(rxLL_NOTICE, "debug break");

    char controller[24];
    snprintf(controller, sizeof(controller), "0.0.0.0:%d", rxGetServerPort());
    auto *redis_node = RedisClientPool<redisContext>::Acquire(controller, "_CONTROLLER", "EXEC_LOCAL");
    if (redis_node != NULL)
    {
        redisReply *rcc = (redisReply *)redisCommand(redis_node, cmd);
        rxServerLog(rxLL_NOTICE, "=> type:%d len:%d elements:%d", rcc->type, rcc->len, rcc->elements);
        if ((options & LOCAL_FREE_CMD) == LOCAL_FREE_CMD)
        {
            rxStringFree(cmd);
        }
        RedisClientPool<redisContext>::Release(redis_node, "EXEC_LOCAL");
        if ((options & LOCAL_NO_RESPONSE) == LOCAL_NO_RESPONSE)
        {
            freeReplyObject(rcc);
            return NULL;
        }
        return rcc;
    }
    return NULL;
}

rxString CreateClusterNode(rxString cluster_key, rxString sha1, const char *role, const char *order, const char *shard, RedisModuleCtx *)
{
                //TODO: Replace with local lookup

    rxString __MERCATOR__SERVERS__ = rxStringNew("__MERCATOR__SERVERS__");
    void *servers = rxFindSetKey(0, __MERCATOR__SERVERS__);
    rxString server = NULL;
    int retry_cnt = 10;
    do
    {
        server = rxRandomSetMember(servers);
    } while (server == NULL && --retry_cnt > 0);

    void *server_info = rxFindHashKey(0, server);

    rxString free_ports_key = rxGetHashField(server_info, PORTS_FIELD);
    if (free_ports_key == NULL)
    {
        free_ports_key = rxStringDup(server);
        free_ports_key = rxStringAppend(free_ports_key, "_FREEPORTS__", '_');
    }

    rxString address = rxGetHashField(server_info, ADDRESS_FIELD);

    void *free_ports = rxFindSetKey(0, free_ports_key);
    if (free_ports == NULL)
    {
        return NULL;
    }
    rxString port = rxRandomSetMember(free_ports);

    rxString key = rxStringFormat("__MERCATOR__INSTANCE__%s_%s_%s", sha1, server, port);
    auto *cmd = rxStringFormat("RXQUERY \"g:"
                               "addV('%s',instance)"
                               ".property(owner,'%s')"
                               ".property(server,'%s')"
                               ".property(address,'%s')"
                               ".property(maxmem,maxmem)"
                               ".property(port,%s)"
                               ".property(role,%s)"
                               ".property(order,%s)"
                               ".property(shard,%s)"
                               ".reset"
                               ".predicate(has_instance,instance_of)"
                               ".object('%s')"
                               ".subject('%s')"
                               "\" ",
                               key,
                               sha1,
                               server,
                               address,
                               port,
                               role,
                               order,
                               shard,
                               sha1,
                               key,
                               sha1);
    ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);

    cmd = rxStringFormat("SADD %s %s", cluster_key, key);
    ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);

    cmd = rxStringFormat("SREM %s %s", free_ports_key, port);
    ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);

    rxStringFree(address);
    rxStringFree(free_ports_key);
    return key;
}

void LoanInstancesToController(redisContext *sub_controller, int no_of_instances)
{
    rxString __MERCATOR__SERVERS__ = rxStringNew("__MERCATOR__SERVERS__");
    while (no_of_instances)
    {

        void *servers = rxFindSetKey(0, __MERCATOR__SERVERS__);
        rxString server = rxRandomSetMember(servers);

        void *server_info = rxFindHashKey(0, server);

        rxString free_ports_key = rxGetHashField2(server_info, PORTS_FIELD);

        rxString address = rxGetHashField2(server_info, ADDRESS_FIELD);

        void *free_ports = rxFindSetKey(0, free_ports_key);
        if (free_ports == NULL)
        {
            continue;
        }
        rxString port = rxRandomSetMember(free_ports);

        redisReply *rcc = (redisReply *)redisCommand(sub_controller, "%s %s:%s %s 6GB base %s 1", "mercator.add.server", address, port, address, port);
        freeReplyObject(rcc);

        rxStringFree(address);
        rxStringFree(free_ports_key);
        --no_of_instances;
    }
    // Save the loaned instances
    redisReply *rcc = (redisReply *)redisCommand(sub_controller, "BGSAVE");
    freeReplyObject(rcc);
}

#define FIND_CONTROLLER "RXGET \"G:"              \
                        "v(instance)"           \
                        ".select(address,port)" \
                        ".where{owner == '%s'}"   \
                        "\""

#define FIND_SUBCONTROLLERS "RXGET G:"                    \
                            "v(instance)"                 \
                            ".has(role,controller)"       \
                            ".select(address,port,owner)" \
                            ""
void *CreateClusterAsync_Go(void *privData)
{
    char buffer[64];
    auto *multiplexer = (CreateClusterAsync *)privData;
    multiplexer->state = Multiplexer::running;

    int replication_requested = 0;
    int clustering_requested = 0;
    int start_requested = 0;
    char *controller_path = NULL;
    rxString c_ip = rxStringEmpty();
    size_t arg_len;
    for (int j = 1; j < multiplexer->argc; ++j)
    {
        char *q = (char *)RedisModule_StringPtrLen(multiplexer->argv[j], &arg_len);
        rxServerLog(rxLL_NOTICE, "#0005# multiplexer->result_text # Create cluster here: %d %s", j, q);
        if (rxStringMatch(q, REPLICATION_ARG, MATCH_IGNORE_CASE) && strlen(q) == strlen(REPLICATION_ARG))
            replication_requested = 1;
        else if (rxStringMatch(q, CLUSTERING_ARG, MATCH_IGNORE_CASE) && strlen(q) == strlen(CLUSTERING_ARG))
            clustering_requested = 2;
        else if (rxStringMatch(q, CONTROLLER_ARG, MATCH_IGNORE_CASE) && strlen(q) == strlen(CONTROLLER_ARG))
        {
            controller_path = (char *)RedisModule_StringPtrLen(multiplexer->argv[j + 1], &arg_len);
            ++j;
        }
        else if (rxStringMatch(q, START_ARG, MATCH_IGNORE_CASE) && strlen(q) == strlen(START_ARG))
            start_requested = 4;
        else
            c_ip = rxStringNew(q);
    }
    rxString sha1 = getSha1("sii", c_ip, replication_requested, clustering_requested);

    if (controller_path)
    {
        auto *first_slash = strchr(controller_path, '/');
        const char *tail = NULL;
        if (first_slash)
        {
            *first_slash = 0x00;
            if (strlen(first_slash + 1) > 0)
                tail = first_slash + 1;
        }
        auto *cmd = rxStringFormat(FIND_CONTROLLER, controller_path);
        auto *rcc = ExecuteLocal(cmd, LOCAL_FREE_CMD);
        bool loan_instances = false;
        if (rcc)
        {
            if (rcc->type == REDIS_REPLY_ARRAY && rcc->elements == 0)
            {
                cmd = rxStringFormat("RXQUERY \"g:"
                                     "ADDV('%s','cluster')"
                                     "\"",
                                     controller_path);
                ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);
                rxString cluster_key = rxStringFormat("__MERCATOR__CONTROLLER__%s", controller_path);

                CreateClusterNode(cluster_key, controller_path, CONTROLLER_ROLE_VALUE, "1", "1", multiplexer->ctx);

                cmd = rxStringFormat("SADD __MERCATOR__CLUSTERS__ %s", cluster_key);
                ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);

                PostCommand(rxStringFormat(UPDATE_CLUSTER__STATE, controller_path, mstime(), "CREATED", "CREATED", GetCurrentDateTimeAsString(buffer)));

                start_cluster(multiplexer->ctx, (char *)controller_path);
                freeReplyObject(rcc);
                cmd = rxStringFormat(FIND_CONTROLLER, controller_path);
                rcc = ExecuteLocal(cmd, LOCAL_FREE_CMD);
                loan_instances = true;
            }
            redisReply *row = rcc->element[0];
            redisReply *hash = row->element[5];
            const char *ip = hash->element[1]->str;
            const char *port = hash->element[3]->str;

            char sub_controller[24];
            snprintf(sub_controller, sizeof(sub_controller), "%s:%s", ip, port);
            redisContext *controller_node = NULL;
            int retry_cnt = 60;
            do
            {
                controller_node = RedisClientPool<redisContext>::Acquire(sub_controller, "_CLIENT", "Invoke_Sub_Controller");
                sleep(1);
            } while (controller_node == NULL && --retry_cnt > 0);
            if (controller_node && loan_instances)
            {
                LoanInstancesToController(controller_node, 20);
            }
            if (controller_node != NULL)
            {
                rxString deferral = rxStringFormat("mercator.create.cluster %s ", c_ip);
                if (replication_requested)
                    deferral = rxStringAppend(deferral, REPLICATION_ARG, ' ');
                if (clustering_requested)
                    deferral = rxStringAppend(deferral, CLUSTERING_ARG, ' ');
                if (start_requested)
                    deferral = rxStringAppend(deferral, START_ARG, ' ');
                if (tail)
                {
                    deferral = rxStringAppend(deferral, CONTROLLER_ARG, ' ');
                    deferral = rxStringAppend(deferral, tail, ' ');
                }
                rxServerLog(rxLL_NOTICE, deferral);
                redisReply *rcc = (redisReply *)redisCommand(controller_node, deferral);
                if (rcc)
                    multiplexer->result_text = strdup(rcc->str);
                else
                    multiplexer->result_text = strdup(controller_node->errstr);
                freeReplyObject(rcc);
                PostCommand("BGSAVE");
                RedisClientPool<redisContext>::Release(controller_node, "Invoke_Sub_Controller");
            }

            freeReplyObject(rcc);
        }
        return multiplexer->StopThread();
    }

    rxServerLog(rxLL_NOTICE, "#1000# multiplexer->result_text # Create cluster here");
    redisReply *nodes = FindInstanceGroup(sha1);
    if (nodes)
    {
        rxServerLog(rxLL_NOTICE, "#1010# multiplexer->result_text # cluster already created");
        multiplexer->result_text = sha1;
        freeReplyObject(nodes);
        return multiplexer->StopThread();
    }
    auto cmd = rxStringFormat("RXQUERY g:v(%s)", sha1);
    redisReply *r = ExecuteLocal(cmd, LOCAL_FREE_CMD);

    if (r && r->elements != 0)
    {
        rxServerLog(rxLL_NOTICE, "#1020# multiplexer->result_text # cluster already created");
        freeReplyObject(r);
        if (start_requested == 4)
        {
            rxServerLog(rxLL_NOTICE, "#1025# multiplexer->result_text # cluster already created");
            start_cluster(multiplexer->ctx, (char *)sha1);
        }
        multiplexer->result_text = sha1;
        return multiplexer->StopThread();
    };
    rxServerLog(rxLL_NOTICE, "#1050# multiplexer->result_text # add cluster");
    cmd = rxStringFormat("RXQUERY \"g:break.addv('%s','cluster')\"", sha1);
    r = ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);
    rxString cluster_key = rxStringFormat("__MERCATOR__CLUSTER__%s", sha1);
    cmd = rxStringFormat("SADD __MERCATOR__CLUSTERS__ __MERCATOR__CLUSTER__%s", sha1);
    ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);

    rxString data;
    rxString index;
    rxString data2;
    rxServerLog(rxLL_NOTICE, "#1060# multiplexer->result_text # create instances");
    switch (replication_requested + clustering_requested)
    {
    case 0: // Not Replicated, Not Clustered
        data = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "1", "1", multiplexer->ctx);
        index = CreateClusterNode(cluster_key, sha1, INDEX_FIELD, "1", "1", multiplexer->ctx);
        if (data != NULL && index != NULL)
        {
            ExecuteLocal(rxStringFormat("RXQUERY \"g:break.addv('%s',instance).property(index,'%s')\"", data, index), LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);
            ExecuteLocal(rxStringFormat("RXQUERY \"g:break.addv('%s',instance).property(data,'%s')\"", index, data), LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);
        }
        else
        {
            multiplexer->result_text = "No server space available";
            return multiplexer->StopThread();
        }
        break;
    case 1: // Replicated, Not Clustered
        data = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "1", "1", multiplexer->ctx);
        index = CreateClusterNode(cluster_key, sha1, INDEX_FIELD, "1", "1", multiplexer->ctx);
        if (data != NULL && index != NULL)
        {
            RedisModule_FreeCallReply(
                RedisModule_Call(multiplexer->ctx, "HSET", "ccc", data, INDEX_FIELD, index));
            data2 = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "2", "1", multiplexer->ctx);
            RedisModule_FreeCallReply(
                RedisModule_Call(multiplexer->ctx, "HSET", "ccccc", data2, INDEX_FIELD, index, PRIMARY_FIELD, data));
        }
        break;
    case 2: // Not Replicated, Clustered
        data = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "1", "1", multiplexer->ctx);
        index = CreateClusterNode(cluster_key, sha1, INDEX_FIELD, "1", "1", multiplexer->ctx);
        if (data != NULL && index != NULL)
        {
            RedisModule_FreeCallReply(
                RedisModule_Call(multiplexer->ctx, "HSET", "ccc", data, INDEX_FIELD, index));
            data = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "1", "2", multiplexer->ctx);
            RedisModule_FreeCallReply(
                RedisModule_Call(multiplexer->ctx, "HSET", "ccc", data, INDEX_FIELD, index));
        }
        else
        {
            multiplexer->result_text = "No server space available";
            return multiplexer->StopThread();
        }
        break;
    case 3: // Not Replicated, Clustered
        data = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "1", "1", multiplexer->ctx);
        index = CreateClusterNode(cluster_key, sha1, INDEX_FIELD, "1", "1", multiplexer->ctx);
        if (data != NULL && index != NULL)
        {
            RedisModule_FreeCallReply(
                RedisModule_Call(multiplexer->ctx, "HSET", "ccc", data, INDEX_FIELD, index));
            data2 = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "2", "1", multiplexer->ctx);
            RedisModule_FreeCallReply(
                RedisModule_Call(multiplexer->ctx, "HSET", "cccc", data2, INDEX_FIELD, index, PRIMARY_FIELD, data));

            if (data != NULL && index != NULL)
            {
                data = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "1", "2", multiplexer->ctx);
                index = CreateClusterNode(cluster_key, sha1, INDEX_FIELD, "1", "2", multiplexer->ctx);
                RedisModule_FreeCallReply(
                    RedisModule_Call(multiplexer->ctx, "HSET", "ccc", data, INDEX_FIELD, index));
                data2 = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "2", "2", multiplexer->ctx);
                RedisModule_FreeCallReply(
                    RedisModule_Call(multiplexer->ctx, "HSET", "ccccc", data2, INDEX_FIELD, index, PRIMARY_FIELD, data));
            }
            else
            {
                multiplexer->result_text = "No server space available";
                return multiplexer->StopThread();
            }
        }
        else
        {
            multiplexer->result_text = "No server space available";
            return multiplexer->StopThread();
        }

        break;
    default:
    {
        multiplexer->result_text = "Invalid parameters";
        return multiplexer->StopThread();
    }
    }
    rxStringFree(cluster_key);
    rxServerLog(rxLL_NOTICE, "#1100# multiplexer->result_text # update cluster status");
    PostCommand(rxStringFormat(UPDATE_CLUSTER__STATE, sha1, mstime(), "CREATED", "CREATED", GetCurrentDateTimeAsString(buffer)));
    PostCommand("BGSAVE");

    if (start_requested == 4)
    {
        rxServerLog(rxLL_NOTICE, "#1200# multiplexer->result_text # start cluster per request");
        start_cluster(multiplexer->ctx, (char *)sha1);
    }
    multiplexer->result_text = sha1;
    return multiplexer->StopThread();
}

/*
    mercator.create.cluster caller %c_ip% [ clustered ] [ replicated ]

                |          set
    ============|=======|=======|=======|=======
    clustered   |   N   |   N   |   Y   |   Y
    replicated  |   N   |   Y   |   N   |   Y
    ============|=======|=======|=======|=======
                |   A   |   B   |   C   |   D

    A) One ip ports is allocated, the data and index node are sharing the same redis
       instance.
       The Redis instance will support 2 databases. DB0 will be the 'data' DB and DB1
       the 'Index' DB.

       The Redis Instance is started on the first server.

    B) The ports are assigned as under A.

       The Redis Instance is started on the first server, as primary node.
       The Redis Instance is started on the auxiliary server as replication node.

    C) Two ip ports are allocated, the data and index node are sharing the same redis
       instance.
       The Redis instance will support 2 databases. DB0 will be the 'data' DB and DB1
       the 'Index' DB.

       A Redis cluster with two Redis Instance is started on the first server.

    B) The ports are assigned as under A.

       A Redis cluster with two Redis Instance is started on the first server.

       A Redis cluster with two Redis Instance is started on the first server, as primary node.
       A Redis cluster with two Redis Instance is started on the auxiliary server as replication node.
*/
int rx_create_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    auto *multiplexer = new CreateClusterAsync(ctx, argv, argc);
    multiplexer->Async(ctx, CreateClusterAsync_Go);

    return C_OK;
}

void *AttachControllerAsync_Go(void *privData)
{
    char buffer[64];
    auto *multiplexer = (CreateClusterAsync *)privData;
    multiplexer->state = Multiplexer::running;

    int replication_requested = 0;
    int clustering_requested = 0;
    int start_requested = 0;
    char *controller_path = NULL;
    size_t arg_len;
    const char *c_ip = RedisModule_StringPtrLen(multiplexer->argv[1], &arg_len);
    const char *address = RedisModule_StringPtrLen(multiplexer->argv[2], &arg_len);
    const char *port = RedisModule_StringPtrLen(multiplexer->argv[3], &arg_len);
    rxString sha1 = getSha1("sii", c_ip, replication_requested, clustering_requested);

    rxServerLog(rxLL_NOTICE, "#1000# multiplexer->result_text # Attach controller here");
    redisReply *nodes = FindInstanceGroup(c_ip);
    if (nodes)
    {
        rxServerLog(rxLL_NOTICE, "#1010# multiplexer->result_text # controller already attached/created");
        multiplexer->result_text = sha1;
        freeReplyObject(nodes);
        return multiplexer->StopThread();
    }

    rxServerLog(rxLL_NOTICE, "#1050# multiplexer->result_text # add cluster");
    auto *cmd = rxStringFormat("RXQUERY \"g:AddV('%s','cluster')\"", c_ip);
    auto *r = ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);

    rxString key = rxStringFormat("__MERCATOR__INSTANCE__%s_%s_%s", sha1, address, port);
    cmd = rxStringFormat("RXQUERY \"g:"
                         "addV('%s',instance)"
                         ".property(server,'%s')"
                         ".property(address,'%s')"
                         ".property(port,%s)"
                         ".property(role,%s)"
                         ".property(order,%s)"
                         ".property(shard,%s)"
                         ".predicate(has_instance,instance_of)"
                         ".object('%s')"
                         ".subject('%s')"
                         "\" ",
                         c_ip,
                         c_ip,
                         address,
                         port,
                         "controller",
                         "1",
                         "1",
                         c_ip,
                         key,
                         c_ip);
    ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);

    rxServerLog(rxLL_NOTICE, "#1100# multiplexer->result_text # update cluster status");
    PostCommand(rxStringFormat(UPDATE_CLUSTER__STATE, sha1, mstime(), "ATTACHED", "ATTACHED", GetCurrentDateTimeAsString(buffer)));

    multiplexer->result_text = c_ip;
    return multiplexer->StopThread();
}

int rx_add_controller(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    auto *multiplexer = new CreateClusterAsync(ctx, argv, argc);
    multiplexer->Async(ctx, AttachControllerAsync_Go);

    return C_OK;
}

int rx_destroy_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int)
{
    size_t arg_len;
    char *q = (char *)RedisModule_StringPtrLen(argv[1], &arg_len);
    return RedisModule_ReplyWithSimpleString(ctx, q);
}

std::string exec(const char *cmd)
{
    long long start = ustime();
    long long stop = ustime();
    int tally = 0;
    do
    {
        rxServerLog(rxLL_WARNING, "cli:  ");
        int rc = system(cmd);
        rxServerLog(rxLL_WARNING, " rc = %d %s \n", rc, cmd);
        if (!(rc == -1 || WEXITSTATUS(rc) != 0))
        {
            return ("");
        }
        stop = ustime();
        ++tally;
    } while (tally < 5 && stop - start <= 15 * 1000);
    rxServerLog(rxLL_WARNING, " tally %d timing %lld :: %lld == %lld  \n", tally, stop, start, stop - start);
    return ("error");
    std::array<char, 128> buffer;
    std::string result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
    if (!pipe)
    {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr)
    {
        result += buffer.data();
    }
    return result;
}

int start_redis(rxString command)
{
    pid_t child_pid;
    child_pid = fork(); // Create a new child process;
    if (child_pid < 0)
    {
        rxServerLog(rxLL_WARNING, "fork failed");
        return 1;
    }
    else if (child_pid == 0)
    { // New process
        rxServerLog(rxLL_WARNING, "Start attempt: %s\n", command);
        int rc = system(command);
        rxServerLog(rxLL_WARNING, "Start attempt rc: %d\n", rc);
        exit(0);
    }
    int status;
    do
    {
        pid_t result = waitpid(child_pid, &status, WNOHANG);
        if (result == 0)
        {
            // Child still alive
        }
        else if (result == -1)
        {
            return result;
        }
        else
        {
            kill(child_pid, SIGKILL);
            return result;
        }
    } while (true);
    return 0;
}

int Is_node_Online(const char *address, const char *port)
{
    int response = 0;
    char controller[24];
    snprintf(controller, sizeof(controller), "%s:%s", address, port);
    auto *redis_node = RedisClientPool<redisContext>::Acquire(controller, "_CLIENT", "Is_node_Online");
    if (redis_node != NULL)
    {
        redisReply *rcc = (redisReply *)redisCommand(redis_node, "mercator.instance.status");
        response = (!rcc) ? 722764 : 722765;
        freeReplyObject(rcc);
        RedisClientPool<redisContext>::Release(redis_node, "Is_node_Online");
    }
    return response;
}

int start_cluster(RedisModuleCtx *ctx, char *sha1)
{
    char buffer[64];

    redisReply *nodes = FindInstanceGroup(sha1, "address,port,role,order,index,primary");
    if (!nodes)
    {
        return RedisModule_ReplyWithSimpleString(ctx, "cluster not found");
    }
    char cwd[FILENAME_MAX]; // create string buffer to hold path
    GetCurrentDir(cwd, FILENAME_MAX);
    char *data = strstr(cwd, "/data");
    if (data != NULL)
        *data = 0x00;

    rxServerLog(rxLL_WARNING, "Starting cluster:%s\n", sha1);
    int n = 0;
    while (n < nodes->elements) 
    {
        char *node = nodes->element[n]->element[1]->str;
        redisReply *values = nodes->element[n]->element[5];
        char *address = values->element[1]->str;
        char *port = values->element[3]->str;
        char *role = values->element[5]->str;
        char *shard = values->element[7]->str;
        char *index_name = values->element[9]->str;
        char *index_address = NULL;
        char *index_port = NULL;
        if (index_name && strlen(index_name))
        {
            rxString cmd = rxStringFormat("rxget \"g:select(address,port).v('%s')\"", index_name);
            auto index_node_info = ExecuteLocal(cmd, LOCAL_FREE_CMD);
            if (index_node_info && index_node_info->type == REDIS_REPLY_ARRAY && index_node_info->elements != 0)
            {
                redisReply *ixvalues = nodes->element[0]->element[5];
                index_address = ixvalues->element[1]->str;
                index_port = ixvalues->element[3]->str;
            }
            else
            {
            index_address = address;
            index_port = port;
            }
        }
        else
        {
            index_address = address;
            index_port = port;
        }
        rxServerLog(rxLL_WARNING, "INDEX: %s %s %s\n", index_name, index_address, index_port);

        auto *config = getRxSuite();
        if (Is_node_Online(address, port) == 0)
        {
            char *primary_name = values->element[11]->str;
            rxServerLog(rxLL_NOTICE, ".... starting node:%s %s:%s role:%s shard:%s index_name:%s primary_name:%s \n", node, address, port, role, shard, index_name, primary_name);
            rxString startup_command = rxStringFormat(
                "python3 %s/extensions/src/start_node.py %s %s %s %s %s %s %s \"%s\" \"%s\" \"%s\" >>$HOME/redis-%s/data/startup.log  2>>$HOME/redis-%s/data/startup.log ",
                cwd,
                sha1,
                address, port,
                role,
                index_address && strlen(index_address) ? index_address : address, 
                index_port && strlen(index_port) ? index_port : port,
                REDIS_VERSION,
                config->cdnRootUrl,
                config->startScript,
                config->installScript,
                REDIS_VERSION,
                REDIS_VERSION,
                cwd);

            rxServerLog(rxLL_NOTICE, "%s\n", startup_command);
            int rc = start_redis(startup_command);
            rxServerLog(rxLL_NOTICE, " rc=%d\n", rc);
            // TODO use a multiplexer!!

            long long start = ustime();
            long long stop = ustime();
            int tally = 0;
            do
            {
                if (Is_node_Online(address, port) == 0)
                    rxServerLog(rxLL_WARNING, " start check: CONNECTion failed\n");
                stop = ustime();
                ++tally;
            } while (tally < 5 && stop - start <= 5 * 1000);
            rxServerLog(rxLL_WARNING, " start check tally %d timing %lld :: %lld == %lld  \n", tally, stop, start, stop - start);

            if (tally != 722764)
            {
                rxServerLog(rxLL_WARNING, " start check: START LOCAL\n");
                start_redis(startup_command);
            }

            if (primary_name && strcmp(primary_name, rxStringEmpty()) != 0)
            {
                //TODO: Replace with local lookup
                void *primary_node_info = rxFindHashKey(0, primary_name);
                rxString primary_address = rxGetHashField2(primary_node_info, ADDRESS_FIELD);
                rxString primary_port = rxGetHashField2(primary_node_info, PORT_FIELD);

                startup_command = rxStringFormat("%s/src/redis-cli -h %s -p %s REPLICAOF %s %s", cwd, address, port, primary_address, primary_port);
                string out = exec(startup_command);
                rxStringFree(startup_command);
                rxStringFree(primary_address);
                rxStringFree(primary_port);
            }

            char buffer[30];
            PostCommand(rxStringFormat(UPDATE_INSTANCE_START, node, mstime(), GetCurrentDateTimeAsString(buffer)));
            ++n;
        }
        PostCommand(rxStringFormat(UPDATE_CLUSTER__STATE_STARTED, sha1, mstime(), GetCurrentDateTimeAsString(buffer)));
        n++;
    }
    freeReplyObject(nodes);
    return C_OK;
}

void *ClusterStartAsync_Go(void *privData)
{
    auto *multiplexer = (CreateClusterAsync *)privData;
    multiplexer->state = Multiplexer::running;

    size_t arg_len;
    char *sha1 = (char *)RedisModule_StringPtrLen(multiplexer->argv[1], &arg_len);

    redisReply *nodes = FindInstanceGroup(sha1);
    if (!nodes)
    {
        PropagateCommandToAllSubordinateControllers(multiplexer);
        // multiplexer->result_text = rxStringFormat("%s%s", info, "]}");
        return multiplexer->StopThread();
    }

    start_cluster(multiplexer->ctx, sha1);
    multiplexer->result_text = sha1;
    return multiplexer->StopThread();
}

int rx_start_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    auto *multiplexer = new CreateClusterAsync(ctx, argv, argc);
    multiplexer->Async(ctx, ClusterStartAsync_Go);

    return C_OK;
}

int clusterKillOperationProc(RedisModuleCtx *, redisContext *redis_node, char *sha1, const char *address, char *, void *)
{
    redisReply *rcc = (redisReply *)redisCommand(redis_node, "SHUTDOWN");
    rxServerLog(rxLL_WARNING, " SHUTDOWN: %s --> %s [%s]\n", address, rcc ? rcc->str : NULL, sha1);
    freeReplyObject(rcc);
    return C_OK;
}

int clusterStopOperationProc(RedisModuleCtx *, redisContext *redis_node, char *sha1, const char *address, char *, void *)
{
    redisReply *rcc = (redisReply *)redisCommand(redis_node, "SHUTDOWN");
    rxServerLog(rxLL_WARNING, " SHUTDOWN: %s --> %s [%s]\n", address, rcc ? rcc->str : NULL, sha1);
    freeReplyObject(rcc);
    return C_OK;
}

int clusterSnapshotOperationProc(RedisModuleCtx *, redisContext *redis_node, char *sha1, const char *address, char *, void *)
{
    redisReply *rcc = (redisReply *)redisCommand(redis_node, "BGSAVE");
    rxServerLog(rxLL_WARNING, " BGSAVE: %s --> %s [%s]\n", address, rcc ? rcc->str : NULL, sha1);
    freeReplyObject(rcc);
    return C_OK;
}

int clusterFlushOperationProc(RedisModuleCtx *, redisContext *redis_node, char *sha1, const char *address, char *, void *)
{
    if (!redis_node)
    {
        rxServerLog(rxLL_WARNING, " FLUSHALL NO redisContext: %s[%s]\n", address, sha1);
        return C_ERR;
    }
    redisReply *rcc = (redisReply *)redisCommand(redis_node, "%s %s", "FLUSHALL", "SYNC");
    if (strcmp("OK", rcc ? rcc->str : "NOK") != 0)
    {
        rxServerLog(rxLL_WARNING, " FLUSHALL *: %s --> %s [%s]\n", address, rcc ? rcc->str : NULL, sha1);
    }
    rxServerLog(rxLL_WARNING, " FLUSHALL *: %s --> %s [%s]\n", address, rcc ? rcc->str : NULL, sha1);
    freeReplyObject(rcc);
    rcc = (redisReply *)redisCommand(redis_node, "%s %s", "RULE.DEL", "*");
    rxServerLog(rxLL_WARNING, " RULE.DEL *: %s --> %s\n", address, rcc ? rcc->str : NULL);
    freeReplyObject(rcc);
    return C_OK;
}

redisReply *FindInstanceGroup(const char *sha1, char *fields = NULL)
{
    if (fields == NULL)
        fields = "address,port";
    rxString cmd = rxStringFormat("rxget \"g:v(cluster).inout(has_instance,instance_of).select(%s).where{owner=='%s'}\"", fields, sha1);
    redisReply *nodes = ExecuteLocal(cmd, LOCAL_FREE_CMD);
    if (nodes->type != REDIS_REPLY_ARRAY || nodes->elements == 0)
    {
        rxServerLog(rxLL_NOTICE, "Controller not found: %s, %s", sha1, nodes->str);
        freeReplyObject(nodes);
        return NULL;
    }

    // rxString cluster_key = rxStringFormat("__MERCATOR__CLUSTER__%s", sha1);
    // void *nodes = rxFindSetKey(0, cluster_key);
    // if (!nodes)
    // {
    //     cluster_key = rxStringFormat("__MERCATOR__CONTROLLER__%s", sha1);
    //     nodes = rxFindSetKey(0, cluster_key);
    // }
    // rxStringFree(cluster_key);
    return nodes;
}

int clusterOperation(RedisModuleCtx *ctx, const char *sha1, const char *state, clusterOperationProc *operation, void *data, clusterOperationProc *summary_operation)
{
    char buffer[64];
    redisReply *nodes = FindInstanceGroup(sha1);
    if (nodes == NULL)
    {
        rxServerLog(rxLL_WARNING, "No nodes found for cluster %s", sha1);
        return C_ERR;
    }
    // if (!nodes)
    // {
    //     PropagateCommandToAllSubordinateControllers(multiplexer);
    //     // multiplexer->result_text = rxStringFormat("%s%s", info, "]}");
    //     return multiplexer->StopThread();

    // auto *multiplexer = new CreateClusterAsync(ctx, argv, argc);
    // multiplexer->Async(ctx, GetClusterInfoAsync_Go);

    // }

    void *si = NULL;
    rxString node;
    int64_t l;
    int instances_ok = 0;
    int instances_err = 0;
    int n = 0;
    while (n < nodes->elements) // rxScanSetMembers(nodes, &si, (char **)&node, &l) != NULL)
    {
        // void *node_info = rxFindHashKey(0, node);
        char *node = nodes->element[n]->element[1]->str;
        redisReply *values = nodes->element[n]->element[5];

        char *address = values->element[1]->str;
        char *port = values->element[3]->str;

        char controller[24];
        snprintf(controller, sizeof(controller), "%s:%s", address, port);

        auto *redis_node = RedisClientPool<redisContext>::Acquire(controller, "_CLIENT", "ClusterOperation");
        if (redis_node != NULL)
        {
            redisReply *rcc = (redisReply *)redisCommand(redis_node, "%s %s %s", "AUTH", "admin", "admin");
            rxServerLog(rxLL_WARNING, " AUTH:  %s\n", rcc ? rcc->str : NULL);
            freeReplyObject(rcc);
            if (operation(ctx, redis_node, sha1, controller, node, data) == C_OK)
                instances_ok++;
            else
                instances_err++;
            RedisClientPool<redisContext>::Release(redis_node, "ClusterOperation");
        }
        else
        {
            operation(ctx, redis_node, sha1, controller, node, data);
            instances_err++;
        }
        n++;
    }
    freeReplyObject(nodes);
    if (summary_operation)
    {
        int p[] = {instances_ok, instances_err};
        summary_operation(ctx, NULL, sha1, NULL, NULL, (void *)p);
    }
    if (state)
        PostCommand(rxStringFormat(UPDATE_CLUSTER__STATE, sha1, mstime(), state, state, GetCurrentDateTimeAsString(buffer)));

    return C_OK;
}

void *StopClusterAsync_Go(void *privData)
{
    auto *multiplexer = (CreateClusterAsync *)privData;
    multiplexer->state = Multiplexer::running;

    size_t arg_len;
    char *sha1 = (char *)RedisModule_StringPtrLen(multiplexer->argv[1], &arg_len);

    clusterOperation(multiplexer->ctx, sha1, "STOPPED", (tClusterFunction)clusterStopOperationProc, NULL, NULL);

    multiplexer->result_text = sha1;
    return multiplexer->StopThread();
}

int rx_stop_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    auto *multiplexer = new CreateClusterAsync(ctx, argv, argc);
    multiplexer->Async(ctx, StopClusterAsync_Go);

    return C_OK;
}

void *KillClusterAsync_Go(void *privData)
{
    auto *multiplexer = (CreateClusterAsync *)privData;
    multiplexer->state = Multiplexer::running;

    size_t arg_len;
    char *sha1 = (char *)RedisModule_StringPtrLen(multiplexer->argv[1], &arg_len);

    clusterOperation(multiplexer->ctx, sha1, "KILLED", (tClusterFunction)clusterKillOperationProc, NULL, NULL);

    multiplexer->result_text = sha1;
    return multiplexer->StopThread();
}

int rx_kill_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    auto *multiplexer = new CreateClusterAsync(ctx, argv, argc);
    multiplexer->Async(ctx, KillClusterAsync_Go);

    return C_OK;
}

void *SnapshotClusterAsync_Go(void *privData)
{
    auto *multiplexer = (CreateClusterAsync *)privData;
    multiplexer->state = Multiplexer::running;

    size_t arg_len;
    char *sha1 = (char *)RedisModule_StringPtrLen(multiplexer->argv[1], &arg_len);

    clusterOperation(multiplexer->ctx, sha1, "SNAPSHOTTED", (tClusterFunction)clusterSnapshotOperationProc, NULL, NULL);

    multiplexer->result_text = sha1;
    return multiplexer->StopThread();
}

int rx_snapshot_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    auto *multiplexer = new CreateClusterAsync(ctx, argv, argc);
    multiplexer->Async(ctx, SnapshotClusterAsync_Go);

    return C_OK;
}

void *FlushClusterAsync_Go(void *privData)
{
    auto *multiplexer = (CreateClusterAsync *)privData;
    multiplexer->state = Multiplexer::running;

    size_t arg_len;
    char *sha1 = (char *)RedisModule_StringPtrLen(multiplexer->argv[1], &arg_len);

    clusterOperation(multiplexer->ctx, sha1, "FLUSHED", (tClusterFunction)&clusterFlushOperationProc, NULL, NULL);

    multiplexer->result_text = sha1;
    return multiplexer->StopThread();
}

int rx_flush_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    auto *multiplexer = new CreateClusterAsync(ctx, argv, argc);
    multiplexer->Async(ctx, FlushClusterAsync_Go);

    return C_OK;
}

void *PropagateCommandToAllSubordinateControllers(CreateClusterAsync *multiplexer)
{
    auto *rcc = ExecuteLocal(FIND_SUBCONTROLLERS, LOCAL_STANDARD);

    size_t arg_len;
    rxString cmd = rxStringNew((char *)RedisModule_StringPtrLen(multiplexer->argv[0], &arg_len));
    for (int n = 1; n < multiplexer->argc; ++n)
    {
        cmd = rxStringAppend(cmd, (char *)RedisModule_StringPtrLen(multiplexer->argv[n], &arg_len), ' ');
    }
    if (rcc->elements > 0)
    {
        rxString aggregation = rxStringNew("[");
        char sep = ' ';
        for (size_t n = 0; n < rcc->elements; ++n)
        {
            redisReply *row = rcc->element[n];
            redisReply *hash = row->element[5];
            const char *ip = hash->element[1]->str;
            const char *port = hash->element[3]->str;
            const char *zone = hash->element[5]->str;

            char sub_controller[24];
            snprintf(sub_controller, sizeof(sub_controller), "%s:%s", ip, port);
            auto *controller_node = RedisClientPool<redisContext>::Acquire(sub_controller, "_CLIENT", "Invoke_Sub_Controller");
            if (!controller_node)
            {
                rxString msg = rxStringFormat("\"controller %s is possibly down\" ", sub_controller);
                aggregation = rxStringAppend(aggregation, msg, sep);
                sep = ',';
                rxStringFree(msg);
                continue;
            }
            redisReply *rcc = (redisReply *)redisCommand(controller_node, cmd);
            if (rcc)
                switch (rcc->type)
                {
                case REDIS_REPLY_ERROR:
                case REDIS_REPLY_STRING:
                {
                    char *left_bracket = strchr(rcc->str, '[');
                    char *right_bracket = strrchr(rcc->str, ']');
                    if (left_bracket && right_bracket)
                    {
                        // *right_bracket = ' ';
                        int sz = (right_bracket - 1) - (left_bracket + 1);
                        auto inside = rxStringNewLen(left_bracket + 1, sz);
                        rxString newAggregation = rxStringFormat("%s%c{\"controller\":\"%s\", \"ip\":\"%s\", \"port\":\"%s\", \"info\": %s}", aggregation, sep, zone, ip, port, rcc->str);
                        // rxString prefix = rxStringFormat("{\"controller\":\"%s\", \"info\": {", zone);
                        // aggregation = rxStringAppend(aggregation, prefix, ' ');
                        // aggregation = rxStringAppend(aggregation, rcc->str + 1, ',');
                        // aggregation = rxStringAppend(aggregation, "}", ' ');
                        // rxStringFree(prefix);
                        rxStringFree(inside);
                        rxStringFree(aggregation);
                        aggregation = newAggregation;
                    }
                    else
                    {
                        aggregation = rxStringAppend(aggregation, rcc->str, sep);
                    }
                    sep = ',';
                }
                break;
                case REDIS_REPLY_NIL:
                default:
                    break;
                }
            else
            {
                aggregation = rxStringAppend(aggregation, controller_node->errstr, sep);
                sep = ',';
            }
            freeReplyObject(rcc);
            RedisClientPool<redisContext>::Release(controller_node, "Invoke_Sub_Controller");
        }
        multiplexer->result_text = rxStringAppend(aggregation, "]", ' ');
    }
    else
        multiplexer->result_text = NULL;

    return NULL;
}

void *GetClusterInfoAsync_Go(void *privData)
{
    auto *multiplexer = (CreateClusterAsync *)privData;
    multiplexer->state = Multiplexer::running;

    size_t arg_len;
    char *sha1 = (char *)RedisModule_StringPtrLen(multiplexer->argv[1], &arg_len);

    rxString info = rxStringFormat("{ \"id\":\"%s\", \"nodes\":[", sha1);

    redisReply *nodes = FindInstanceGroup(sha1, "address,port,role,order,index,primary");
    if (!nodes)
    {
        PropagateCommandToAllSubordinateControllers(multiplexer);
        // multiplexer->result_text = rxStringFormat("%s%s", info, "]}");
        freeReplyObject(nodes);
        return multiplexer->StopThread();
    }

    rxServerLog(rxLL_WARNING, "Info for cluster:%s\n", sha1);
    char sep[3] = {' ', 0x00, 0x00};
    int n = 0;
    while (n < nodes->elements) // rxScanSetMembers(nodes, &si, (char **)&node, &l) != NULL)
    {
        char *node = nodes->element[n]->element[1]->str;
        redisReply *values = nodes->element[n]->element[5];
        char *address = values->element[1]->str;
        char *port = values->element[3]->str;
        char *role = values->element[5]->str;
        char *shard = values->element[7]->str;
        char *index_name = values->element[9]->str;
        char *primary_name = values->element[11]->str;

        info = rxStringFormat("%s%s{ \"node\":\"%s\", \"ip\":\"%s\", \"port\":\"%s\", \"role\":\"%s\", \"shard\":\"%s\", \"index_name\":\"%s\", \"primary_name\":\"%s\" }\n", info, sep, node, address, port, role, shard, index_name, primary_name);
        sep[0] = ',';
        n++;
    }

    multiplexer->result_text = rxStringFormat("%s%s", info, "]}");
    return multiplexer->StopThread();
}

int rx_info_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    auto *multiplexer = new CreateClusterAsync(ctx, argv, argc);
    multiplexer->Async(ctx, GetClusterInfoAsync_Go);

    return C_OK;
}

int rx_info_config(RedisModuleCtx *ctx, RedisModuleString **, int)
{
    rxSuiteShared *config = getRxSuite();

    rxString info = rxStringNew("{ \"defaultQueryOperator\":\"");
    info = rxStringFormat("%s%s", info, config->defaultQueryOperator);
    info = rxStringFormat("%s%s", info, "\", \"cdnRootUrl\":\"");
    info = rxStringFormat("%s%s", info, config->cdnRootUrl);
    info = rxStringFormat("%s%s", info, "\", \"startScript\":\"");
    info = rxStringFormat("%s%s", info, config->startScript);
    info = rxStringFormat("%s%s", info, "\", \"installScript\":\"");
    info = rxStringFormat("%s%s", info, config->installScript);
    info = rxStringFormat("%s%s", info, "\",");
    redisNodeInfo *nodes = &config->indexNode;
    const char *names[] = {"indexNode", "dataNode", "controllerNode"};
    for (int n = 0; n < 3; ++n)
    {
        info = rxStringFormat("%s, \"%s\":{", info, names[n]);
        info = rxStringFormat("%s\"host_reference\":\"%s\",", info, nodes[n].host_reference);
        info = rxStringFormat("%s\"database_id\":%d,", info, nodes[n].database_id);
        info = rxStringFormat("%s\"is_local\":%d}", info, nodes[n].is_local);
    }

    info = rxStringFormat("%s%s", info, "}");
    return RedisModule_ReplyWithSimpleString(ctx, info);
}

int rx_help(RedisModuleCtx *ctx, RedisModuleString **, int)
{
    return RedisModule_ReplyWithSimpleString(ctx, HELP_STRING);
}

int rx_healthcheck(RedisModuleCtx *ctx, RedisModuleString **, int)
{
    // Return
    // 1. Used Memory
    // 2. Use Peak Memory
    // 3. Available Memory
    auto info = rxGetClientInfoForHealthCheck();

    RedisModule_ReplyWithArray(ctx, 22);
    RedisModule_ReplyWithSimpleString(ctx, "used_memory");
    RedisModule_ReplyWithLongLong(ctx, info.used_memory);
    RedisModule_ReplyWithSimpleString(ctx, "used_memory_peak");
    RedisModule_ReplyWithLongLong(ctx, info.used_memory_peak);
    RedisModule_ReplyWithSimpleString(ctx, "maxmemory");
    RedisModule_ReplyWithLongLong(ctx, info.maxmemory);
    RedisModule_ReplyWithSimpleString(ctx, "server_memory_available");
    RedisModule_ReplyWithLongLong(ctx, info.server_memory_available);

    RedisModule_ReplyWithSimpleString(ctx, "connected_clients");
    RedisModule_ReplyWithLongLong(ctx, info.connected_clients);
    RedisModule_ReplyWithSimpleString(ctx, "cluster_connections");
    RedisModule_ReplyWithLongLong(ctx, info.cluster_connections);
    RedisModule_ReplyWithSimpleString(ctx, "blocked_clients");
    RedisModule_ReplyWithLongLong(ctx, info.blocked_clients);
    RedisModule_ReplyWithSimpleString(ctx, "tracking_clients");
    RedisModule_ReplyWithLongLong(ctx, info.tracking_clients);
    RedisModule_ReplyWithSimpleString(ctx, "clients_in_timeout_table");
    RedisModule_ReplyWithLongLong(ctx, info.clients_in_timeout_table);
    RedisModule_ReplyWithSimpleString(ctx, "total_keys");
    RedisModule_ReplyWithLongLong(ctx, info.total_keys);
    RedisModule_ReplyWithSimpleString(ctx, "bytes_per_key");
    RedisModule_ReplyWithLongLong(ctx, info.bytes_per_key);

    return C_OK;
}

int rx_setconfig(RedisModuleCtx *ctx, RedisModuleString **, int)
{
    char *libpath = getenv("LD_LIBRARY_PATH");
    if (ctx == NULL)
    {
        char var[4096] = "LD_LIBRARY_PATH=";
        char *executable = rxGetExecutable();
        char *name = strstr(executable, "/src/redis-server");
        int h = strlen(var);
        int n = name - executable;
        strncat(var, executable, n);
        strcpy(var + h + n, "/extensions/src:");
        strcat(var, libpath);
        putenv(var);
    }

    if (ctx == NULL)
        return REDISMODULE_OK;

    return RedisModule_ReplyWithSimpleString(ctx, libpath);
}

int rx_add_server(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    for (int j = 1; j < argc; j += 4)
    {
        size_t arg_len;
        char *node_name = (char *)RedisModule_StringPtrLen(argv[j], &arg_len);
        char *node_ip = (char *)RedisModule_StringPtrLen(argv[j + 1], &arg_len);
        char *node_mem = (char *)RedisModule_StringPtrLen(argv[j + 2], &arg_len);
        int node_base_port = 6400;
        char *node_ports = (char *)RedisModule_StringPtrLen(argv[j + 3], &arg_len);
        if (rxStringMatch(node_ports, "BASE", MATCH_IGNORE_CASE))
        {
            node_ports = (char *)RedisModule_StringPtrLen(argv[j + 3 + 1], &arg_len);
            node_base_port = atoi(node_ports);
            node_ports = (char *)RedisModule_StringPtrLen(argv[j + 3 + 2], &arg_len);
            j += 2;
        }
        rxString key = rxStringNew("__MERCATOR__SERVER__");
        key = rxStringFormat("%s%s", key, node_name);
        rxString key2 = rxStringNew("__MERCATOR__SERVER__");
        key2 = rxStringFormat("%s%s", key2, node_name);
        key2 = rxStringFormat("%s%s", key2, "__FREEPORTS__");
        rxString key3 = rxStringNew("__MERCATOR__");
        key3 = rxStringFormat("%s%s", key3, node_name);
        key3 = rxStringFormat("%s%s", key3, "__USEDPORTS__");
        RedisModule_FreeCallReply(
            RedisModule_Call(ctx, HMSET_COMMAND, "ccccccccccccc", (char *)key, "type", "server", "name", node_name, ADDRESS_FIELD, node_ip, MAXMEM_FIELD, node_mem, PORTS_FIELD, key2, "claims", key3));
        RedisModule_FreeCallReply(
            RedisModule_Call(ctx, "SADD", "cc", (char *)"__MERCATOR__SERVERS__", key));
        int nr_of_ports = atoi(node_ports);
        for (long long int p = node_base_port + nr_of_ports - 1; p >= node_base_port; --p)
        {
            RedisModule_FreeCallReply(
                RedisModule_Call(ctx, "SADD", "cl", (char *)key2, p));
        }
    }
    return RedisModule_ReplyWithSimpleString(ctx, "OK" /*HELP_STRING*/);
}

int rx_start_monitor(RedisModuleCtx *ctx, RedisModuleString **, int)
{
    startInstanceMonitor();
    return RedisModule_ReplyWithSimpleString(ctx, "OK" /*HELP_STRING*/);
}

int rx_stop_monitor(RedisModuleCtx *ctx, RedisModuleString **, int)
{
    stopInstanceMonitor();
    return RedisModule_ReplyWithSimpleString(ctx, "OK" /*HELP_STRING*/);
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    char *libpath = getenv("LD_LIBRARY_PATH");
    rxServerLog(rxLL_NOTICE, "1) rxMercator LD_LIBRARY_PATH=%s", libpath);
    if (RedisModule_Init(ctx, "rxMercator", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    {
        rxServerLog(rxLL_NOTICE, "OnLoad . Init error!");
        return REDISMODULE_ERR;
    }

    rxRegisterConfig((void **)argv, argc);

    if (RedisModule_CreateCommand(ctx, "mercator.info.config", rx_info_config, "admin write", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "mercator.config.info", rx_info_config, "admin write", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (argc == 1 && strcmp((char *)rxGetContainedObject(argv[0]), "CLIENT") == 0)
    {
        RedisModule_CreateCommand(ctx, "mercator.config", rx_setconfig, "admin write", 1, 1, 0);
        rx_setconfig(NULL, NULL, 0);
        RedisModule_CreateCommand(ctx, "mercator.healthcheck", rx_healthcheck, "admin write", 1, 1, 0);
        RedisModule_CreateCommand(ctx, "mercator.instance.status", rx_healthcheck, "admin write", 1, 1, 0);
        libpath = getenv("LD_LIBRARY_PATH");
        if (libpath)
            rxServerLog(rxLL_NOTICE, "3) rxMercator LD_LIBRARY_PATH=%s", libpath);
        startClientMonitor();
    }
    else
    {
        if (RedisModule_CreateCommand(ctx, "mercator.create.cluster", rx_create_cluster, "admin write", 1, 1, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.destroy.cluster", rx_destroy_cluster, "admin write", 1, 1, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.start.cluster", rx_start_cluster, "admin write", 1, 1, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.stop.cluster", rx_stop_cluster, "admin write", 1, 1, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.kill.cluster", rx_kill_cluster, "admin write", 1, 1, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.snapshot.cluster", rx_snapshot_cluster, "readonly", 1, 1, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.info.cluster", rx_info_cluster, "admin write", 1, 1, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.cluster.info", rx_info_cluster, "admin write", 1, 1, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.flush.cluster", rx_flush_cluster, "admin write", 1, 1, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.add.server", rx_add_server, "admin write", 1, 1, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.attach.controller", rx_add_controller, "admin write", 1, 1, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.help", rx_help, "readonly", 1, 1, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.start.monitor", rx_start_monitor, "admin write", 1, 1, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.stop.monitor", rx_stop_monitor, "admin write", 1, 1, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        libpath = getenv("LD_LIBRARY_PATH");
        rxServerLog(rxLL_NOTICE, "2) rxMercator LD_LIBRARY_PATH=%s", libpath);
        startInstanceMonitor();
    }

    rxServerLog(rxLL_NOTICE, "9) rxMercator client health cron started");

    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *)
{
    stopInstanceMonitor();
    return REDISMODULE_OK;
}
