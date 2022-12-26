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
#include <version.h>
#include <sys/wait.h>

    /* Utils */
    long long ustime(void);
    long long mstime(void);

#include <unistd.h>
#define GetCurrentDir getcwd

#undef RXSUITE_SIMPLE
#include "rxSuite.h"
#include "rxSuiteHelpers.h"
#include "sha1.h"

    int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
#ifdef __cplusplus
}
#endif
#include <cstdarg>

/*
 * This module execute rx query commands
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

const char *CLUSTERING_ARG = "CLUSTERED";
const char *REPLICATION_ARG = "REPLICATED";
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

rxString CreateClusterNode(rxString cluster_key, rxString sha1, const char *role, const char *order, const char *shard, RedisModuleCtx *ctx)
{
    rxString __MERCATOR__SERVERS__ = rxStringNew("__MERCATOR__SERVERS__");
    void *servers = rxFindSetKey(0, __MERCATOR__SERVERS__);
    rxString server = rxRandomSetMember(servers);

    void *server_info = rxFindHashKey(0, server);

    rxString free_ports_key = rxGetHashField2(server_info, PORTS_FIELD);

    rxString address = rxGetHashField2(server_info, ADDRESS_FIELD);

    void *free_ports = rxFindSetKey(0, free_ports_key);
    if (free_ports == NULL)
    {
        return NULL;
    }
    rxString port = rxRandomSetMember(free_ports);

    rxString key = rxStringFormat("__MERCATOR__INSTANCE__%s_%s_%s", sha1, server, port);

    RedisModule_FreeCallReply(
        RedisModule_Call(ctx, HMSET_COMMAND, "ccccccccccccccccc", (char *)key, OWNER_FIELD, sha1, SERVER_FIELD, server, ADDRESS_FIELD, address, MAXMEM_FIELD, MAXMEM_FIELD, PORT_FIELD, port, ROLE_FIELD, role, ORDER_FIELD, order, ORDER_FIELD, shard));

    RedisModule_FreeCallReply(
        RedisModule_Call(ctx, "SADD", "cc", cluster_key, key));

    RedisModule_FreeCallReply(
        RedisModule_Call(ctx, "SREM", "cc", free_ports_key, port));

    rxStringFree(address);
    rxStringFree(free_ports_key);
    return key;
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
    int replication_requested = 0;
    int clustering_requested = 0;
    int start_requested = 0;
    rxString c_ip = rxStringEmpty();
    size_t arg_len;
    for (int j = 1; j < argc; ++j)
    {
        char *q = (char *)RedisModule_StringPtrLen(argv[j], &arg_len);
        if (rxStringMatch(q, REPLICATION_ARG, 1) && strlen(q) == strlen(REPLICATION_ARG))
            replication_requested = 1;
        else if (rxStringMatch(q, CLUSTERING_ARG, 1) && strlen(q) == strlen(CLUSTERING_ARG))
            clustering_requested = 2;
        else if (rxStringMatch(q, START_ARG, 1) && strlen(q) == strlen(START_ARG))
            start_requested = 4;
        else
            c_ip = rxStringNew(q);
    }
    rxString sha1 = getSha1("sii", c_ip, replication_requested, clustering_requested);
    rxString cluster_key = rxStringFormat("__MERCATOR__CLUSTER__%s", sha1);
    void *nodes = rxFindSetKey(0, cluster_key);
    if (nodes)
        return RedisModule_ReplyWithSimpleString(ctx, sha1);

    RedisModule_FreeCallReply(
        RedisModule_Call(ctx, "SADD", "cc", (char *)"__MERCATOR__CLUSTERS__", cluster_key));
    rxString data;
    rxString index;
    rxString data2;
    switch (replication_requested + clustering_requested)
    {
    case 0: // Not Replicated, Not Clustered
        data = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "1", "1", ctx);
        index = CreateClusterNode(cluster_key, sha1, INDEX_FIELD, "1", "1", ctx);
        if (data != NULL && index != NULL)
        {
            RedisModule_FreeCallReply(
                RedisModule_Call(ctx, "HSET", "ccc", data, INDEX_FIELD, index));
        }
        else
            return RedisModule_ReplyWithSimpleString(ctx, "No server space available");
        break;
    case 1: // Replicated, Not Clustered
        data = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "1", "1", ctx);
        index = CreateClusterNode(cluster_key, sha1, INDEX_FIELD, "1", "1", ctx);
        if (data != NULL && index != NULL)
        {
            RedisModule_FreeCallReply(
                RedisModule_Call(ctx, "HSET", "ccc", data, INDEX_FIELD, index));
            data2 = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "2", "1", ctx);
            RedisModule_FreeCallReply(
                RedisModule_Call(ctx, "HSET", "ccccc", data2, INDEX_FIELD, index, PRIMARY_FIELD, data));
        }
        break;
    case 2: // Not Replicated, Clustered
        data = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "1", "1", ctx);
        index = CreateClusterNode(cluster_key, sha1, INDEX_FIELD, "1", "1", ctx);
        if (data != NULL && index != NULL)
        {
            RedisModule_FreeCallReply(
                RedisModule_Call(ctx, "HSET", "ccc", data, INDEX_FIELD, index));
            data = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "1", "2", ctx);
            RedisModule_FreeCallReply(
                RedisModule_Call(ctx, "HSET", "ccc", data, INDEX_FIELD, index));
        }
        else
            return RedisModule_ReplyWithSimpleString(ctx, "No server space available");
        break;
    case 3: // Not Replicated, Clustered
        data = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "1", "1", ctx);
        index = CreateClusterNode(cluster_key, sha1, INDEX_FIELD, "1", "1", ctx);
        if (data != NULL && index != NULL)
        {
            RedisModule_FreeCallReply(
                RedisModule_Call(ctx, "HSET", "ccc", data, INDEX_FIELD, index));
            data2 = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "2", "1", ctx);
            RedisModule_FreeCallReply(
                RedisModule_Call(ctx, "HSET", "cccc", data2, INDEX_FIELD, index, PRIMARY_FIELD, data));

            if (data != NULL && index != NULL)
            {
                data = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "1", "2", ctx);
                index = CreateClusterNode(cluster_key, sha1, INDEX_FIELD, "1", "2", ctx);
                RedisModule_FreeCallReply(
                    RedisModule_Call(ctx, "HSET", "ccc", data, INDEX_FIELD, index));
                data2 = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "2", "2", ctx);
                RedisModule_FreeCallReply(
                    RedisModule_Call(ctx, "HSET", "ccccc", data2, INDEX_FIELD, index, PRIMARY_FIELD, data));
            }
            else
                return RedisModule_ReplyWithSimpleString(ctx, "No server space available");
        }
        else
            return RedisModule_ReplyWithSimpleString(ctx, "No server space available");

        break;
    default:
        return RedisModule_ReplyWithSimpleString(ctx, "Invalid parameters");
    }
    if (start_requested == 4)
    {
        RedisModule_FreeCallReply(
            RedisModule_Call(ctx, "mercator.start.cluster", "c", sha1));
    }
    return RedisModule_ReplyWithSimpleString(ctx, sha1);
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
    }while(true);
    return 0;
}

int rx_start_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int)
{
    size_t arg_len;
    char *sha1 = (char *)RedisModule_StringPtrLen(argv[1], &arg_len);

    rxString cluster_key = rxStringFormat("__MERCATOR__CLUSTER__%s", sha1);
    void *nodes = rxFindSetKey(0, cluster_key);
    if (!nodes)
        return RedisModule_ReplyWithSimpleString(ctx, "cluster not found");
    char cwd[FILENAME_MAX]; // create string buffer to hold path
    GetCurrentDir(cwd, FILENAME_MAX);
    char *data = strstr(cwd, "/data");
    if (data != NULL)
        *data = 0x00;

    void *si = NULL;
    rxString node;
    int64_t l;
    rxServerLogRaw(rxLL_WARNING, rxStringFormat("Starting cluster:%s\n", sha1));
    while (rxScanSetMembers(nodes, &si, (char **)&node, &l) != NULL)
    {
        void *node_info = rxFindHashKey(0, node);
        rxString port = rxGetHashField(node_info, PORT_FIELD);
        rxString address = rxGetHashField(node_info, ADDRESS_FIELD);
        rxString role = rxGetHashField(node_info, ROLE_FIELD);
        rxString shard = rxGetHashField(node_info, ORDER_FIELD);
        rxString index_name = rxGetHashField(node_info, INDEX_FIELD);
        void *index_node_info = rxFindHashKey(0, index_name);
        rxString index_address = rxGetHashField(index_node_info, ADDRESS_FIELD);
        rxString index_port = rxGetHashField(index_node_info, PORT_FIELD);
        if (strlen(index_address) == 0)
        {
            index_address = rxStringDup(address);
            index_port = rxStringDup(port);
        }
        rxServerLog(rxLL_WARNING, "INDEX: %s %s %s\n", index_name, index_address, index_port);

        auto *config = getRxSuite();
        rxString primary_name = rxGetHashField2(node_info, PRIMARY_FIELD);
        rxServerLog(rxLL_NOTICE, ".... starting node:%s %s:%s role:%s shard:%s index_name:%s primary_name:%s \n", node, address, port, role, shard, index_name, primary_name);
        rxString startup_command = rxStringFormat(
            "python3 %s/extensions/src/start_node.py %s %s %s %s %s %s %s \"%s\" \"%s\" \"%s\" >>$HOME/redis-%s/data/startup.log  2>>$HOME/redis-%s/data/startup.log ",
            cwd,
            sha1,
            address, port,
            role,
            index_address, index_port,
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
            char controller[24];
            snprintf(controller, sizeof(controller), "%s:%s", address, port);
            auto *redis_node = RedisClientPool<redisContext>::Acquire(controller);
            if (redis_node != NULL)
            {
                rxServerLog(rxLL_WARNING, " start check: CONNECTED\n");
                RedisClientPool<redisContext>::Release(redis_node);
                tally = 2069722764;
                break;
            }
            rxServerLog(rxLL_WARNING, " start check: CONNECTion failed\n");
            stop = ustime();
            ++tally;
        } while (tally < 5 && stop - start <= 5 * 1000);
        rxServerLog(rxLL_WARNING, " start check tally %d timing %lld :: %lld == %lld  \n", tally, stop, start, stop - start);

        if (tally != 2069722764)
        {
            rxServerLog(rxLL_WARNING, " start check: START LOCAL\n");
            start_redis(startup_command);
        }

        if (strcmp(primary_name, rxStringEmpty()) != 0)
        {
            void *primary_node_info = rxFindHashKey(0, primary_name);
            rxString primary_address = rxGetHashField2(primary_node_info, ADDRESS_FIELD);
            rxString primary_port = rxGetHashField2(primary_node_info, PORT_FIELD);

            startup_command = rxStringFormat("%s/src/redis-cli -h %s -p %s REPLICAOF %s %s", cwd, address, port, primary_address, primary_port);
            string out = exec(startup_command);
            rxStringFree(startup_command);
            rxStringFree(primary_address);
            rxStringFree(primary_port);
        }
        rxStringFree(port);
        rxStringFree(address);
        rxStringFree(role);
        rxStringFree(shard);
        rxStringFree(index_name);
        rxStringFree(index_address);
        rxStringFree(index_port);
    }

    return RedisModule_ReplyWithSimpleString(ctx, cwd);
}

int rx_stop_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int)
{
    size_t arg_len;
    char *sha1 = (char *)RedisModule_StringPtrLen(argv[1], &arg_len);

    rxString cluster_key = rxStringFormat("__MERCATOR__CLUSTER__%s", sha1);
    void *nodes = rxFindSetKey(0, cluster_key);
    if (!nodes)
        return RedisModule_ReplyWithSimpleString(ctx, "cluster not found");
    char cwd[FILENAME_MAX]; // create string buffer to hold path
    GetCurrentDir(cwd, FILENAME_MAX);

    void *si = NULL;
    rxString node;
    int64_t l;
    rxServerLog(rxLL_WARNING, "Stopping cluster:%s\n", sha1);
    while (rxScanSetMembers(nodes, &si, (char **)&node, &l) != NULL)
    {
        void *node_info = rxFindHashKey(0, node);
        rxString port = rxGetHashField2(node_info, PORT_FIELD);
        rxString address = rxGetHashField2(node_info, ADDRESS_FIELD);
        rxServerLog(rxLL_WARNING, ".... stopping node:%s %s:%s\n", node, address, port);

        rxString startup_command = rxStringFormat("%s/src/redis-cli -h %s -p %s SHUTDOWN", (const char *)cwd, (const char *)address, (const char *)port);
        string out = exec(startup_command);
        rxServerLog(rxLL_WARNING, "%s\n%s\n", (const char *)startup_command, (const char *)out.c_str());
        rxStringFree(startup_command);
        rxStringFree(port);
        rxStringFree(address);
    }
    return RedisModule_ReplyWithSimpleString(ctx, cwd);
}

int rx_kill_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int)
{
    size_t arg_len;
    char *q = (char *)RedisModule_StringPtrLen(argv[1], &arg_len);
    return RedisModule_ReplyWithSimpleString(ctx, q);
}

int rx_snapshot_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int)
{
    size_t arg_len;
    char *sha1 = (char *)RedisModule_StringPtrLen(argv[1], &arg_len);

    rxString cluster_key = rxStringFormat("__MERCATOR__CLUSTER__%s", sha1);
    void *nodes = rxFindSetKey(0, cluster_key);
    if (!nodes)
        return RedisModule_ReplyWithSimpleString(ctx, "cluster not found");
    char cwd[FILENAME_MAX]; // create string buffer to hold path
    GetCurrentDir(cwd, FILENAME_MAX);

    void *si = NULL;
    rxString node;
    int64_t l;
    while (rxScanSetMembers(nodes, &si, (char **)&node, &l) != NULL)
    {
        void *node_info = rxFindHashKey(0, node);
        rxString port = rxGetHashField2(node_info, PORT_FIELD);
        rxString address = rxGetHashField2(node_info, ADDRESS_FIELD);

        rxString startup_command = rxStringFormat("%s/src/redis-cli -h %s -p %s BGSAVE", cwd, address, port);
        string out = exec(startup_command);
        rxServerLogRaw(rxLL_WARNING, rxStringFormat("%s\n%s\n", startup_command, out.c_str()));
        rxStringFree(startup_command);
        rxStringFree(port);
        rxStringFree(address);
    }

    return RedisModule_ReplyWithSimpleString(ctx, cwd);
}

int rx_flush_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int)
{
    size_t arg_len;
    char *sha1 = (char *)RedisModule_StringPtrLen(argv[1], &arg_len);

    rxString cluster_key = rxStringFormat("__MERCATOR__CLUSTER__%s", sha1);
    void *nodes = rxFindSetKey(0, cluster_key);
    if (!nodes)
        return RedisModule_ReplyWithSimpleString(ctx, "cluster not found");

    void *si = NULL;
    rxString node;
    int64_t l;
    while (rxScanSetMembers(nodes, &si, (char **)&node, &l) != NULL)
    {
        void *node_info = rxFindHashKey(0, node);
        rxString port = rxGetHashField2(node_info, PORT_FIELD);
        rxString address = rxGetHashField2(node_info, ADDRESS_FIELD);
        char controller[24];
        snprintf(controller, sizeof(controller), "%s:%s", address, port);

        auto *redis_node = RedisClientPool<redisContext>::Acquire(controller);
        if (redis_node != NULL)
        {
            redisReply *rcc = (redisReply *)redisCommand(redis_node, "AUTH", "%s %s", "admin", "admin");
            rxServerLogRaw(rxLL_WARNING, rxStringFormat(" AUTH:  %s\n", rcc ? rcc->str : NULL));
            freeReplyObject(rcc);
            rcc = (redisReply *)redisCommand(redis_node, "FLUSHALL");
            rxServerLogRaw(rxLL_WARNING, rxStringFormat(" flush: %s:%s --> %s\n", address, port, rcc ? rcc->str : NULL));
            freeReplyObject(rcc);
            rcc = (redisReply *)redisCommand(redis_node, "RULE.DEL", "*", "");
            rxServerLogRaw(rxLL_WARNING, rxStringFormat(" RULE.DEL *: %s:%s --> %s\n", address, port, rcc ? rcc->str : NULL));
            freeReplyObject(rcc);
            RedisClientPool<redisContext>::Release(redis_node);
        }
        rxStringFree(port);
        rxStringFree(address);
    }

    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int rx_info_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int)
{
    size_t arg_len;
    char *sha1 = (char *)RedisModule_StringPtrLen(argv[1], &arg_len);

    rxString info = rxStringNew("{ \"id\":\"");
    info = rxStringFormat("%s%s", info, sha1);
    info = rxStringFormat("%s%s", info, "\", \"nodes\":[");

    rxString cluster_key = rxStringFormat("__MERCATOR__CLUSTER__%s", sha1);
    void *nodes = rxFindSetKey(0, cluster_key);
    if (!nodes)
    {
        info = rxStringFormat("%s%s", info, "]}");
        return RedisModule_ReplyWithSimpleString(ctx, info);
    }

    void *si = NULL;
    rxString node;
    int64_t l;
    rxServerLogRaw(rxLL_WARNING, rxStringFormat("Starting cluster:%s\n", sha1));
    char sep[3] = {' ', 0x00, 0x00};
    while (rxScanSetMembers(nodes, &si, (char **)&node, &l) != NULL)
    {
        void *node_info = rxFindHashKey(0, node);
        rxString port = rxGetHashField2(node_info, PORT_FIELD);
        rxString address = rxGetHashField2(node_info, ADDRESS_FIELD);
        rxString role = rxGetHashField2(node_info, ROLE_FIELD);
        rxString shard = rxGetHashField2(node_info, "shard");
        rxString index_name = rxGetHashField2(node_info, INDEX_FIELD);
        rxString primary_name = rxGetHashField2(node_info, PRIMARY_FIELD);

        info = rxStringFormat("%s%s{ \"node\":\"%s\", \"ip\":\"%s\", \"port\":\"%s\", \"role\":\"%s\", \"shard\":\"%s\", \"index_name\":\"%s\", \"primary_name\":\"%s\" }\n", info, sep, node, address, port, role, shard, index_name, primary_name);
        sep[0] = ',';
        rxStringFree(port);
        rxStringFree(address);
        rxStringFree(role);
        rxStringFree(shard);
        rxStringFree(index_name);
        rxStringFree(primary_name);
    }

    rxStringFree(cluster_key);
    info = rxStringFormat("%s%s", info, "]}");
    return RedisModule_ReplyWithSimpleString(ctx, info);
}

int rx_info_config(RedisModuleCtx *ctx, RedisModuleString **argv, int)
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
    return RedisModule_ReplyWithSimpleString(ctx, HELP_STRING);
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
        char *node_ports = (char *)RedisModule_StringPtrLen(argv[j + 3], &arg_len);
        rxString key = rxStringNew("__MERCATOR__SERVER__");
        key = rxStringFormat("%s%s", key, node_name);
        rxString key2 = rxStringNew("__MERCATOR__SERVER__");
        key2 = rxStringFormat("%s%s", key2, node_name);
        key2 = rxStringFormat("%s%s", key2, "__FREEPORTS__");
        rxString key3 = rxStringNew("__MERCATOR__");
        key3 = rxStringFormat("%s%s", key3, node_name);
        key3 = rxStringFormat("%s%s", key3, "__USEDPORTS__");
        RedisModule_FreeCallReply(
            RedisModule_Call(ctx, HMSET_COMMAND, "ccccccccccc", (char *)key, "name", node_name, ADDRESS_FIELD, node_ip, MAXMEM_FIELD, node_mem, PORTS_FIELD, key2, "claims", key3));
        RedisModule_FreeCallReply(
            RedisModule_Call(ctx, "SADD", "cc", (char *)"__MERCATOR__SERVERS__", key));
        int nr_of_ports = atoi(node_ports);
        for (long long int p = 6400; nr_of_ports >= 0; --nr_of_ports)
        {
            char port_no[128];
            snprintf(port_no, sizeof(port_no), "%lli", p + nr_of_ports);
            RedisModule_FreeCallReply(
                RedisModule_Call(ctx, "SADD", "cl", (char *)key2, p + nr_of_ports));
        }
    }
    return RedisModule_ReplyWithSimpleString(ctx, HELP_STRING);
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

    if (argc == 1 && strcmp((char *)rxGetContainedObject(argv[0]), "CLIENT") == 0)
    {
        RedisModule_CreateCommand(ctx, "mercator.config", rx_setconfig, "admin write", 1, 1, 0);
        rx_setconfig(NULL, NULL, 0);
        RedisModule_CreateCommand(ctx, "mercator.healthcheck", rx_healthcheck, "admin write", 1, 1, 0);
        libpath = getenv("LD_LIBRARY_PATH");
        rxServerLog(rxLL_NOTICE, "3) rxMercator LD_LIBRARY_PATH=%s", libpath);
        return REDISMODULE_OK;
    }
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

    if (RedisModule_CreateCommand(ctx, "mercator.flush.cluster", rx_flush_cluster, "admin write", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "mercator.add.server", rx_add_server, "admin write", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "mercator.help", rx_help, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    libpath = getenv("LD_LIBRARY_PATH");
    rxServerLog(rxLL_NOTICE, "2) rxMercator LD_LIBRARY_PATH=%s", libpath);
    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *)
{
    return REDISMODULE_OK;
}
