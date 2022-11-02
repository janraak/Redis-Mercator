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
#include "zmalloc.h"
#include <string.h>

    /* Utils */
    long long ustime(void);
    long long mstime(void);

#include <unistd.h>
#define GetCurrentDir getcwd

#include "rxSuiteHelpers.h"
#include "sha1.h"
#include "util.h"

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

const char *HELP_STRING = "Mercator Demo Cluster Manager Commands:\n"
                          "\n"
                          " mercator.create.cluster caller %c_ip% [ clustered ] [ replicated ]\n"
                          " mercator.create.cluster caller %c_ip% [ clustered ] [ replicated ] [ managed %keys% %avg_size% ]\n"
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

sds getSha1(const char *codes, ...)
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
            sds s = va_arg(args, sds);
            SHA1Update(&ctx, (const unsigned char *)s, sdslen(s));
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

    char *sha1 = (char *)zmalloc(41);
    char *filler = sha1;
    for (i = 0; i < 20; i++, filler += 2)
    {
        sprintf(filler, "%02x", hash[i]);
    }
    *filler = 0x00;
    return sdsnew(sha1);
}

void freeSha1(sds sha1)
{
    sdsfree(sha1);
}

sds CreateClusterNode(sds cluster_key, sds sha1, const char *role, const char *order, const char *shard, RedisModuleCtx *ctx)
{
    sds __MERCATOR__SERVERS__ = sdsnew("__MERCATOR__SERVERS__");
    void *servers = rxFindSetKey(0, __MERCATOR__SERVERS__);
    sds server = rxRandomSetMember(servers);

    void *server_info = rxFindHashKey(0, server);

    sds ports_field = sdsnew("ports");
    sds free_ports_key = rxGetHashField(server_info, ports_field);

    sds address_field = sdsnew("address");
    sds address = rxGetHashField(server_info, address_field);

    void *free_ports = rxFindSetKey(0, free_ports_key);
    sds port = rxRandomSetMember(free_ports);

    sds key = sdscatfmt(sdsempty(), "__MERCATOR__INSTANCE__%s_%s_%s", sha1, server, port);

    RedisModule_FreeCallReply(
        RedisModule_Call(ctx, "HMSET", "ccccccccccccccccc", (char *)key, "owner", sha1, "server", server, "address", address, "maxmem", "1MB", "port", port, "role", role, "order", order, "shard", shard));

    RedisModule_FreeCallReply(
        RedisModule_Call(ctx, "SADD", "cc", cluster_key, key));

    RedisModule_FreeCallReply(
        RedisModule_Call(ctx, "SREM", "cc", free_ports_key, port));

    sdsfree(ports_field);
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
    sds c_ip = sdsempty();
    size_t arg_len;
    for (int j = 1; j < argc; ++j)
    {
        char *q = (char *)RedisModule_StringPtrLen(argv[j], &arg_len);
        if (stringmatchlen(q, strlen(REPLICATION_ARG), REPLICATION_ARG, strlen(REPLICATION_ARG), 1) && strlen(q) == strlen(REPLICATION_ARG))
            replication_requested = 1;
        else if (stringmatchlen(q, strlen(CLUSTERING_ARG), CLUSTERING_ARG, strlen(CLUSTERING_ARG), 1) && strlen(q) == strlen(CLUSTERING_ARG))
            clustering_requested = 2;
        else if (stringmatchlen(q, strlen(START_ARG), START_ARG, strlen(START_ARG), 1) && strlen(q) == strlen(START_ARG))
            start_requested = 4;
        else
            c_ip = sdsnew(q);
    }
    sds sha1 = getSha1("sii", c_ip, replication_requested, clustering_requested);
    sds cluster_key = sdscatfmt(sdsempty(), "__MERCATOR__CLUSTER__%s", sha1);
    void *nodes = rxFindSetKey(0, cluster_key);
    if (nodes)
        return RedisModule_ReplyWithSimpleString(ctx, sha1);

    RedisModule_FreeCallReply(
        RedisModule_Call(ctx, "SADD", "cc", (char *)"__MERCATOR__CLUSTERS__", cluster_key));
    sds data;
    sds index;
    sds data2;
    switch (replication_requested + clustering_requested)
    {
    case 0: // Not Replicated, Not Clustered
        data = CreateClusterNode(cluster_key, sha1, "data", "1", "1", ctx);
        index = CreateClusterNode(cluster_key, sha1, "index", "1", "1", ctx);
        RedisModule_FreeCallReply(
            RedisModule_Call(ctx, "HSET", "ccc", data, "index", index));
        break;
    case 1: // Replicated, Not Clustered
        data = CreateClusterNode(cluster_key, sha1, "data", "1", "1", ctx);
        index = CreateClusterNode(cluster_key, sha1, "index", "1", "1", ctx);
        RedisModule_FreeCallReply(
            RedisModule_Call(ctx, "HSET", "ccc", data, "index", index));
        data2 = CreateClusterNode(cluster_key, sha1, "data", "2", "1", ctx);
        RedisModule_FreeCallReply(
            RedisModule_Call(ctx, "HSET", "ccccc", data2, "index", index, "primary", data));
        break;
    case 2: // Not Replicated, Clustered
        data = CreateClusterNode(cluster_key, sha1, "data", "1", "1", ctx);
        index = CreateClusterNode(cluster_key, sha1, "index", "1", "1", ctx);
        RedisModule_FreeCallReply(
            RedisModule_Call(ctx, "HSET", "ccc", data, "index", index));
        data = CreateClusterNode(cluster_key, sha1, "data", "1", "2", ctx);
        RedisModule_FreeCallReply(
            RedisModule_Call(ctx, "HSET", "ccc", data, "index", index));
        break;
    case 3: // Not Replicated, Clustered
        data = CreateClusterNode(cluster_key, sha1, "data", "1", "1", ctx);
        index = CreateClusterNode(cluster_key, sha1, "index", "1", "1", ctx);
        RedisModule_FreeCallReply(
            RedisModule_Call(ctx, "HSET", "ccc", data, "index", index));
        data2 = CreateClusterNode(cluster_key, sha1, "data", "2", "1", ctx);
        RedisModule_FreeCallReply(
            RedisModule_Call(ctx, "HSET", "cccc", data2, "index", index, "primary", data));

        data = CreateClusterNode(cluster_key, sha1, "data", "1", "2", ctx);
        index = CreateClusterNode(cluster_key, sha1, "index", "1", "2", ctx);
        RedisModule_FreeCallReply(
            RedisModule_Call(ctx, "HSET", "ccc", data, "index", index));
        data2 = CreateClusterNode(cluster_key, sha1, "data", "2", "2", ctx);
        RedisModule_FreeCallReply(
            RedisModule_Call(ctx, "HSET", "ccccc", data2, "index", index, "primary", data));
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
        printf("cli:  ");
        int rc = system(cmd);
        printf(" rc = %d %s \n", rc, cmd);
        if (!(rc == -1 || WEXITSTATUS(rc) != 0))
        {
            return ("");
        }
        stop = ustime();
        ++tally;
    } while (tally < 5 && stop - start <= 15 * 1000);
    printf(" tally %d timing %lld :: %lld == %lld  \n", tally, stop, start, stop - start);
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
int start_redis(sds command)
{
    pid_t child_pid;
    child_pid = fork(); // Create a new child process;
    if (child_pid < 0)
    {
        printf("fork failed");
        return 1;
    }
    else if (child_pid == 0)
    { // New process
        printf("Start attempt: %s\n", command);
        int rc = system(command);
        printf("Start attempt rc: %d\n", rc);
        exit(0);
    }
    return 0;
}

int rx_start_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int)
{
    size_t arg_len;
    char *sha1 = (char *)RedisModule_StringPtrLen(argv[1], &arg_len);

    sds cluster_key = sdscatfmt(sdsempty(), "__MERCATOR__CLUSTER__%s", sha1);
    void *nodes = rxFindSetKey(0, cluster_key);
    if (!nodes)
        return RedisModule_ReplyWithSimpleString(ctx, "cluster not found");
    char cwd[FILENAME_MAX]; // create string buffer to hold path
    GetCurrentDir(cwd, FILENAME_MAX);

    void *si = NULL;
    sds node;
    int64_t l;
    sds server_field = sdsnew("server");
    sds address_field = sdsnew("address");
    sds port_field = sdsnew("port");
    sds role_field = sdsnew("role");
    sds index_field = sdsnew("index");
    sds primary_field = sdsnew("primary");
    sds shard_field = sdsnew("shard");
    printf("Starting cluster:%s\n", sha1);
    while (rxScanSetMembers(nodes, &si, &node, &l) != NULL)
    {
        void *node_info = rxFindHashKey(0, node);
        sds port = rxGetHashField(node_info, port_field);
        sds address = rxGetHashField(node_info, address_field);
        sds role = rxGetHashField(node_info, role_field);
        sds shard = rxGetHashField(node_info, shard_field);
        sds index_name = rxGetHashField(node_info, index_field);
        void *index_node_info = rxFindHashKey(0, index_name);
        sds index_address = rxGetHashField(index_node_info, address_field);
        sds index_port = rxGetHashField(index_node_info, port_field);
        if (sdslen(index_address) == 0)
        {
            index_address = sdsdup(address);
            index_port = sdsdup(port);
        }
        printf("INDEX: %s %s %s\n", index_name, index_address, index_port);

        sds primary_name = rxGetHashField(node_info, primary_field);
        printf(".... starting node:%s %s:%s role:%s shard:%s index_name:%s primary_name:%s \n", node, address, port, role, shard, index_name, primary_name);
        sds startup_command = sdscatprintf(sdsempty(),
                                           "python3 %s/extensions/src/start_node.py %s %s %s %s %s",
                                           cwd, address, port,
                                           role,
                                           index_address, index_port);

        // sds ssh_command = sdscatprintf(sdsempty(), "ssh %s %s  &",
        //                                address, startup_command);
        start_redis(startup_command);
        // TODO use a duplexer!!

        long long start = ustime();
        long long stop = ustime();
        int tally = 0;
        do
        {
            auto *redis_node = RedisClientPool<redisContext>::Acquire(address, atoi(port));
            if (redis_node != NULL)
            {
                printf(" start check: CONNECTED\n");
                RedisClientPool<redisContext>::Release(redis_node);
                tally = 2069722764;
                break;
            }
                printf(" start check: CONNECTion failed\n");
            stop = ustime();
            ++tally;
        } while (tally < 5 && stop - start <= 5 * 1000);
        printf(" start check tally %d timing %lld :: %lld == %lld  \n", tally, stop, start, stop - start);

        if (tally != 2069722764){
            printf(" start check: START LOCAL\n");
            start_redis(startup_command);
        }

        if (sdscmp(primary_name, sdsempty()) != 0)
        {
            void *primary_node_info = rxFindHashKey(0, primary_name);
            sds primary_address = rxGetHashField(primary_node_info, address_field);
            sds primary_port = rxGetHashField(primary_node_info, port_field);

            startup_command = sdscatprintf(sdsempty(), "%s/src/redis-cli -h %s -p %s REPLICAOF %s %s", cwd, address, port, primary_address, primary_port);
            string out = exec(startup_command);
            sdsfree(startup_command);
        }
    }

    sdsfree(server_field);
    sdsfree(port_field);
    sdsfree(address_field);
    sdsfree(role_field);
    sdsfree(primary_field);
    sdsfree(index_field);
    return RedisModule_ReplyWithSimpleString(ctx, cwd);
}

int rx_stop_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int)
{
    size_t arg_len;
    char *sha1 = (char *)RedisModule_StringPtrLen(argv[1], &arg_len);

    sds cluster_key = sdscatfmt(sdsempty(), "__MERCATOR__CLUSTER__%s", sha1);
    void *nodes = rxFindSetKey(0, cluster_key);
    if (!nodes)
        return RedisModule_ReplyWithSimpleString(ctx, "cluster not found");
    char cwd[FILENAME_MAX]; // create string buffer to hold path
    GetCurrentDir(cwd, FILENAME_MAX);

    void *si = NULL;
    sds node;
    int64_t l;
    sds address_field = sdsnew("address");
    sds port_field = sdsnew("port");
    printf("Stopping cluster:%s\n", sha1);
    while (rxScanSetMembers(nodes, &si, &node, &l) != NULL)
    {
        void *node_info = rxFindHashKey(0, node);
        sds port = rxGetHashField(node_info, port_field);
        sds address = rxGetHashField(node_info, address_field);
        printf(".... stopping node:%s %s:%s\n", node, address, port);

        sds startup_command = sdscatprintf(sdsempty(), "%s/src/redis-cli -h %s -p %s SHUTDOWN", cwd, address, port);
        string out = exec(startup_command);
        printf("%s\n%s\n", startup_command, out.c_str());
        sdsfree(startup_command);
    }

    sdsfree(port_field);
    sdsfree(address_field);
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

    sds cluster_key = sdscatfmt(sdsempty(), "__MERCATOR__CLUSTER__%s", sha1);
    void *nodes = rxFindSetKey(0, cluster_key);
    if (!nodes)
        return RedisModule_ReplyWithSimpleString(ctx, "cluster not found");
    char cwd[FILENAME_MAX]; // create string buffer to hold path
    GetCurrentDir(cwd, FILENAME_MAX);

    void *si = NULL;
    sds node;
    int64_t l;
    sds address_field = sdsnew("address");
    sds port_field = sdsnew("port");
    while (rxScanSetMembers(nodes, &si, &node, &l) != NULL)
    {
        void *node_info = rxFindHashKey(0, node);
        sds port = rxGetHashField(node_info, port_field);
        sds address = rxGetHashField(node_info, address_field);

        sds startup_command = sdscatprintf(sdsempty(), "%s/src/redis-cli -h %s -p %s BGSAVE", cwd, address, port);
        string out = exec(startup_command);
        printf("%s\n%s\n", startup_command, out.c_str());
        sdsfree(startup_command);
    }

    sdsfree(port_field);
    sdsfree(address_field);
    return RedisModule_ReplyWithSimpleString(ctx, cwd);
}

int rx_flush_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int)
{
    size_t arg_len;
    char *sha1 = (char *)RedisModule_StringPtrLen(argv[1], &arg_len);

    sds cluster_key = sdscatfmt(sdsempty(), "__MERCATOR__CLUSTER__%s", sha1);
    void *nodes = rxFindSetKey(0, cluster_key);
    if (!nodes)
        return RedisModule_ReplyWithSimpleString(ctx, "cluster not found");

    void *si = NULL;
    sds node;
    int64_t l;
    sds address_field = sdsnew("address");
    sds port_field = sdsnew("port");
    while (rxScanSetMembers(nodes, &si, &node, &l) != NULL)
    {
        void *node_info = rxFindHashKey(0, node);
        sds port = rxGetHashField(node_info, port_field);
        sds address = rxGetHashField(node_info, address_field);

        auto *redis_node = RedisClientPool<redisContext>::Acquire(address, atoi(port));
        if (redis_node != NULL)
        {
        	redisReply *rcc = (redisReply *)redisCommand(redis_node, "FLUSHALL");
            
            printf(" flush: %s:%s --> %s\n", address, port, rcc?rcc->str:NULL);
	        freeReplyObject(rcc);            
            RedisClientPool<redisContext>::Release(redis_node);
        }
    }

    sdsfree(port_field);
    sdsfree(address_field);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int rx_info_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int)
{
    size_t arg_len;
    char *sha1 = (char *)RedisModule_StringPtrLen(argv[1], &arg_len);

    sds info = sdsnew("{ \"id\":\"");
    info = sdscat(info, sha1);
    info = sdscat(info, "\", \"nodes\":[");

    sds cluster_key = sdscatfmt(sdsempty(), "__MERCATOR__CLUSTER__%s", sha1);
    void *nodes = rxFindSetKey(0, cluster_key);
    if (!nodes)
    {
        info = sdscat(info, "]}");
        return RedisModule_ReplyWithSimpleString(ctx, info);
    }

    void *si = NULL;
    sds node;
    int64_t l;
    sds server_field = sdsnew("server");
    sds address_field = sdsnew("address");
    sds port_field = sdsnew("port");
    sds role_field = sdsnew("role");
    sds index_field = sdsnew("index");
    sds primary_field = sdsnew("primary");
    sds shard_field = sdsnew("shard");
    printf("Starting cluster:%s\n", sha1);
    sds sep = sdsempty();
    while (rxScanSetMembers(nodes, &si, &node, &l) != NULL)
    {
        void *node_info = rxFindHashKey(0, node);
        sds port = rxGetHashField(node_info, port_field);
        sds address = rxGetHashField(node_info, address_field);
        sds role = rxGetHashField(node_info, role_field);
        sds shard = rxGetHashField(node_info, shard_field);
        sds index_name = rxGetHashField(node_info, index_field);
        sds primary_name = rxGetHashField(node_info, primary_field);

        info = sdscatprintf(info, "%s{ \"node\":\"%s\", \"ip\":\"%s\", \"port\":\"%s\", \"role\":\"%s\", \"shard\":\"%s\", \"index_name\":\"%s\", \"primary_name\":\"%s\" }\n", sep, node, address, port, role, shard, index_name, primary_name);
        sep = sdsnew(", ");
    }

    sdsfree(sep);
    sdsfree(server_field);
    sdsfree(port_field);
    sdsfree(address_field);
    sdsfree(role_field);
    sdsfree(primary_field);
    sdsfree(index_field);
    info = sdscat(info, "]}");
    return RedisModule_ReplyWithSimpleString(ctx, info);
}

int rx_help(RedisModuleCtx *ctx, RedisModuleString **, int)
{
    return RedisModule_ReplyWithSimpleString(ctx, HELP_STRING);
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
        sds key = sdsnew("__MERCATOR__SERVER__");
        key = sdscat(key, node_name);
        sds key2 = sdsnew("__MERCATOR__SERVER__");
        key2 = sdscat(key2, node_name);
        key2 = sdscat(key2, "__FREEPORTS__");
        sds key3 = sdsnew("__MERCATOR__");
        key3 = sdscat(key3, node_name);
        key3 = sdscat(key3, "__USEDPORTS__");
        RedisModule_FreeCallReply(
            RedisModule_Call(ctx, "HMSET", "ccccccccccc", (char *)key, "name", node_name, "address", node_ip, "maxmem", node_mem, "ports", key2, "claims", key3));
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
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **, int)
{
    if (RedisModule_Init(ctx, "rxMercator", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "mercator.create.cluster", rx_create_cluster, "", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "mercator.destroy.cluster", rx_destroy_cluster, "", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "mercator.start.cluster", rx_start_cluster, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "mercator.stop.cluster", rx_stop_cluster, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "mercator.kill.cluster", rx_kill_cluster, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "mercator.snapshot.cluster", rx_snapshot_cluster, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "mercator.info.cluster", rx_info_cluster, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "mercator.flush.cluster", rx_flush_cluster, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "mercator.add.server", rx_add_server, "", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "mercator.help", rx_help, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx)
{
    REDISMODULE_NOT_USED(ctx);
    return REDISMODULE_OK;
}
