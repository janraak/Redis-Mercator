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

#include "uuid.hpp"
extern std::string generate_uuid();

using namespace std;

#ifdef __cplusplus
extern "C"
{
#endif
#include "sdsWrapper.h"
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>

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

#include "../../deps/hiredis/async.h"

#ifdef __cplusplus
}
#endif

#include "client-pool.hpp"
#include "command-multiplexer.hpp"

#include "sjiboleth-fablok.hpp"
#include "sjiboleth.h"

typedef int (*tClusterFunction)(RedisModuleCtx *, redisContext *, const char *, const char *, const char *, void *);

const char *CLUSTERING_ARG = "CLUSTERED";
const char *REPLICATION_ARG = "REPLICATED";
const char *CONTROLLER_ARG = "ZONE";
const char *START_ARG = "START";
const char *REDIS_VERSION_ARG = "REDIS";

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
                          " mercator.create.cluster caller %c_ip% [ clustered ] [ replicated ] [start] [zone %tree%] [redis %version%]\n"
                          " mercator.create.cluster caller %c_ip% [ clustered ] [ replicated ] [ serverless %keys% %avg_size% ] [redis %version%]\n"
                          "\nAllows sharing of servers and instances, the will be created in terms of database rather than nodes"
                          "\n"
                          " mercator.connect.cluster primary %tree% secondary %tree%\n"
                          "\nMake one cluster a replica of another cluster, cross zoned replication"
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

#define FIND_CONTROLLER "RXGET \"G:"            \
                        "v(instance)"           \
                        ".select(address,port)" \
                        ".has(owner, '%s')"     \
                        "\""

#define FIND_SUBCONTROLLERS "RXGET G:"                    \
                            ".v(instance)"                \
                            ".has(role,controller)"       \
                            ".select(address,port,owner)" \
                            ""

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
            if (s)
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
#include <arpa/inet.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>

bool address_is_local(const char *address)
{
    if (address == NULL)
        return true;
    struct ifaddrs *ifAddrStruct = NULL;
    struct ifaddrs *ifa = NULL;
    void *tmpAddrPtr = NULL;
    bool is_local = false;
    getifaddrs(&ifAddrStruct);

    for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next)
    {
        if (!ifa->ifa_addr)
        {
            continue;
        }
        if (ifa->ifa_addr->sa_family == AF_INET)
        { // check it is IP4
            // is a valid IP4 Address
            tmpAddrPtr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
            char addressBuffer[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
            if (strcmp(address, addressBuffer))
            {
                is_local = true;
                break;
            }
        }
        else if (ifa->ifa_addr->sa_family == AF_INET6)
        { // check it is IP6
            // is a valid IP6 Address
            tmpAddrPtr = &((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
            char addressBuffer[INET6_ADDRSTRLEN];
            inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
            if (strcmp(address, addressBuffer))
            {
                is_local = true;
                break;
            }
        }
    }
    if (ifAddrStruct != NULL)
        freeifaddrs(ifAddrStruct);
    return is_local;
}

std::string execWithPipe(const char *cmd, char *address)
{
    char buffer[1024];
    string result = "";
    rxSuiteShared *config = getRxSuite();

    char username[1024];
    getlogin_r(username, sizeof(username));
    if (config->ssh_identity == NULL)
    {
        config->ssh_identity = rxStringFormat("id_rsa_%s", username);
    }

    rxString ssh_cmd = rxStringFormat("ssh -i /home/%s/.ssh/id_rsa_%s %s@%s \"%s\"",
                                      username, username, username, address, cmd);
    rxString to_run = address_is_local(address) ? cmd : ssh_cmd;

    // Open pipe to file
    int retries_left = 2;
    while (retries_left > 0)
    {
        FILE *pipe = popen(to_run, "r");
        if (!pipe)
        {
            return "popen failed!";
        }

        // read till end of process:
        if (!feof(pipe))
        {

            // use buffer to read and add to result

            char *b = NULL;
            do
            {
                b = fgets(buffer, sizeof(buffer), pipe);
                if (b == NULL)
                    break;
                result += b;
            } while (b != NULL);
        }
        pclose(pipe);
        if (result.length() > 0)
            break;
        --retries_left;
        to_run = cmd;
    }

    rxStringFree(ssh_cmd);
    return result;
}

std::string exec(const char *cmd, char *address)
{
    rxSuiteShared *config = getRxSuite();

    char username[1024];
    getlogin_r(username, sizeof(username));
    if (config->ssh_identity == NULL)
    {
        config->ssh_identity = rxStringFormat("id_rsa_%s", username);
    }

    rxString ssh_cmd = rxStringFormat("ssh -i /home/%s/.ssh/id_rsa_%s %s@%s \"%s\"",
                                      username, username, username, address, cmd);
    rxString to_run = address_is_local(address) ? cmd : ssh_cmd;

    long long start = ustime();
    long long stop = ustime();
    int tally = 0;
    do
    {
        printf("%s", to_run);
        int rc = system(to_run);
        if (!(rc == -1 || WEXITSTATUS(rc) != 0))
        {
            rxStringFree(ssh_cmd);
            // goto exitje;
            return ("");
        }
        stop = ustime();
        ++tally;
        to_run = cmd;

    } while (tally < 5 && (stop - start) <= (600000 * 1000));
    rxStringFree(ssh_cmd);
    return ("error");
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

enum eInstalationStatus
{
    _undefined,
    _notInstalled,
    _installed,
    _partial_build,
    _build
};

static const char *eInstalationStatusLabels[] = {"undefined", "not Installed", "installed", "partial build", "build"};

class ServerInstallationStatus
{
protected:
    eInstalationStatus FromLabel(const char *status)
    {
        if(status)
            for (int l = 0; l < 5; ++l)
            {
                if (rxStringMatch(status, eInstalationStatusLabels[l], true))
                {
                    return (eInstalationStatus)l;
                }
            }
        return _undefined;
    }
    // Key: address + scope
public:
    rxString Version;
    rxString Scope;
    rxString Address;
    eInstalationStatus Redis;
    eInstalationStatus Mercator;
    int no_of_shared_objects;

    ServerInstallationStatus *Init(const char *version,
                                   const char *scope,
                                   const char *address)
    {
        this->Version = version;
        this->Scope = scope;
        this->Address = address;
        this->Redis = _installed;
        this->Mercator = _notInstalled;
        this->no_of_shared_objects = 0;
        return this;
    }

    ServerInstallationStatus *Init(const char *version,
                                   const char *scope,
                                   const char *address,
                                   eInstalationStatus redis,
                                   eInstalationStatus mercator)
    {
        this->Version = version;
        this->Scope = scope;
        this->Address = address;
        this->Redis = redis;
        this->Mercator = mercator;
        this->no_of_shared_objects = 0;
        return this;
    }

    static ServerInstallationStatus *New(const char *version, const char *scope, const char *address)
    {
        int vl = strlen(version);
        int zl = scope ? strlen(scope) : 0;
        int al = address ? strlen(address) : 0;
        void *sis = rxMemAlloc(sizeof(ServerInstallationStatus) + vl + zl + al + 3);

        char *v = (char *)sis + sizeof(ServerInstallationStatus);
        strncpy(v, version, vl);
        v[vl] = 0x00;
        char *z = v + vl + 1;
        
        if(scope)
            strncpy(z, scope, zl);        
        z[zl] = 0x00;
        char *a = z + zl + 1;
        if(address)
            strncpy(a, address, al);
        a[al] = 0x00;
        return ((ServerInstallationStatus *)sis)->Init(v, z, a);
    }
    static ServerInstallationStatus *Delete(ServerInstallationStatus *sis)
    {
        rxMemFree(sis);
        return NULL;
    }

    int Write(RedisModuleCtx *ctx)
    {
        RedisModule_ReplyWithArray(ctx, 10);
        RedisModule_ReplyWithSimpleString(ctx, "Version");
        RedisModule_ReplyWithSimpleString(ctx, this->Version);
        RedisModule_ReplyWithSimpleString(ctx, "Scope");
        RedisModule_ReplyWithSimpleString(ctx, this->Scope);
        RedisModule_ReplyWithSimpleString(ctx, "Address");
        RedisModule_ReplyWithSimpleString(ctx, this->Address);
        RedisModule_ReplyWithSimpleString(ctx, "Redis");
        RedisModule_ReplyWithSimpleString(ctx, eInstalationStatusLabels[this->Redis]);
        RedisModule_ReplyWithSimpleString(ctx, "rxMercator");
        RedisModule_ReplyWithSimpleString(ctx, eInstalationStatusLabels[this->Mercator]);
        return C_OK;
    }
    void RedisFromLabel(const char *status)
    {
        this->Redis = FromLabel(status);
    }
    void MercatorFromLabel(const char *status)
    {
        this->Mercator = FromLabel(status);
    }
};

class GetSoftwareStatusAync : public CreateClusterAsync
{
public:
    rax *result_set;
    GetSoftwareStatusAync()
        : CreateClusterAsync()
    {
        this->result_set = raxNew();
    }

    ~GetSoftwareStatusAync()
    {
        raxFree(this->result_set);
    }

    GetSoftwareStatusAync(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
        : CreateClusterAsync(ctx, argv, argc)
    {
        this->result_set = raxNew();
    }

    int Write(RedisModuleCtx *ctx)
    {
        if (this->result_set)
        {

            RedisModule_ReplyWithArray(ctx, raxSize(this->result_set));

            raxIterator setIterator;
            raxStart(&setIterator, this->result_set);
            raxSeek(&setIterator, "^", NULL, 0);
            while (raxNext(&setIterator))
            {
                auto sis = (ServerInstallationStatus *)setIterator.data;
                sis->Write(ctx);
            }
            raxStop(&setIterator);
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

    void InterrogateServerStatus(char *address)
    {

        auto sw_check = execWithPipe("cd $HOME;ls -d1 redis*.*;ls -1 redis*/src/redis-server;ls -d1 redis*/extensions;ls -1 redis*/extensions/src/*.so", address);
        // /home/redisdev
        // redis-7.0.11
        // redis-7.0.11/src/redis-server
        // redis-7.0.11/extensions
        // redis-7.0.11/extensions/src/rxGraphdb.so
        // redis-7.0.11/extensions/src/rxIndexer.so
        // redis-7.0.11/extensions/src/rxIndexStore.so
        // redis-7.0.11/extensions/src/rxMercator.so
        // redis-7.0.11/extensions/src/rxQuery.so
        // redis-7.0.11/extensions/src/rxRule.so
        // redis-7.0.11/extensions/src/sjiboleth.so

        int tally;
        auto items = this->result_set;
        rxString *lines = rxStringSplitLen(sw_check.c_str(), sw_check.length(), "\n", 1, &tally);
        for (int n = 0; n < tally; ++n)
        {
            ServerInstallationStatus *sis = NULL;
            rxString version;
            char *dash = strchr((char *)lines[n], '-');
            if (!dash)
                continue;
            ++dash;
            char *first_slash = strchr(dash, '/');
            if (!first_slash)
                version = rxStringNew(dash);
            else
                version = rxStringNewLen(dash, first_slash - dash);

            if (address == NULL)
            {
                auto ip_check = execWithPipe("ip -o -h -br -ts  a | grep UP", address);
                strtok((char *)ip_check.c_str(), " ");
                strtok(NULL, " ");
                char *link_ip4 = strtok(NULL, " ");
                address = strtok(link_ip4, "/");
            }

            auto sha = address ? getSha1("ss", version, address) : getSha1("s", version);
            auto *o = raxFind(items, (UCHAR *)sha, strlen(sha));
            if (o == raxNotFound)
            {
                sis = ServerInstallationStatus::New(version, "", address ? address : "");
                raxInsert(items, (UCHAR *)sha, strlen(sha), sis, NULL);
            }
            else
                sis = (ServerInstallationStatus *)o;
            char *last_slash = strrchr((char *)lines[n], '/');
            if (!last_slash)
                continue;
            ++last_slash;
            if (strcmp(last_slash, "redis-server") == 0)
            {
                sis->Redis = _build;
                continue;
            }
            char *last_dot = strrchr((char *)lines[n], '.');
            if (last_dot)
            {
                ++last_dot;
                if (strcmp(last_dot, "so") == 0)
                {
                    sis->no_of_shared_objects++;
                    if (sis->no_of_shared_objects >= 7)
                        sis->Mercator = _build;
                    else
                        sis->Mercator = _partial_build;
                    continue;
                }
            }
        }
        rxStringFreeSplitRes(lines, tally);
    }

    void *PropagateCommandToAllSubordinateControllers()
    {
        auto *rcc = ExecuteLocal(FIND_SUBCONTROLLERS, LOCAL_STANDARD);
        if (rcc->elements == 0)
        {
            freeReplyObject(rcc);
            return NULL;
        }

        rxString cmd = rxStringNew(this->GetArgument(0));
        for (int n = 1; n < this->argc; ++n)
        {
            auto e = this->GetArgument(n);
            cmd = rxStringAppend(cmd, e, ' ');
            rxStringFree(e);
        }
        if (rcc->elements > 0)
        {
            for (size_t n = 0; n < rcc->elements; ++n)
            {
                redisReply *row = extractGroupFromRedisReplyByIndex(rcc, n);
                redisReply *hash = extractGroupFromRedisReply(row, "value");
                const char *ip = extractStringFromRedisReply(hash,"address");
                const char *port = extractStringFromRedisReply(hash,"port");
                const char *zone = extractStringFromRedisReply(hash,"zone");
                cmd = rxStringAppend(cmd, "SCOPE", ' ');
                cmd = rxStringAppend(cmd, zone, ' ');
                cmd = rxStringAppend(cmd, "ADDRESS", ' ');
                cmd = rxStringAppend(cmd, ip, ' ');

                char sub_controller[24];
                snprintf(sub_controller, sizeof(sub_controller), "%s:%s", ip, port);
                auto *controller_node = RedisClientPool<redisContext>::Acquire(sub_controller, "_CLIENT", "Invoke_Sub_Controller");
                if (!controller_node)
                {
                    continue;
                }
                redisReply *rcc = (redisReply *)redisCommand(controller_node, cmd);
                if (rcc)
                {
                    switch (rcc->type)
                    {
                    case REDIS_REPLY_ARRAY:
                    {
                    }
                        for (size_t e = 0; e < rcc->elements; ++e)
                        {
                            auto sha = getSha1("sss", rcc->element[e][1].str, rcc->element[e][3].str, rcc->element[e][5].str);
                            auto sis_check = raxFind(this->result_set, (UCHAR *)sha, strlen(sha));
                            if (sis_check == raxNotFound)
                            {
                                auto *sis = ServerInstallationStatus::New(rcc->element[e][1].str, rcc->element[e][3].str, rcc->element[e][5].str);
                                sis->RedisFromLabel(rcc->element[e][7].str);
                                sis->MercatorFromLabel(rcc->element[e][7].str);
                                raxInsert(this->result_set, (UCHAR *)sha, strlen(sha), sis, NULL);
                            }
                        }
                        break;
                    case REDIS_REPLY_ERROR:
                    case REDIS_REPLY_STRING:
                        break;
                    default:
                        break;
                    }
                }
                freeReplyObject(rcc);
                RedisClientPool<redisContext>::Release(controller_node, "Invoke_Sub_Controller");
            }
        }

        return NULL;
    }
};

redisReply *FindInstanceGroup(const char *sha1, const char *fields = NULL);
void LoanInstancesToController(redisContext *sub_controller, int no_of_instances);
void *PropagateCommandToAllSubordinateControllers(CreateClusterAsync *multiplexer, bool async = false);

typedef enum
{
    not_started,
    attach_shadow,
    wait_sync_complete,
    clusters_swapped
} tUpgradeStatus;

class ReplicationGuard
{
public:
    int no_of_replicas;
    int retries;
    size_t no_of_keys;
    char *origin;
    char *shadow;

    void Init()
    {
        this->no_of_keys = 0;
        this->no_of_replicas = 0;
        this->retries = 0;
    }

    static ReplicationGuard *New(char *origin, char *shadow)
    {
        int l_origin = strlen(origin);
        int l_shadow = strlen(shadow);
        void *v = (ReplicationGuard *)rxMemAlloc(sizeof(ReplicationGuard) + l_origin + l_shadow + 2);
        ReplicationGuard *g = (ReplicationGuard *)v;
        g->Init();
        g->origin = (char *)v + sizeof(ReplicationGuard);
        strcpy(g->origin, origin);
        g->shadow = g->origin + l_origin + 1;
        strcpy(g->shadow, shadow);
        return g;
    }

    static ReplicationGuard *Discard(ReplicationGuard *g)
    {
        rxMemFree(g);
        return NULL;
    }
};
class UpgradeClusterAync : public CreateClusterAsync
{
public:
    rxString clusterId = NULL;
    rxString shadow = NULL;
    rxString redisVersion = NULL;
    int redisVersion_num = 0;
    rxString org_redisVersion = NULL;
    int org_redisVersion_num = 0;
    GraphStack<ReplicationGuard> guards;
    tUpgradeStatus upgrade_status;

    int AsNumber(const char *version){
        char *dots[4];
        breakupPointer((char *)version, '.', dots, sizeof(dots) / sizeof(char *));
        char n[16];
        strncpy(n, dots[0], dots[1] - dots[0]);
        n[dots[1] - dots[0]] = 0x00;
        int number = atoi(n);
        strncpy(n, dots[1] + 1, dots[2] - dots[1] - 1);
        n[dots[2] - dots[1] - 1] = 0x00;
        number = (number << 8) +atoi(n);
        return number;
    }

    UpgradeClusterAync(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
        : CreateClusterAsync(ctx, argv, argc)
    {
        size_t arg_len;
        if(argc != 3){
            this->result_text = "Invalid command, syntax: mercator.upgrade.cluster %CLUSTERID% %REDISVERSION%";
            this->StopThread();
            return;
        }
        this->clusterId = rxStringDup((char *)RedisModule_StringPtrLen(argv[1], &arg_len));
        this->redisVersion = rxStringDup((char *)RedisModule_StringPtrLen(argv[2], &arg_len));
        this->redisVersion_num = this->AsNumber(this->redisVersion);
        this->upgrade_status = tUpgradeStatus::not_started;
    }

    ~UpgradeClusterAync()
    {
        rxStringFree(this->clusterId);
        rxStringFree(this->redisVersion);
        rxStringFree(this->org_redisVersion);
        rxStringFree(this->shadow);
    }

    int Done()
    {
        return -1;
    }

    int Execute()
    {
        switch (this->upgrade_status)
        {
        case tUpgradeStatus::not_started:
        case attach_shadow:
        case clusters_swapped:
        {
            return -1;
        }
        case wait_sync_complete:
        {
            ReplicationGuard *guard = this->guards.Dequeue();
            if (!guard)
            {
                return this->SwapClusters();
            }
            WaitForSync(guard);
            return -1;
        }
        }
        return -1;
    };

    void *StopThread()
    {
        this->state = Multiplexer::done;
        return NULL;
    }

    /*
        1) Find controller for clusterid
        2) If served else where:
           a) Pass command on to the correct controller.
           b) Forward upgrade response and exit
        3) If served here:
           a) Create shadow cluster
           b) Start shadow cluster
           c) Attach all shadow cluster nodes as replicas to the original cluster
           d) Wait until full sync completed
           e) Block all update on the original cluster
           e) Swap shadow cluster and original registry entries
           f) fail over to the shadow cluster which by now is the upgraded cluster
    */
    const char *node_role = "role:*";
    const char *connected_slaves = "connected_slaves:*";
    const char *keys = "keys=";

    size_t Parse_KeySpace(redisReply *rcc)
    {
        if(!rcc)
            return 0;
        char *lines[20];
        int max_lines = sizeof(lines) / sizeof(char *);
        breakupPointer(rcc->str, '\r', lines, max_lines);
        for (int n = 0; n < max_lines; ++n)
        {
            int adjustment = 1;
            if (lines[n] == NULL)
                break;
            if (*lines[n] == '\r')
                ++adjustment;
            char *k = strstr(lines[n] + adjustment, keys);
            if (k)
            {
                return atol(k + strlen(keys));
            }
        }
        return 0;
    }

    bool isNodeInRole(redisContext *redis_node, const char *role)
    {
        redisReply *rcc = (redisReply *)redisCommand(redis_node, "INFO REPLICATION");
        if (rcc && rcc->type == REDIS_REPLY_STRING)
        {
            bool inRole = (rxStringMatch(role, rcc->str, MATCH_IGNORE_CASE));
            freeReplyObject(rcc);
            return inRole;
        }
        return false;
    }

    void GetNumberOfReplicasAndKeysFromOrigin(char *origin_node, char *shadow_node)
    {
        auto *redis_node = RedisClientPool<redisContext>::Acquire(origin_node, "_CONTROLLER", "EXEC_ORIGIN");
        if (redis_node != NULL)
        {
            redisReply *rcc = (redisReply *)redisCommand(redis_node, "INFO REPLICATION");
            char *lines[20];
            int max_lines = sizeof(lines) / sizeof(char *);
            ReplicationGuard *guard = ReplicationGuard::New(origin_node, shadow_node);
            if (rcc && rcc->type == REDIS_REPLY_STRING)
            {
                breakupPointer(rcc->str, '\r', lines, max_lines);
                for (int n = 0; n < max_lines; ++n)
                {
                    int adjustment = 1;
                    if (lines[n] == NULL)
                        break;
                    if (*lines[n] == '\r')
                        ++adjustment;
                    if (rxStringMatch(connected_slaves, lines[n] + adjustment, MATCH_IGNORE_CASE))
                    {
                        guard->no_of_replicas = atoi(lines[n] + strlen(connected_slaves) - 1);
                        if(guard->no_of_replicas == 0)
                            guard->no_of_replicas = 1;
                    }
                }

                freeReplyObject(rcc);
            }
            rcc = (redisReply *)redisCommand(redis_node, "INFO KEYSPACE");
            if (rcc && rcc->type == REDIS_REPLY_STRING)
            {
                guard->no_of_keys += Parse_KeySpace(rcc);
                freeReplyObject(rcc);
            }
            RedisClientPool<redisContext>::Release(redis_node, "EXEC_ORIGIN");
            ReplicationGuard *peek = this->guards.Peek();
            if (peek == NULL || peek->no_of_keys < guard->no_of_keys)
                this->guards.Add(guard);
            else
                this->guards.Push(guard);
        }
    }

    void *Upgrade()
    {
        rxString cmd = rxStringFormat("RXGET \"G:V('%s').select('redis')\"", this->clusterId);
        redisReply *org_redisVersion = ExecuteLocal(cmd, LOCAL_FREE_CMD);
        if (org_redisVersion)
        {   auto row = extractGroupFromRedisReply(extractGroupFromRedisReplyByIndex(org_redisVersion,0), "value");
            this->org_redisVersion = rxStringDup((const char*)extractStringFromRedisReply(row,"redis"));
            this->org_redisVersion_num = this->AsNumber(this->org_redisVersion);

            freeReplyObject(org_redisVersion);
        }
        rxServerLog(rxLL_NOTICE, "Upgrading cluster %s from Redis %s to %s", this->clusterId, this->org_redisVersion, this->redisVersion);

        this->upgrade_status = attach_shadow;
        cmd = rxStringFormat("mercator.create.cluster %s_%s REDIS %s", this->clusterId, this->redisVersion, this->redisVersion);
        redisReply *nodes = ExecuteLocal(cmd, LOCAL_FREE_CMD);
        if (nodes->type == REDIS_REPLY_STRING)
        {
            this->shadow = rxStringNewLen(nodes->str, nodes->len);
            rxServerLog(rxLL_NOTICE, "Shadow cluster %s using Redis %s for %s", this->shadow, this->redisVersion, this->clusterId);

            cmd = rxStringFormat("mercator.start.cluster %s", this->shadow);
            ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);
            redisReply *origin = FindInstanceGroup(this->clusterId, "address,port,role,shard,order");
            if (origin->type == REDIS_REPLY_ARRAY)
            {
                // Attach shadow as replica to the origin
                for (size_t rowNo = 0; rowNo < origin->elements; ++rowNo)
                {
                    redisReply *row = extractGroupFromRedisReplyByIndex(origin, rowNo);

                    redisReply *rowFields = extractGroupFromRedisReply(row, "value");
                    auto address = extractStringFromRedisReply(rowFields,"address");
                    auto port = atoi(extractStringFromRedisReply(rowFields,"port"));
                    auto role = extractStringFromRedisReply(rowFields,"role");
                    auto shard = extractStringFromRedisReply(rowFields,"shard");
                    auto order = extractStringFromRedisReply(rowFields,"order");
                    char origin_node[24];
                    snprintf(origin_node, sizeof(origin_node), "%s:%d", address, port);

                    cmd = rxStringFormat("G \"v('%s').out(has_instance).has(role,%s).eq(shard,%s).eq(order,%s).select(address,port)\"",
                                         this->shadow, role, shard, order);
                    redisReply *replica = ExecuteLocal(cmd, LOCAL_FREE_CMD);
                    if (replica->type == REDIS_REPLY_ARRAY && replica->elements > 0)
                    {
                        // Attach shadow as replica to the origin
                        redisReply *row = extractGroupFromRedisReplyByIndex(replica, 0);
                        redisReply *rowFields = extractGroupFromRedisReply(row, "value");
                        char *r_address = extractStringFromRedisReply(rowFields,"address");
                        int r_port = atoi(extractStringFromRedisReply(rowFields,"port"));

                        char node[24];
                        snprintf(node, sizeof(node), "%s:%d", r_address, r_port);
                        char attach[128];
                        snprintf(attach, sizeof(attach), "REPLICAOF %s %d", address, port);
                        auto *redis_node = RedisClientPool<redisContext>::Acquire(node, "_CONTROLLER", "EXEC_REMOTE");
                        if (redis_node != NULL)
                        {
                            redisReply *rcc = (redisReply *)redisCommand(redis_node, attach);
                            freeReplyObject(rcc);
                            RedisClientPool<redisContext>::Release(redis_node, "EXEC_REMOTE");
                        }
                        GetNumberOfReplicasAndKeysFromOrigin(origin_node, node);

                        rxServerLog(rxLL_NOTICE, "Attached Shadow instance %s to %s as replica", node, origin_node);
                    }
                }
            }
        }
        freeReplyObject(nodes);

        this->upgrade_status = wait_sync_complete;

        while (guards.HasEntries())
        {
            ReplicationGuard *guard = this->guards.Dequeue();
            WaitForSync(guard);
        }
        this->SwapClusters();

        return NULL;
    }

    int WaitForSync(ReplicationGuard *guard)
    {
        auto *redis_node = RedisClientPool<redisContext>::Acquire(guard->origin, "_CONTROLLER", "EXEC_ORIGIN");
        if (redis_node != NULL)
        {
            char cmd[256];
            snprintf(cmd, sizeof(cmd), "WAIT %d %ld", guard->no_of_replicas, guard->no_of_keys ? guard->no_of_keys / 1000 : 1000);
            redisReply *rcc = (redisReply *)redisCommand(redis_node, cmd);
            if (rcc)
            {
                auto *shadow_node = RedisClientPool<redisContext>::Acquire(guard->shadow, "_CONTROLLER", "EXEC_SHADOW");
                if (shadow_node != NULL)
                {
                    redisReply *srcc = (redisReply *)redisCommand(shadow_node, "INFO KEYSPACE");
                    if (guard->no_of_keys == Parse_KeySpace(srcc) || guard->retries > 32)
                    {
                        /* FAILOVER Command as of Redis 6.2.0 */
                        if (this->redisVersion_num >= 0x602 && this->org_redisVersion_num >= 0x602)
                        {
                            rxString cmd = rxStringFormat("FAILOVER TO %s", guard->shadow);
                            char *colon = strchr((char *)cmd, ':');
                            if (colon)
                                *colon = ' ';
                            redisReply *frcc = (redisReply *)redisCommand(redis_node, cmd);
                            auto start = mstime();
                            while (!isNodeInRole(redis_node, "*role:slave*") && !isNodeInRole(shadow_node, "*role:master*"))
                            {
                                if (mstime() - start > (long long int)(guard->no_of_keys / 100 )) // Assume 100 writes per sec
                                    break;
                                sched_yield();
                            }
                            redisReply *srcc = (redisReply *)redisCommand(redis_node, "SHUTDOWN NOW");
                            rxStringFree(cmd);
                            rxServerLog(rxLL_NOTICE, "Synced for Shadow instance %s to be fully synced with %s (%d replicas, %ld keys, %d retries), FAILOVER=%s, SHUTDOWN=%s",
                                        guard->shadow, guard->origin,
                                        guard->no_of_replicas, guard->no_of_keys, guard->retries,
                                        frcc ? frcc->str : "?", srcc ? srcc->str : redis_node->errstr);
                            freeReplyObject(frcc);
                            freeReplyObject(srcc);
                        }
                        else
                        {
                            redisReply *frcc = (redisReply *)redisCommand(shadow_node, "REPLICAOF NO ONE");
                            rxString cmd = rxStringFormat("REPLICAOF %s", guard->shadow);
                            char *colon = strchr((char *)cmd, ':');
                            if (colon)
                                *colon = ' ';
                            redisReply *forcc = (redisReply *)redisCommand(redis_node, cmd);
                            redisReply *srcc = (redisReply *)redisCommand(redis_node, "SHUTDOWN ");
                            rxStringFree(cmd);
                            rxServerLog(rxLL_NOTICE, "Synced for Shadow instance %s to be fully synced with %s (%d replicas, %ld keys, %d retries), FAILOVER=%s, SHUTDOWN=%s",
                                        guard->shadow, guard->origin,
                                        guard->no_of_replicas, guard->no_of_keys, guard->retries,
                                        frcc ? frcc->str : "?", srcc ? srcc->str : redis_node->errstr);
                            freeReplyObject(forcc);
                            freeReplyObject(frcc);
                            freeReplyObject(srcc);
                        }
                        ReplicationGuard::Discard(guard);
                    }
                    else
                    {
                        guard->retries++;
                        rxServerLog(rxLL_NOTICE, "Waiting for Shadow instance %s to be fully synced with %s (%d replicas, %ld keys, %d retries)",
                                    guard->shadow, guard->origin,
                                    guard->no_of_replicas, guard->no_of_keys, guard->retries);
                        this->guards.Push(guard);
                    }
                    freeReplyObject(srcc);
                    RedisClientPool<redisContext>::Release(shadow_node, "EXEC_SHADOW");
                }

                freeReplyObject(rcc);
            }
            RedisClientPool<redisContext>::Release(redis_node, "EXEC_ORIGIN");
        }


        return -1;
    }

    int SwapClusters()
    {
        rxServerLog(rxLL_NOTICE, "Swap cluster redisversion between %s %s and %s %s", this->clusterId, this->org_redisVersion, this->shadow, this->redisVersion);
        // cmd = rxStringFormat("RXQUERY \"G:V('%s').property('redis','%s').V('%s').property('redis','%s')\"", this->clusterId, this->redisVersion, this->shadow, this->org_redisVersion);
        rxString cmd = rxStringFormat("RXQUERY \"G:swapKeys('%s','%s')\"", this->clusterId, this->shadow);
        ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);
        rxServerLog(rxLL_NOTICE, "Swap instance ownership between %s and %s", this->clusterId, this->shadow);
        // rxString temp_sha1 = getSha1("ss", this->clusterId, this->shadow);
        // rxString cmd = rxStringFormat("RXQUERY \"G:V(instance).has(owner,'%s').property(owner,'%s').V(instance).has(owner,'%s').property(owner,'%s').V(instance).has(owner,'%s').property(owner,'%s')\"",
        cmd = rxStringFormat("RXQUERY \"G:V('%s').out(has_instance).property(owner,'%s').V('%s').out(has_instance).property(owner,'%s')\"",
                                      this->clusterId, this->clusterId,
                                      this->shadow, this->shadow);
        ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);
        cmd = rxStringFormat("MERCATOR.STOP.CLUSTER %s", this->shadow);
        redisReply *rcc = ExecuteLocal(cmd, LOCAL_FREE_CMD);
        cmd = rxStringFormat("MERCATOR.DESTROY.CLUSTER %s", this->shadow);
        rcc = ExecuteLocal(cmd, LOCAL_FREE_CMD);
        cmd = rxStringFormat("MERCATOR.INFO.CLUSTER %s", this->clusterId);
        rcc = ExecuteLocal(cmd, LOCAL_FREE_CMD);
        this->result_text = rxStringFormat("Cluster %s is now running Redis version %s, all clients must reconnect, cluster info: %s", this->clusterId, this->redisVersion, rcc->str);
        freeReplyObject(rcc);
        return -1;
    }
};

int start_cluster(RedisModuleCtx *ctx, char *sha1);

#define LOCAL_STANDARD 0
#define LOCAL_FREE_CMD 1
#define LOCAL_NO_RESPONSE 2

redisReply *ExecuteLocal(const char *cmd, int options)
{

    char controller[24];
    snprintf(controller, sizeof(controller), "0.0.0.0:%d", rxGetServerPort());
    auto *redis_node = RedisClientPool<redisContext>::Acquire(controller, "_CONTROLLER", "EXEC_LOCAL");
    if (redis_node != NULL)
    {
        redisReply *rcc = (redisReply *)redisCommand(redis_node, cmd);
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

rxString TryCreateClusterNode(rxString cluster_key, rxString sha1, const char *role, const char *order, const char *shard, RedisModuleCtx *)
{
    // TODO: Replace with local lookup

    rxString __MERCATOR__SERVERS__ = rxStringNew("__MERCATOR__SERVERS__");
    void *servers = rxFindSetKey(0, __MERCATOR__SERVERS__);
    rxString server = NULL;
    int retry_cnt = 10;
    do
    {
        server = rxRandomSetMember(servers);
    } while (server == NULL && --retry_cnt > 0);
    if (!server)
    {
        return NULL;
    }
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

    rxString key = rxStringFormat("%s",generate_uuid().c_str() /*"__MERCATOR__INSTANCE__%s_%s_%s", sha1, server, port*/);
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
                               ".property(STATUS,CREATED)"
                               ".reset"
                               ".pushdup()"
                               ".predicate(has_instance,instance_of)"
                               ".object('%s')"
                               ".subject('%s')"
                               ".pop()"
                               ".predicate(has_status,status_of)"
                               ".object('%s__HEALTH')"
                               ".subject('%s')"
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
                               server,
                               key,
                               key,
                               key,
                               key,
                               cluster_key);
    ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);

    cmd = rxStringFormat("SADD %s %s", cluster_key, key);
    ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);

    cmd = rxStringFormat("SREM %s %s", free_ports_key, port);
    ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);

    rxStringFree(address);
    rxStringFree(free_ports_key);
    return key;
}

rxString CreateClusterNode(rxString cluster_key, rxString sha1, const char *role, const char *order, const char *shard, RedisModuleCtx *ctx)
{
    int retries_left = 0;
    rxString node = NULL;
    do
    {
        node = TryCreateClusterNode(cluster_key, sha1, role, order, shard, ctx);
    } while (!node && --retries_left);
    return node;
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


void *CreateClusterAsync_Go(void *privData)
{
    char buffer[64];
    auto *multiplexer = (CreateClusterAsync *)privData;
    multiplexer->state = Multiplexer::running;

    int replication_requested = 0;
    int clustering_requested = 0;
    int start_requested = 0;
    const char *controller_path = NULL;
    char *redis_version = (char *)REDIS_VERSION;
    rxString that_cmd = rxStringFormat("%s", multiplexer->GetArgument(0));
    rxString c_ip = multiplexer->GetArgument(1);
    for (int j = 1; j < multiplexer->argc; ++j)
    {
        auto q = multiplexer->GetArgument(j);
        that_cmd = rxStringAppend(that_cmd, q, ' ');
        if (rxStringMatch(REPLICATION_ARG, q, MATCH_IGNORE_CASE) && strlen(q) == strlen(REPLICATION_ARG))
            replication_requested = 1;
        else if (rxStringMatch(CLUSTERING_ARG, q, MATCH_IGNORE_CASE) && strlen(q) == strlen(CLUSTERING_ARG))
            clustering_requested = 2;
        else if (rxStringMatch(REDIS_VERSION_ARG, q, MATCH_IGNORE_CASE) && strlen(q) == strlen(REDIS_VERSION_ARG))
        {
            redis_version = (char *)multiplexer->GetArgument(j + 1);
            that_cmd = rxStringAppend(that_cmd, redis_version, ' ');
            ++j;
        }
        else if (rxStringMatch(CONTROLLER_ARG, q, MATCH_IGNORE_CASE) && strlen(q) == strlen(CONTROLLER_ARG))
        {
            controller_path = multiplexer->GetArgument(j + 1);
            that_cmd = rxStringAppend(that_cmd, controller_path, ' ');
            ++j;
        }
        else if (rxStringMatch(START_ARG, q, MATCH_IGNORE_CASE) && strlen(q) == strlen(START_ARG))
            start_requested = 4;
        rxStringFree(q);
    }
    if (!redis_version)
        redis_version = (char *)REDIS_VERSION;

    rxString sha1 = getSha1("sii", c_ip, replication_requested, clustering_requested);

    if (controller_path)
    {
        char *first_slash = (char *)strchr(controller_path, '/');
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
                                     ".PROPERTY('redis','%s')"
                                     ".PROPERTY('reference','%s')"
                                     ".pushdup()"
                                    ".predicate(has_status,status_of)"
                                    ".object('%s__HEALTH')"
                                    ".subject('%s')"
                                    ".pop()"
                                     "\"",
                                     controller_path, redis_version, c_ip,
                                     controller_path,
                                     controller_path);
                ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);
                rxString cluster_key = rxStringFormat("__MERCATOR__CONTROLLER__%s", controller_path);

                if (CreateClusterNode(cluster_key, controller_path, CONTROLLER_ROLE_VALUE, "1", "1", multiplexer->ctx) == NULL)
                {
                    multiplexer->result_text = rxStringFormat("No server available for cluster:%s cid:%s role:%s ", cluster_key, controller_path, CONTROLLER_ROLE_VALUE);
                    return multiplexer->StopThread();
                }

                PostCommand(rxStringFormat(UPDATE_CLUSTER__STATE, controller_path, "CREATED", "CREATED", GetCurrentDateTimeAsString(buffer)));

                start_cluster(multiplexer->ctx, (char *)controller_path);
                freeReplyObject(rcc);
                cmd = rxStringFormat(FIND_CONTROLLER, controller_path);
                rcc = ExecuteLocal(cmd, LOCAL_FREE_CMD);
                loan_instances = true;
            }
            if (rcc->type != REDIS_REPLY_ARRAY)
            {
                multiplexer->result_text = rcc->str;
                return multiplexer->StopThread();
            }
            if (rcc->elements == 0)
            {
                multiplexer->result_text = "Cluster not found";
                return multiplexer->StopThread();
            }
            redisReply *row = extractGroupFromRedisReplyByIndex(rcc, 0);
            redisReply *hash = extractGroupFromRedisReply(row, "value");
            const char *ip = extractStringFromRedisReply(hash, "address");
            const char *port = extractStringFromRedisReply(hash, "port");

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
                if (redis_version)
                {
                    deferral = rxStringAppend(deferral, REDIS_VERSION_ARG, ' ');
                    deferral = rxStringAppend(deferral, redis_version, ' ');
                }
                if (start_requested)
                    deferral = rxStringAppend(deferral, START_ARG, ' ');
                if (tail)
                {
                    deferral = rxStringAppend(deferral, CONTROLLER_ARG, ' ');
                    deferral = rxStringAppend(deferral, tail, ' ');
                }
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

    redisReply *nodes = FindInstanceGroup(sha1);
    if (nodes)
    {
        multiplexer->result_text = sha1;
        freeReplyObject(nodes);
        return multiplexer->StopThread();
    }
    auto cmd = rxStringFormat("RXQUERY g:v(%s)", sha1);
    redisReply *r = ExecuteLocal(cmd, LOCAL_FREE_CMD);

    if (r && r->elements != 0)
    {
        freeReplyObject(r);
        if (start_requested == 4)
        {
            start_cluster(multiplexer->ctx, (char *)sha1);
        }
        multiplexer->result_text = sha1;
        return multiplexer->StopThread();
    };

    cmd = rxStringFormat("RXQUERY \"g:addv('%s','cluster').PROPERTY('redis','%s').PROPERTY('reference','%s')"
                                    ".pushdup()"
                                    ".predicate(has_status,status_of)"
                                    ".object('%s__HEALTH')"
                                    ".subject('%s')"
                                    ".pop()"
                                    "\"", sha1, redis_version, c_ip, sha1, sha1);
    r = ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);
    rxString cluster_key = sha1;

    rxString data;
    rxString index;
    rxString data2;
    switch (replication_requested + clustering_requested)
    {
    case 0: // Not Replicated, Not Clustered
        data = CreateClusterNode(cluster_key, sha1, DATA_ROLE_VALUE, "1", "1", multiplexer->ctx);
        index = CreateClusterNode(cluster_key, sha1, INDEX_FIELD, "1", "1", multiplexer->ctx);
        if (data != NULL && index != NULL)
        {
            ExecuteLocal(rxStringFormat("RXQUERY \"g:addv('%s',instance).property(index,'%s')\"", data, index), LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);
            ExecuteLocal(rxStringFormat("RXQUERY \"g:addv('%s',instance).property(data,'%s')\"", index, data), LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);
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
    PostCommand(rxStringFormat(UPDATE_CLUSTER__STATE, sha1, "CREATED", "CREATED", GetCurrentDateTimeAsString(buffer)));
    PostCommand("BGSAVE");

    if (start_requested == 4)
    {
        start_cluster(multiplexer->ctx, (char *)sha1);
    }
    multiplexer->result_text = sha1;
    return multiplexer->StopThread();
}

/*
    mercator.create.cluster caller %c_ip% [ clustered ] [ replicated ]  [redis %version%]

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
    size_t arg_len;
    const char *c_ip = RedisModule_StringPtrLen(multiplexer->argv[1], &arg_len);
    const char *address = RedisModule_StringPtrLen(multiplexer->argv[2], &arg_len);
    const char *port = RedisModule_StringPtrLen(multiplexer->argv[3], &arg_len);
    rxString sha1 = getSha1("sii", c_ip, replication_requested, clustering_requested);

    redisReply *nodes = FindInstanceGroup(c_ip);
    if (nodes)
    {
        multiplexer->result_text = sha1;
        freeReplyObject(nodes);
        return multiplexer->StopThread();
    }

    auto *cmd = rxStringFormat("RXQUERY \"g:AddV('%s','cluster').PROPERTY('reference','%s')\"", c_ip, c_ip);
    ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);

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

    PostCommand(rxStringFormat(UPDATE_CLUSTER__STATE, sha1, "ATTACHED", "ATTACHED", GetCurrentDateTimeAsString(buffer)));

    multiplexer->result_text = c_ip;
    return multiplexer->StopThread();
}

void *DestroyControllerAsync_Go(void *privData)
{
    auto *multiplexer = (CreateClusterAsync *)privData;
    multiplexer->state = Multiplexer::running;

    auto *sha1 = multiplexer->GetArgument(1);

    redisReply *nodes = FindInstanceGroup(sha1, "port,server");
    if (!nodes)
    {
        PropagateCommandToAllSubordinateControllers(multiplexer);
        // multiplexer->result_text = rxStringFormat("%s%s", info, "]}");
        freeReplyObject(nodes);
        return multiplexer->StopThread();
    }

    size_t n = 0;
    rxString msg = rxStringEmpty();
    while (n < nodes->elements) // rxScanSetMembers(nodes, &si, (char **)&node, &l) != NULL)
    {
        redisReply *row = extractGroupFromRedisReplyByIndex(nodes, n);
        char *node = extractStringFromRedisReply(row, "key");
        redisReply *values = extractGroupFromRedisReply(row, "value");
        char *port = extractStringFromRedisReply(values,"port");
        char *server = extractStringFromRedisReply(values,"server");

        rxString cmd = rxStringFormat("SADD %s %s", server, port);
        ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);

        rxString release_notification = rxStringFormat("Released instance %s from %s, %s returned to %s", node, sha1, port, server);
        rxServerLog(rxLL_NOTICE, release_notification);
        msg = rxStringAppend(msg, release_notification, '\n');
        n++;
    }
    rxString cmd = rxStringFormat("G \"V('%s').as(clusters).property('STATUS','DESTROYED').OUT(has_status).property('STATUS','DESTROYED').use(clusters).out(has_instance).as(instances).pushdup().property('STATUS','DESTROYED').as(instances).out(has_status).property('STATUS','DESTROYED')\"", sha1, sha1);
    ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);

    multiplexer->result_text = msg;
    return multiplexer->StopThread();
}

int rx_add_controller(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    auto *multiplexer = new CreateClusterAsync(ctx, argv, argc);
    multiplexer->Async(ctx, AttachControllerAsync_Go);

    return C_OK;
}

int rx_destroy_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    auto *multiplexer = new CreateClusterAsync(ctx, argv, argc);
    multiplexer->Async(ctx, DestroyControllerAsync_Go);

    return C_OK;
}

int start_redis(rxString command, char *address)
{
    exec(command, address);
    return 0;

    pid_t child_pid;
    child_pid = fork(); // Create a new child process;
    if (child_pid < 0)
    {
        return 1;
    }
    else if (child_pid == 0)
    { // New process
        system(command);
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
    int response = 722765;
    char controller[24];
    snprintf(controller, sizeof(controller), "%s:%s", address, port);
    auto *redis_node = RedisClientPool<redisContext>::Acquire(controller, "_CLIENT", "Is_node_Online");
    if (redis_node != NULL)
    {
        redisReply *rcc = (redisReply *)redisCommand(redis_node, "mercator.instance.status");
        response = (rcc == NULL || rcc->type == REDIS_REPLY_ERROR) ? 722764 : 0;
        freeReplyObject(rcc);
        RedisClientPool<redisContext>::Release(redis_node, "Is_node_Online");
    }
    return response;
}

int start_cluster(RedisModuleCtx *ctx, char *osha1)
{
    char *sha1 = strdup(osha1);
    char *redis_version = (char *)REDIS_VERSION;
    char buffer[64];
    rxString cmd = rxStringFormat("rxget \"g:v('%s').select('redis')\"", sha1);
    redisReply *cluster_info = ExecuteLocal(cmd, LOCAL_FREE_CMD);
    if (cluster_info && cluster_info->type == REDIS_REPLY_ARRAY && cluster_info->elements == 1)
    {
        redisReply *values = extractGroupFromRedisReply(extractGroupFromRedisReplyByIndex(cluster_info,0), "value");
        if (strcmp(extractStringFromRedisReply(values,"redis"), "(null)") != 0)
            redis_version = extractStringFromRedisReply(values,"redis");
    }
    if (!redis_version)
        redis_version = (char *)REDIS_VERSION;

    redisReply *nodes = FindInstanceGroup(sha1, "address,port,role,order,index,primary");
    if (!nodes)
    {
        return RedisModule_ReplyWithSimpleString(ctx, "cluster not found");
    }
    rxString cwd = rxStringFormat("$HOME/redis-%s", redis_version);
    rxString data = rxStringFormat("$HOME/redis-%s/data", redis_version);

    size_t n = 0;
    while (n < nodes->elements)
    {
        redisReply *row = extractGroupFromRedisReplyByIndex(nodes, n);
        char *node = extractStringFromRedisReply(row, "key");
        redisReply *values = extractGroupFromRedisReply(row, "value");
        char *address = extractStringFromRedisReply(values,"address");
        char *port = extractStringFromRedisReply(values,"port");
        char *role = extractStringFromRedisReply(values,"role");
        // char *shard = extractStringFromRedisReply(values,"shard");
        char *index_name = extractStringFromRedisReply(values,"index");
        char *index_address = NULL;
        char *index_port = NULL;
        if (index_name && strlen(index_name))
        {
            rxString cmd = rxStringFormat("rxget \"g:select(address,port).v('%s')\"", index_name);
            auto index_node_info = ExecuteLocal(cmd, LOCAL_FREE_CMD);
            if (index_node_info && index_node_info->type == REDIS_REPLY_ARRAY && index_node_info->elements != 0)
            {
                redisReply *ixvalues = extractGroupFromRedisReply(extractGroupFromRedisReplyByIndex(index_node_info, 0), "value");
                index_address = extractStringFromRedisReply(ixvalues, "address");
                index_port = extractStringFromRedisReply(ixvalues, "port");
            }
        }
        if (!index_address && !index_port)
        {
            index_address = address;
            index_port = port;
        }
        auto *config = getRxSuite();
        if (Is_node_Online(address, port) != 0)
        {
            char *primary_name = extractStringFromRedisReply(values, "primary");
            rxString startup_command = rxStringFormat(
                "cd %s;python3 extensions/src/start_node.py %s %s %s %s %s %s %s %s %s %s >>%s/startup.log  2>>%s/startup.log ",
                cwd,
                sha1,          /*1*/
                address, port, /*2-3*/
                role,          /*4*/
                index_address,
                index_port,
                redis_version,
                config->cdnRootUrl,
                config->startScript,
                config->installScript,
                data,
                data,
                cwd);

            start_redis(startup_command, address);

            long long start = ustime();
            long long stop = ustime();
            int tally = 0;
            do
            {
                stop = ustime();
                ++tally;
            } while (tally < 5 && stop - start <= 5 * 1000);

            if (tally != 722764)
            {
                start_redis(startup_command, address);
            }

            if (primary_name && strcmp(primary_name, rxStringEmpty()) != 0)
            {
                // TODO: Replace with local lookup
                void *primary_node_info = rxFindHashKey(0, primary_name);
                rxString primary_address = rxGetHashField2(primary_node_info, ADDRESS_FIELD);
                rxString primary_port = rxGetHashField2(primary_node_info, PORT_FIELD);

                // TODO: via hiredis!
                startup_command = rxStringFormat("%s/src/redis-cli -h %s -p %s REPLICAOF %s %s", cwd, address, port, primary_address, primary_port);
                string out = exec(startup_command, (char *)address);
                rxStringFree(startup_command);
                rxStringFree(primary_address);
                rxStringFree(primary_port);
            }

            PostCommand(rxStringFormat(UPDATE_INSTANCE_START, node, GetCurrentDateTimeAsString(buffer)));
        }
        PostCommand(rxStringFormat(UPDATE_CLUSTER__STATE_STARTED, sha1, GetCurrentDateTimeAsString(buffer)));
        n++;
    }
    freeReplyObject(nodes);
    rxStringFree(data);
    rxStringFree(cwd);
    free(sha1);
    return C_OK;
}

void *ClusterStartAsync_Go(void *privData)
{
    auto *multiplexer = (CreateClusterAsync *)privData;
    multiplexer->state = Multiplexer::running;

    auto *sha1 = multiplexer->GetArgument(1);

    redisReply *nodes = FindInstanceGroup(sha1);
    if (!nodes)
    {
        PropagateCommandToAllSubordinateControllers(multiplexer);
        // multiplexer->result_text = rxStringFormat("%s%s", info, "]}");
        return multiplexer->StopThread();
    }

    start_cluster(multiplexer->ctx, (char *)sha1);
    multiplexer->result_text = sha1;
    return multiplexer->StopThread();
}

int rx_start_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    auto *multiplexer = new CreateClusterAsync(ctx, argv, argc);
    multiplexer->Async(ctx, ClusterStartAsync_Go);

    return C_OK;
}

int clusterKillOperationProc(RedisModuleCtx *, redisContext *redis_node, char *, const char *, char *, void *)
{
    if (redis_node)
    {
        redisReply *rcc = (redisReply *)redisCommand(redis_node, "SHUTDOWN");
        freeReplyObject(rcc);
        return C_OK;
    }
    else
        return C_ERR;
}

int clusterStopOperationProc(RedisModuleCtx *, redisContext *redis_node, char *, const char *, char *, void *)
{
    if (redis_node)
    {
        redisReply *rcc = (redisReply *)redisCommand(redis_node, "SHUTDOWN");
        freeReplyObject(rcc);
        return C_OK;
    }
    else
        return C_ERR;
}

int clusterSnapshotOperationProc(RedisModuleCtx *, redisContext *redis_node, char *, const char *, char *, void *)
{
    redisReply *rcc = (redisReply *)redisCommand(redis_node, "BGSAVE");
    freeReplyObject(rcc);
    return C_OK;
}

int clusterFlushOperationProc(RedisModuleCtx *, redisContext *redis_node, const char *, const char *, char *, void *)
{
    if (!redis_node)
    {
        return C_ERR;
    }
    redisReply *rcc = (redisReply *)redisCommand(redis_node, "%s %s", "FLUSHALL", "SYNC");
    freeReplyObject(rcc);
    rcc = (redisReply *)redisCommand(redis_node, "%s %s", "RULE.DEL", "*");
    freeReplyObject(rcc);
    return C_OK;
}

redisReply *FindInstanceGroup(const char *sha1, const char *fields)
{
    if (fields == NULL)
        fields = "address,port,STATUS";
    // rxString cmd = cmd = rxStringFormat("rxget \"g:v(cluster).inout(has_instance,instance_of).select(%s).has(owner,'%s')\"", fields, sha1);
    rxString cmd = rxStringFormat("rxget \"g:v(instance).select(%s).has(owner,'%s')\"", fields, sha1);
    redisReply *nodes = ExecuteLocal(cmd, LOCAL_FREE_CMD);
    if (nodes->type != REDIS_REPLY_ARRAY || nodes->elements == 0)
    {
        freeReplyObject(nodes);
        return NULL;
    }

    return nodes;
}

redisReply *recover_cluster(RedisModuleCtx *, const char *sha1, char *fields = NULL)
{
    rxString cmd = rxStringFormat("rxget \"g:v(instance).has(owner,'%s'}.hasnot('STATUS','DESTROYED').adde(instance_of,has_instance).to('%s')\"", sha1, sha1);
    ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);
    return FindInstanceGroup(sha1, fields);
}

int clusterOperation(RedisModuleCtx *ctx, const char *sha1, const char *state, clusterOperationProc *operation, void *data, clusterOperationProc *summary_operation)
{
    char buffer[64];
    redisReply *nodes = FindInstanceGroup(sha1);
    if (nodes == NULL)
    {
        nodes = recover_cluster(ctx, sha1);
        if (nodes == NULL)
        {
            return C_ERR;
        }
    }

    int instances_ok = 0;
    int instances_err = 0;
    size_t n = 0;
    while (n < nodes->elements) // rxScanSetMembers(nodes, &si, (char **)&node, &l) != NULL)
    {
        // void *node_info = rxFindHashKey(0, node);
        redisReply *row = extractGroupFromRedisReplyByIndex(nodes, n);
        char *node = extractStringFromRedisReply(row, "key");
        redisReply *values = extractGroupFromRedisReply(row, "value");

        char *address = extractStringFromRedisReply(values,"address");
        char *port = extractStringFromRedisReply(values,"port");
        char *status = extractStringFromRedisReply(values,"status");
        if(rxStringMatch("DESTROYED", status, MATCH_IGNORE_CASE) || rxStringMatch("KILLED", status, MATCH_IGNORE_CASE))
            continue;

        char controller[24];
        snprintf(controller, sizeof(controller), "%s:%s", address, port);

        auto *redis_node = RedisClientPool<redisContext>::Acquire(controller, "_CLIENT", "ClusterOperation");
        if (redis_node != NULL)
        {
            redisReply *rcc = (redisReply *)redisCommand(redis_node, "%s %s %s", "AUTH", "admin", "admin");
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
        PostCommand(rxStringFormat(UPDATE_CLUSTER__STATE, sha1, state, state, GetCurrentDateTimeAsString(buffer)));

    return C_OK;
}

void *StopClusterAsync_Go(void *privData)
{
    auto *multiplexer = (CreateClusterAsync *)privData;
    multiplexer->state = Multiplexer::running;

    auto *sha1 = multiplexer->GetArgument(1);
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

    auto *sha1 = multiplexer->GetArgument(1);

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

    auto *sha1 = multiplexer->GetArgument(1);

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
    auto *sha1 = (const char *)RedisModule_StringPtrLen(multiplexer->argv[1], &arg_len);

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

void propagateCommandAsyncCompletion(redisAsyncContext *, void *r, void *)
{
    redisReply *reply = (redisReply *)r;
    if (reply == NULL)
        return;
    freeReplyObject(reply);
}

void *PropagateCommandToAllSubordinateControllers(CreateClusterAsync *multiplexer, bool async)
{
    auto *rcc = ExecuteLocal(FIND_SUBCONTROLLERS, LOCAL_STANDARD);
    if (rcc->elements == 0)
    {
        freeReplyObject(rcc);
        return NULL;
    }

    rxString cmd = rxStringNew(multiplexer->GetArgument(0));
    for (int n = 1; n < multiplexer->argc; ++n)
    {
        auto e = multiplexer->GetArgument(n);
        cmd = rxStringAppend(cmd, e, ' ');
        rxStringFree(e);
    }
    if (rcc->elements > 0)
    {
        rxString aggregation = rxStringNew("[");
        char sep = ' ';
        for (size_t n = 0; n < rcc->elements; ++n)
        {
            redisReply *row = extractGroupFromRedisReplyByIndex(rcc, n);
            redisReply *hash = extractGroupFromRedisReply(row, "value");
            const char *ip = extractStringFromRedisReply(hash,"address");
            const char *port = extractStringFromRedisReply(hash,"port");
            const char *zone = extractStringFromRedisReply(hash,"zone");

            char sub_controller[24];
            snprintf(sub_controller, sizeof(sub_controller), "%s:%s", ip, port);
            if (async)
            {
                auto *controller_node = RedisClientPool<redisAsyncContext>::Acquire(sub_controller, "_CLIENT", "Invoke_Sub_Controller");
                if (!controller_node)
                {
                    continue;
                }
                switch (redisAsyncCommand(controller_node, propagateCommandAsyncCompletion, (void *)cmd, cmd))
                {
                case REDIS_OK:
                    break;
                default:
                    break;
                };
                RedisClientPool<redisAsyncContext>::Release(controller_node, "Invoke_Sub_Controller");
                continue;
            }
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

    auto *sha1 = multiplexer->GetArgument(1);

    rxString info = rxStringFormat("{ \"id\":\"%s\", \"nodes\":[", sha1);

    redisReply *nodes = FindInstanceGroup(sha1, "address,port,role,order,index,primary,STATUS");
    if (!nodes)
    {
        PropagateCommandToAllSubordinateControllers(multiplexer);
        // multiplexer->result_text = rxStringFormat("%s%s", info, "]}");
        freeReplyObject(nodes);
        return multiplexer->StopThread();
    }

    char sep[3] = {' ', 0x00, 0x00};
    size_t n = 0;
    while (n < nodes->elements) // rxScanSetMembers(nodes, &si, (char **)&node, &l) != NULL)
    {
        redisReply *row = extractGroupFromRedisReplyByIndex(nodes, n);
        char *node = extractStringFromRedisReply(row, "key");
        redisReply *values = extractGroupFromRedisReply(row, "value");
        char *address = extractStringFromRedisReply(values,"address");
        char *port = extractStringFromRedisReply(values, "port");
        char *role = extractStringFromRedisReply(values, "role");
        char *shard = extractStringFromRedisReply(values, "shard");
        char *index_name = extractStringFromRedisReply(values, "index");
        char *primary_name = extractStringFromRedisReply(values, "primary");
        char *status = extractStringFromRedisReply(values, "STATUS");

        info = rxStringFormat("%s%s{ \"node\":\"%s\", \"ip\":\"%s\", \"port\":\"%s\", \"role\":\"%s\", \"shard\":\"%s\", \"index_name\":\"%s\", \"primary_name\":\"%s\", \"status\":\"%s\" }\n", info, sep, node, address, port, role, shard, index_name, primary_name, status);
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

// mercator.install.redis [%tree%/...] | [server]
//
// Install a redis version on all cluster nodes from the cluster id or controller tree
//
void *InstallRedisAsync_Go(void *privData)
{
    auto *multiplexer = (CreateClusterAsync *)privData;
    multiplexer->state = Multiplexer::running;

    auto redis_version = multiplexer->GetArgument(1);
    if (rxStringMatch(REDIS_VERSION, redis_version, 1))
    {
        multiplexer->result_text = rxStringNew("Can not reinstalling running Redis version!");
        return multiplexer->StopThread();
    }

    int rv_v = 0;
    int rv_sv = 0;
    int rv_r = 0;

    char *dots[4];
    breakupPointer((char *)redis_version, '.', dots, 4);
    rv_v = toInt(dots, 0, 1);
    rv_sv = toInt(dots, 1, 2);
    rv_r = toInt(dots, 2, 3);

    PropagateCommandToAllSubordinateControllers(multiplexer);

    redisReply *nodes = ExecuteLocal("rxget g:v(server).select(address)", LOCAL_STANDARD);
    if (!nodes)
    {
        multiplexer->result_text = rxStringNew("install.redis command propogated, no hardware inventory here!");
        return multiplexer->StopThread();
    }
    size_t n = 0;
    rxSuiteShared *config = getRxSuite();

    char username[1024];
    getlogin_r(username, sizeof(username));
    if (config->ssh_identity == NULL)
    {
        config->ssh_identity = rxStringFormat("id_rsa_%s", username);
    }

    while (n < nodes->elements)
    {
        redisReply *row = extractGroupFromRedisReplyByIndex(nodes, n);
        redisReply *values = extractGroupFromRedisReply(row, "value");
        char *address = extractStringFromRedisReply(values, "address");
        rxString install_log = rxStringFormat(">>$HOME/_install_%s.log", redis_version);
        rxString cmd = rxStringFormat("cd $HOME;"
                                      "rm -rf redis-%s %s;"
                                      "pwd %s;"
                                      "ls -l %s;"
                                      "wget  --timestamping  %s/%s %s;"
                                      "dos2unix %s %s;"
                                      "chmod +x %s %s;"
                                      "bash __install_rxmercator.sh %s 0x00%02hhx%02hhx%02hhx %s  2%s &",
                                      redis_version,
                                      install_log,
                                      install_log,
                                      install_log,
                                      config->cdnRootUrl,
                                      config->installScript,
                                      install_log,
                                      config->installScript,
                                      install_log,
                                      config->installScript,
                                      install_log,
                                      redis_version,
                                      rv_v, rv_sv, rv_r,
                                      install_log,
                                      install_log);
        execWithPipe(cmd, address);
        rxStringFree(cmd);
        rxStringFree(install_log);
        n++;
    }
    multiplexer->result_text = rxStringNew("OK, Software installation started, use the mercator.redis.status command to verify the installation.");
    return multiplexer->StopThread();
}

int rx_install_redis(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    auto *multiplexer = new CreateClusterAsync(ctx, argv, argc);
    multiplexer->Async(ctx, InstallRedisAsync_Go);

    return C_OK;
}
// mercator.install.redis [%tree%/...] | [server]
//
// Install a redis version on all cluster nodes from the cluster id or controller tree
//
void *GetRedisAndMercatorStatusAsync_Go(void *privData)
{
    auto *multiplexer = (GetSoftwareStatusAync *)privData;
    multiplexer->state = Multiplexer::running;

    multiplexer->PropagateCommandToAllSubordinateControllers();

    redisReply *nodes = ExecuteLocal("rxget g:v(server).select(address)", LOCAL_STANDARD);
    if (nodes)
    {
        size_t n = 0;

        while (n < nodes->elements)
        {
            redisReply *row = extractGroupFromRedisReplyByIndex(nodes, n);
            redisReply *values = extractGroupFromRedisReply(row, "value");
            char *address = extractStringFromRedisReply(values, "address");
            multiplexer->InterrogateServerStatus(address);
            n++;
        }
    }
    // TODO: multiplexer->InterrogateServerStatus(NULL);

    return multiplexer->StopThread();
}

int rx_status_redis(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    auto *multiplexer = new GetSoftwareStatusAync(ctx, argv, argc);
    multiplexer->Async(ctx, GetRedisAndMercatorStatusAsync_Go);

    return C_OK;
}

// mercator.install.redis [%tree%/...] | [server]
//
// Install a redis version on all cluster nodes from the cluster id or controller tree
//
void *UpgradeClusterAync_Go(void *privData)
{
    auto *multiplexer = (UpgradeClusterAync *)privData;
    multiplexer->state = Multiplexer::running;

    rxString cmd = rxStringFormat("rxget \"g:v('%s').select('redis')\"", multiplexer->clusterId);
    redisReply *nodes = ExecuteLocal(cmd, LOCAL_FREE_CMD);
    if (nodes->type != REDIS_REPLY_ARRAY || nodes->elements != 1)
    {
        PropagateCommandToAllSubordinateControllers(multiplexer);
        freeReplyObject(nodes);
        return multiplexer->StopThread();
    }
    redisReply *row0 = extractGroupFromRedisReplyByIndex(nodes, 0);
    redisReply *vertex0 = extractGroupFromRedisReply(row0, "value");
    if (rxStringMatch(multiplexer->redisVersion, extractStringFromRedisReply(vertex0,"redis"), 1))
    {
        auto cmd = rxStringFormat("MERCATOR.INFO.CLUSTER %s", multiplexer->clusterId);
        redisReply *rcc = ExecuteLocal(cmd, LOCAL_FREE_CMD);
        multiplexer->result_text = rxStringFormat("Cluster %s is already running Redis version %s, all clients must reconnect, cluster info: %s", multiplexer->clusterId, multiplexer->redisVersion, rcc->str);
        freeReplyObject(rcc);
        freeReplyObject(nodes);
        return multiplexer->StopThread();
    }
    freeReplyObject(nodes);

    multiplexer->Upgrade();

    return multiplexer->StopThread();
}

int rx_upgrade_cluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    auto *multiplexer = new UpgradeClusterAync(ctx, argv, argc);
    multiplexer->Async(ctx, UpgradeClusterAync_Go);

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
    for (size_t n = 0; n < 3; ++n)
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
        auto key = rxStringNew(generate_uuid().c_str() /*"__MERCATOR__SERVER__"*/);
        auto key2 = rxStringFormat("%s%s", key, "__FREEPORTS__");
        RedisModule_FreeCallReply(
            RedisModule_Call(ctx, HMSET_COMMAND, "ccccccccccc", (char *)key, "type", "server", "name", node_name, ADDRESS_FIELD, node_ip, MAXMEM_FIELD, node_mem, PORTS_FIELD, key2));
        RedisModule_FreeCallReply(
            RedisModule_Call(ctx, "SADD", "cc", (char *)"__MERCATOR__SERVERS__", key));
        auto cmd = rxStringFormat("G \"predicate(has_pool,pool_for).object('%s').subject(__MERCATOR__SERVERS__)\"", key);
        RedisModule_Call(ctx, "SADD", "c", (char *)cmd);

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
    if (RedisModule_Init(ctx, "rxMercator", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }

    rxRegisterConfig((void **)argv, argc);

    if (RedisModule_CreateCommand(ctx, "mercator.info.config", rx_info_config, "admin write", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "mercator.config.info", rx_info_config, "admin write", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (argc == 1 && strcmp((char *)rxGetContainedObject(argv[0]), "CLIENT") == 0)
    {
        RedisModule_CreateCommand(ctx, "mercator.config", rx_setconfig, "admin write", 0, 0, 0);
        rx_setconfig(NULL, NULL, 0);
        RedisModule_CreateCommand(ctx, "mercator.healthcheck", rx_healthcheck, "admin write", 0, 0, 0);
        RedisModule_CreateCommand(ctx, "mercator.instance.status", rx_healthcheck, "admin write", 0, 0, 0);
        libpath = getenv("LD_LIBRARY_PATH");
        if (libpath)
            startClientMonitor();
    }
    else
    {
        if (RedisModule_CreateCommand(ctx, "mercator.create.cluster", rx_create_cluster, "admin write", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.destroy.cluster", rx_destroy_cluster, "admin write", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;
        if (RedisModule_CreateCommand(ctx, "mercator.drop.cluster", rx_destroy_cluster, "admin write", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.start.cluster", rx_start_cluster, "admin write", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.stop.cluster", rx_stop_cluster, "admin write", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.kill.cluster", rx_kill_cluster, "admin write", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.snapshot.cluster", rx_snapshot_cluster, "readonly", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.info.cluster", rx_info_cluster, "admin write", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.cluster.info", rx_info_cluster, "admin write", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.cluster.upgrade", rx_upgrade_cluster, "admin write", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.upgrade.cluster", rx_upgrade_cluster, "admin write", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.flush.cluster", rx_flush_cluster, "admin write", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.add.server", rx_add_server, "admin write", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.attach.controller", rx_add_controller, "admin write", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.help", rx_help, "readonly", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.start.monitor", rx_start_monitor, "admin write", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.stop.monitor", rx_stop_monitor, "admin write", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.redis.install", rx_install_redis, "admin write", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        if (RedisModule_CreateCommand(ctx, "mercator.redis.status", rx_status_redis, "admin write", 0, 0, 0) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        libpath = getenv("LD_LIBRARY_PATH");
        startInstanceMonitor();
    }

    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *)
{
    stopInstanceMonitor();
    return REDISMODULE_OK;
}
