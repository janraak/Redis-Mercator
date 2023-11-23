/* rxMercatorMonitor -- Mercator Instance Health Monitor
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
#include "rxMercatorMonitor.hpp"
#include <fstream>
#include <iostream>
#include <string>

#include <array>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string.h>

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
}
#undef RXSUITE_SIMPLE
#include "rxSuite.h"
#include "rxSuiteHelpers.h"
#include "sdsWrapper.h"
#include <pthread.h>

#include "adlist.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "client-pool.hpp"
#include "sha1.h"
#include "simpleQueue.hpp"

extern int start_cluster(RedisModuleCtx *, char *sha1);

pthread_t health_check_thread;

SimpleQueue *status_complete_queue = NULL;
SimpleQueue *status_update_queue = NULL;

int recover_instance(RedisModuleCtx *ctx, char *sha1, const char *)
{
    start_cluster(ctx, sha1);
    return C_OK;
}

extern void hincrbyCommand(client *c);
extern void hincrbyfloatCommand(client *c);
#define HEALTH_CHECKS_SUCCESS "HEALTH_CHECKS_SUCCESS"
#define HEALTH_CHECKS_FAILED "HEALTH_CHECKS_FAILED"
#define HEALTH_CHECKS_RESTART "HEALTH_RESTART"
#define HEALTH_CHECK_SHIFT "HEALTH_CHECK_SHIFT"
#define HEALTH_TALLY "HEALTH_TALLY"
#define HEALTH_AVG_LATENCY_0_us "HEALTH_AVG_LATENCY_0_us"
#define RXQUERY_cmd "RXQUERY"

void *RXQUERY = rxCreateStringObject(RXQUERY_cmd, strlen(RXQUERY_cmd));

#define UPDATE_FAILURE_STATE "RXQUERY \""                                                                                     \
                             "g:addv('%s',status)"                      \
                             ".property('HEALTH_CHECKS_FAILED','%lld')" \
                             ".property('HEALTH_CHECKS_RESTART','%s')"\
                             "\""

#define UPDATE_SUCCESS_STATE "RXQUERY \""                                                                                     \
                             "g:addv('%s',status)"                                     \
                             ".property(HEALTH_CHECKS_SUCCESS,'%lld')"                 \
                             ".property(HEALTH_TALLY,'%lld')"                          \
                             ".property(HEALTH_AVG_LATENCY_0_us,'%lf')"                 \
                             ".property(HEALTH_AVG_used_memory_0,'%lld')"          \
                             ".property(HEALTH_AVG_used_memory_peak_0,'%lld')"         \
                             ".property(HEALTH_AVG_maxmemory_0,'%lld')"                \
                             ".property(HEALTH_AVG_server_memory_available_0,'%lld')"  \
                             ".property(HEALTH_AVG_connected_clients_0,'%lld')"        \
                             ".property(HEALTH_AVG_cluster_connections_0,'%lld')"      \
                             ".property(HEALTH_AVG_blocked_clients_0,'%lld')"          \
                             ".property(HEALTH_AVG_tracking_clients_0,'%lld')"         \
                             ".property(HEALTH_AVG_clients_in_timeout_table_0,'%lld')" \
                             ".property(HEALTH_AVG_total_keys_0,'%lld')"               \
                             ".property(HEALTH_AVG_bytes_per_key_0,'%lld')"            \
                             "\""

#define UPDATE_SHIFT_STATS "RXQUERY \""                                                                                     \
                           "g:"                                                                              \
                           "v('%s'}"                                                                              \
                           ".WHERE{'%lld' - HEALTH_CHECK_SHIFT >= '900000'}"                                            \
                           ".property{HEALTH_CHECK_SHIFT='%lld'}"                                                     \
                           ".property{SHIFT_AT_1=SHIFT_AT_0}"                                                       \
                           ".property(SHIFT_AT_0,'%s')"                                                             \
                           ".property(HEALTH_TALLY,0)"                                                              \
                           ".property{HEALTH_AVG_LATENCY_4_us=(HEALTH_AVG_LATENCY_3_us)}"                             \
                           ".property{HEALTH_AVG_LATENCY_3_us=(HEALTH_AVG_LATENCY_2_us)}"                             \
                           ".property{HEALTH_AVG_LATENCY_2_us=(HEALTH_AVG_LATENCY_1_us)}"                             \
                           ".property{HEALTH_AVG_LATENCY_1_us=(HEALTH_AVG_LATENCY_0_us)}"                             \
                           ".property(HEALTH_AVG_LATENCY_0,0)"                                                      \
                           ".property{HEALTH_AVG_used_memory_4=(HEALTH_AVG_used_memory_3)}"                           \
                           ".property{HEALTH_AVG_used_memory_3=(HEALTH_AVG_used_memory_2)}"                           \
                           ".property{HEALTH_AVG_used_memory_2=(HEALTH_AVG_used_memory_1)}"                           \
                           ".property{HEALTH_AVG_used_memory_1=(HEALTH_AVG_used_memory_0)}"                           \
                           ".property(HEALTH_AVG_used_memory_0,0)"                                                  \
                           ".property{HEALTH_AVG_used_memory_peak_4=(HEALTH_AVG_used_memory_peak_3)}"                 \
                           ".property{HEALTH_AVG_used_memory_peak_3=(HEALTH_AVG_used_memory_peak_2)}"                 \
                           ".property{HEALTH_AVG_used_memory_peak_2=(HEALTH_AVG_used_memory_peak_1)}"                 \
                           ".property{HEALTH_AVG_used_memory_peak_1=(HEALTH_AVG_used_memory_peak_0)}"                 \
                           ".property(HEALTH_AVG_used_memory_peak_0,0)"                                             \
                           ".property(HEALTH_AVG_maxmemory_0,0)"                                                    \
                           ".property{'HEALTH_AVG_server_memory_available_4=('HEALTH_AVG_server_memory_available_3)}" \
                           ".property{'HEALTH_AVG_server_memory_available_3=('HEALTH_AVG_server_memory_available_2)}" \
                           ".property{'HEALTH_AVG_server_memory_available_2=('HEALTH_AVG_server_memory_available_1)}" \
                           ".property{'HEALTH_AVG_server_memory_available_1=('HEALTH_AVG_server_memory_available_0)}" \
                           ".property(HEALTH_AVG_server_memory_available_0,0)"                                      \
                           ".property{HEALTH_AVG_connected_clients_4=(HEALTH_AVG_connected_clients_3)}"               \
                           ".property{HEALTH_AVG_connected_clients_3=(HEALTH_AVG_connected_clients_2)}"               \
                           ".property{HEALTH_AVG_connected_clients_2=(HEALTH_AVG_connected_clients_1)}"               \
                           ".property{HEALTH_AVG_connected_clients_1=(HEALTH_AVG_connected_clients_0)}"               \
                           ".property(HEALTH_AVG_connected_clients_0,0)"                                            \
                           ".property{HEALTH_AVG_cluster_connections_4=(HEALTH_AVG_cluster_connections_3)}"           \
                           ".property{HEALTH_AVG_cluster_connections_3=(HEALTH_AVG_cluster_connections_2)}"           \
                           ".property{HEALTH_AVG_cluster_connections_2=(HEALTH_AVG_cluster_connections_1)}"           \
                           ".property{HEALTH_AVG_cluster_connections_1=(HEALTH_AVG_cluster_connections_0)}"           \
                           ".property(HEALTH_AVG_cluster_connections_0,0)"                                          \
                           ".property{HEALTH_AVG_blocked_clients_4=(HEALTH_AVG_blocked_clients_3)}"                   \
                           ".property{HEALTH_AVG_blocked_clients_3=(HEALTH_AVG_blocked_clients_2)}"                   \
                           ".property{HEALTH_AVG_blocked_clients_2=(HEALTH_AVG_blocked_clients_1)}"                   \
                           ".property{HEALTH_AVG_blocked_clients_1=(HEALTH_AVG_blocked_clients_0)}"                   \
                           ".property(HEALTH_AVG_blocked_clients_0,0)"                                              \
                           ".property{HEALTH_AVG_tracking_clients_4=(HEALTH_AVG_tracking_clients_3)}"                 \
                           ".property{HEALTH_AVG_tracking_clients_3=(HEALTH_AVG_tracking_clients_2)}"                 \
                           ".property{HEALTH_AVG_tracking_clients_2=(HEALTH_AVG_tracking_clients_1)}"                 \
                           ".property{HEALTH_AVG_tracking_clients_1=(HEALTH_AVG_tracking_clients_0)}"                 \
                           ".property(HEALTH_AVG_tracking_clients_0,0)"                                             \
                           ".property{HEALTH_AVG_clients_in_timeout_table_4=(HEALTH_AVG_clients_in_timeout_table_3)}" \
                           ".property{HEALTH_AVG_clients_in_timeout_table_3=(HEALTH_AVG_clients_in_timeout_table_2)}" \
                           ".property{HEALTH_AVG_clients_in_timeout_table_2=(HEALTH_AVG_clients_in_timeout_table_1)}" \
                           ".property{HEALTH_AVG_clients_in_timeout_table_1=(HEALTH_AVG_clients_in_timeout_table_0)}" \
                           ".property(HEALTH_AVG_clients_in_timeout_table_0,0)"                                     \
                           ".property{HEALTH_AVG_total_keys_4=(HEALTH_AVG_total_keys_3)}"                             \
                           ".property{HEALTH_AVG_total_keys_3=(HEALTH_AVG_total_keys_2)}"                             \
                           ".property{HEALTH_AVG_total_keys_2=(HEALTH_AVG_total_keys_1)}"                             \
                           ".property{HEALTH_AVG_total_keys_1=(HEALTH_AVG_total_keys_0)}"                             \
                           ".property(HEALTH_AVG_total_keys_0,0)"                                                   \
                           ".property{HEALTH_AVG_bytes_per_key_4=(HEALTH_AVG_bytes_per_key_3)}"                       \
                           ".property{HEALTH_AVG_bytes_per_key_3=(HEALTH_AVG_bytes_per_key_2)}"                       \
                           ".property{HEALTH_AVG_bytes_per_key_2=(HEALTH_AVG_bytes_per_key_1)}"                       \
                           ".property{HEALTH_AVG_bytes_per_key_1=(HEALTH_AVG_bytes_per_key_0)}"                       \
                           ".property(HEALTH_AVG_bytes_per_key_0,0)"                       \
                           "\""

size_t CalcAverage(void *o, const char *field, long long tally, long long value)
{
    rxString old_avg = rxGetHashField(o, field);
    long long avg = old_avg ? atof(old_avg) : 0.0;

    return (avg * tally + value) / (tally + 1);
}

char *GetCurrentDateTimeAsString(char *buffer)
{
    struct timeval tv;
    time_t curtime;

    gettimeofday(&tv, NULL);
    curtime = tv.tv_sec;

    strftime(buffer, 30, "%m-%d-%Y  %T.", localtime(&curtime));
    return buffer;
}

void PostCommand(const char *cmdP)
{
    // status_update_queue->Enqueue((void *)cmd);
    // printf("%s\n", cmd);
        ExecuteLocal(cmdP, LOCAL_NO_RESPONSE);
}

timeval oneminute = {60,0};
int queryInstance(RedisModuleCtx *ctx, redisContext *redis_node, char *sha1, const char *address, char *instance_key, void *)
{
    int instance_state;
    rxString sKey = rxStringFormat("%s__HEALTH", instance_key);
    auto probe_start = ustime();
    auto shift_time = mstime();

    char buffer[30];

    // Shift statistics every 15 minutes
    redisReply *rcc = NULL;
    if (redis_node)
    {
        rxString cmd = rxStringFormat(UPDATE_SHIFT_STATS, sKey, shift_time, shift_time, GetCurrentDateTimeAsString(buffer));
        // printf("%s\n", cmd);
        // status_update_queue->Enqueue((void *)cmd);
        ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);
        #if REDIS_VERSION_NUM >= 0x00070000
            auto timeout_saved = redis_node->command_timeout;
            redis_node->command_timeout = &oneminute;
        #endif
        rcc = (redisReply *)redisCommand(redis_node, "mercator.instance.status");
        #if REDIS_VERSION_NUM >= 0x00070000
            redis_node->command_timeout = timeout_saved;
        #endif
    }
    auto probe_stop = ustime();
    auto probe_latency = probe_stop - probe_start;
    double avg_probe_latency = 0.0;
    void *o = rxFindHashKey(0, sKey);
    rxClientInfo info = {};

    if (rcc == NULL)
    {
        if(redis_node && redis_node->err != 0)
            rxServerLog(rxLL_NOTICE, "For %s [%s|%s] error: %s", address, sha1, instance_key, redis_node->errstr);
        recover_instance(ctx, sha1, address);
        auto failure_tally = rxGetHashFieldAsLong(o, HEALTH_CHECKS_FAILED);
        PostCommand(rxStringFormat(UPDATE_FAILURE_STATE, sKey, 1 + failure_tally, GetCurrentDateTimeAsString(buffer)));
        instance_state = C_ERR;
    }
    else
    {
        instance_state = C_OK;
        auto success_tally = rxGetHashFieldAsLong(o, HEALTH_CHECKS_SUCCESS);
        auto latency_avg = rxGetHashFieldAsDouble(o, HEALTH_AVG_LATENCY_0_us);
        auto latency_tally = rxGetHashFieldAsLong(o, HEALTH_TALLY);
        avg_probe_latency = (latency_tally * latency_avg + probe_latency) / (latency_tally + 1);

        /*
        127.0.0.1:6569> mercator.instance.status
         1) used_memory
         2) (integer) 1165432
         3) used_memory_peak
         4) (integer) 1344664
         5) maxmemory
         6) (integer) 0
         7) server_memory_available
         8) (integer) 1165432
         9) connected_clients
        10) (integer) 10
        11) cluster_connections
        12) (integer) 0
        13) blocked_clients
        14) (integer) 0
        15) tracking_clients
        16) (integer) 0
        17) clients_in_timeout_table
        18) (integer) 0
        19) total_keys
        20) (integer) 0
        21) bytes_per_key
        22) (integer) 0
        */
        if (rcc->type == REDIS_REPLY_ARRAY && rcc->elements > 0)
        {

            info.used_memory = CalcAverage(o, "HEALTH_AVG_used_memory_0", latency_tally, rcc->element[1]->integer);
            info.used_memory_peak = CalcAverage(o, "HEALTH_AVG_used_memory_peak_0", latency_tally, rcc->element[3]->integer);
            info.maxmemory = CalcAverage(o, "HEALTH_AVG_maxmemory_0", latency_tally, rcc->element[5]->integer);
            info.server_memory_available = CalcAverage(o, "HEALTH_AVG_server_memory_available_0", latency_tally, rcc->element[7]->integer);

            info.connected_clients = CalcAverage(o, "HEALTH_AVG_connected_clients_0", latency_tally, rcc->element[9]->integer);
            info.cluster_connections = CalcAverage(o, "HEALTH_AVG_cluster_connections_0", latency_tally, rcc->element[11]->integer);
            info.blocked_clients = CalcAverage(o, "HEALTH_AVG_blocked_clients_0", latency_tally, rcc->element[13]->integer);
            info.tracking_clients = CalcAverage(o, "HEALTH_AVG_tracking_clients_0", latency_tally, rcc->element[15]->integer);
            info.clients_in_timeout_table = CalcAverage(o, "HEALTH_AVG_clients_in_timeout_table_0", latency_tally, rcc->element[17]->integer);
            info.total_keys = CalcAverage(o, "HEALTH_AVG_total_keys_0", latency_tally, rcc->element[19]->integer);
            info.bytes_per_key = CalcAverage(o, "HEALTH_AVG_bytes_per_key_0", latency_tally, rcc->element[21]->integer);

            rxString cmd = rxStringFormat(UPDATE_SUCCESS_STATE,
                                          sKey,
                                          success_tally + 1,
                                          latency_tally + 1,
                                          avg_probe_latency,
                                          info.used_memory,
                                          info.used_memory_peak,
                                          info.maxmemory,
                                          info.server_memory_available,
                                          info.connected_clients,
                                          info.cluster_connections,
                                          info.blocked_clients,
                                          info.tracking_clients,
                                          info.clients_in_timeout_table,
                                          info.total_keys,
                                          info.bytes_per_key);
            PostCommand(cmd);

            freeReplyObject(rcc);
        }
    }
    rxStringFree(sKey);
    return instance_state;
}

#define UPDATE_CLUSTER_STATE "RXQUERY \""                                 \
                             "g:addv('%s__HEALTH','cluster_status'}" \
                             ".property(INSTANCES_OK_0,'%lld')"     \
                             ".property(INSTANCES_ERR_0,'%lld')"    \
                             "\""

#define UPDATE_SHIFT_CLUSTER_STATE "RXQUERY \""                                 \
                                   "g:v('%s__HEALTH'}"                          \
                                   ".WHERE{'%lld' - CHECK_SHIFT >= 900000}"     \
                                   ".property(CHECK_SHIFT,'%lld')"              \
                                   ".property{SHIFT_AT_1=SHIFT_AT_0}"           \
                                   ".property(SHIFT_AT_0,'%s')"                 \
                                   ".property{INSTANCES_OK_4=INSTANCES_OK_3}"   \
                                   ".property{INSTANCES_OK_3=INSTANCES_OK_2}"   \
                                   ".property{INSTANCES_OK_2=INSTANCES_OK_1}"   \
                                   ".property{INSTANCES_OK_1=INSTANCES_OK_0}"   \
                                   ".property(INSTANCES_OK_0,0)"                \
                                   ".property{INSTANCES_ERR_4=INSTANCES_ERR_3}" \
                                   ".property{INSTANCES_ERR_3=INSTANCES_ERR_2}" \
                                   ".property{INSTANCES_ERR_2=INSTANCES_ERR_1}" \
                                   ".property{INSTANCES_ERR_1=INSTANCES_ERR_0}" \
                                   ".property(INSTANCES_ERR_0,0)"               \
                                   "\""

int clusterState(RedisModuleCtx *, redisContext *, char *sha1, const char *, char *, void *oCounters)
{
    auto *counters = (int *)oCounters;

    auto probe_start = mstime();
    char buffer[30];
    PostCommand(rxStringFormat(UPDATE_SHIFT_CLUSTER_STATE, sha1, probe_start, probe_start, GetCurrentDateTimeAsString(buffer)));
    PostCommand(rxStringFormat(UPDATE_CLUSTER_STATE, sha1, counters[0], counters[1]));
    return C_OK;
}

int allClustersOperation(SimpleQueue *status_update_queue)
{
    RedisModuleCtx *ctx = NULL;

    redisReply *clusters = ExecuteLocal("rxquery g:v(cluster)", LOCAL_STANDARD);
    if(clusters && clusters->type == REDIS_REPLY_ARRAY){
        size_t n = 0;
        while( n < clusters->elements){
            auto *clusterId =  clusters->element[n]->element[1]->str;
            void *params[] = {(void *)clusterId, (void *)status_update_queue};
            clusterOperation(ctx, clusterId, NULL, (clusterOperationProc *)queryInstance, params, (clusterOperationProc *)clusterState);

            ++n;
        }
    }
    freeReplyObject(clusters);

//     rxString cluster_key = "__MERCATOR__CLUSTERS__";
//     void *nodes = rxFindSetKey(0, cluster_key);
//     if (!nodes)
//         return 0;

//     void *si = NULL;
//     rxString node;
//     int64_t l;
//     while (rxScanSetMembers(nodes, &si, (char **)&node, &l) != NULL)
//     {
//         const char *clusterId =  strrchr(node, '_') + 1;
//         void *params[] = {(void *)node, (void *)status_update_queue};
//         clusterOperation(ctx, clusterId, NULL, (clusterOperationProc *)queryInstance, params, (clusterOperationProc *)clusterState);
//         #if REDIS_VERSION_NUM < 0x00070200
//         rxStringFree(node);
// #endif
//     }

    auto *ready_queue = (SimpleQueue *)status_update_queue->response_queue;
    void *update_request = ready_queue->Dequeue();
    while (update_request != NULL)
    {
        rxStringFree((const char *)update_request);
        update_request = ready_queue->Dequeue();
    }

    return C_OK;
}
// int query_requests_interval = 100;
// long long query_requests_cron = -1;
int must_stop = 0;

/* Execute rxquery commands on the main thread */
// int execute_queries_timer_handler(struct aeEventLoop *, long long int, void *)
// {
//     if (must_stop == 1)
//         return -1;

//     void *update_request = status_update_queue->Dequeue();
//     while (update_request != NULL)
//     {
//         char controller[24];
//         snprintf(controller, sizeof(controller),  "0.0.0.0:%d", rxGetServerPort());
//         auto *local_node = RedisClientPool<struct client>::Acquire(controller, "_MONITOR", "ClusterStatistics");
//         auto *cmd = rxStringNew("RXQUERY");
//         cmd = rxStringAppend(cmd, update_request, ' ');
//         ExecuteLocal(cmd, LOCAL_FREE_CMD | LOCAL_NO_RESPONSE);
//         RedisClientPool<struct client>::Release(local_node, "ClusterStatistics");
//         status_complete_queue->Enqueue(update_request);
//         update_request = status_update_queue->Dequeue();
//     }

//     return query_requests_interval;
// }
typedef void (*tThreadHandler)(aeEventLoop *, void *);
static void *execClusterInstanceHealthCheckThread(void *)
{
    // auto *status_update_queue = (SimpleQueue *)ptr;
    // auto *status_complete_queue = status_update_queue->response_queue;

    // query_requests_interval = 100;
    // query_requests_cron = rxCreateTimeEvent(1, &execute_queries_timer_handler, NULL, (tThreadHandler)status_complete_queue);

    rxServerLog(rxLL_NOTICE, "InstanceHealthCheck started");

    long long last_stop = ustime();

    while (true)
    {
        // Query all clusters, at least every 5 seconds
        if (ustime() - last_stop >= 15000000)
        {
            allClustersOperation(status_update_queue);
            last_stop = ustime();
        }
        else
        {
            sched_yield();
        }
    }

    rxServerLog(rxLL_NOTICE, "InstanceHealthCheck stopped");
    return NULL;
}

static void *execSelfHealthCheckThread(void *)
{
    rxServerLog(rxLL_NOTICE, "SelfHealthCheck started");
    rxServerLog(rxLL_NOTICE, "SelfHealthCheck stopped");
    return NULL;
}

int startMonitor(void *handler, const char *)
{
    status_complete_queue = new SimpleQueue("HMDONE");
    status_update_queue = new SimpleQueue("HMSTORE", (void *)handler, 1, status_complete_queue);

    return C_OK;
}

int startClientMonitor()
{
    rxServerLog(rxLL_NOTICE, "SelfHealthCheck starting");
    return startMonitor((void *)execSelfHealthCheckThread, "SHEALTHCHK");
}

int startInstanceMonitor()
{
    rxServerLog(rxLL_NOTICE, "InstanceHealthCheck starting");
    return startMonitor((void *)execClusterInstanceHealthCheckThread, "CHEALTHCHK");
}

int stopInstanceMonitor()
{
    rxServerLog(rxLL_NOTICE, "InstanceHealthCheck stopping");
    must_stop = 1;
    return must_stop;
}
