#ifndef __RXMERCATOR_H__
#define __RXMERCATOR_H__

#define REDISMODULE_EXPERIMENTAL_API

#define REDISMODULE_EXPERIMENTAL_API
#ifdef __cplusplus
extern "C"
{
#endif
#include "redismodule.h"
#include <string.h>
#include <stdlib.h>

#include "../../src/dict.h"
#define RXSUITE_SIMPLE
#include "rxSuite.h"

#define REDISMODULE_EXPERIMENTAL_API
#ifdef __cplusplus
}
#endif
#include "rxSessionMemory.hpp"
extern rxString getSha1(const char *codes, ...);

typedef int clusterOperationProc(RedisModuleCtx *ctx, redisContext *redis_node, const char *sha1, const char *address, const char *instance_key, void *data);

int clusterOperation(RedisModuleCtx *ctx, const char *sha1, const char *state, clusterOperationProc *operation, void *data, clusterOperationProc *summary_operation);


extern const char *CLUSTERING_ARG;
extern const char *REPLICATION_ARG;
extern const char *CONTROLLER_ARG;
extern const char *START_ARG;


#define RXQUERY_cmd "RXQUERY"

extern void *RXQUERY;

#define UPDATE_CLUSTER__STATE "RXQUERY \"g:v('%s')"                    \
                              ".property('STATUS','%s')" \
                              ".property('LAST_%s','%s')\""

#define UPDATE_CLUSTER__STATE_CREATED "RXQUERY \"g:v('%s')"                    \
                              ".property('STATUS','CREATED')" \
                              ".property('LAST_CREATED','%s')\""

#define UPDATE_CLUSTER__STATE_STARTED  "RXQUERY \"g:v('%s')"                    \
                              ".property('STATUS','STARTED')" \
                              ".property('LAST_STARTED','%s')\""

#define UPDATE_CLUSTER__STATE_STOPPED  "RXQUERY \"g:v('%s')"                    \
                              ".property('STATUS','STOPPED')" \
                              ".property('LAST_STOPPED','%s')\""

#define UPDATE_INSTANCE_START "RXQUERY \"g:addv('%s__HEALTH',status)"                    \
                              ".property{INSTANCE_START_4=INSTANCE_START_3}" \
                              ".property{INSTANCE_START_3=INSTANCE_START_2}" \
                              ".property{INSTANCE_START_2=INSTANCE_START_1}" \
                              ".property{INSTANCE_START_1=INSTANCE_START_0}" \
                              ".property('INSTANCE_START_0','%s')\""

#define LOCAL_STANDARD 0
#define LOCAL_FREE_CMD 1
#define LOCAL_NO_RESPONSE 2

redisReply *ExecuteLocal(const char *cmd, int options);
#endif