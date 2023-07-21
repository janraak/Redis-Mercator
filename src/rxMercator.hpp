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

typedef int clusterOperationProc(RedisModuleCtx *ctx, redisContext *redis_node, char *sha1, const char *address, const char *instance_key, void *data);

extern int clusterOperation(RedisModuleCtx *ctx, char *sha1, const char *state, clusterOperationProc *operation, void *data, clusterOperationProc *summary_operation);


extern const char *CLUSTERING_ARG;
extern const char *REPLICATION_ARG;
extern const char *CONTROLLER_ARG;
extern const char *START_ARG;

#endif