
#ifndef __RXSUITE_H__
#define __RXSUITE_H__

#ifndef C_OK
    #define C_OK 0
    #define C_ERR -1
#endif

#define REDISMODULE_EXPERIMENTAL_API
#ifdef __cplusplus
extern "C" {
#endif
#include "/usr/include/arm-linux-gnueabihf/bits/types/siginfo_t.h"
#include <sched.h>
#include <signal.h>
#include <ctype.h>



#include "../../src/redismodule.h"
#include "../../src/dict.h"
#include "../../deps/hiredis/hiredis.h"

#define rxUNUSED(x) (void)(x)

#define POINTER unsigned int
#define UCHAR unsigned char

// #ifndef RXSUITE_SIMPLE
typedef struct{
    sds host_reference;
    sds host;
    int port;
    int database_id;
    int is_local;
    void *executor;
} redisNodeInfo;
typedef struct
{
    dict *OperationMap;
    dict *KeysetCache;
    int parserClaimCount;
    dictType *tokenDictType;
    redisNodeInfo indexNode;
    redisNodeInfo dataNode;
    sds defaultQueryOperator;
} rxSuiteShared;

void initRxSuite();
rxSuiteShared *getRxSuite();
redisNodeInfo *rxIndexNode();
redisNodeInfo *rxDataNode();
void rxRegisterConfig(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

void finalizeRxSuite();
// #endif

int sdscharcount(char *s, char c);

/* Apply tolower() to every character of the string 's'. */
void strtolower(const char *s);
/* Apply toupper() to every character of the string 's'. */
void strtoupper(const char *s);

#ifdef __cplusplus
}
#endif

#endif