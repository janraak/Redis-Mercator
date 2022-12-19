
#ifndef __RXSUITE_H__
#define __RXSUITE_H__

#ifndef C_OK
    #define C_OK 0
    #define C_ERR -1
#endif

#ifdef __cplusplus
extern "C" {
#endif
#include <sched.h>
#include <signal.h>
#include <ctype.h>



// #define REDISMODULE_EXPERIMENTAL_API
// #include "../../src/redismodule.h"
// #include "../../src/dict.h"
#include "../../deps/hiredis/hiredis.h"
#include "sdsWrapper.h"

#define rxUNUSED(x) (void)(x)

#define POINTER unsigned int
#define UCHAR unsigned char

typedef void CSimpleQueue;

// #ifndef RXSUITE_SIMPLE
typedef struct{
    const char * host_reference;
    const char * host;
    int port;
    int database_id;
    int is_local;
    void *executor;
} redisNodeInfo;
typedef struct
{
    // dict *OperationMap;
    // dict *KeysetCache;
    int parserClaimCount;
    redisNodeInfo indexNode;
    redisNodeInfo dataNode;
    redisNodeInfo controllerNode;
    const char *defaultQueryOperator;
    const char *cdnRootUrl;
    const char *startScript;
    const char *installScript;
    CSimpleQueue *cron_command_request_queue;
} rxSuiteShared;

void *initRxSuite();
rxSuiteShared *getRxSuite();
redisNodeInfo *rxIndexNode();
redisNodeInfo *rxDataNode();
redisNodeInfo *rxControllerNode();
void rxRegisterConfig(void **argv, int argc);
void rxRegisterCronCommandQueue(CSimpleQueue *queue);
CSimpleQueue *rxGetCronCommandQueue();

char *rxGetExecutable();
void finalizeRxSuite();
// #endif


#ifdef __cplusplus
}
#endif

#endif