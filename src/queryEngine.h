

#ifndef __RXQ_ENGINE_H__
#define __RXQ_ENGINE_H__
#include "rxSuite.h"

#include "parser.h"

#ifndef KeysetDescriptor_TYPE
#define KeysetDescriptor_TYPE
#define KeysetDescriptor_TYPE_UNKNOWN 0
#define KeysetDescriptor_TYPE_KEYSET 0x01
#define KeysetDescriptor_TYPE_GREMLINSET 0x02
#define KeysetDescriptor_TYPE_SINGULAR 0x04
#define KeysetDescriptor_TYPE_PARAMETER_LIST 0x08
#define KeysetDescriptor_TYPE_MONITORED_SET 0x10
#define KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET 0x20
#define KeysetDescriptor_TYPE_GREMLIN_EDGE_SET 0x40
#define KeysetDescriptor_TYPE_GREMLIN_AS_SET 0x80

#define GENERIC_SET 0
#define VERTEX_SET 1
#define EDGE_SET 2

#endif

#define QE_LOAD_NONE 0
#define QE_LOAD_LEFT_ONLY 1
#define QE_LOAD_RIGHT_ONLY 2
#define QE_LOAD_LEFT_AND_RIGHT (QE_LOAD_LEFT_ONLY | QE_LOAD_RIGHT_ONLY)
#define QE_CREATE_SET 4
#define QE_SWAP_SMALLEST_FIRST 8
#define QE_SWAP_LARGEST_FIRST 16

typedef struct KeysetDescriptor
{
    long long creationTime;
    long long start;
    long long latency;
    sds setname;
    size_t reuse_count;
    size_t size;
    dict *keyset;
    list *parameter_list;
    unsigned char is_temp;
    unsigned char is_on_stack;
    unsigned char value_type;
    unsigned char vertices_or_edges;
    int dirty;
    struct KeysetDescriptor *left;
    struct KeysetDescriptor *right;
} KeysetDescriptor;

KeysetDescriptor *findKeysetDescriptor(sds sn);
KeysetDescriptor *getKeysetDescriptor(sds sn, char value_type);

int connectIndex(Parser *p, RedisModuleCtx *ctx, char *h, int port);
void selectDatabase(sds db_id);
long long setDirty();

dict *executeQuery(Parser *p);
dict *executeQueryForKey(Parser *p, const char *key);
int simulateQuery(Parser *p, int start_depth);
void optimizeQuery(Parser *p);
sds rpnToString(Parser *p);

void emitResults(RedisModuleCtx *ctx, int fetch_rows, dict *keyset);
sds getCacheReport();
void releaseCacheReport( sds);
void clearCache();
void deleteAllTempKeysetDescriptors();
void deleteAllKeysetDescriptors();

sds get_uml();

struct QueryContext;

typedef void pushImpl(list *stack, KeysetDescriptor *d);
typedef     KeysetDescriptor *popImpl(list *stack);
typedef     KeysetDescriptor *dequeueImpl(list *stack);

typedef     void pushXImpl(struct QueryContext *ctx, KeysetDescriptor *d);
typedef     KeysetDescriptor *popXImpl(struct QueryContext *ctx);
typedef     KeysetDescriptor *dequeueXImpl(struct QueryContext *ctx);


typedef int expressionHandler(Parser *p, Token *t, list *stack);
typedef int expressionHandlerX(struct QueryContext *ctx, Token*t);

// typedef struct {
//     const char *operator;
//     int priority;
//     expressionHandler *handlerV1;
//     expressionHandlerX *handlerV2;
// } FunctionDefinition;

// typedef int RegisterImpl(struct QueryContext *ctx, FunctionDefinition *f);

typedef struct QueryContext 
{
    Parser *parser;
    list *stack;
    
    pushImpl *push;
    popImpl *pop;
    dequeueImpl *dequeue;

    pushXImpl *pushX;
    popXImpl *popX;
    dequeueXImpl *dequeueX;

    // RegisterImpl *register;
    // RegisterImpl *deregister;

} QueryContext;

struct robj;
typedef struct{
    list *path;
    sds object_key;
    struct robj *object;
} Graph_Triplet_Edge;
typedef struct
{
    sds subject_key;
    struct robj *subject;
    list *edges;
} Graph_Triplet;

void deleteTripletResult(Graph_Triplet *triplet);

#define OBJ_TRIPLET 15 /* Hash object. */

#endif