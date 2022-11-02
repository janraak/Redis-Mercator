#define REDISMODULE_EXPERIMENTAL_API
#include "redismodule.h"

#include "/usr/include/arm-linux-gnueabihf/bits/types/siginfo_t.h"
#include <sched.h>
#include <signal.h>

#include "adlist.h"
#include "../../src/dict.h"
#include "server.h"
#include "util.h"

#include "../../deps/hiredis/hiredis.h"

#include "memory.h"
#include "parser.h"
#include "queryEngine.h"
#include "token.h"
#include <ctype.h>

#define MATCH_PATTERN 1
#define NO_MATCH_PATTERN 0

#define POINTER unsigned int
#define AS_POINTER(p) ((POINTER)p)

#define REDIS_DB0 (&server.db[0])
extern void *dictSdsDup(void *privdata, const void *key);
dict *dictCopy(dict *d);

void dictDeleteDescriptor(void *privdata, void *val);
void deleteAllKeysetDescriptors();
/* Db->dict, keys are sds strings, vals are Redis objects. */
dictType resultSetDescriptorDictType = {
    dictSdsHash,         /* hash function */
    dictSdsDup,          /* key dup */
    NULL,                /* val dup */
    dictSdsKeyCompare,   /* key compare */
    dictSdsDestructor,   /* key destructor */
    dictDeleteDescriptor /* val destructor */
};

rxSuiteShared *rxShared = NULL;

extern sds hashTypeGetFromHashTable(robj *o, sds field);
extern int hashTypeGetValue(robj *o, sds field, unsigned char **vstr, POINTER *vlen, long long *vll);

#define Graph_vertex_or_edge_type sdsnew("type")

#define Graph_Subject_to_Edge sdsnew("SE")
#define Graph_Object_to_Edge sdsnew("OE")
#define Graph_Edge_to_Subject sdsnew("ES")
#define Graph_Edge_to_Object sdsnew("EO")

#define GRAPH_TRAVERSE_OUT 1
#define GRAPH_TRAVERSE_IN 2
#define GRAPH_TRAVERSE_INOUT 3

KeysetDescriptor *propagate_set_type(KeysetDescriptor *t, KeysetDescriptor *l, KeysetDescriptor *r);

void initOperationsStatic();
dict *pushIndexEntries(redisReply *reply, KeysetDescriptor *kd);
int fetchKeyset(redisContext *index_context, KeysetDescriptor *kd, sds l, sds r, sds cmp);
void deleteAllTempKeysetDescriptors();

#define QE_LOAD_NONE 0
#define QE_LOAD_LEFT_ONLY 1
#define QE_LOAD_RIGHT_ONLY 2
#define QE_LOAD_LEFT_AND_RIGHT (QE_LOAD_LEFT_ONLY | QE_LOAD_RIGHT_ONLY)
#define QE_CREATE_SET 4
#define QE_SWAP_SMALLEST_FIRST 8
#define QE_SWAP_LARGEST_FIRST 16

KeysetDescriptor *getKeysetDescriptor2(Parser *p, Token *t, list *stack, int load_left_and_or_right);
void putKeysetDescriptor(list *stack, KeysetDescriptor *kd, dictIterator *iter);

int executeGremlinNoop(Parser *p, Token *t, list *stack)
{
    rxUNUSED(p);
    rxUNUSED(t);
    rxUNUSED(stack);
    return C_OK;
}

typedef struct indexerThread
{
    sds index_address;
    int index_port;
    int dirty;
    sds database_id;
    redisContext *index_context;
} IndexerInfo;

IndexerInfo index_info_qe = {0};

void push(list *stack, KeysetDescriptor *d);
KeysetDescriptor *pop(list *stack);
KeysetDescriptor *dequeue(list *stack);
void *deleteKeysetDescriptor(KeysetDescriptor *kd);

void defaultKeysetDescriptorAsTemp(KeysetDescriptor *kd)
{
    switch (kd->value_type)
    {
    case KeysetDescriptor_TYPE_UNKNOWN:
        break;
    case KeysetDescriptor_TYPE_KEYSET:
        break;
    case KeysetDescriptor_TYPE_GREMLINSET:
        break;
    case KeysetDescriptor_TYPE_SINGULAR:
        kd->is_temp = 1;
        break;
    case KeysetDescriptor_TYPE_PARAMETER_LIST:
        kd->is_temp = 1;
        break;
    case KeysetDescriptor_TYPE_MONITORED_SET:
        break;
    }
}
typedef struct graph_leg
{
    sds key;
    float length;
    struct graph_leg *start;
    struct graph_leg *origin;
    dictEntry *de;
} graph_leg;

Graph_Triplet *newTripletResult();
Graph_Triplet *pop_Graph_Triplet(list *stack);

void push_leg(list *traversal, graph_leg *d);
graph_leg *pop_leg(list *traversal);
sds sdspop(list *stack);
typedef int operationProc(Parser *p, Token *t, list *stack);
typedef void redisCommandProc(client *c);

void addError(Parser *p, const char *error)
{
    sds e = sdsdup(sdsnew(error));
    listAddNodeTail(p->errors, e);
}

int redis_HSET(Parser *p, const char *key, const char *field, const char *value)
{
    RedisModuleCallReply *reply = RedisModule_Call(
        p->module_contex,
        "HSET",
        "ccc",
        key,
        field,
        value);
    if (reply)
        RedisModule_FreeCallReply(reply);
    return C_OK;
}

int redis_SADD(Parser *p, const char *key, const char *member)
{
    RedisModuleCallReply *reply = RedisModule_Call(
        p->module_contex,
        "SADD",
        "cc",
        key,
        member);
    if (reply)
        RedisModule_FreeCallReply(reply);
    return C_OK;
}

extern int hashTypeGetFromZiplist(robj *o, sds field,
                                  unsigned char **vstr,
                                  unsigned int *vlen,
                                  long long *vll);

static sds getHashField(robj *o, sds field)
{
    int ret;

    if (o == NULL)
    {
        return sdsempty();
    }

    if (o->encoding == OBJ_ENCODING_ZIPLIST)
    {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        ret = hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll);
        if (ret >= 0)
        {
            if (vstr)
            {
                return sdsnewlen(vstr, vlen);
            }
            else
            {
                return sdscatfmt(sdsempty(), "%i", vll);
            }
        }
    }
    else if (o->encoding == OBJ_ENCODING_HT)
    {
        sds value = hashTypeGetFromHashTable(o, field);
        if (value != NULL)
            return sdsnewlen(value, sdslen(value));
    }
    else
    {
        serverPanic("Unknown hash encoding");
    }
    return sdsempty();
}

void clearStack(list *stack)
{
    rxUNUSED(stack);
    deleteAllTempKeysetDescriptors();
    // while (listLength(stack) > 0)
    // {
    //     KeysetDescriptor *e = pop(stack);
    //     if (e->is_temp == KeysetDescriptor_TYPE_MONITORED_SET || e->value_type == KeysetDescriptor_TYPE_MONITORED_SET)
    //     {
    //         // ///**/ serverlog(LL_NOTICE, "Retained(2) 0x%x %s temp:%d type=%d d:0x%x", (POINTER)e, e->setname, e->is_temp, e->value_type, (POINTER)e->keyset);
    //         continue;
    //     }
    //     deleteKeysetDescriptor(e);
    // }
}

void dumpStack(list *stack)
{
    return;
    for (int n = listLength(stack); n >= 0; n--)
    {
        KeysetDescriptor *e = (KeysetDescriptor *)getTokenAt(stack, n);
        if(e)
            serverLog(LL_NOTICE, "%2d temp=%d type=%d size=%d %s", n, e->is_temp, e->value_type, e->size, e->setname);
    }
}

void optimizeQuery(Parser *p)
{
    list *o = listCreate();
    while (listLength(p->rpn) > 0)
    {
        Token *t = removeTokenAt(p->rpn, 0);

        dictEntry *ode = dictFind(getRxSuite()->OperationMap, (void *)t->token);
        if (ode != NULL)
        {
            operationProc *op = (operationProc *)ode->v.val;
            if (op == &executeGremlinNoop)
                continue;
        }
        listAddNodeTail(o, t);
    }
    listRelease(p->rpn);
    p->rpn = o;
}

int simulateQuery(Parser *p, int start_depth)
{
    int steps = p->rpn->len;
    int step = 0;
    int stack_depth = start_depth;
    while (step < steps)
    {
        Token *t = getTokenAt(p->rpn, step);
        switch (t->token_type)
        {
        case _operand:
        case _literal:
            stack_depth++;
            break;
        case _operator:
            stack_depth--;
            stack_depth--;
            stack_depth++;
            break;
        default:
            // ///**/ serverlog(LL_NOTICE, "Invalid operand/operation %s\n", t->token);
            break;
        }
        ++step;
    }
    return stack_depth;
}

sds rpnToString(Parser *p)
{
    int steps = p->rpn->len;
    int step = 0;
    sds result = sdsempty();
    while (step < steps)
    {
        Token *t = getTokenAt(p->rpn, step);
        result = sdscatprintf(result, " %s", t->token);
        ++step;
    }
    return result;
}

robj *findKey(redisDb *db, sds key);
void load_key(redisDb *db, sds key, KeysetDescriptor *kd)
{
    robj *zobj = findKey(db, key);
    if (zobj == NULL)
        return;
    if (zobj->type == OBJ_HASH)
    {
        if (!kd->keyset)
            kd->keyset = dictCreate(rxShared->tokenDictType, (void *)NULL);
        dictAdd(kd->keyset, (void *)key, zobj);
        kd->size = dictSize(kd->keyset);
    }
}

dict *executeQueryForKey(Parser *p, const char *key)
{
    if (rxShared == NULL || rxShared->OperationMap == NULL)
        initOperationsStatic();
    optimizeQuery(p);
    int steps = p->rpn->len;
    int step = 0;
    dictEntry *o;
    list *stack = listCreate();
    if (key)
    {
        KeysetDescriptor *v = getKeysetDescriptor((char *)key, KeysetDescriptor_TYPE_KEYSET);
        defaultKeysetDescriptorAsTemp(v);
        load_key(REDIS_DB0, v->setname, v);
        push(stack, v);
    }
    while (step < steps)
    {
        dumpStack(stack);
        Token *t = getTokenAt(p->rpn, step);
        switch (t->token_type)
        {
        case _operand:
            push(stack, getKeysetDescriptor((char *)t->token, KeysetDescriptor_TYPE_SINGULAR));
            break;
        case _literal:
            push(stack, getKeysetDescriptor((char *)t->token, KeysetDescriptor_TYPE_SINGULAR));
            break;
        case _operator:
            o = dictFind(rxShared->OperationMap, (void *)t->token);
            if (o)
            {
                operationProc *op = (operationProc *)o->v.val;
                if (op(p, t, stack) == C_ERR)
                {
                    step = steps;
                    break;
                }
            }
            else
            {
                char error[256];
                snprintf(error, sizeof(error), "Invalid operation %s", t->token);
                serverLog(LL_NOTICE, error);
                addError(p, error);
            }
            break;
        default:
            break;
        }
        ++step;
    }
        dumpStack(stack);
    sds error;
    // serverLog(LL_NOTICE, "\n\n%s\n\n", getCacheReport());
    if (listLength(p->errors) > 0)
    {
        clearStack(stack);
    }
    else
    {
        Token *t = NULL;
        KeysetDescriptor *r = NULL;
        switch (listLength(stack))
        {
        case 0:
            t = getTokenAt(p->rpn, p->rpn->len - 1);
            if (sdscmp((const sds)t->token, "=") != 0)
            {
                addError(p, "Invalid expression, no result yielded!");
            }
            break;
        case 1:
            r = pop(stack);
            if (!r->keyset)
            {
                fetchKeyset(p->index_context, r, NULL, r->setname, NULL);
                push(stack, r);
            }
            p->final_result_value_type = r->value_type;
            // p->final_result_set_iterator = dictGetSafeIterator(r->keyset);
            dict *k = dictCopy(r->keyset);
            // ///**/ serverlog(LL_NOTICE, "Result 0x%x", (POINTER)k);
            return k;
        default:
            error = sdsnew("Invalid expression, ");
            error = sdscatfmt(error, "%i  results yielded!", stack->len);
            addError(p, error);
            serverLog(LL_NOTICE, error);
            dumpStack(stack);
            clearStack(stack);
        }
    }
    deleteAllTempKeysetDescriptors();
    return dictCreate(rxShared->tokenDictType, (void *)NULL);
}

dict *executeQuery(Parser *p)
{
    return executeQueryForKey(p, NULL);
}

// sds getNextKey(Parser *p)
// {
//     if (!p->final_result_set_iterator)
//         return NULL;

//     dictEntry *match;
//     if ((match = dictNext(p->final_result_set_iterator)))
//     {
//         if (p->final_result_value_type == KeysetDescriptor_TYPE_KEYSET)
//         {
//             p->index_entry = (char *)(char *)"1.0\tH";
//             return sdsnewlen((char *)match->key, strlen((char *)match->key));
//         }
//         else
//         {
//             p->index_entry = (char *)(char *)match->v.val;
//             return sdsnewlen((char *)match->key, strlen((char *)match->key));
//         }
//     }
//     dictReleaseIterator(p->final_result_set_iterator);
//     p->final_result_set_iterator = NULL;
//     return NULL;
// }

const char *getKeyValue(Parser *p)
{
    return p->index_entry;
}

void freeKey(sds key)
{
    if (key)
        sdsfree(key);
}

void emitResults(RedisModuleCtx *ctx, int fetch_rows, dict *keyset)
{
    size_t items = 0;
    size_t keys = dictSize(keyset);
    RedisModule_ReplyWithArray(ctx, keys);
    if (keyset)
    {
        dictIterator *iter = dictGetSafeIterator(keyset);
        dictEntry *match;
        RedisModuleCallReply *reply = NULL;
        while ((match = dictNext(iter)))
        {
            RedisModule_ReplyWithStringBuffer(ctx, (char *)match->key, strlen((char *)match->key));
            if (fetch_rows == 1)
            {
                char *index_entry = (char *)(char *)match->v.val;
                char *first_tab = strchr(index_entry, '\t');
                char key_type = *(first_tab + 1);
                switch (key_type)
                {
                case 'H':
                    reply = RedisModule_Call(ctx, "HGETALL", "c", (char *)match->key);
                    break;
                case 'S':
                    reply = RedisModule_Call(ctx, "GET", "c", (char *)match->key);
                    break;
                default:
                    RedisModule_ReplyWithError(ctx, "Unsupported key type found!");
                    return;
                }
                // size_t l = RedisModule_CallReplyLength(reply);
                // const char *ele = RedisModule_CallReplyProto(reply, &l);
                RedisModule_ReplyWithCallReply(ctx, reply);
                items++;
            }
            items++;
        }
        dictReleaseIterator(iter);
        // dictRelease(keyset);
    }
    // RedisModule_ReplySetArrayLength(ctx, items);
}

// Would be better if we could use zset!

int executeOr(Parser *p, Token *t, list *stack)
{
    if (listLength(stack) < 2)
    {
        addError(p, "OR requires 2 sets!");
        return C_ERR;
    }
    KeysetDescriptor *out = getKeysetDescriptor2(p, t, stack, QE_LOAD_LEFT_AND_RIGHT | QE_CREATE_SET | QE_SWAP_LARGEST_FIRST);
    propagate_set_type(out, out->left, out->right);

    // Copy larger set to out
    dictIterator *iter = dictGetSafeIterator(out->right->keyset);
    dictEntry *match;
    while ((match = dictNext(iter)))
    {
        sds key_to_match = (sds)match->key;
        dictAdd(out->keyset, sdsdup(key_to_match), match->v.val);
    }
    dictReleaseIterator(iter);
    // Copy elements not in out from smaller set
    iter = dictGetSafeIterator(out->left->keyset);
    while ((match = dictNext(iter)))
    {
        sds key_to_match = (sds)match->key;
        dictEntry *entry_in_target = dictFind(out->keyset, key_to_match);
        if (!entry_in_target)
        {
            dictAdd(out->keyset, sdsdup(key_to_match), match->v.val);
        }
    }
    putKeysetDescriptor(stack, out, iter);
    return C_OK;
}

rax *convertTripletDictToRax(KeysetDescriptor *kd)
{
    rax *r = raxNew();
    dictIterator *iter = dictGetSafeIterator(kd->keyset);
    dictEntry *tde;
    while ((tde = dictNext(iter)))
    {
        sds k = (sds)tde->key;
        robj *o = dictGetVal(tde);
        if (o->type == OBJ_TRIPLET)
        {
            int segments;
            sds *parts = sdssplitlen(k, sdslen(k), ":", 1, &segments);
            sds sk = sdscatprintf(sdsempty(), "%s:%s:%s", parts[1], parts[2], parts[0]);
            raxInsert(r, (UCHAR *)sk, sdslen(sk), o, NULL);
            sdsfreesplitres(parts, segments);
        }
        else
        {

            raxInsert(r, (UCHAR *)k, sdslen(k), o, NULL);
        }
    }
    dictReleaseIterator(iter);
    // /**/ raxShow(r);
    return r;
}

int executeGremlinJoin(KeysetDescriptor *out)
{
    rax *left = convertTripletDictToRax(out->left);
    rax *right = convertTripletDictToRax(out->right);

    raxIterator ril;
    raxStart(&ril, left);
    raxSeek(&ril, "^", NULL, 0);

    raxIterator rir;
    raxStart(&rir, right);
    raxSeek(&rir, "^", NULL, 0);
    while (raxNext(&ril))
    {
        robj *lo = ril.data;
        int lsegments;
        sds lk = sdsnewlen(ril.key, ril.key_len);
        sds *lparts = sdssplitlen(lk, sdslen(lk), ":", 1, &lsegments);
        // /**/ serverLog(LL_NOTICE, "raxL: key=(%d)%s subject=%s", ril.key_len, ril.key, lparts[0]);
        raxSeek(&rir, ">=", (UCHAR *)lparts[0], sdslen(lparts[0]));
        int merges = 0;
        while (raxNext(&rir))
        {
            // /**/ serverLog(LL_NOTICE, "raxR: key=(%d)%s", rir.key_len, rir.key);
            robj *ro = rir.data;
            int rsegments;
            sds rk = sdsnewlen(rir.key, rir.key_len);
            sds *rparts = sdssplitlen(rk, sdslen(rk), ":", 1, &rsegments);
            if(sdscmp(lparts[0], rparts[0]) == 0){
                ++merges;
            // Merge entry_in_target to match (will be pushed to result)!
                Graph_Triplet *sj = (Graph_Triplet *)lo->ptr;
                Graph_Triplet *oj = (Graph_Triplet *)ro->ptr;
                listIter *oji = listGetIterator(oj->edges, 0);
                listNode *ojn;
                while ((ojn = listNext(oji)))
                {
                    Graph_Triplet_Edge *t = (Graph_Triplet_Edge *)ojn->value;
                    listAddNodeHead(sj->edges, t);
                }
                listReleaseIterator(oji);
            }else{
                merges = -1;
                raxPrev(&rir);
            }
            if (merges == 1)
            {
                dictAdd(out->keyset, sdsdup(lk), lo);
            }
            sdsfreesplitres(rparts, rsegments);
            if(merges == -1)
                break;
        }
        sdsfreesplitres(lparts, lsegments);
    }
    raxStop(&rir);
    raxStop(&ril);
    return C_OK;
}

int executeAnd(Parser *p, Token *t, list *stack)
{
    if (listLength(stack) < 2)
    {
        addError(p, "AND requires 2 sets!");
        return C_ERR;
    }
    KeysetDescriptor *out = getKeysetDescriptor2(p, t, stack, QE_LOAD_LEFT_AND_RIGHT | QE_CREATE_SET | QE_SWAP_SMALLEST_FIRST);
    if(( out->left->value_type == out->right->value_type )
        && ((out->left->value_type & (KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET
                                    + KeysetDescriptor_TYPE_GREMLIN_AS_SET)) != 0))
        { int rc = executeGremlinJoin(out);
            putKeysetDescriptor(stack, out, NULL);
            return rc;
        }

    propagate_set_type(out, out->left, out->right);
    // Iterate the smaller set, Copy any item both in left and right
    dictIterator *iter = dictGetSafeIterator(out->left->keyset);
    dictEntry *match;
    while ((match = dictNext(iter)))
    {
        sds key_to_match = (sds)match->key;

        dictEntry *entry_in_target = dictFind(out->right->keyset, key_to_match);
        if (entry_in_target)
        {
            // Merge entry_in_target to match (will be pushed to result)!
            robj *so = (robj *)dictGetVal(entry_in_target);
            robj *object_to_match = (robj *)dictGetVal(match);
            if (object_to_match->type == so->type && so->type == OBJ_TRIPLET)
            {
                list *cs = (list *)so->ptr;
                list *co = (list *)object_to_match->ptr;
                listIter *ci = listGetIterator(cs, 0);
                listNode *cn;
                while ((cn = listNext(ci)))
                {
                    Graph_Triplet *t = (Graph_Triplet *)cn->value;
                    listAddNodeHead(co, t);
                }
                listReleaseIterator(ci);
            }
            dictAdd(out->keyset, sdsdup(key_to_match), match->v.val);
        }
    }
    putKeysetDescriptor(stack, out, iter);
    return C_OK;
}

int executeXor(Parser *p, Token *t, list *stack)
{
    if (listLength(stack) < 2)
    {
        addError(p, "XOR requires 2 sets!");
        return C_ERR;
    }
    KeysetDescriptor *out = getKeysetDescriptor2(p, t, stack, QE_LOAD_LEFT_AND_RIGHT | QE_CREATE_SET);
    propagate_set_type(out, out->left, out->right);
    // Iterate the smaller set, Copy any item both in left and right
    dictIterator *iter = dictGetSafeIterator(out->left->keyset);
    dictEntry *match;
    while ((match = dictNext(iter)))
    {
        sds key_to_match = (sds)match->key;
        dictEntry *entry_in_target = dictFind(out->right->keyset, key_to_match);
        if (!entry_in_target)
        {
            dictAdd(out->keyset, sdsdup(key_to_match), match->v.val);
        }
    }
    dictReleaseIterator(iter);
    iter = dictGetSafeIterator(out->right->keyset);
    while ((match = dictNext(iter)))
    {
        sds key_to_match = (sds)match->key;
        dictEntry *entry_in_target = dictFind(out->left->keyset, key_to_match);
        if (!entry_in_target)
        {
            dictAdd(out->keyset, sdsdup(key_to_match), match->v.val);
        }
    }
    putKeysetDescriptor(stack, out, iter);
    return C_OK;
}

int executeIn(Parser *p, Token *t, list *stack)
{
    KeysetDescriptor *out = getKeysetDescriptor2(p, t, stack, QE_LOAD_LEFT_AND_RIGHT | QE_CREATE_SET);
    propagate_set_type(out, out->left, out->right);
    // Iterate the left set, Copy when left item in right
    dictIterator *iter = dictGetSafeIterator(out->left->keyset);
    dictEntry *match;
    while ((match = dictNext(iter)))
    {
        sds key_to_match = (sds)match->key;
        dictEntry *entry_in_target = dictFind(out->right->keyset, key_to_match);
        if (entry_in_target)
        {
            dictAdd(out->keyset, sdsdup(key_to_match), match->v.val);
        }
    }
    putKeysetDescriptor(stack, out, iter);
    return C_OK;
}

int executeGremlinDisjoined(KeysetDescriptor *out)
{
    rax *left = convertTripletDictToRax(out->left);
    rax *right = convertTripletDictToRax(out->right);

    raxIterator ril;
    raxStart(&ril, left);
    raxSeek(&ril, "^", NULL, 0);

    raxIterator rir;
    raxStart(&rir, right);
    raxSeek(&rir, "^", NULL, 0);
    while (raxNext(&ril))
    {
        robj *lo = ril.data;
        int lsegments;
        sds lk = sdsnewlen(ril.key, ril.key_len);
        sds *lparts = sdssplitlen(lk, sdslen(lk), ":", 1, &lsegments);
        // /**/ serverLog(LL_NOTICE, "raxL: key=(%d)%s subject=%s", ril.key_len, ril.key, lparts[0]);
        raxSeek(&rir, ">=", (UCHAR *)lparts[0], sdslen(lparts[0]));
        if (raxNext(&rir))
        {
            // /**/ serverLog(LL_NOTICE, "raxR: key=(%d)%s", rir.key_len, rir.key);
            int rsegments;
            sds rk = sdsnewlen(rir.key, rir.key_len);
            sds *rparts = sdssplitlen(rk, sdslen(rk), ":", 1, &rsegments);
            if(sdscmp(lparts[0], rparts[0]) != 0){
                dictAdd(out->keyset, sdsdup(lk), lo);
            }
            sdsfreesplitres(rparts, rsegments);
        }
        sdsfreesplitres(lparts, lsegments);
    }
    raxStop(&rir);
    raxStop(&ril);
    return C_OK;
}

int executeNotIn(Parser *p, Token *t, list *stack)
{
    KeysetDescriptor *out = getKeysetDescriptor2(p, t, stack, QE_LOAD_LEFT_AND_RIGHT | QE_CREATE_SET);
    if(((out->left->value_type & (KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET
                                    + KeysetDescriptor_TYPE_GREMLIN_AS_SET)) != 0)
        || ((out->right->value_type & (KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET
                                    + KeysetDescriptor_TYPE_GREMLIN_AS_SET)) != 0))
        { int rc = executeGremlinDisjoined(out);
            putKeysetDescriptor(stack, out, NULL);
            return rc;
        }
    propagate_set_type(out, out->left, out->right);
    // Iterate the left set, Copy when left item in right
    dictIterator *iter = dictGetSafeIterator(out->left->keyset);
    dictEntry *match;
    while ((match = dictNext(iter)))
    {
        sds key_to_match = (sds)match->key;
        dictEntry *entry_in_target = dictFind(out->right->keyset, key_to_match);
        if (!entry_in_target)
        {
            dictAdd(out->keyset, sdsdup(key_to_match), match->v.val);
        }
    }
    putKeysetDescriptor(stack, out, iter);
    return C_OK;
}

dict *pushIndexEntries(redisReply *reply, KeysetDescriptor *kd)
{
    kd->keyset = dictCreate(rxShared->tokenDictType, (void *)NULL);
    kd->value_type = KeysetDescriptor_TYPE_KEYSET;

    // 1) member
    // 2) score
    for (size_t j = 0; j < reply->elements; j += 2)
    {
        sds index_entry = sdsnew(reply->element[j]->str);

        int segments = 0;
        sds *parts = sdssplitlen(index_entry, sdslen(index_entry), "\t", 1, &segments);
        robj *o = createStringObject(index_entry, sdslen(index_entry));
        dictAdd(kd->keyset, (void *)sdsdup(parts[0]), o);
        sdsfreesplitres(parts, segments);
    }
    kd->size = dictSize(kd->keyset);
    kd->value_type = KeysetDescriptor_TYPE_KEYSET;
    return kd->keyset;
}

int fetchKeyset(redisContext *index_context, KeysetDescriptor *kd, sds l, sds r, sds cmp)
{
    redisReply *rcc;
    long long start = ustime();
    if (cmp && isdigit(*l))
        rcc = redisCommand(index_context, "rxfetch '%s' '%s' '%s'", r, l, cmp);
    else if (l)
        rcc = redisCommand(index_context, "rxfetch %s %s", r, l);
    else
        rcc = redisCommand(index_context, "rxfetch %s", r);
    kd->latency = ustime() - start;
    // ///**/ serverlog(LL_NOTICE, "............ executeEquals ... %lld ... %i %s", kd->latency, rcc->elements, rcc->str);
    if (rcc->type == REDIS_REPLY_ARRAY)
    {
        pushIndexEntries(rcc, kd);
    }
    freeReplyObject(rcc);
    return C_OK;
}

KeysetDescriptor *propagate_set_type(KeysetDescriptor *t, KeysetDescriptor *l, KeysetDescriptor *r)
{
    t->vertices_or_edges = l->vertices_or_edges | r->vertices_or_edges;
    return t;
}

int executeEquals(Parser *p, Token *t, list *stack)
{

    KeysetDescriptor *kd = getKeysetDescriptor2(p, t, stack, QE_LOAD_NONE);
    propagate_set_type(kd, kd->left, kd->right);
    long long start = ustime();
    if (!kd->keyset)
        fetchKeyset(p->index_context, kd, kd->right->setname, kd->left->setname, (char *)t->token);
    kd->latency = ustime() - start;
    push(stack, kd);
    return C_OK;
}

int executeStore(Parser *p, Token *t, list *stack)
{
    KeysetDescriptor *kd = getKeysetDescriptor2(p, t, stack, QE_LOAD_RIGHT_ONLY);
    propagate_set_type(kd, kd->left, kd->right);
    long long start = ustime();
    // TODO: kd->left := kd->right
    kd->latency = ustime() - start;
    push(stack, kd);
    return C_OK;
}

int executePlusMinus(Parser *p, Token *t, list *stack)
{
    KeysetDescriptor *r = pop(stack);
    KeysetDescriptor *l = pop(stack);
    rxUNUSED(p);
    rxUNUSED(t);
    rxUNUSED(r);
    rxUNUSED(l);
    rxUNUSED(stack);
    sds keyset_name = sdscatfmt(sdsempty(), "%s%s%s", l->setname, t->token, r->setname);
    long long start = ustime();
    KeysetDescriptor *kd = getKeysetDescriptor(keyset_name, KeysetDescriptor_TYPE_KEYSET);
    // TODO
    kd->latency = ustime() - start;
    push(stack, kd);
    return C_OK;
}

/* * * * * * *  Gremlin Operations * * * * * * */
int executeGremlin(Parser *p, Token *t, list *stack)
{
    rxUNUSED(p);
    rxUNUSED(t);
    rxUNUSED(stack);
    return C_OK;
}

robj *findKey(redisDb *db, sds key)
{
    if(!db)
        serverPanic("findKey: No REDIS DB!");
    if(!key)
        serverPanic("findKey: No key to search!");
    dictEntry *de = dictFind(db->dict, key);
    if (de)
    {
        robj *val = dictGetVal(de);
        return val;
    }
    else
    {
        return NULL;
    }
}

KeysetDescriptor *getAllKeysByRegex(const char *regex, int on_matched, const char *set_name)
{
    sds keyset_name = sdscatfmt(sdsempty(), set_name);
    long long start = ustime();
    KeysetDescriptor *kd = getKeysetDescriptor(keyset_name, KeysetDescriptor_TYPE_GREMLINSET);
    kd->keyset = dictCreate(rxShared->tokenDictType, (void *)NULL);
    dictIterator *di;
    dictEntry *de;
    sds pattern = sdsnew(regex);
    int allkeys;
    unsigned long numkeys = 0;

    redisDb *db = REDIS_DB0;
    di = dictGetSafeIterator(db->dict);
    allkeys = strcmp(pattern, "*") == 0;
    while ((de = dictNext(di)) != NULL)
    {

        sds key = dictGetKey(de);
        // sds key2 =sdstrim(key, "^");
        // robj *h = findKey(db, key2);
        // if(h){
        if (*key == '^')
            continue;
        // }else{
        //     key = key2;
        // }
        robj *value = dictGetVal(de);

        int stringmatch = sdscharcount(key, ':') >= 2;
        if (allkeys || on_matched == stringmatch)
        {
            numkeys++;
            dictAdd(kd->keyset, (void *)sdsdup(key), value);
        }
    }
    dictReleaseIterator(di);

    // TODO
    kd->latency = ustime() - start;
    kd->size = numkeys;
    return kd;
}

extern zskiplistNode *zslGetElementByRank(zskiplist *zsl, unsigned long rank);
extern int hashTypeGetFromZiplist(robj *o, sds field,
                                  unsigned char **vstr,
                                  POINTER *vlen,
                                  long long *vll);

Graph_Triplet *newTripletResult()
{
    Graph_Triplet *triplet = zmalloc(sizeof(Graph_Triplet));
    triplet->subject = NULL;
    triplet->subject_key = sdsempty();
    triplet->edges = listCreate();
    return triplet;
}

robj *createTripletResultInitial(redisDb *db, graph_leg *terminal, sds subject_key, sds member, sds edge)
{
    robj *subject = findKey(db, subject_key);
    if (!subject)
        subject = createHashObject();
    else if (subject->type != OBJ_HASH)
        return NULL;

    list *path = listCreate();

    graph_leg *p = terminal;
    robj *object = NULL;
    sds object_key = member;
    if (!member)
    {
        while (p)
        {
            if (!p->origin)
            {
                object = findKey(db, p->key);
                if (object->type != OBJ_HASH)
                    return NULL;
                object_key = sdsdup(p->key);
            }
            listAddNodeTail(path, sdsdup(p->key));
            p = p->origin;
        }
    }
    else
    {
        member = sdstrim(member, "^");
        object = findKey(db, member);
        if (edge)
            listAddNodeTail(path, sdsdup(edge));
    }
    if (!subject /*|| !object*/)
        return NULL;
    Graph_Triplet *triplet = newTripletResult();
    triplet->subject = (struct robj *)subject;
    triplet->subject_key = sdsdup(subject_key);
    if (object)
    {
        Graph_Triplet_Edge *e = zmalloc(sizeof(Graph_Triplet_Edge));
        e->object = (struct robj *)object;
        e->object_key = sdsdup(object_key);
        e->path = path;
        listAddNodeTail(triplet->edges, e);
    }
    robj *t = createObject(OBJ_TRIPLET, triplet);
    return t;
}

robj *createTripletResultInitialForMatch(redisDb *db, graph_leg *terminal, sds subject_key, sds member, sds edge)
{
    robj *subject = findKey(db, subject_key);
    if (!subject)
        subject = createHashObject();
    else if (subject->type != OBJ_HASH)
        return NULL;

    list *path = listCreate();

    graph_leg *p = terminal;
    robj *object = NULL;
    sds object_key = (member) ? member : terminal->key;
    while (p)
    {
        listAddNodeTail(path, sdsdup(p->key));
        p = p->origin;
    }
    object_key = sdstrim(object_key, "^");
    object = findKey(db, object_key);
    if (edge)
        listAddNodeTail(path, sdsdup(edge));

    if (!subject /*|| !object*/)
        return NULL;
    Graph_Triplet *triplet = newTripletResult();
    triplet->subject = (struct robj *)subject;
    triplet->subject_key = sdsdup(subject_key);
    if (object)
    {
        Graph_Triplet_Edge *e = zmalloc(sizeof(Graph_Triplet_Edge));
        e->object = (struct robj *)object;
        e->object_key = sdsdup(object_key);
        e->path = path;
        listAddNodeTail(triplet->edges, e);
    }
    robj *t = createObject(OBJ_TRIPLET, triplet);
    return t;
}

robj *addObjectToTripletResult(robj *ge, sds object_key, sds edge)
{
    Graph_Triplet *t = ge->ptr; // ln->value;
    robj *object = findKey(REDIS_DB0, object_key);
    Graph_Triplet_Edge *e = zmalloc(sizeof(Graph_Triplet_Edge));
    e->object = (struct robj *)object;
    e->object_key = sdsdup(object_key);
    e->path = listCreate();
    if (edge)
        listAddNodeTail(e->path, edge);
    listAddNodeTail(t->edges, e);
    return ge;
}

robj *createTripletResult(redisDb *db, graph_leg *terminal, sds member, sds edge)
{
    if ((!terminal || (terminal == terminal->start)) && !member)
        return NULL;
    return createTripletResultInitial(db, terminal, terminal->key, member, edge);
}

robj *createTripletResultForMatch(redisDb *db, graph_leg *terminal, sds member, sds edge)
{
    if ((!terminal || (terminal == terminal->start)) && !member)
        return NULL;
    return createTripletResultInitialForMatch(db, terminal, terminal->key, member, edge);
}

void deleteTripletResult(Graph_Triplet *triplet)
{
    // while (listLength(collection))
    // {
    //     Graph_Triplet *triplet = pop_Graph_Triplet(collection);
    triplet->subject = NULL;

    while (listLength(triplet->edges))
    {
        Graph_Triplet_Edge *e = (Graph_Triplet_Edge *)pop_Graph_Triplet(triplet->edges);
        e->object = NULL;
        sdsfree(e->object_key);
        while (listLength(e->path))
        {
            sds p = sdspop(e->path);
            sdsfree(p);
        }
        listRelease(e->path);
        e->path = NULL;
        zfree(e);
    }
    listRelease(triplet->edges);
    zfree(triplet);
    // }
}

int addMemberToKeyset(redisDb *db, unsigned char *vstr, POINTER vlen, KeysetDescriptor *kd, graph_leg *terminal, short filterOnly)
{
    // look up target key
    int segments = 0;
    int link_segments = 0;
    sds some_str = sdsnew((char *)vstr);
    sds *parts = sdssplitlen(some_str, vlen, "|", 1, &segments);

    if (filterOnly /*&& terminal->de*/)
    {
        if (!terminal->de)
            return 0;
        sds k = dictGetKey(terminal->de);
        robj *v = dictGetVal(terminal->de);
        robj *tobj = findKey(db, k);
        /*
            serverLog(LL_NOTICE, "test only terminal->de=0x%x vstr=%s k=%s v=0x%x tobj=0x%x",
               (POINTER)terminal->de, vstr, k, (POINTER)v, (POINTER)tobj);
               */
        dictAdd(kd->keyset, (void *)sdsdup(k), tobj ? tobj : v);
        return 1;
    }

    robj *tobj = createTripletResult(db, terminal, sdsempty(), sdsempty());
    if (segments == 1)
    {
        sds member = sdsnew((const char *)vstr);
        if (!tobj)
            if ((tobj = findKey(db, member)) == NULL || tobj->type != OBJ_HASH)
                return 0;
        dictAdd(kd->keyset, (void *)sdsdup(member), tobj);
        return 1;
    }

    sds link = sdsnew(parts[1] + 1);
    sds *link_parts = sdssplitlen(link, sdslen(link), ":", 1, &link_segments);
    if (link_segments == 1)
    {
        if (!tobj)
            if ((tobj = findKey(db, link)) == NULL || tobj->type != OBJ_HASH)
                return 0;
        dictAdd(kd->keyset, (void *)sdsdup(link), tobj);
        return 1;
    }
    ///////**/ serverLog(LL_NOTICE, "link 1: %s %s kd=0x%x ", parts[2], link, (POINTER)kd);
    sds edge_key = sdscatprintf(sdsempty(), "^%s", link);
    robj *members = findKey(db, edge_key);
    if (members && members->type == OBJ_SET)
    {
        setTypeIterator *si = setTypeInitIterator(members);
        sds member;
        int64_t l;
        while (setTypeNext(si, &member, &l) != -1)
        {
            int msegments = 0;
            sds *mparts = sdssplitlen(member, sdslen(member), "|", 1, &msegments);
            dictEntry *ddee = dictFind(kd->keyset, (void *)link /*terminal->key*/);
            robj *mobj = NULL;
            /////**/ serverLog(LL_NOTICE, "\nresult: %s", terminal->key);
            if (ddee)
            {
                /////**/ serverLog(LL_NOTICE, "\nmulti result: %s link: %s", terminal->key, link);
                mobj = dictGetVal(ddee);
            }
            else
            {
                mobj = createTripletResult(db, terminal, mparts[1], sdstrim(parts[1], "^"));
            }
            dictAdd(kd->keyset, (void *)sdsdup(link /*terminal->key*/), mobj);
            sdsfreesplitres(mparts, msegments);
        }
        setTypeReleaseIterator(si);
        kd->size = dictSize(kd->keyset);
    }
    else
    {

        if (*parts[2] == 'S' || *(parts[2] + 1) == 'S')
            link = sdsnew(link_parts[2]);
        if (*parts[2] == 'O' || *(parts[2] + 1) == 'O')
            link = sdsnew(link_parts[1]);

        if (!tobj)
            if ((tobj = findKey(db, link)) == NULL || tobj->type != OBJ_HASH)
                return 0;
        dictAdd(kd->keyset, (void *)sdsdup(link), tobj);
    }
    /////**/ serverLog(LL_NOTICE, " => %s", link);
    sdsfreesplitres(parts, segments);
    sdsfreesplitres(link_parts, link_segments);
    return 1;
}

int addMemberToKeysetForMatch(redisDb *db, unsigned char *vstr, POINTER vlen, KeysetDescriptor *kd, graph_leg *terminal, short filterOnly)
{
    // look up target key
    int segments = 0;
    int link_segments = 0;
    sds some_str = sdsnew((char *)vstr);
    sds *parts = sdssplitlen(some_str, vlen, "|", 1, &segments);

    if (filterOnly /*&& terminal->de*/)
    {
        graph_leg *leader = terminal;
        while(leader->origin){
            leader = leader->origin;
        }
        if(leader != NULL && leader != terminal){
            sds tk = sdscatprintf(sdsempty(), ":%s:%s:", leader->key, terminal->key);
            robj *tobj = createTripletResultForMatch(db, terminal, leader->key, NULL);
            dictAdd(kd->keyset, (void *)sdsdup(tk), tobj);
            sdsfree(tk);
            return 1;
        }

        if (!terminal->de)
            return 0;
        sds k = dictGetKey(terminal->de);
        robj *v = dictGetVal(terminal->de);
        robj *tobj = findKey(db, k);
        /*
            serverLog(LL_NOTICE, "test only terminal->de=0x%x vstr=%s k=%s v=0x%x tobj=0x%x",
               (POINTER)terminal->de, vstr, k, (POINTER)v, (POINTER)tobj);
               */
            sds tk = sdscatprintf(sdsempty(), ":%s:%s:", leader->key, terminal->key);
        dictAdd(kd->keyset, (void *)sdsdup(tk), tobj ? tobj : v);
            sdsfree(tk);
        return 1;
    }

    robj *tobj = createTripletResult(db, terminal, sdsempty(), sdsempty());
    if (segments == 1)
    {
        sds member = sdsnew((const char *)vstr);
        if (!tobj)
            if ((tobj = findKey(db, member)) == NULL || tobj->type != OBJ_HASH)
                return 0;
        dictAdd(kd->keyset, (void *)sdsdup(member), tobj);
        return 1;
    }

    sds link = sdsnew(parts[1] + 1);
    sds *link_parts = sdssplitlen(link, sdslen(link), ":", 1, &link_segments);
    if (link_segments == 1)
    {
        if (!tobj)
            if ((tobj = findKey(db, link)) == NULL || tobj->type != OBJ_HASH)
                return 0;
        dictAdd(kd->keyset, (void *)sdsdup(link), tobj);
        return 1;
    }
    ///**/ serverLog(LL_NOTICE, "link 1: %s %s kd=0x%x ", parts[2], link, (POINTER)kd);
    sds edge_key = sdscatprintf(sdsempty(), "^%s", link);
    robj *members = findKey(db, edge_key);
    if (members && members->type == OBJ_SET)
    {
        setTypeIterator *si = setTypeInitIterator(members);
        sds member;
        int64_t l;
        while (setTypeNext(si, &member, &l) != -1)
        {
            int msegments = 0;
            sds *mparts = sdssplitlen(member, sdslen(member), "|", 1, &msegments);
            dictEntry *ddee = dictFind(kd->keyset, (void *)link /*terminal->key*/);
            robj *mobj = NULL;
            ///**/ serverLog(LL_NOTICE, "\nresult: %s", terminal->key);
            if (ddee)
            {
                ///**/ serverLog(LL_NOTICE, "\nmulti result: %s link: %s", terminal->key, link);
                mobj = dictGetVal(ddee);
            }
            else
            {
                mobj = createTripletResult(db, terminal, mparts[1], sdstrim(parts[1], "^"));
            }
            dictAdd(kd->keyset, (void *)sdsdup(link /*terminal->key*/), mobj);
            sdsfreesplitres(mparts, msegments);
        }
        setTypeReleaseIterator(si);
        kd->size = dictSize(kd->keyset);
    }
    else
    {

        if (*parts[2] == 'S' || *(parts[2] + 1) == 'S')
            link = sdsnew(link_parts[2]);
        if (*parts[2] == 'O' || *(parts[2] + 1) == 'O')
            link = sdsnew(link_parts[1]);

        if (!tobj)
            if ((tobj = findKey(db, link)) == NULL || tobj->type != OBJ_HASH)
                return 0;
        dictAdd(kd->keyset, (void *)sdsdup(link), tobj);
    }
    ///**/ serverLog(LL_NOTICE, " => %s", link);
    sdsfreesplitres(parts, segments);
    sdsfreesplitres(link_parts, link_segments);
    return 1;
}

void load_set(redisDb *db, sds key, KeysetDescriptor *kd)
{
    robj *zobj = findKey(db, key);
    if (zobj == NULL)
        return;
    if (zobj->type == OBJ_HASH)
    {
        if (!kd->keyset)
            kd->keyset = dictCreate(rxShared->tokenDictType, (void *)NULL);
        dictAdd(kd->keyset, (void *)key, zobj);
        return;
    }
    if (zobj->type != OBJ_SET)
        return;

    if (!kd->keyset)
        kd->keyset = dictCreate(rxShared->tokenDictType, (void *)NULL);
    setTypeIterator *si;
    sds elesds;
    int64_t intobj;

    /* Iterate all the elements of the first (smallest) set, and test
     * the element against all the other sets, if at least one set does
     * not include the element it is discarded */
    si = setTypeInitIterator(zobj);
    while (setTypeNext(si, &elesds, &intobj) != -1)
    {
        addMemberToKeyset(db, (unsigned char *)elesds, sdslen(elesds), kd, NULL, 0);
    }
    setTypeReleaseIterator(si);
}

int executeAllVertices(Parser *p, Token *t, list *stack)
{
    /*
        1) When a stack entry exists
           a) Use the keyset from the stack
        2) When the stack is empty
           a) Find all 'hash' keys
        3) Push the set on the stack
    */
    rxUNUSED(p);
    rxUNUSED(t);
    KeysetDescriptor *kd = NULL;
    if (listLength(stack) == 0)
        kd = getAllKeysByRegex("[A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", NO_MATCH_PATTERN, "vertices");
    else
    {
        kd = pop(stack);
        load_set(REDIS_DB0, kd->setname, kd);
    }
    defaultKeysetDescriptorAsTemp(kd);
    kd->vertices_or_edges = VERTEX_SET;
    kd->value_type = KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET;
    push(stack, kd);
    return C_OK;
}

int executeAllEdges(Parser *p, Token *t, list *stack)
{
    /* 1) Find all 'hash' keys
       2) Push list on stack
    */
    rxUNUSED(p);
    rxUNUSED(t);
    KeysetDescriptor *kd = NULL;
    if (listLength(stack) == 0)
        kd = getAllKeysByRegex("[A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", MATCH_PATTERN, "edges");
    else
    {
        kd = pop(stack);
        load_set(REDIS_DB0, kd->setname, kd);
    }
    defaultKeysetDescriptorAsTemp(kd);
    kd->vertices_or_edges = EDGE_SET;
    kd->value_type = KeysetDescriptor_TYPE_GREMLIN_EDGE_SET;
    push(stack, kd);
    return C_OK;
}

#define MATCH_IS_FALSE 0
#define MATCH_IS_TRUE 1
int matchHasValue(robj *o, sds field, sds pattern, int plen)
{
    if (!o || o->type != OBJ_HASH)
        return MATCH_IS_FALSE;

    if (o->encoding == OBJ_ENCODING_ZIPLIST)
    {
        unsigned char *vstr = NULL;
        POINTER vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        int ret = hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll);
        if (ret < 0)
            return MATCH_IS_FALSE;
        if (pattern == NULL)
            return MATCH_IS_TRUE;
        sds k = sdsnewlen(vstr, vlen);
        sdstolower(k);
        if (ret == 0 && stringmatchlen(pattern, plen, (const char *)k, vlen, 1))
        {
            sdsfree(k);
            return MATCH_IS_TRUE;
        }
        sdsfree(k);
    }
    else if (o->encoding == OBJ_ENCODING_HT)
    {
        sds value = hashTypeGetFromHashTable(o, field);
        if (value && stringmatchlen(pattern, plen, value, strlen(value), 1))
        {
            return MATCH_IS_TRUE;
        }
    }
    return MATCH_IS_FALSE;
}

int executeGremlinHas(Parser *p, Token *t, list *stack)
{
    sds tok = sdsdup((const sds)t->token);
    sdstoupper(tok);
    short must_match = (sdscmp((const sds)tok, sdsnew("HAS")) == 0) ? MATCH_IS_TRUE : MATCH_IS_FALSE;
    sdsfree(tok);
    /* 1) Use top of stack = dict of keys/values.
       2) Filter all keys matching kvp
       3) Push filtered dict on the stack
    */
    rxUNUSED(p);
    KeysetDescriptor *vd = pop(stack);
    KeysetDescriptor *ad;
    if (vd->parameter_list)
    {
        KeysetDescriptor *p = vd;
        list *pl = p->parameter_list;
        // serverLog(LL_NOTICE, "parameter list");
        // dumpStack(pl);
        vd = pop(pl);
        ad = pop(pl);
        // deleteKeysetDescriptor(p);
    }
    else
        ad = pop(stack);

    KeysetDescriptor *od = pop(stack);
    sds set_name;
    if (od)
    {
        set_name = sdscatprintf(sdsempty(), "%s:%s==%s", od->setname, ad->setname, vd->setname);
    }
    else
    {
        set_name = sdscatprintf(sdsempty(), "%s==%s", ad->setname, vd->setname);
    }
    sds field = sdsnew(ad->setname);
    long long start = ustime();
    KeysetDescriptor *kd = getKeysetDescriptor(set_name, KeysetDescriptor_TYPE_GREMLINSET);
    // defaultKeysetDescriptorAsTemp(kd);
    kd->is_temp = 1;
    kd->keyset = dictCreate(rxShared->tokenDictType, (void *)NULL);
    dictIterator *di;
    dictEntry *de;
    unsigned long numkeys = 0;
    sds pattern = sdsnew(vd->setname);
    int plen = sdslen(pattern);
    redisDb *db = REDIS_DB0;
    di = dictGetSafeIterator(od && od->keyset ? od->keyset : db->dict);
    while ((de = dictNext(di)) != NULL)
    {
        sds key = dictGetKey(de);
        robj *o = dictGetVal(de);
        if (matchHasValue(o, field, pattern, plen) == must_match)
        {
            numkeys++;
            dictAdd(kd->keyset, (void *)sdsdup(key), o);
        }
    }
    dictReleaseIterator(di);
    sdsfree(set_name);
    sdsfree(field);
    kd->latency = ustime() - start;
    kd->size = numkeys;
    defaultKeysetDescriptorAsTemp(ad);
    defaultKeysetDescriptorAsTemp(vd);
    defaultKeysetDescriptorAsTemp(kd);
    propagate_set_type(kd, ad, vd);
    push(stack, kd);
    return C_OK;
}

int executeGremlinHasLabel(Parser *p, Token *t, list *stack)
{
    sds tok = sdsdup((const sds)t->token);
    sdstoupper(tok);
    short must_match = (sdscmp((const sds)tok, sdsnew("HASLABEL")) == 0) ? MATCH_IS_TRUE : MATCH_IS_FALSE;
    sdsfree(tok);
    /* 1) Use top of stack = dict of keys/values.
       2) Filter all keys matching kvp
       3) Push filtered dict on the stack
    */
    rxUNUSED(p);
    KeysetDescriptor *ad;
    ad = pop(stack);

    KeysetDescriptor *od = pop(stack);
    sds set_name;
    if (od)
    {
        set_name = sdscatprintf(sdsempty(), "%s:%s %s", od->setname, t->token, ad->setname);
    }
    else
    {
        set_name = sdscatprintf(sdsempty(), "%s %s", t->token, ad->setname);
    }
    sds field = sdsnew(ad->setname);
    long long start = ustime();
    KeysetDescriptor *kd = getKeysetDescriptor(set_name, KeysetDescriptor_TYPE_GREMLINSET);
    defaultKeysetDescriptorAsTemp(kd);
    kd->keyset = dictCreate(rxShared->tokenDictType, (void *)NULL);
    dictIterator *di;
    dictEntry *de;
    unsigned long numkeys = 0;
    redisDb *db = REDIS_DB0;
    di = dictGetSafeIterator(od && od->keyset ? od->keyset : db->dict);
    while ((de = dictNext(di)) != NULL)
    {
        sds key = dictGetKey(de);
        robj *o = dictGetVal(de);
        if (matchHasValue(o, field, NULL, 0) == must_match)
        {
            numkeys++;
            dictAdd(kd->keyset, (void *)sdsdup(key), o);
        }
    }
    dictReleaseIterator(di);

    kd->latency = ustime() - start;
    kd->size = numkeys;
    defaultKeysetDescriptorAsTemp(ad);
    defaultKeysetDescriptorAsTemp(kd);
    propagate_set_type(kd, ad, ad);
    push(stack, kd);
    return C_OK;
}

/*
 * Aggregate a set of triplets, grouped by subject or object.
 * The subject/object are swapped on the result.
 */
int executeGremlinGroupby(Parser *p, Token *t, list *stack)
{
    if (listLength(stack) < 2)
    {
        addError(p, "AND requires 2 sets!");
        return C_ERR;
    }
    KeysetDescriptor *gbd = pop(stack);
    KeysetDescriptor *kd = pop(stack);

    sds setname = sdscatprintf(sdsempty(), "%s %s %s", kd->setname, t->token, gbd->setname);
    KeysetDescriptor *out = getKeysetDescriptor(setname, KeysetDescriptor_TYPE_GREMLINSET);
    out->keyset = dictCreate(rxShared->tokenDictType, (void *)NULL);

    dictIterator *iter = dictGetSafeIterator(kd->keyset);
    dictEntry *match;
    while ((match = dictNext(iter)))
    {
        sds key_to_match = (sds)dictGetKey(match);
        UNUSED(key_to_match); // TODO
        sds groupKey;
        robj *o = dictGetVal(match);
        switch (o->type)
        {
        case OBJ_HASH:
        {
            groupKey = getHashField(o, gbd->setname);
            dictEntry *groupee = dictFind(out->keyset, groupKey);
            robj *ge = NULL;
            if (groupee != NULL)
            {
                ge = dictGetVal(groupee);
                ge = addObjectToTripletResult(ge, key_to_match, sdsempty());
            }
            else
            {
                ge = createTripletResultInitial(REDIS_DB0, NULL, groupKey, key_to_match, sdsempty());
                dictAdd(out->keyset, sdsdup(groupKey), ge);
            }
        }
        break;
        case OBJ_TRIPLET:
        {
            Graph_Triplet *t = (Graph_Triplet *)o->ptr; // lnc->value;
            if (gbd->setname[0] == 's')
            {
                groupKey = t->subject_key;
                dictEntry *groupee = dictFind(out->keyset, groupKey);
                robj *ge = NULL;
                if (groupee != NULL)
                {
                    ge = dictGetVal(groupee);
                }
                else
                {
                    ge = createTripletResultInitial(REDIS_DB0, NULL, groupKey, NULL, sdsempty());
                    dictAdd(out->keyset, sdsdup(groupKey), ge);
                }
                listJoin(((Graph_Triplet *)ge->ptr)->edges, listDup(t->edges));
            }
            else
            {
                listIter *li = listGetIterator(t->edges, 0);
                listNode *ln;
                while ((ln = listNext(li)) != NULL)
                {
                    Graph_Triplet_Edge *e = ln->value;
                    groupKey = e->object_key;

                    dictEntry *groupee = dictFind(out->keyset, groupKey);
                    robj *ge = NULL;
                    if (groupee != NULL)
                    {
                        ge = dictGetVal(groupee);
                    }
                    else
                    {
                        ge = createTripletResultInitial(REDIS_DB0, NULL, groupKey, NULL, sdsempty());
                        dictAdd(out->keyset, sdsdup(groupKey), ge);
                    }
                    ge = addObjectToTripletResult(ge, t->subject_key, sdsempty());
                }
            }
        }
        break;
        default:
            addError(p, "Unsupported object type, only hash keys or rx triplets are supported.");
            return C_ERR;
        }
    }
    putKeysetDescriptor(stack, out, iter);
    return C_OK;
}

void matchinterCommand(robj *sets)
{
    setTypeIterator *si;
    sds elesds;
    int64_t intobj;
    unsigned long cardinality = 0;
    int encoding;

    /* Iterate all the elements of the first (smallest) set, and test
     * the element against all the other sets, if at least one set does
     * not include the element it is discarded */
    si = setTypeInitIterator(sets);
    while ((encoding = setTypeNext(si, &elesds, &intobj)) != -1)
    {

        cardinality++;
    }
    setTypeReleaseIterator(si);
}

graph_leg *add_graph_leg(sds key, float weight, graph_leg *origin, list *bsf_q, dict *touches)
{
    graph_leg *leg = zmalloc(sizeof(graph_leg));
    /////**/ serverLog(LL_NOTICE, "0x%x %s #add_graph_leg# ", (POINTER)leg, key);
    leg->key = sdsdup(key);
    leg->length += weight;
    leg->start = NULL;
    leg->origin = origin;
    leg->de = NULL;
    if (touches)
    {
        dictEntry *de;
        if ((de = dictFind(touches, key)) != NULL)
        {
            // /**/serverLog(LL_NOTICE, "Already touched: %s", key);
            return NULL;
        }
        dictAdd(touches, (void *)leg->key, leg);
    }
    listAddNodeTail(bsf_q, leg);
    return leg;
}

void delete_graph_leg(graph_leg *leg)
{
    /////**/ serverLog(LL_NOTICE, "0x%x %s #delete_graph_leg# ", (POINTER)leg, leg->key);
    sdsfree(leg->key);
    zfree(leg);
}

int getEdgeDirection(sds flowy)
{
    if (sdscmp(flowy, Graph_Subject_to_Edge) == 0)
        return GRAPH_TRAVERSE_OUT;
    if (sdscmp(flowy, Graph_Edge_to_Object) == 0)
        return GRAPH_TRAVERSE_OUT;
    if (sdscmp(flowy, Graph_Edge_to_Subject) == 0)
        return GRAPH_TRAVERSE_IN;
    if (sdscmp(flowy, Graph_Object_to_Edge) == 0)
        return GRAPH_TRAVERSE_IN;
    return GRAPH_TRAVERSE_INOUT;
}

int matchEdges(redisDb *db, graph_leg *leg, list *patterns, KeysetDescriptor *kd, list *bsf_q, dict *touches, int traverse_direction, short filter_only, dict *terminators, rax* includes, rax* excludes)
{
    if (terminators)
    {
        if (dictFind(terminators, leg->key))
        {
            ///**/ serverLog(LL_NOTICE, "terminator! 0x%x member %s ", (POINTER)leg, leg->key);
            addMemberToKeysetForMatch(db, (unsigned char *)leg->key, sdslen(leg->key), kd, leg, filter_only);
            return 1;
        }
    }
    sds key = sdscatprintf(sdsempty(), "^%s", leg->key);
    /////**/ serverLog(LL_NOTICE, "pretest: 0x%x %0.0f %s 0x%x 0x%x db=0x%x %s", (POINTER)leg, leg->length, leg->key, (POINTER)leg->start, (POINTER)leg->origin, (POINTER)db, key);
    robj *zobj = findKey(db, key);
    /////**/ serverLog(LL_NOTICE, "test: 0x%x %0.0f %s 0x%x 0x%x zobj=0x%x type=%d", (POINTER)leg, leg->length, leg->key, (POINTER)leg->start, (POINTER)leg->origin, (POINTER)zobj, zobj->type);
    if (zobj == NULL || zobj->type != OBJ_SET)
        return 0;
    int numkeys = 0;

    setTypeIterator *si;
    sds elesds;
    int64_t intobj;

    /* Iterate all the elements of the first (smallest) set, and test
     * the element against all the other sets, if at least one set does
     * not include the element it is discarded */
    si = setTypeInitIterator(zobj);
    while (setTypeNext(si, &elesds, &intobj) != -1)
    {
        int segments = 0;
        sds *parts = sdssplitlen(elesds, sdslen(elesds), "|", 1, &segments);
        double weight = atof(parts[3]);
        sds link = sdsnew(parts[1] + 1);
        int link_segments = 0;
        sds *link_parts = sdssplitlen(link, sdslen(link), ":", 1, &link_segments);
        // Does the link match the  pattern?
        /////**/ serverLog(LL_NOTICE, "try: 0x%x %s member %s with %s == ...?", (POINTER)leg, leg->key, elesds, parts[0]);
        if (segments >= 3)
        {
            void *excluded = (excludes) ? raxFind(excludes, (UCHAR *)parts[0], sdslen(parts[0])) : raxNotFound;
            if(excluded != raxNotFound)
                {    
                    /////**/ serverLog(LL_NOTICE, "excluded: 0x%x %s member %s with %s == ...?", (POINTER)leg, leg->key, elesds, parts[0]);
                    continue;
                }
            // Only traverse edges in the requested direction
            // /**/serverLog(LL_NOTICE, "test traverse direction: %d %s %d", traverse_direction, parts[2], getEdgeDirection(parts[2]));
            if (traverse_direction != GRAPH_TRAVERSE_INOUT && getEdgeDirection(parts[2]) != traverse_direction)
            {
                ///////**/ serverLog(LL_NOTICE, "reject: 0x%x %s member %s", (POINTER)leg, leg->key, elesds);
                sdsfreesplitres(parts, segments);
                sdsfreesplitres(link_parts, link_segments);
                sdsfree(link);
                continue;
            }
        }
        int doesMatchOneOfThePatterns = 0;
        void *included = (includes) ? raxFind(includes, (UCHAR *)parts[0], sdslen(parts[0])) : raxNotFound;
        if(included != raxNotFound)
            doesMatchOneOfThePatterns = 1;
        if (doesMatchOneOfThePatterns == 0 && patterns != NULL)
        {
            listIter *li = listGetIterator(patterns, 0);
            listNode *ln;
            while ((ln = listNext(li)) && doesMatchOneOfThePatterns == 0)
            {
                sds pattern = ln->value;
                doesMatchOneOfThePatterns = (stringmatchlen(pattern, sdslen(pattern), (const char *)parts[0], sdslen(parts[0]), 1));
            }
            listReleaseIterator(li);
        }
        if (doesMatchOneOfThePatterns != 0)
        {
            ///////**/ serverLog(LL_NOTICE, "match: 0x%x %s member %s start=0x%x", (POINTER)leg, leg->key, elesds, (POINTER)leg->start);
            // if (leg->start)
            //     serverLog(LL_NOTICE, "     : start %s", leg->start->key);
            graph_leg *path = leg;
            while (path)
            {
                ///////**/ serverLog(LL_NOTICE, " -> %s", path->key);
                path = path->origin;
            }
            ///////**/ serverLog(LL_NOTICE, "");
            numkeys += addMemberToKeyset(db, (unsigned char *)elesds, sdslen(elesds), kd, leg, filter_only);
        }
        else
        {
            // serverLog(LL_NOTICE, "no match: 0x%x %s member %s start=0x%x", (POINTER)leg, leg->key, elesds, (POINTER)leg->start);
            // Is the subject or object of the requested type?

            // serverLog(LL_NOTICE, "follow: 0x%x %s member %s", (POINTER)leg, leg->key, elesds);
            graph_leg *next = add_graph_leg(link, weight, leg, bsf_q, touches);
            if(next){
                dictEntry *nextDe = dictFind(db->dict, link);
                next->de = nextDe;
            }
        }
        sdsfreesplitres(parts, segments);
        sdsfreesplitres(link_parts, link_segments);
        sdsfree(link);
    }
    setTypeReleaseIterator(si);
    sdsfree(key);
    return numkeys;
}

int breadthFirstSearch(KeysetDescriptor *od, list *patterns, KeysetDescriptor *kd, int traverse_direction, short filter_only, dict *terminators, rax *includes, rax *excludes)
{
    int numkeys = 0;

    if (!od->keyset || dictSize(od->keyset) <= 0)
        load_key(REDIS_DB0, od->setname, od);

    if (od && od->keyset && dictSize(od->keyset) > 0)
    {
        list *bsf_q = listCreate();
        list *bsf_c = listCreate();
        dict *touches = dictCreate(rxShared->tokenDictType, (void *)NULL);
        // Seed the search
        dictEntry *de;
        dictIterator *di;
        di = dictGetSafeIterator(od->keyset);
        while ((de = dictNext(di)) != NULL)
        {
            sds key = dictGetKey(de);
            graph_leg *leg = add_graph_leg(key, 0.0, NULL, bsf_q, NULL);
            leg->start = (struct graph_leg *)leg;
            leg->de = de;
        }
        dictReleaseIterator(di);

        redisDb *db = REDIS_DB0;
        while (listLength(bsf_q))
        {
            graph_leg *leg = pop_leg(bsf_q);
            /////**/ serverLog(LL_NOTICE, "scan: 0x%x %0.0f %s 0x%x 0x%x  de=0x%x", (POINTER)leg, leg->length, leg->key, (POINTER)leg->start, (POINTER)leg->origin, (POINTER)leg->de);
            numkeys += matchEdges(db, leg, patterns, kd, bsf_q, touches, traverse_direction, filter_only, terminators, includes, excludes);
            listAddNodeHead(bsf_c, leg);
        }

        while (listLength(bsf_c))
        {
            graph_leg *leg = pop_leg(bsf_c);
            delete_graph_leg(leg);
        }
        listRelease(bsf_c);
        listRelease(bsf_q);
        dictRelease(touches);
    }
    return numkeys;
}
// Do a breadth first for any vertex or edge of type <out>
int executeGremlinTraverse(list *stack, int traverse_direction, short filterOnly)
{
    /* 1) Use top of stack = dict of keys/values.
       2) Filter all outgoing edges having the designated attribute value
       3) Push filtered dict on the stack
    */

    KeysetDescriptor *vd = pop(stack);
    KeysetDescriptor *od = pop(stack);
    list *patterns = listCreate();
    if (vd->parameter_list)
    {
        while (listLength(vd->parameter_list))
        {
            KeysetDescriptor *p = pop(vd->parameter_list);
            listAddNodeTail(patterns, sdsdup(p->setname));
            if (p->is_temp == KeysetDescriptor_TYPE_MONITORED_SET || p->value_type == KeysetDescriptor_TYPE_MONITORED_SET)
            {
                // /////**/ serverlog(LL_NOTICE, "Retained(3) 0x%x %s temp:%d type=%d d:0x%x", (POINTER)p, p->setname, p->is_temp, p->value_type, (POINTER)p->keyset);
                continue;
            }
            // deleteKeysetDescriptor(p);
        }
        listRelease(vd->parameter_list);
        vd->parameter_list = NULL;
        // deleteKeysetDescriptor(vd);
    }
    else
    {
        listAddNodeHead(patterns, sdsdup(vd->setname));
    }
    if (!od)
    {
        od = getAllKeysByRegex("[A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", MATCH_PATTERN, "edges");
    }

    sds set_name = sdscatprintf(sdsempty(), "%s->%s", od->setname, vd->setname);
    long long start = ustime();
    KeysetDescriptor *kd = getKeysetDescriptor(set_name, KeysetDescriptor_TYPE_GREMLINSET);
    kd->keyset = dictCreate(rxShared->tokenDictType, (void *)NULL);
    int numkeys = breadthFirstSearch(od, patterns, kd, traverse_direction, filterOnly, NULL, NULL, NULL);
    kd->latency = ustime() - start;
    kd->size = numkeys;
    propagate_set_type(kd, vd, od);
    push(stack, kd);
    listRelease(patterns);
    return C_OK;
}

// Do a breadth first for any vertex or edge of type <out>
int executeGremlinOut(Parser *p, Token *t, list *stack)
{
    rxUNUSED(p);
    rxUNUSED(t);
    return executeGremlinTraverse(stack, GRAPH_TRAVERSE_OUT, 0);
}

// Do a breadth first for any vertex or edge of type <out>
int executeGremlinIn(Parser *p, Token *t, list *stack)
{
    rxUNUSED(p);
    rxUNUSED(t);
    return executeGremlinTraverse(stack, GRAPH_TRAVERSE_IN, 0);
}

// Do a breadth first for any vertex or edge of type <out>
int executeGremlinInOut(Parser *p, Token *t, list *stack)
{
    rxUNUSED(p);
    rxUNUSED(t);
    return executeGremlinTraverse(stack, GRAPH_TRAVERSE_INOUT, 0);
}

// Do a breadth first to filter any vertex or edge with an <out> type
int executeGremlinHasOut(Parser *p, Token *t, list *stack)
{
    rxUNUSED(p);
    rxUNUSED(t);
    return executeGremlinTraverse(stack, GRAPH_TRAVERSE_OUT, 1);
}

// Do a breadth first to filter any vertex or edge with an <in> type
int executeGremlinHasIn(Parser *p, Token *t, list *stack)
{
    rxUNUSED(p);
    rxUNUSED(t);
    return executeGremlinTraverse(stack, GRAPH_TRAVERSE_IN, 1);
}

// Do a breadth first to filter any vertex or edge with an <in/out> type
int executeGremlinHasInOut(Parser *p, Token *t, list *stack)
{
    rxUNUSED(p);
    rxUNUSED(t);
    return executeGremlinTraverse(stack, GRAPH_TRAVERSE_INOUT, 1);
}

dict *dictCopy(dict *d)
{
    dict *c = dictCreate(rxShared->tokenDictType, (void *)NULL);
    dictEntry *de;
    dictIterator *di;
    di = dictGetSafeIterator(d);
    while ((de = dictNext(di)) != NULL)
    {
        sds key = dictGetKey(de);
        robj *o = dictGetVal(de);
        dictAdd(c, (void *)sdsdup(key), o);
    }
    dictReleaseIterator(di);
    return c;
}

// Pop the 'as' set
// When the top stack entry has a dictionary
// Then
//     a) push the that set
// Else
//     a) pop the
//     b) copy set from the stack
//     c) push the copied set
int executeGremlinAs(Parser *p, Token *t, list *stack)
{
    rxUNUSED(p);
    rxUNUSED(t);
    rxUNUSED(stack);
    KeysetDescriptor *sd = pop(stack);
    if (    sd->value_type == KeysetDescriptor_TYPE_MONITORED_SET 
        || ((sd->value_type & KeysetDescriptor_TYPE_GREMLIN_AS_SET) == KeysetDescriptor_TYPE_GREMLIN_AS_SET)
        )
    {
        // Reuse set
        sd->reuse_count++;
    }
    else
    {
        // Save set as
        KeysetDescriptor *vd = pop(stack);
        if (vd != NULL)
        {
            sd->keyset = dictCopy(vd->keyset);
            sd->size = dictSize(sd->keyset);
        }
        defaultKeysetDescriptorAsTemp(sd);
        sd->value_type = KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET + KeysetDescriptor_TYPE_GREMLIN_AS_SET;
    }
    push(stack, sd);
    return C_OK;
}

int executeGremlinMatchInExclude(Parser *p, Token *t, list *stack)
{
    rax *filter = raxNew();
    KeysetDescriptor *pl = pop(stack);
    if (pl->parameter_list)
    {
        while(listLength(pl->parameter_list) > 0 ){
            KeysetDescriptor *f = pop(pl->parameter_list);
            raxInsert(filter, (UCHAR*)f->setname, sdslen(f->setname), p, NULL);
            deleteKeysetDescriptor(f);
        }
        listRelease(pl->parameter_list);
        pl->parameter_list = NULL;
        // deleteKeysetDescriptor(pl);
    }
    else{
        raxInsert(filter, (UCHAR*)pl->setname, sdslen(pl->setname), p, NULL);
    }
    deleteKeysetDescriptor(pl);
    if(toupper(t->token[0]) == 'I'){
        p->matchIncludes = filter;
    }else{
        p->matchExcludes = filter;
    }
    // /**/ raxShow(filter);
    return C_OK;
}

int executeMatch(Parser *p, Token *t, list *stack)
{
    rxUNUSED(p);
    rxUNUSED(t);
    KeysetDescriptor *pl = pop(stack);
    if (pl->parameter_list)
    {
        list *cpy = listDup(pl->parameter_list);
        while (listLength(pl->parameter_list) >= 2)
        {
            executeAnd(p, t, pl->parameter_list);
        }

        KeysetDescriptor *u = pop(pl->parameter_list);
        if(u->size == 0){
            if(p->matchIncludes && p->matchExcludes){
                addError(p, "Cannot use an exclude and an include filter together!");
                return C_ERR;
            }

            KeysetDescriptor *terminators = pop(cpy);
            KeysetDescriptor *leaders = pop(cpy);
            dictIterator *ldi = dictGetSafeIterator(leaders->keyset);
            dictEntry *lde;
                dict *from = dictCreate(leaders->keyset->type, NULL);
                dict *to = dictCreate(terminators->keyset->type, NULL);
            sds temp_key = sdscatprintf(sdsempty(), "%s:::PATH:::%lld", t->token, ustime());
            KeysetDescriptor *tempD = getKeysetDescriptor(temp_key, KeysetDescriptor_TYPE_KEYSET);
            tempD->keyset = from;

            while ((lde = dictNext(ldi)))
            {
                if( sdscharcount(lde->key, ':') >= 2 )
                    continue;
                /////**/ serverLog(LL_NOTICE, "match outer: %s (%ld)", (char*)lde->key, dictSize(from));
                dictAdd(from, lde->key, lde->v.val);
                dictIterator *tdi = dictGetSafeIterator(terminators->keyset);
                dictEntry *tde;
                while ((tde = dictNext(tdi)))
                {
                    if( sdscharcount(tde->key, ':') >= 2 )
                        continue;
                    /////**/ serverLog(LL_NOTICE, "match inner: %s (%ld)", (char*)tde->key, dictSize(to));
                    dictAdd(to, tde->key, tde->v.val);
                    int n = breadthFirstSearch(tempD, NULL, u, GRAPH_TRAVERSE_INOUT, 1, to, p->matchIncludes, p->matchExcludes);
                    /**/ serverLog(LL_NOTICE, "match outer: %s (%ld) inner: %s (%ld) => %d result(s)", (char*)lde->key, dictSize(from), (char*)tde->key, dictSize(to), n);
                    dictDelete(to, tde->key);
                }
                dictDelete(from, lde->key);
                dictReleaseIterator(tdi);
            }
            dictReleaseIterator(ldi);
                dictRelease(to);
                deleteKeysetDescriptor(tempD);
                // TODO only two set to match
                // do
                // {
                // } while (listLength(cpy) > 0);
        }
        u->size = dictSize(u->keyset);
        push(stack, u);
        listRelease(cpy);
    }
    else
        push(stack, pl);
    // deleteKeysetDescriptor(pl);
    if (p->matchIncludes)
    {
        raxFree(p->matchIncludes);
        p->matchIncludes = NULL;
    }
    if(    p->matchExcludes){
        raxFree(p->matchExcludes);
        p->matchExcludes = NULL;
    }

    return C_OK;
}

int executeNomatch(Parser *p, Token *t, list *stack)
{
    rxUNUSED(p);
    rxUNUSED(t);
    KeysetDescriptor *pl = pop(stack);
    if (pl->parameter_list)
    {
        while (listLength(pl->parameter_list) >= 2)
        {
            executeNotIn(p, t, pl->parameter_list);
        }

        push(stack, pop(pl->parameter_list));
        // deleteKeysetDescriptor(pl);
    }
    else
        push(stack, pl);
    return C_OK;
}

KeysetDescriptor *createVertex(Parser *p, list *pl)
{
    if (pl == NULL || listLength(pl) < 2)
    {
        addError(p, "Incorrect parameters for AddV(ertex), must be <iri>, <type>");
        return NULL;
    }
    KeysetDescriptor *i = dequeue(pl);
    KeysetDescriptor *t = dequeue(pl);
    redis_HSET(p, i->setname, Graph_vertex_or_edge_type, t->setname);
    robj *o = findKey(REDIS_DB0, i->setname);
    KeysetDescriptor *v = getKeysetDescriptor(i->setname, KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET);
    v->keyset = dictCreate(rxShared->tokenDictType, (void *)NULL);
    dictAdd(v->keyset, (void *)sdsdup(i->setname), o);
    return v;
}

int executeGremlinAddVertex(Parser *p, Token *tok, list *stack)
{
    rxUNUSED(tok);
    // Get parameters g:addV(<iri>, <type>)
    // Get parameters g:e().addV(<iri>, <type>)
    KeysetDescriptor *pl = pop(stack);
    KeysetDescriptor *v = createVertex(p, pl->parameter_list);
    v->vertices_or_edges = VERTEX_SET;
    push(stack, v);
    return C_OK;
}

/*
g.V(1).as('a').out('created').in('created').where(neq('a')).
  addE('co-developer').from('a').property('year',2009) //// (1)
g.V(3,4,5).aggregate('x').has('name','josh').as('a').
  select('x').unfold().hasLabel('software').addE('createdBy').to('a') //// (2)
g.V().as('a').out('created').addE('createdBy').to('a').property('acl','public') //// (3)
g.V(1).as('a').out('knows').
  addE('livesNear').from('a').property('year',2009).
  inV().inE('livesNear').values('year') //// (4)
g.V().match(
        __.as('a').out('knows').as('b'),
        __.as('a').out('created').as('c'),
        __.as('b').out('created').as('c')).
      addE('friendlyCollaborator').from('a').to('b').
        property(id,23).property('project',select('c').values('name')) //// (5)
g.E(23).valueMap()
vMarko = g.V().has('name','marko').next()
vPeter = g.V().has('name','peter').next()
g.V(vMarko).addE('knows').to(vPeter) //// (6)
g.addE('knows').from(vMarko).to(vPeter) //7
*/
int executeGremlinAddEdge(Parser *p, Token *tok, list *stack)
{
    rxUNUSED(tok);
    // Get parameters g:addE(<iri>, <type>)
    // Get parameters g:v().addV(<iri>, <type>)
    KeysetDescriptor *t = pop(stack);
    size_t no_vertex_parms = t->parameter_list ? listLength(t->parameter_list) : 0;
    if (t->parameter_list && no_vertex_parms >= 3)
    {
        // addE(dge)(<subject>, <type>, <object>)
        KeysetDescriptor *s = dequeue(t->parameter_list);    // iri of subject
        KeysetDescriptor *pred = dequeue(t->parameter_list); // predicate(type)
        KeysetDescriptor *inv_pred;
        sds backLink = sdsempty();
        if (no_vertex_parms >= 4)
            inv_pred = dequeue(t->parameter_list);
        else
            inv_pred = pred;
        KeysetDescriptor *o = dequeue(t->parameter_list); // iri of subject

        sds edge_name = sdscatprintf(sdsempty(), "%s:%s:%s", pred->setname, s->setname, o->setname);
        sds edge_link = sdscatprintf(sdsempty(), "^%s", edge_name);
        sds inv_edge_name;
        sds inv_edge_link;
        if (pred == inv_pred)
        {
            inv_edge_name = edge_name;
            inv_edge_link = edge_link;
        }
        else
        {
            inv_edge_name = sdscatprintf(sdsempty(), "%s:%s:%s", inv_pred->setname, o->setname, s->setname);
            inv_edge_link = sdscatprintf(sdsempty(), "^%s", inv_edge_name);
        }
        sds subject_link = sdscatprintf(sdsempty(), "^%s", s->setname);
        sds object_link = sdscatprintf(sdsempty(), "^%s", o->setname);
        if (pred != inv_pred)
        {
            backLink = sdscatprintf(backLink, "%s:%s:%s:%s", s->setname, pred->setname, inv_pred->setname, o->setname);
        }

        sds bridge_keys[2] = {edge_name, inv_edge_name};
        KeysetDescriptor *bridges[2] = {pred, inv_pred};
        for (int j = 0; j < 2; ++j)
        {
            redis_HSET(p, bridge_keys[j], Graph_vertex_or_edge_type, bridges[j]->setname);
            if (sdslen(backLink))
            {
                redis_HSET(p, bridge_keys[j], "edge", backLink);
            }
        }

        sds SE = sdscatprintf(sdsempty(), "%s|%s|SE|1.0|+", pred->setname, edge_link);
        sds ES = sdscatprintf(sdsempty(), "%s|^%s|ES|1.0", inv_pred->setname, s->setname);
        sds EO = sdscatprintf(sdsempty(), "%s|%s|OE|1.0", inv_pred->setname, inv_edge_link);
        sds OE = sdscatprintf(sdsempty(), "%s|^%s|EO|1.0", pred->setname, o->setname);

        sds keys[4] = {subject_link, edge_link, inv_edge_link, object_link};
        sds member[4] = {SE, OE, ES, EO};
        for (int j = 0; j < 4; ++j)
        {
            redis_SADD(p, keys[j], member[j]);
        }
        KeysetDescriptor *e = getKeysetDescriptor(edge_name, KeysetDescriptor_TYPE_GREMLIN_EDGE_SET);
        e->keyset = dictCreate(rxShared->tokenDictType, (void *)NULL);
        robj *oo = findKey(REDIS_DB0, edge_name);
        dictAdd(e->keyset, (void *)sdsdup(edge_name), oo);
        e->vertices_or_edges = EDGE_SET;
        push(stack, e);
        // deleteKeysetDescriptor(t);
    }
    else
    {
        // addE(dge)(<type>)
        // Create temp key
        // the temp key will be renamed from the to(subject)/from(object) vertices
        sds temp_key = sdscatprintf(sdsempty(), "%s:::%lld", t->setname, ustime());
        redis_HSET(p, temp_key, Graph_vertex_or_edge_type, t->setname);
        KeysetDescriptor *v = getKeysetDescriptor(sdsnew(sdsdup(temp_key)), KeysetDescriptor_TYPE_GREMLIN_EDGE_SET);
        v->keyset = dictCreate(rxShared->tokenDictType, (void *)NULL);
        robj *oo = findKey(REDIS_DB0, temp_key);
        dictAdd(v->keyset, (void *)sdsdup(temp_key), oo);

        v->vertices_or_edges = EDGE_SET;
        push(stack, v);
        return C_OK;
    }
    return C_OK;
}

/*
    IN:  (0) <vertex> := <iri>[, <type>]
        (-1) <edge set>
    OUT: (0) <edge set>

    {from|subject}({ <iri>[,<type>] | <set> })
    {to|object}({ <iri>[,<type>] | <set> })
*/
int executeGremlinLinkVertex(Parser *p, Token *tok, list *stack)
{
    sds forward_edge;
    sds backward_edge;
    if (strcmp(tok->token, "to") == 0 || strcmp(tok->token, "object") == 0)
    {
        forward_edge = sdsnew("SE");
        backward_edge = sdsnew("ES");
    }
    else
    {
        forward_edge = sdsnew("OE");
        backward_edge = sdsnew("EO");
    }
    KeysetDescriptor *v = pop(stack);
    KeysetDescriptor *e = pop(stack);

    dict *vertex_set = NULL;
    if (v->parameter_list && listLength(v->parameter_list) == 2)
    {
        KeysetDescriptor *nv = createVertex(p, v->parameter_list); // Use new vertex!
        vertex_set = nv->keyset;
    }
    else
    {
        // Use existing vertex!
        vertex_set = dictCreate(rxShared->tokenDictType, (void *)NULL);
        robj *o = findKey(REDIS_DB0, v->setname);
        dictAdd(vertex_set, (void *)sdsdup(sdsnew(v->setname)), o);
    }
    dictEntry *e_de;
    dictEntry *v_de;
    dictIterator *e_di = dictGetSafeIterator(e->keyset);
    while ((e_de = dictNext(e_di)) != NULL)
    {
        sds e_key = dictGetKey(e_de);
        robj *e = findKey(REDIS_DB0, e_key);
        unsigned char *vstr;
        POINTER vlen;
        long long vll;
        hashTypeGetValue(e, Graph_vertex_or_edge_type, &vstr, &vlen, &vll);
        sds edge_type = sdsnewlen(vstr, vlen);
        dictIterator *v_di = dictGetSafeIterator(vertex_set);
        while ((v_de = dictNext(v_di)) != NULL)
        {
            sds v_key = dictGetKey(v_de);
            // robj *v = dictGetVal(v_de);

            // serverLog(LL_NOTICE, "rename? e:%s p:%s s|o:%s", e_key, tok->token, v_key);
            sds edge_key = sdscatfmt(sdsempty(), "^%s", e_key);
            sds vertex_key = sdscatfmt(sdsempty(), "^%s", v_key);
            sds link = sdscatfmt(sdsempty(), "%s|%s|%s|1.0", edge_type, edge_key, forward_edge);

            // serverLog(LL_NOTICE, "%s | %s | %s", e_key, v_key, link);
            redis_SADD(p, vertex_key, link);
            link = sdscatfmt(sdsempty(), "%s|%s|%s|1.0", edge_type, vertex_key, backward_edge);

            // serverLog(LL_NOTICE, "%s | %s | %s", e_key, v_key, link);
            redis_SADD(p, edge_key, link);
        }
        dictReleaseIterator(v_di);
    }
    dictReleaseIterator(e_di);

    push(stack, e);
    e->vertices_or_edges = VERTEX_SET;
    return C_OK;
}

int executeGremlinAddProperty(Parser *p, Token *t, list *stack)
{
    rxUNUSED(t);
    KeysetDescriptor *pl = pop(stack);
    KeysetDescriptor *s = pop(stack);

    if (s->keyset && pl->parameter_list)
    {
        if ((listLength(pl->parameter_list) & 1) == 1)
        {
            addError(p, "Incorrect parameters for AddProperty, must be [<a>, <v>]...");
            return C_ERR;
        }
        dictEntry *de;
        dictIterator *di = dictGetSafeIterator(s->keyset);
        while ((de = dictNext(di)) != NULL)
        {
            sds key = dictGetKey(de);
            while (listLength(pl->parameter_list) >= 2)
            {
                KeysetDescriptor *a = dequeue(pl->parameter_list);
                KeysetDescriptor *v = dequeue(pl->parameter_list);
                if (p->module_contex)
                    redis_HSET(p, key, a->setname, v->setname);
                else
                {
                    robj *o = dictGetVal(de);
                    hashTypeSet(o, a->setname, v->setname, 0);
                }
            }
        }
        dictReleaseIterator(di);
        // deleteKeysetDescriptor(s);
    }
    else
    {
        addError(p, "Incorrect parameters for AddProperty, must be [<a>, <v>]...");
        return C_ERR;
    }
    push(stack, s);
    return C_OK;
}

int executeGremlinParameters(Parser *p, Token *t, list *stack)
{
    rxUNUSED(p);
    rxUNUSED(t);
    if (listLength(stack) < 2)
    {
        addError(p, "',' requires 2 tokens.");
        return C_ERR;
    }

    KeysetDescriptor *s = pop(stack);
    KeysetDescriptor *s_l = pop(stack);
    if (!s_l->parameter_list)
    {
        // two sets parameter on stack
        sds setname = sdscatprintf(sdsempty(), "P_%lld", ustime());
        KeysetDescriptor *pl = getKeysetDescriptor(setname, KeysetDescriptor_TYPE_PARAMETER_LIST);
        defaultKeysetDescriptorAsTemp(pl);
        pl->is_temp = 1;
        pl->parameter_list = listCreate();
        listAddNodeHead(pl->parameter_list, s_l);
        listAddNodeHead(pl->parameter_list, s);
        push(stack, pl);
        sdsfree(setname);
        return C_OK;
    }
    // parameter list and set on stack
    listAddNodeHead(s_l->parameter_list, s);
    push(stack, s_l);
    return C_OK;
}
/* * * * * * * End of Gremlin Operations * * * * * * */

void initOperationsStatic()
{
    if (!rxShared)
        rxShared = getRxSuite();
    if (!rxShared->KeysetCache)
        rxShared->KeysetCache = dictCreate(&resultSetDescriptorDictType, (void *)NULL);
    if (!rxShared->OperationMap)
        rxShared->OperationMap = dictCreate(rxShared->tokenDictType, (void *)NULL);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("-"), &executePlusMinus);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("+"), &executePlusMinus);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("="), &executeStore);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("=="), &executeEquals);
    dictAdd(rxShared->OperationMap, (void *)sdsnew(">="), &executeEquals);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("<="), &executeEquals);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("<"), &executeEquals);
    dictAdd(rxShared->OperationMap, (void *)sdsnew(">"), &executeEquals);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("!="), &executeEquals);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("|"), &executeOr);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("&"), &executeAnd);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("|&"), &executeXor);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("not"), &executeNotIn);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("!"), &executeNotIn);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("g"), &executeGremlin);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("v"), &executeAllVertices);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("V"), &executeAllVertices);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("e"), &executeAllEdges);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("E"), &executeAllEdges);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("as"), &executeGremlinAs);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("has"), &executeGremlinHas);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("hasLabel"), &executeGremlinHasLabel);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("missing"), &executeGremlinHas);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("missingLabel"), &executeGremlinHasLabel);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("by"), &executeGremlinGroupby);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("out"), &executeGremlinOut);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("in"), &executeGremlinIn);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("inout"), &executeGremlinInOut);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("hasout"), &executeGremlinHasOut);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("hasin"), &executeGremlinHasIn);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("hasinout"), &executeGremlinHasInOut);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("match"), &executeMatch);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("nomatch"), &executeNomatch);
	dictAdd(rxShared->OperationMap, (void *)sdsdup(sdsnew("incl")), &executeGremlinMatchInExclude);
	dictAdd(rxShared->OperationMap, (void *)sdsdup(sdsnew("include")), &executeGremlinMatchInExclude);
	dictAdd(rxShared->OperationMap, (void *)sdsdup(sdsnew("excl")), &executeGremlinMatchInExclude);
	dictAdd(rxShared->OperationMap, (void *)sdsdup(sdsnew("exclude")), &executeGremlinMatchInExclude);
    dictAdd(rxShared->OperationMap, (void *)sdsnew(","), &executeGremlinParameters);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("."), &executeGremlinNoop);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("addEdge"), &executeGremlinAddEdge);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("addE"), &executeGremlinAddEdge);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("property"), &executeGremlinAddProperty);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("addVertex"), &executeGremlinAddVertex);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("addV"), &executeGremlinAddVertex);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("to"), &executeGremlinLinkVertex);
    // dictAdd(rxShared->OperationMap, (void *)sdsnew("object"), &executeGremlinLinkVertex);
    // dictAdd(rxShared->OperationMap, (void *)sdsnew("from"), &executeGremlinLinkVertex);
    dictAdd(rxShared->OperationMap, (void *)sdsnew("subject"), &executeGremlinLinkVertex);
}

long long setDirty()
{
    index_info_qe.dirty++;
    return index_info_qe.dirty; // + server.dirty;
}

int getDirty()
{
    return index_info_qe.dirty; // + server.dirty;
}

void selectDatabase(sds db_id)
{
    index_info_qe.database_id = db_id;
}

int connectIndex(Parser *p, RedisModuleCtx *ctx, char *h, int port)
{
    p->module_contex = ctx;
    index_info_qe.index_address = sdsnew(h);
    index_info_qe.index_port = port;
    index_info_qe.index_context = redisConnect(h, port);
    p->index_context = index_info_qe.index_context;
    if (p->index_context == NULL || p->index_context->err)
    {
        if (p->index_context)
        {
            serverLog(LL_NOTICE, "Connection error: %s", p->index_context->errstr);
            redisFree(p->index_context);
            return C_ERR;
        }
        else
        {
            serverLog(LL_NOTICE, "Connection error: can't allocate lzf context");
            return C_ERR;
        }
        return C_ERR;
    }
    return C_OK;
}

KeysetDescriptor *findKeysetDescriptor(sds sn)
{
    if (rxShared == NULL || rxShared->KeysetCache == NULL)
        initOperationsStatic();
    dictEntry *de = dictFind(rxShared->KeysetCache, sn);
    KeysetDescriptor *kd;
    if (de)
    {
        kd = (KeysetDescriptor *)(KeysetDescriptor *)de->v.val;
        // serverLog(LL_NOTICE, "#findKeysetDescriptor#1 0x%x nm=%s reuse=%d", (POINTER) kd, kd->setname, kd->reuse_count);
        return kd;
    }
    else
    {
        return NULL;
    }
    // serverLog(LL_NOTICE, "#findKeysetDescriptor#2 0x%x nm=%s reuse=%d", (POINTER) kd, kd->setname, kd->reuse_count);
    return kd;
}

KeysetDescriptor *getKeysetDescriptor(sds sn, char value_type)
{
    KeysetDescriptor *kd = findKeysetDescriptor(sn);
    int add_cnt = getDirty();
    if (kd)
    {
        kd->reuse_count++;
    }
    else
    {
        kd = (KeysetDescriptor *)zmalloc(sizeof(KeysetDescriptor));
        kd->setname = sdsdup(sn);
        kd->is_temp = 0;
        kd->is_on_stack = 0;
        kd->keyset = NULL;
        kd->value_type = value_type;
        kd->vertices_or_edges = GENERIC_SET;
        kd->reuse_count = 0;
        kd->size = 0;
        kd->creationTime = ustime();
        kd->latency = 0;
        kd->dirty = add_cnt;
        kd->left = NULL;
        kd->right = NULL;
        kd->parameter_list = NULL;
        dictAdd(rxShared->KeysetCache, (void *)sdsdup(sn), kd);
    }
    // serverLog(LL_NOTICE, "#getKeysetDescriptor#  0x%x nm=%s reuse=%d", (POINTER) kd, kd->setname, kd->reuse_count);
    // /////**/ serverlog(LL_NOTICE, "Acquired 0x%x %s temp:%d type=%d d:0x%x", (POINTER)kd, kd->setname, kd->is_temp, kd->value_type, (POINTER)kd->keyset);
    return kd;
}

KeysetDescriptor *getKeysetDescriptor2(Parser *p, Token *t, list *stack, int load_left_and_or_right)
{
    KeysetDescriptor *l = pop(stack);
    if (!l->keyset && (load_left_and_or_right & QE_LOAD_LEFT_ONLY))
        fetchKeyset(p->index_context, l, NULL, l->setname, NULL);

    KeysetDescriptor *r = pop(stack);
    if (!r->keyset && (load_left_and_or_right & QE_LOAD_RIGHT_ONLY))
        fetchKeyset(p->index_context, r, NULL, r->setname, NULL);

    int do_swap = ((load_left_and_or_right && QE_SWAP_LARGEST_FIRST) && (dictSize(l->keyset) < dictSize(r->keyset))) || ((load_left_and_or_right && QE_SWAP_SMALLEST_FIRST) && (dictSize(l->keyset) > dictSize(r->keyset)));
    if (do_swap)
    {
        KeysetDescriptor *swap = l;
        l = r;
        r = swap;
    }

    sds keyset_name = sdscatfmt(sdsempty(), "%s %s %s", l->setname, t->token, r->setname);
    KeysetDescriptor *kd = getKeysetDescriptor(keyset_name, KeysetDescriptor_TYPE_KEYSET);
    kd->left = l;
    kd->right = r;
    kd->start = ustime();

    if (load_left_and_or_right && QE_CREATE_SET)
    {
        kd->keyset = dictCreate(rxShared->tokenDictType, (void *)NULL);
        kd->value_type = KeysetDescriptor_TYPE_KEYSET;
    }
    // serverLog(LL_NOTICE, "#getKeysetDescriptor2# 0x%x nm=%s reuse=%d", (POINTER) kd, kd->setname, kd->reuse_count);
    return kd;
}

void putKeysetDescriptor(list *stack, KeysetDescriptor *kd, dictIterator *iter)
{
    if(iter)
        dictReleaseIterator(iter);
    kd->size = dictSize(kd->keyset);
    kd->latency = ustime() - kd->start;
    push(stack, kd);
    // serverLog(LL_NOTICE, "#putKeysetDescriptor2# 0x%x nm=%s reuse=%d", (POINTER) kd, kd->setname, kd->reuse_count);
}

void push(list *stack, KeysetDescriptor *d)
{
    listAddNodeHead(stack, d);
    d->is_on_stack++;
}

sds sdspop(list *stack)
{
    sds d = (sds)removeTokenAt(stack, 0);
    return d;
}

KeysetDescriptor *pop(list *stack)
{
    KeysetDescriptor *d = (KeysetDescriptor *)removeTokenAt(stack, 0);
    if (d)
        d->is_on_stack--;
    return d;
}

Graph_Triplet *pop_Graph_Triplet(list *stack)
{
    Graph_Triplet *d = (Graph_Triplet *)removeTokenAt(stack, 0);
    return d;
}

KeysetDescriptor *dequeue(list *stack)
{
    KeysetDescriptor *d = (KeysetDescriptor *)removeTokenAt(stack, listLength(stack) - 1);
    d->is_on_stack--;
    return d;
}

void push_leg(list *traversal, graph_leg *d)
{
    listAddNodeHead(traversal, d);
}

graph_leg *pop_leg(list *traversal)
{
    graph_leg *d = (graph_leg *)removeTokenAt(traversal, 0);
    return d;
}

sds getCacheReport()
{
    if (!rxShared)
        return sdsempty();
    if (!rxShared->KeysetCache)
        return sdsempty();
    int add_cnt = getDirty();

    sds report = sdscatprintf(sdsempty(), "%16s\t%9s\t%9s\t%6s\t%6s\t%6s\t%6s\t%s\r",
                              "created",
                              "dirty",
                              "latency",
                              "Re-use",
                              "Tally",
                              "temp",
                              "monitor",
                              "name");
    report = sdscatprintf(report, "%16s\t%9s\t%9s\t%6s\t%6s\t%6s\t%6s\t%s\r",
                          "----------------",
                          "---------",
                          "---------",
                          "------",
                          "------",
                          "------",
                          "------",
                          "------------------------------------------------------------------------------------------");
    dictIterator *iter = dictGetSafeIterator(rxShared->KeysetCache);
    dictEntry *de;
    while ((de = dictNext(iter)))
    {
        KeysetDescriptor *kd = de->v.val;
        report = sdscatprintf(report, "%16lld\t%9d\t%9lld\t%6d\t%6d\t%6d\t%6d\t%s\r", kd->creationTime, add_cnt - kd->dirty, kd->latency, kd->reuse_count, kd->size, kd->is_temp, kd->value_type, kd->setname);
    }
    dictReleaseIterator(iter);
    return report;
}

void releaseCacheReport(sds info)
{
    sdsfree(info);
}

void clearCache()
{
    if (!rxShared)
        return;
    if (!rxShared->KeysetCache)
        return;

    deleteAllKeysetDescriptors();
}

void *deleteKeysetDescriptor(KeysetDescriptor *kd)
{
    // serverLog(LL_NOTICE, "#deleteKeysetDescriptor# 0x%x nm=%s reuse=%d", (POINTER) kd, kd->setname, kd->reuse_count);
    // /////**/ serverlog(LL_NOTICE, "Deleting 0x%x %s temp:%d type=%d d:0x%x", (POINTER)kd, kd->setname, kd->is_temp, kd->value_type, (POINTER)kd->keyset);
    if (kd->is_temp == KeysetDescriptor_TYPE_MONITORED_SET || kd->value_type == KeysetDescriptor_TYPE_MONITORED_SET)
    {
        // /////**/ serverlog(LL_NOTICE, "panic(1) 0x%x %s temp:%d type=%d d:0x%x", (POINTER)kd, kd->setname, kd->is_temp, kd->value_type, (POINTER)kd->keyset);
        return NULL;
    }

    sds setname = kd->setname;
    if (kd->keyset)
    {
        dictRelease(kd->keyset);
        kd->keyset = NULL;
    }
    if (kd->parameter_list)
    {
        listRelease(kd->parameter_list);
        kd->parameter_list = NULL;
    }
    dictDelete(rxShared->KeysetCache, setname);
    // sdsfree(setname);
    return NULL;
}

void dictDeleteDescriptor(void *privdata, void *val)
{
    rxUNUSED(privdata);
    if (val)
        zfree(val);
}

void deleteAllTempKeysetDescriptors()
{
    dictIterator *iter = dictGetSafeIterator(rxShared->KeysetCache);
    dictEntry *de;
    while ((de = dictNext(iter)))
    {
        KeysetDescriptor *kd = de->v.val;
        if (kd->is_temp == KeysetDescriptor_TYPE_MONITORED_SET || kd->value_type == KeysetDescriptor_TYPE_MONITORED_SET)
        {
            // ///**/ serverlog(LL_NOTICE, "Retained(1) 0x%x %s temp:%d type=%d d:0x%x", (POINTER)kd, kd->setname, kd->is_temp, kd->value_type, (POINTER)kd->keyset);
            continue;
        }
        else if (kd->is_temp == 1 && kd->value_type != KeysetDescriptor_TYPE_MONITORED_SET)
        {
            deleteKeysetDescriptor(kd);
        }
    }
    dictReleaseIterator(iter);
}

void deleteAllKeysetDescriptors()
{
    dictIterator *iter = dictGetSafeIterator(rxShared->KeysetCache);
    dictEntry *de;
    while ((de = dictNext(iter)))
    {
        KeysetDescriptor *kd = de->v.val;
        if (kd->is_temp == KeysetDescriptor_TYPE_MONITORED_SET || kd->value_type == KeysetDescriptor_TYPE_MONITORED_SET)
        {
            // ///**/ serverlog(LL_NOTICE, "Retained(2) 0x%x %s temp:%d type=%d", (POINTER)kd, kd->setname, kd->is_temp, kd->value_type);
            continue;
        }
        if (kd->is_temp != KeysetDescriptor_TYPE_MONITORED_SET)
        {
            deleteKeysetDescriptor(kd);
        }
    }
    dictReleaseIterator(iter);
}

sds get_uml()
{
    sds result = sdsnew("@StartUml\n");

    redisDb *db = REDIS_DB0;
    dictIterator *di = dictGetSafeIterator(db->dict);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL)
    {
        sds key = dictGetKey(de);
        if (key[0] == '^')
            continue;
        sds ref = sdsmapchars(sdsdup(key), "^-:|/", "_____", 5);
        ref = sdscat(sdsnew("V"), ref);
        robj *o = dictGetVal(de);
        if (!o)
            continue;
        result = sdscatprintf(result, "\nobject \"%s\" as %s %s", key, sdsmapchars(ref,"^-:|/", "_____", 5), (key[0] == '^') ? "#20c0ff" : "#80ffff");
    }
    dictReleaseIterator(di);
    di = dictGetSafeIterator(db->dict);
    int tally = 0;
    while ((de = dictNext(di)) != NULL)
    {
        sds key = dictGetKey(de);
        // sds ref = sdsmapchars(sdsdup(key), "^-:|/", "_____", 5);
        // ref = sdscat(sdsnew("V"), ref);
        sds ref = sdsmapchars(sdsdup(key), "^-:|/", "V____", 5);
        if (key[0] != '^')
            ref = sdscat(sdsnew("V"), ref);
        robj *o = dictGetVal(de);
        if (!o)
            continue;
        if (o)
        {
            sds elesds;
            int64_t intobj;
            hashTypeIterator *hi;
            setTypeIterator *si;

            switch (o->type)
            {
            case 4: // HASH
                hi = hashTypeInitIterator(o);
                while (hashTypeNext(hi) != C_ERR)
                {
                    sds field = hashTypeCurrentObjectNewSds(hi, OBJ_HASH_KEY);
                    sds value = hashTypeCurrentObjectNewSds(hi, OBJ_HASH_VALUE);

                    result = sdscatprintf(result, "\n%s : %s = \"%s\"", ref, field, value);
                    if (sdscmp(field, sdsnew("edge")) == 0)
                    {
                        sds edgy = sdsmapchars(value, "^-:|/", "V____", 5);
                        result = sdscatprintf(result, "\n%s .[#FF6060]. %s", ref, sdscat(sdsnew("V"), edgy));
                    }
                }
                hashTypeReleaseIterator(hi);
                break;
            case 2: // SET
                si = setTypeInitIterator(o);
                while (setTypeNext(si, &elesds, &intobj) != -1)
                {
                    result = sdscatprintf(result, "\n%s : member = \"%s\"", ref, elesds);
                }
                setTypeReleaseIterator(si);
                break;
            }
        }
        if(++tally > 100)
            break;
    }
    dictReleaseIterator(di);
    di = dictGetSafeIterator(db->dict);
    tally = 0;
    while ((de = dictNext(di)) != NULL)
    {
        sds key = dictGetKey(de);
        sds ref = sdsmapchars(sdsdup(key), "^-:|/", "V____", 5);
        if (key[0] == '^')
            ref = sdsmapchars(sdsdup(key), "^-:|/", "V____", 5);
        else
            ref = sdscat(sdsnew("V"), ref);
        robj *o = dictGetVal(de);
        if (!o)
            continue;
        if (o)
        {
            sds elesds;
            int64_t intobj;
            setTypeIterator *si;

            switch (o->type)
            {
            case 2: // SET
                si = setTypeInitIterator(o);
                while (setTypeNext(si, &elesds, &intobj) != -1)
                {
                    int segments = 0;
                    sds *parts = sdssplitlen(elesds, sdslen(elesds), "|", 1, &segments);

                    sds annex = sdsmapchars(sdsdup(parts[1]), "^-:|/", "V____", 5);
                    // annex = sdscat(sdsnew("V"), annex);
                    sds lineColor = sdsnew("DD000");
                    sds direction = sdsnew("??");
                    if (sdscmp(parts[2], Graph_Subject_to_Edge) == 0)
                    {
                        lineColor = sdsnew("00ff00");
                        direction = sdsnew("OUT");
                    }
                    else if (sdscmp(parts[2], Graph_Edge_to_Object) == 0)
                    {
                        lineColor = sdsnew("006000");
                        direction = sdsnew("OUT");
                    }
                    else if (sdscmp(parts[2], Graph_Edge_to_Subject) == 0)
                    {
                        lineColor = sdsnew("0080ff");
                        direction = sdsnew("IN");
                    }
                    else if (sdscmp(parts[2], Graph_Object_to_Edge) == 0)
                    {
                        lineColor = sdsnew("004080");
                        direction = sdsnew("IN");
                    }
                    result = sdscatprintf(result, "\n %s -[#%s]-> %s : %s %s %s", annex, lineColor, ref, parts[0], parts[2], direction);
                    sdsfreesplitres(parts, segments);
                }
                setTypeReleaseIterator(si);
                break;
            }
        }
        if(++tally > 100)
            break;
    }
    dictReleaseIterator(di);

    result = sdscatprintf(result, "\n@EndUml\n");
    return result;
}