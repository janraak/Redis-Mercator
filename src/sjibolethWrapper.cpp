
#include <iostream>
#include <algorithm>
#include <vector>

#ifdef __cplusplus
extern "C"
{
#endif

#include "rxSuite.h"
#include "sjiboleth.h"
#include <string.h>
#include "sdsWrapper.h"
#define REDISMODULE_EXPERIMENTAL_API
#include "../../src/redismodule.h"

#include "zmalloc.h"

#ifdef __cplusplus
}
#endif
#include "sjiboleth-fablok.hpp"
#include "sjiboleth.hpp"
#include "sjiboleth-graph.hpp"

const char *REDIS_CMD_SADD = "SADD";
const char *REDIS_CMD_SMEMBERS = "SMEMBERS";

const char *REDIS_CMD_HSET = "HSET";
const char *REDIS_CMD_HGETALL = "HGETALL";
const char *REDIS_CMD_GET = "GET";
const char *OK = "OK";

#define HASHTYPE 'H'
#define STRINGTYPE 'S'
#define LISTTYPE 'L'
#define TRIPLET 'T'
#define TRIPLET 'T'

CSjiboleth *newQueryEngine()
{
    QueryDialect *qd = new QueryDialect();
    return (CSjiboleth *)qd;
}

CSjiboleth *newJsonEngine()
{
    JsonDialect *qd = new JsonDialect();
    return (CSjiboleth *)qd;
}

CSjiboleth *newTextEngine()
{
    TextDialect *qd = new TextDialect();
    return (CSjiboleth *)qd;
}

CSjiboleth *newGremlinEngine()
{
    GremlinDialect *qd = new GremlinDialect();
    return (CSjiboleth *)qd;
}

CParserToken *lookupToken(CSjiboleth *s, rxString token)
{
    return ((Sjiboleth *)s)->LookupToken(token);
}

CParsedExpression *parseQ(CSjiboleth *s, const char *query)
{
    Sjiboleth *so = (Sjiboleth *)s;
    return so->Parse(query);
}

int parsedWithErrors(CParsedExpression *sO)
{
    ParsedExpression *s = (ParsedExpression *)sO;

    return s->HasErrors() ? C_ERR : C_OK;
}

int writeParsedErrors(CParsedExpression *sO, RedisModuleCtx *ctx)
{
    ParsedExpression *s = (ParsedExpression *)sO;
    s->writeErrors(ctx);
    return C_OK;
}

rax *executeQ(CParsedExpression *s, redisNodeInfo *serviceConfig, RedisModuleCtx *module_context, list **errors)
{
    ParsedExpression *so = (ParsedExpression *)s;
    auto *e = new SilNikParowy_Kontekst(serviceConfig, module_context);
    rax *result = e->Execute(so);
    if (errors)
    {
        *errors = e->Errors();
    }
    return result;
}

CParsedExpression *releaseQuery(CParsedExpression *s)
{
    ParsedExpression *so = (ParsedExpression *)s;
    delete so;
    return NULL;
}

CSjiboleth *releaseParser(CSjiboleth */*s*/)
{
    // Sjiboleth *so = (Sjiboleth *)s;
    // delete so;
    return NULL;
}

int HasMinimumStackEntries(CSilNikParowy_Kontekst *stackO, int size)
{
    auto *stack = (SilNikParowy_Kontekst *)stackO;
    return stack->Size() >= size ? C_OK : C_ERR;
}

CFaBlok *PopStack(CSilNikParowy_Kontekst *stackO)
{
    auto *stack = (SilNikParowy_Kontekst *)stackO;
    return stack->Pop();
}

int PushStack(CSilNikParowy_Kontekst *stackO, CFaBlok *entryO)
{
    auto *stack = (SilNikParowy_Kontekst *)stackO;
    auto *entry = (FaBlok *)entryO;
    stack->Push(entry);
    return stack->Size();
}

CFaBlok *GetOperationPair(CParserToken *operationO, CSilNikParowy_Kontekst *stackO, int load_left_and_or_right)
{
    auto *stack = (SilNikParowy_Kontekst *)stackO;
    auto *operation = (ParserToken *)operationO;
    auto *pair = stack->GetOperationPair(operation->TokenAsSds(), load_left_and_or_right);
    return (CFaBlok *)pair;
}

void PushResult(CFaBlok *hereO, CSilNikParowy_Kontekst *stackO)
{
    auto *stack = (SilNikParowy_Kontekst *)stackO;
    auto *here = (FaBlok *)hereO;
    here->PushResult(stack);
    here->Close();
}

CFaBlok *GetRight(CFaBlok *hereO)
{
    FaBlok *here = (FaBlok *)hereO;
    return here->Right();
}

CFaBlok *GetLeft(CFaBlok *hereO)
{
    FaBlok *here = (FaBlok *)hereO;
    return here->Left();
}

int CopyKeySet(CFaBlok *inO, CFaBlok *outO)
{
    FaBlok *in = (FaBlok *)inO;
    FaBlok *out = (FaBlok *)outO;
    return in->CopyTo(out);
}

int FetchKeySet(CSilNikParowy_Kontekst *pO, CFaBlok *outO, CFaBlok *leftO, CFaBlok *rightO, CParserToken *tO)
{
    auto *p = (SilNikParowy_Kontekst *)pO;

    FaBlok *out = (FaBlok *)outO;
    FaBlok *right = (FaBlok *)rightO;
    FaBlok *left = (FaBlok *)leftO;
    ParserToken *t = (ParserToken *)tO;
    return p->FetchKeySet(out, right, left, t);
}

int MergeInto(CFaBlok *inO, CFaBlok *outO)
{
    FaBlok *in = (FaBlok *)inO;
    FaBlok *out = (FaBlok *)outO;
    return in->MergeInto(out);
}

int MergeFrom(CFaBlok *outO, CFaBlok *leftO, CFaBlok *rightO)
{
    FaBlok *out = (FaBlok *)outO;
    FaBlok *left = (FaBlok *)leftO;
    FaBlok *right = (FaBlok *)rightO;
    return out->MergeFrom(left, right);
}

int MergeDisjunct(CFaBlok *outO, CFaBlok *leftO, CFaBlok *rightO)
{
    FaBlok *out = (FaBlok *)outO;
    FaBlok *left = (FaBlok *)leftO;
    FaBlok *right = (FaBlok *)rightO;
    return out->MergeDisjunct(left, right);
}

int CopyNotIn(CFaBlok *outO, CFaBlok *leftO, CFaBlok *rightO)
{
    FaBlok *out = (FaBlok *)outO;
    FaBlok *left = (FaBlok *)leftO;
    FaBlok *right = (FaBlok *)rightO;
    return out->CopyNotIn(left, right);
}

// define the function:
bool score_comparator(const rxIndexEntry *lhs, const rxIndexEntry *rhs)
{
    if (lhs->key_score == rhs->key_score)
        return lhs->key < rhs->key;
    // hi-lo sort
    return lhs->key_score > rhs->key_score;
}
bool score_comparator_lo_hi(const rxIndexEntry *lhs, const rxIndexEntry *rhs)
{
    if (lhs->key_score == rhs->key_score)
        return lhs->key > rhs->key;
    // hi-lo sort
    return lhs->key_score < rhs->key_score;
}

std::vector<rxIndexEntry *> FilterAndSortResults(rax *result, bool ranked, double ranked_lower_bound, double ranked_upper_bound)
{
    std::vector<rxIndexEntry *> vec;
    raxIterator resultsIterator;
    raxStart(&resultsIterator, result);
    raxSeek(&resultsIterator, "^", NULL, 0);
    bool loHi = false;
    while (raxNext(&resultsIterator))
    {
        void *o = resultsIterator.data;
        rxIndexEntry *ro = NULL;
        if (rxGetObjectType(o) == rxOBJ_TRIPLET)
        {
            Graph_Triplet *t = (Graph_Triplet *)rxGetContainedObject(o);
            if (t->length >= ranked_lower_bound && t->length <= ranked_upper_bound)
            {
                ro = rxIndexEntry::New((const char *)resultsIterator.key, resultsIterator.key_len, t->length, t);
                ro->key_type = rxGetObjectType(o);
                loHi = true;
            }
        }
        else if (rxGetObjectType(o) == rxOBJ_INDEX_ENTRY)
        {
            auto *t = (rxIndexEntry *)rxGetContainedObject(o);
            if (t->key_score >= ranked_lower_bound && t->key_score <= ranked_upper_bound)
            {
                ro = t;
            }
        }
        else
        {
            ro = rxIndexEntry::New((const char *)resultsIterator.key, resultsIterator.key_len);
            ro->key_type = rxGetObjectType(o); // type_chars[rxGetObjectType(o)];
            ro->key_type = rxGetObjectType(o);
            ranked = false; // Traversal objects are not sorted!
        }
        if(ro != NULL)
        if(ro != NULL)
            vec.push_back(ro);
    }
    raxStop(&resultsIterator);
    if (ranked)
    {
        // Using lambda expressions in C++11
        sort(vec.begin(), vec.end(), loHi ? &score_comparator_lo_hi : &score_comparator);
    }
    return vec;
}

int WriteResults(rax *result, RedisModuleCtx *ctx, int fetch_rows, const char *, bool ranked, double ranked_lower_bound, double ranked_upper_bound, CGraphStack *fieldSelector, CGraphStack * /*sortSelector*/)
{

    std::vector<rxIndexEntry *> resultsFilteredAndSort;
    try
    {
        resultsFilteredAndSort = FilterAndSortResults(result, ranked, ranked_lower_bound, ranked_upper_bound);
        RedisModule_ReplyWithArray(ctx, resultsFilteredAndSort.size());
    }
    catch (void *)
    {
        RedisModule_ReplyWithSimpleString(ctx, "Pointer exception!");
        return C_ERR;
    }
    RedisModuleCallReply *reply = NULL;

    for (auto r : resultsFilteredAndSort)
    {
        if (r->obj != NULL)
            // Traversal objects do not need fetching
            switch(r->key_type){
                case rxOBJ_TRIPLET:
                    ((Graph_Triplet *)r->obj)->Write(ctx);
                    break;
                default:
                {
                    auto *msg = rxStringFormat("Unexpected object type: %d for Triplet: %s r:%p r->obj:%p", rxGetObjectType(r->obj), r->key, r, r->obj);
                    RedisModule_ReplyWithSimpleString(ctx, msg);
                    rxStringFree(msg);
                }
                break;
                }
        else if (fetch_rows == 0)
            r->Write(ctx);
        else
        {
            // rxString k = rxString(r->key);

            // r->obj = rxFindKey(0, k);
            // if(r->obj == NULL){
            //     RedisModule_ReplyWithArray(ctx, 4);
            //     RedisModule_ReplyWithSimpleString(ctx, "key");
            //     RedisModule_ReplyWithSimpleString(ctx, r->key);
            //     RedisModule_ReplyWithSimpleString(ctx, "status");
            //     RedisModule_ReplyWithSimpleString(ctx, "Missing/Deleted");
            // }else{
                RedisModule_ReplyWithArray(ctx, 6);
                RedisModule_ReplyWithSimpleString(ctx, "key");
                RedisModule_ReplyWithSimpleString(ctx, r->key);
                RedisModule_ReplyWithSimpleString(ctx, "score");
                char sc[32];
                snprintf(sc, sizeof(sc), "%f", r->key_score);
                RedisModule_ReplyWithSimpleString(ctx, sc);
                rxString e;
                switch (r->key_type)
                {
                case rxOBJ_HASH:
                case HASHTYPE:
                    if (fieldSelector == NULL){
                        reply = RedisModule_Call(ctx, REDIS_CMD_HGETALL, "c", (char *)r->key);
                    }
                    else
                    {
                        reply = NULL;
                        rxString k = rxStringNew(r->key);
                        void *o = rxFindHashKey(0, k);
                        rxStringFree(k);
                        if(o){
                            auto *fields = (GraphStack<const char> *)fieldSelector;
                            RedisModule_ReplyWithSimpleString(ctx, "value");
                            RedisModule_ReplyWithArray(ctx, fields->Size() * 2);
                            fields->StartHead();
                            const char *f;
                            while ((f = fields->Next()))
                            {
                                auto *v = rxGetHashField(o, f);
                                RedisModule_ReplyWithSimpleString(ctx, f);
                                RedisModule_ReplyWithSimpleString(ctx, v ? v : "");
                            }
                        }
                    }
                    break;
                case rxOBJ_STRING:
                case STRINGTYPE:
                    reply = RedisModule_Call(ctx, REDIS_CMD_GET, "c", (char *)r->key);
                    break;
                case rxOBJ_LIST:
                case LISTTYPE:
                    reply = RedisModule_Call(ctx, "LRANGE", "ccc", (char *)r->key, (char *)"0", (char *)"100");
                    break;
                default:
                    e = rxStringFormat("%c: Unsupported key type found: %s!", r->Key_Type(), r->key);
                    RedisModule_ReplyWithSimpleString(ctx, e);
                    rxStringFree(e);
                    reply = NULL;
                    // return RedisModule_ReplyWithError(ctx, "Unsupported key type found!");
                }
                if(reply){
                    RedisModule_ReplyWithSimpleString(ctx, "value");
                    RedisModule_ReplyWithCallReply(ctx, reply);
                }
                if (reply)
                    RedisModule_FreeCallReply(reply);
            // }
        }
    }
    return 0;
}

void FreeResultObject(void *o)
{
    switch (rxGetObjectType(o))
    {
    case rxOBJ_TRIPLET:
    {
        auto *t = (Graph_Triplet *)rxGetContainedObject(o);
        if (t != NULL)
        {
                if (t->DecrRefCnt() <= 1)
                {
                    if (t->subject != NULL && t->edges.sequencer != NULL)
                    {
                        t->Clear();
                        rxMemFree(t);
                    }
                    rxMemFree(o);
                }
        }
    }
    break;
    case rxOBJ_INDEX_ENTRY:
    {
        //TODO: free properly
        // auto *t = (Graph_Triplet *)rxGetContainedObject(o);
    }
    break;
    }
}

void FreeResults(rax *result)
{
    rxRaxFreeWithCallback(result, FreeResultObject);
    // raxFreeWithCallback(result, NULL);
}

bool HasParkedToken(CParsedExpression *pO, const char *token)
{
    ParsedExpression *p = (ParsedExpression *)pO;
    return p->hasParkedToken(token);
}

CParserToken *CopyParserToken(CParserToken *tO)
{
    ParserToken *t = (ParserToken *)tO;
    return t->Copy(722764001);
}

void SetParserTokenType(ParserToken *tO, eTokenType tt)
{
    ParserToken *t = (ParserToken *)tO;
    t->TokenType(tt);
}

void SetParserTokenPriority(ParserToken *tO, short pri)
{
    ParserToken *t = (ParserToken *)tO;
    t->Priority(pri);
}
