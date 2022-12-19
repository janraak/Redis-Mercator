
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

CSjiboleth *releaseParser(CSjiboleth *s)
{
    Sjiboleth *so = (Sjiboleth *)s;
    delete so;
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
    if (lhs->key_score < rhs->key_score)
        return lhs->key < rhs->key;
    // hi-lo sort
    return lhs->key_score > rhs->key_score;
}

std::vector<rxIndexEntry *> FilterAndSortResults(rax *result, bool ranked, double ranked_lower_bound, double ranked_upper_bound)
{
    std::vector<rxIndexEntry *> vec;
    raxIterator resultsIterator;
    raxStart(&resultsIterator, result);
    raxSeek(&resultsIterator, "^", NULL, 0);
    int tally = 0;
    while (raxNext(&resultsIterator))
    {
        void *o = resultsIterator.data;
        rxIndexEntry *ro = NULL;
        if (rxGetObjectType(o) == rxOBJ_TRIPLET)
        {
            Graph_Triplet *t = (Graph_Triplet *)rxGetContainedObject(o);
            ro = rxIndexEntry::New((const char *)resultsIterator.key, resultsIterator.key_len, 1.0, t);
            ranked = false; // Traversal objects are not sorted!
        }
        else if (rxGetObjectType(o) == rxOBJ_INDEX_ENTRY)
        {
            rxIndexEntry *t = (rxIndexEntry *)rxGetContainedObject(o);
            if (t->key_score >= ranked_lower_bound && t->key_score <= ranked_upper_bound)
            {
                ro = t;
            }
        }
        if (ro != NULL)
            vec.push_back(ro);
    }
    raxStop(&resultsIterator);
    if (ranked)
    {
        // Using lambda expressions in C++11
        sort(vec.begin(), vec.end(), &score_comparator);
    }
    return vec;
}

int WriteResults(rax *result, RedisModuleCtx *ctx, int fetch_rows, const char *target_setname, bool ranked, double ranked_lower_bound, double ranked_upper_bound)
{
    auto resultsFilteredAndSort = FilterAndSortResults(result, ranked, ranked_lower_bound, ranked_upper_bound);
    RedisModule_ReplyWithArray(ctx, resultsFilteredAndSort.size());

    RedisModuleCallReply *reply = NULL;

    for (auto r : resultsFilteredAndSort)
    {
        if (r->obj != NULL)
            // Traversal objects do not need fetching
            ((Graph_Triplet *)r->obj)->Write(ctx);
        else if (fetch_rows == 0)
            r->Write(ctx);
        else
        {
            RedisModule_ReplyWithArray(ctx, 6);
            RedisModule_ReplyWithStringBuffer(ctx, "key", 3);
            RedisModule_ReplyWithStringBuffer(ctx, (char *)r->key, strlen(r->key));
            RedisModule_ReplyWithStringBuffer(ctx, "score", 5);
            RedisModule_ReplyWithDouble(ctx, r->key_score);
            rxString e;
            switch (r->key_type)
            {
            case HASHTYPE:
                reply = RedisModule_Call(ctx, REDIS_CMD_HGETALL, "c", (char *)r->key);
                break;
            case STRINGTYPE:
                reply = RedisModule_Call(ctx, REDIS_CMD_GET, "c", (char *)r->key);
                break;
            default:
                e = rxStringFormat("%c: Unsupported key type found: %s!", r->key_type, r->key);
                RedisModule_ReplyWithStringBuffer(ctx, e, strlen(e));
                // return RedisModule_ReplyWithError(ctx, "Unsupported key type found!");
            }
            RedisModule_ReplyWithStringBuffer(ctx, "value", 5);
            RedisModule_ReplyWithCallReply(ctx, reply);
            if (reply)
                RedisModule_FreeCallReply(reply);
        }
    }
    return 0;
}

void FreeResultObject(void *o)
{
    if (rxGetObjectType(o) == rxOBJ_TRIPLET)
    {
        auto *t = (Graph_Triplet *)rxGetContainedObject(o);
        if (t->DecrRefCnt() <= 1)
        {
            delete t;
            rxMemFree(o);
        }
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
    return t->Copy();
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
