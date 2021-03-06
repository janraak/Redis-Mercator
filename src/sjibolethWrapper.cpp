
#ifdef __cplusplus
extern "C"
{
#endif

#include "rxSuite.h"
#include "sjiboleth.h"
#include <string.h>
#include "../../src/zmalloc.h"

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

CParserToken *lookupToken(CSjiboleth *s, sds token){
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

rax *executeQ(CParsedExpression *s, char *h, int port, RedisModuleCtx *module_context, list **errors)
{
    ParsedExpression *so = (ParsedExpression *)s;
    auto *e = new SilNikParowy_Kontekst(h, port, module_context);
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
    auto  *stack = (SilNikParowy_Kontekst *)stackO;
    return stack->Size() >= size ? C_OK : C_ERR;
}

CFaBlok *PopStack(CSilNikParowy_Kontekst *stackO)
{
    auto  *stack = (SilNikParowy_Kontekst *)stackO;
    return stack->Pop();
}

int PushStack(CSilNikParowy_Kontekst *stackO, CFaBlok *entryO)
{
    auto  *stack = (SilNikParowy_Kontekst *)stackO;
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
    auto  *stack = (SilNikParowy_Kontekst *)stackO;
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

int WriteResults(rax *result, RedisModuleCtx *ctx, int fetch_rows, const char *target_setname)
{
    RedisModule_ReplyWithArray(ctx, raxSize(result));
    RedisModuleCallReply *reply = NULL;

    raxIterator resultsIterator;
    raxStart(&resultsIterator, result);
    raxSeek(&resultsIterator, "^", NULL, 0);
    while (raxNext(&resultsIterator))
    {
        sds key = sdsnewlen(resultsIterator.key, resultsIterator.key_len);

        void *o = resultsIterator.data;
        if (target_setname)
        {
            RedisModuleCallReply *r = RedisModule_Call(ctx,
                                                       REDIS_CMD_SADD, "cc",
                                                       target_setname,
                                                       (char *)key);
            if (r)
                RedisModule_FreeCallReply(r);
        }
        if (fetch_rows == 0)
        {
            if (rxGetObjectType(o) == rxOBJ_TRIPLET)
            {
                Graph_Triplet *t = (Graph_Triplet *)rxGetContainedObject(o);
                t->Write(ctx);
            }
            else
                RedisModule_ReplyWithStringBuffer(ctx, (char *)key, sdslen((char *)key));
        }
        else
        {
            if (rxGetObjectType(o) == rxOBJ_TRIPLET)
            {
                Graph_Triplet *t = (Graph_Triplet *)rxGetContainedObject(o);
                t->Write(ctx);
            }
            else
            {
                RedisModule_ReplyWithArray(ctx, 2);
                RedisModule_ReplyWithStringBuffer(ctx, (char *)key, strlen((char *)key));
                char *index_entry = (char *)(char *)rxGetContainedObject(o);
                char *first_tab = strchr(index_entry, '\t');
                char key_type = first_tab ? *(first_tab + 1) : HASHTYPE;
                sds e;
                switch (key_type)
                {
                case HASHTYPE:
                    reply = RedisModule_Call(ctx, REDIS_CMD_HGETALL, "c", (char *)key);
                    break;
                case STRINGTYPE:
                    reply = RedisModule_Call(ctx, REDIS_CMD_GET, "c", (char *)key);
                    break;
                default:
                    e = sdscatfmt(sdsnewlen(&key_type, 1), ": Unsupported key type found: %s!", key);
                    RedisModule_ReplyWithStringBuffer(ctx, e, sdslen(e));
                    // return RedisModule_ReplyWithError(ctx, "Unsupported key type found!");
                }
                RedisModule_ReplyWithCallReply(ctx, reply);
                if (reply)
                    RedisModule_FreeCallReply(reply);
                // RedisModule_ReplySetArrayLength(ctx, 2);
            }
        }
    }
    raxStop(&resultsIterator);
    return 0;
}

void FreeResultObject(void *o){
    if(rxGetObjectType(o) == rxOBJ_TRIPLET){
        auto *t = (Graph_Triplet *)rxGetContainedObject(o);
        if(t->DecrRefCnt() <= 1){
            delete t;
            zfree(o);
        }
    }
}

void FreeResults(rax *result){
    raxFreeWithCallback(result, FreeResultObject);
    // raxFreeWithCallback(result, NULL);
}

bool HasParkedToken(CParsedExpression *pO, const char *token){
    ParsedExpression *p = (ParsedExpression *)pO;
    return p->hasParkedToken(token);
}

    CParserToken *CopyParserToken(CParserToken *tO){
        ParserToken *t = (ParserToken *)tO;
        return t->Copy();
    }

    void SetParserTokenType(ParserToken *tO, eTokenType tt){
        ParserToken *t = (ParserToken *)tO;
        t->TokenType(tt);
    }

    void SetParserTokenPriority(ParserToken *tO, short pri){
        ParserToken *t = (ParserToken *)tO;
        t->Priority(pri);
    }

