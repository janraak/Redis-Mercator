
#ifndef eTokenType_TYPE
enum eTokenType
{
    _operand = 1,
    _literal = 2,
    _operator = 3,
    _open_bracket = 4,
    _close_bracket = 5,
    _key_set = 6,
    _immediate_operator = 7
};
#define eTokenType_TYPE
#endif

#ifndef __SJIBOLETH_H__
#define __SJIBOLETH_H__

typedef void CSjiboleth;
typedef void CParsedExpression;
typedef void CParserToken;
typedef void CSilNikParowy_Kontekst;

typedef void CStack;
typedef void CFaBlok;
#ifdef __cplusplus
extern "C"
{
#endif
#include "../../deps/hiredis/sds.h"
#include "../../src/adlist.h"
#include "../../src/dict.h"
#include "../../src/rax.h"
#include "rxSuite.h"

#define pri5 5
#define pri10 10
#define pri20 20
#define pri30 30
#define pri50 50
#define pri60 60
#define pri6 6
#define pri70 70
#define pri100 100
#define pri200 200
#define pri500 500
#define priIgnore -1
#define priImmediate 0

#define QE_LOAD_NONE 0
#define QE_LOAD_LEFT_ONLY 1
#define QE_LOAD_RIGHT_ONLY 2
#define QE_LOAD_LEFT_AND_RIGHT (QE_LOAD_LEFT_ONLY | QE_LOAD_RIGHT_ONLY)
#define QE_CREATE_SET 4
#define QE_SWAP_SMALLEST_FIRST 8
#define QE_SWAP_LARGEST_FIRST 16


    typedef void CSilNikParowy; // Opaque Execution Engine 

    typedef int operationProc(CParserToken *tO, CSilNikParowy_Kontekst *stackO);
    typedef CParserToken *parserContextProc(CParserToken *t, char *head, CParsedExpression *expression);

    CSjiboleth *newQueryEngine();
    CSjiboleth *newGremlinEngine();
    CSjiboleth *newJsonEngine();
    CSjiboleth *newTextEngine();
    CSjiboleth *releaseParser(CSjiboleth *s);

    CParsedExpression *parseQ(CSjiboleth *s, const char *query);
    CParserToken *CopyParserToken(CParserToken *t);
    void SetParserTokenType(CParserToken *t, eTokenType tt);
    void SetParserTokenPriority(CParserToken *t, short pri);

    bool HasParkedToken(CParsedExpression *p, const char *token);
    int parsedWithErrors(CParsedExpression *p);
    int writeParsedErrors(CParsedExpression *p, RedisModuleCtx *ctx);

    rax *executeQ(CParsedExpression *e, char *h, int port, RedisModuleCtx *module_context, list **errors);
    int WriteResults(rax *result, RedisModuleCtx *ctx, int fetch_rows, const char *target_setname);
    void FreeResults(rax *result);
    
    CParsedExpression *releaseQuery(CParsedExpression *e);

    /* Opaque stack methods */
    int HasMinimumStackEntries(CStack *stack, int size);
    CFaBlok *PopStack(CStack *stack);
    int PushStack(CStack *stack, CFaBlok *entry);
    CFaBlok *GetOperationPair(CParserToken *operation, CStack *stack, int load_left_and_or_right);
    void PushResult(CFaBlok *hereO, CStack *stackO);

    int RegisterSyntax(const char *op, int token_priority, operationProc *opf, parserContextProc *pcf);
    CFaBlok *GetRight(CFaBlok *here);
    CFaBlok *GetLeft(CFaBlok *here);
    int FetchKeySet(CSilNikParowy_Kontekst *pO, CFaBlok *out, CFaBlok *left, CFaBlok *right, CParserToken *t);
    int CopyKeySet(CFaBlok *in, CFaBlok *out);
    int MergeInto(CFaBlok *in, CFaBlok *out);
    int MergeFrom(CFaBlok *out, CFaBlok *left, CFaBlok *right);
    int MergeDisjunct(CFaBlok *out, CFaBlok *left, CFaBlok *right);
    int CopyNotIn(CFaBlok *out, CFaBlok *left, CFaBlok *right);
#ifdef __cplusplus
}
#endif

#define DECLARE_SJIBOLETH_HANDLER(fn)                                                           \
    static int fn(CParserToken *tO, CSilNikParowy_Kontekst *stackO);


#define SJIBOLETH_PARSER_CONTEXT_CHECKER(fn)                                                    \
    CParserToken *fn(CParserToken *t, char *head, CParsedExpression *expression) \
    {                                                                                           

#define END_SJIBOLETH_PARSER_CONTEXT_CHECKER(fn)                                                \
        return t;                                                                               \
    }

#endif