#ifndef __FABLOK_HPP__
#define __FABLOK_HPP__

#include "sjiboleth.hpp"

#define C_OK                    0
#define C_ERR                   -1

#ifdef __cplusplus
extern "C"
{
#endif
    /* Utils */
    long long ustime(void);
    long long mstime(void);

#include <stddef.h>
#include <stdint.h>
#include "rax.h"
#include "sds.h"

#include "rxSuiteHelpers.h"

#ifdef __cplusplus
}
#endif

#ifndef KeysetDescriptor_TYPE
#define KeysetDescriptor_TYPE

#define KeysetDescriptor_TYPE_UNKNOWN (UCHAR)0x0
#define KeysetDescriptor_TYPE_KEYSET (UCHAR)0x01
#define KeysetDescriptor_TYPE_GREMLINSET (UCHAR)0x02
#define KeysetDescriptor_TYPE_SINGULAR (UCHAR)0x04
#define KeysetDescriptor_TYPE_PARAMETER_LIST (UCHAR)0x08
#define KeysetDescriptor_TYPE_MONITORED_SET (UCHAR)0x10
#define KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET (UCHAR)0x20
#define KeysetDescriptor_TYPE_GREMLIN_EDGE_SET (UCHAR)0x40
#define KeysetDescriptor_TYPE_GREMLIN_AS_SET (UCHAR)0x80

#define GENERIC_SET (UCHAR)0x0
#define VERTEX_SET (UCHAR)0x1
#define EDGE_SET (UCHAR)0x2

#endif
typedef bool RaxCopyCallProc(unsigned char *s, size_t len, void *data, void **privData);


class SilNikParowy;
class FaBlok
{
private:
    static rax *Registry;
    void pushIndexEntries(redisReply *reply);

    UCHAR value_type;
public:
    /*
        Since  expression are parsed into Reversed Polish Notation
        the moving parts for the expression execution are named
        after the Polish Manufacturer of Steam Trains.
    */
    long long creationTime;
    long long start;
    long long latency;
    sds setname;
    size_t reuse_count;
    size_t size;
    rax keyset;
    GraphStack<FaBlok> *parameter_list;
    FaBlok *left;
    FaBlok *right;

    public:
        int is_temp;
        UCHAR is_on_stack;
        UCHAR vertices_or_edges;
        int dirty;
        int marked_for_deletion;

    public:
        int AsTemp();
        bool HasKeySet();
        void LoadKey(int dbNo, sds key);

        UCHAR ValueType();
        bool IsValueType(int mask);
        int ValueType(int value_type);
        bool IsValid();
        FaBlok *Open();
        FaBlok *Close();
        

    protected:
        friend class SilNikParowy;

    public:
        int FetchKeySet(sds host, int port, sds rh);
        int FetchKeySet(sds host, int port, sds lh, sds rh, sds cmp);

        static FaBlok *Get(const char *sn);
        static FaBlok *Get(const char *sn, UCHAR value_type);
        static FaBlok *Delete(FaBlok *d);
        FaBlok *Rename(sds setname);
        static void DeleteAllTempDescriptors();
        static void ClearCache();
        static sds GetCacheReport();
        void AddKey(sds key, void *obj);
        void InsertKey(sds key, void *obj);
        void InsertKey(unsigned char *s, size_t len, void *obj);
        void *RemoveKey(sds key);
        void *RemoveKey(unsigned char *s, size_t len);
        void *LookupKey(sds key);
        void PushResult(GraphStack<FaBlok> *stack);

        FaBlok *Right();
        FaBlok *Left();

        FaBlok *Copy(sds set_name, int value_type, RaxCopyCallProc *fnCallback, void **privData);

        int CopyTo(FaBlok *out);
        int MergeInto(FaBlok *out);
        int MergeFrom(FaBlok *left, FaBlok *right);
        int MergeDisjunct(FaBlok *left, FaBlok *right);
        int CopyNotIn(FaBlok *left, FaBlok *right);
        FaBlok *Attach(rax *d);
        rax *AsRax();

        sds AsSds();

        bool IsParameterList();
        FaBlok();
        FaBlok(sds sn, UCHAR value_type);
        void InitKeyset(bool withRootNode);
        ~FaBlok();
};

/*
    The execution context allows the parsed expression to be executed in
    parallel.
*/
class SilNikParowy_Kontekst
{
    public:
        SilNikParowy *engine;
        ParsedExpression *expression;
        RedisModuleCtx *module_contex;
        rax *Memoization;

        SilNikParowy_Kontekst(SilNikParowy *engine, ParsedExpression *e, char *h, int port);
        SilNikParowy_Kontekst(SilNikParowy *engine, ParsedExpression *e, const char *key, char *h, int port);
};

class SilNikParowy
{
    /*
        Since  expression are parsed into Reversed Polish Notation
        the moving parts for the expression execution are named
        after Polish translation of Steam Engine.
    */
private:
    sds host;
    int port;

    rax *Memoization;

protected:
    GraphStack<FaBlok> *stack;

    int can_delete_result;

public:
    RedisModuleCtx *module_contex;


public:
    virtual rax *Execute(ParsedExpression *e);
    virtual rax *Execute(ParsedExpression *e, const char *key);
    virtual list *Errors(ParsedExpression *e);

    void RetainResult();
    bool CanDeleteResult();
    bool IsMemoized(char const *field);
    void Memoize(char const *field, /*T*/void *value);
    /*template<typename T>T */ void *Forget(char const  *field);
    /*template<typename T>T */ void *Recall(char const  *field);

    void ClearStack();
    void DumpStack();
    FaBlok *GetOperationPair(sds operation, GraphStack<FaBlok> *stack, int load_left_and_or_right);
    int FetchKeySet(FaBlok *out, FaBlok *left, FaBlok *right, ParserToken *token);

    SilNikParowy();
    SilNikParowy(char *h, int port, RedisModuleCtx *module_context);
    virtual ~SilNikParowy();
    void Reset();
};

#define STACK_CHECK(minSize) \
    if (HasMinimumStackEntries(stack, minSize) == C_ERR) \
    {                                                    \
        auto *e = (ParsedExpression *)eO;             \
        auto *t = (ParserToken *)tO;                     \
        sds msg = sdscatprintf(sdsempty(), "%s requires %d sets!", t->Token(), minSize); \
        e->AddError(msg);                                \
        sdsfree(msg);                                    \
        return C_ERR;                                    \
    }

#define ERROR(msg)                                    \
    {                                                 \
        auto *e = (ParsedExpression *)eO;             \
        sds m = sdsnew(msg);                          \
        e->AddError(m);                             \
        sdsfree(m);                                   \
        return C_ERR;                                 \
    }

#define ERROR_RETURN_NULL(msg)                                    \
    {                                                 \
        auto *e = (ParsedExpression *)eO;             \
        sds m = sdsnew(msg);                          \
        e->AddError(m);                             \
        sdsfree(m);                                   \
        return NULL;                                 \
    }

#define SJIBOLETH_HANDLER(fn)                                                                                        \
    int fn(CSilNikParowy *pO, CParsedExpression *eO, CParserToken *tO, CStack *stackO) \
    {                                                                                  \
        UNWRAP_SJIBOLETH_HANDLER_PARAMETERS();


#define END_SJIBOLETH_HANDLER(fn)                                                                                        \
        return C_OK;                                \
    }

#define END_SJIBOLETH_HANDLER_X(fn)                                                                                        \
    }

#define UNWRAP_SJIBOLETH_HANDLER_PARAMETERS()       \
    auto *p = (SilNikParowy *)pO;                   \
    auto *t = (ParserToken *)tO;                    \
    auto *stack = (GraphStack<FaBlok> *)stackO;

#endif