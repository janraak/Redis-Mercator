#ifndef __FABLOK_HPP__
#define __FABLOK_HPP__

#include "sjiboleth.hpp"

#include <pthread.h>
#include <unistd.h>

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
#include "sdsWrapper.h"

#include "rxSuite.h"
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
public:
    static rax *FaBlokRegistry;
    void pushIndexEntries(redisReply *reply);

    UCHAR value_type;
public:
      static  rax *Get_FaBlok_Registry();
      static void Free_Thread_Registry();
      /*
          Since  expression are parsed into Reversed Polish Notation
          the moving parts for the expression execution are named
          after the Polish Manufacturer of Steam Trains.
      */
      long long creationTime;
      long long start;
      long long latency;
      const char *setname;
      size_t reuse_count;
      size_t size;
      rax *keyset;
      char rumble_strip1[16];
      GraphStack<FaBlok> *parameter_list;
      char rumble_strip2[16];
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
      void LoadKey(int dbNo, const char *k);

      UCHAR ValueType();
      bool IsValueType(int mask);
      int ValueType(int value_type);
      bool IsValid();
      FaBlok *Open();
      FaBlok *Close();

      pid_t pid;
      pthread_t thread_id;

  protected:
      friend class SilNikParowy;
      friend class SilNikParowy_Kontekst;

  public:
      int FetchKeySet(redisNodeInfo *serviceConfig, const char *rh);
      int FetchKeySet(redisNodeInfo *serviceConfig, const char *lh, const char *rh, rxString cmp);

      static FaBlok *Get(const char *sn);
      static FaBlok *Get(const char *sn, UCHAR value_type);
      static FaBlok *Delete(FaBlok *d);
      FaBlok *Rename(const char *setname);
      static void DeleteAllTempDescriptors();
      static void ClearCache();
      static rxString GetCacheReport();
      void AddKey(const char *key, void *obj);
      void InsertKey(const char *key, void *obj);
      void InsertKey(unsigned char *s, size_t len, void *obj);
      void *RemoveKey(const char *key);
      void *RemoveKey(unsigned char *s, size_t len);
      void *LookupKey(const char *key);
      void PushResult(GraphStack<FaBlok> *stack);

      FaBlok *Right();
      FaBlok *Left();

      FaBlok *Copy(rxString set_name, int value_type, RaxCopyCallProc *fnCallback, void **privData);

      int CopyTo(FaBlok *out);
      int MergeInto(FaBlok *out);
      int MergeFrom(FaBlok *left, FaBlok *right);
      int MergeDisjunct(FaBlok *left, FaBlok *right);
      int CopyNotIn(FaBlok *left, FaBlok *right);
      rax *AsRax();

      rxString AsSds();

      bool IsParameterList();
      FaBlok();
      FaBlok *Init(void *r);
      FaBlok(rxString sn, UCHAR value_type);
      void InitKeyset(void *r, bool withRootNode);
      ~FaBlok();

      static FaBlok *New(const char *sn, UCHAR value_type);
};

/*
    The execution context allows the parsed expression to be executed in
    parallel.
*/
class SilNikParowy_Kontekst: public GraphStack<FaBlok>
{
public:
    redisNodeInfo *serviceConfig;

    rax *memoization;


protected:

    int can_delete_result;

    public:
        // SilNikParowy *engine;
        ParsedExpression *expression;
        RedisModuleCtx *module_contex;

       virtual rax *Execute(ParsedExpression *e);
        virtual rax *Execute(ParsedExpression *e, const char *key)
        ;
     

        SilNikParowy_Kontekst(SilNikParowy *engine, ParsedExpression *e, redisNodeInfo *serviceConfig);
        SilNikParowy_Kontekst(SilNikParowy *engine, ParsedExpression *e, const char *key, redisNodeInfo *serviceConfig);

    virtual list *Errors();

    void RetainResult();
    bool CanDeleteResult();
    bool IsMemoized(char const *field);
    void Memoize(char const *field, /*T*/void *value);
    /*template<typename T>T */ void *Forget(char const  *field);
    /*template<typename T>T */ void *Recall(char const  *field);
    FaBlok *GetOperationPair(char const  *operation, int load_left_and_or_right);
        int FetchKeySet(FaBlok *out, FaBlok *left, FaBlok *right, ParserToken *token);
        int FetchKeySet(FaBlok *out, ParserToken *token);
        int FetchKeySet(FaBlok *out);

    void ClearStack();
    void DumpStack();

    SilNikParowy_Kontekst();
    SilNikParowy_Kontekst(redisNodeInfo *serviceConfig);
    SilNikParowy_Kontekst(redisNodeInfo *serviceConfig, RedisModuleCtx *module_context);
    virtual ~SilNikParowy_Kontekst();
    void Reset();
    void AddError(rxString msg);

};

class SilNikParowy
{
    /*
        Since  expression are parsed into Reversed Polish Notation
        the moving parts for the expression execution are named
        after Polish translation of Steam Engine.
    */
public:
    static void Preload(ParsedExpression *e, SilNikParowy_Kontekst *ctx);
    static rax *Execute(ParsedExpression *e, SilNikParowy_Kontekst *stack);
    static rax *Execute(ParsedExpression *e, SilNikParowy_Kontekst *stack, const char *key);

    SilNikParowy();
    virtual ~SilNikParowy();
};

#define STACK_CHECK(minSize) \
    if (HasMinimumStackEntries(stack, minSize) == C_ERR) \
    {                                                    \
        auto *t = (ParserToken *)tO;                     \
        rxString msg = rxStringFormat("%s requires %d sets!", t->Token(), minSize); \
        stack->AddError(msg);                                \
        rxStringFree(msg);                                    \
        return C_ERR;                                    \
    }

#define ERROR(msg)                                    \
    {                                                 \
        rxString m = rxStringNew(msg);                          \
        stack->AddError(m);                             \
        rxServerLog(rxLL_NOTICE, "ERROR: %s\n", m);                             \
        rxStringFree(m);                                   \
        return C_ERR;                                 \
    }

#define ERROR_RETURN_NULL(msg)                        \
    {                                                 \
        rxString m = rxStringNew(msg);                          \
        rxServerLog(rxLL_NOTICE, "ERROR: %s\n", m);                     \
        stack->AddError(m);                           \
        rxStringFree(m);                                   \
        return NULL;                                  \
    }


#define SJIBOLETH_HANDLER(fn)                                \
    int fn(CParserToken *tO, CSilNikParowy_Kontekst *stackO) \
    {                                                        \
        UNWRAP_SJIBOLETH_HANDLER_PARAMETERS();

#define SJIBOLETH_HANDLER_STUB(fn)                           \
    int fn(CParserToken *, CSilNikParowy_Kontekst *)         \
    {                                                        

#define END_SJIBOLETH_HANDLER(fn)                   \
        return C_OK;                                \
    }

#define END_SJIBOLETH_HANDLER_X(fn)                 \
    }

#define UNWRAP_SJIBOLETH_HANDLER_PARAMETERS()       \
    auto *t = (ParserToken *)tO;                    \
    auto *stack = (SilNikParowy_Kontekst *)stackO;  

#endif