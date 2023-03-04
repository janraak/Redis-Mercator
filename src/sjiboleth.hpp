#ifndef __SJIBOLETH_HPP__
#define __SJIBOLETH_HPP__

#include <string>
#include <string.h>

#include "rxSuite.h"
#include "rxSuiteHelpers.h"
#include "sjiboleth.h"

#include "graphstack.hpp"

// class Parser;
class ParsedExpression;
class SilNikParowy;
class ParserToken
{
protected:
    const char *op;
    int op_len;
    eTokenType token_type;
    short token_priority;
    bool is_volatile;
    bool crlftab_as_operator;
    short no_of_stack_entries_consumed;
    short no_of_stack_entries_produced;
    operationProc *opf;
    parserContextProc *pcf;
    ParserToken *copy_of;
    long reference;

    // longjmp/setjmp control
    static int ExecuteExceptionReturn;

public:
    ParserToken();
    static ParserToken *New(eTokenType token_type,
                            const char *op,
                            int op_len);
    static ParserToken *New(const char *op,
                            eTokenType token_type,
                            short token_priority,
                            short no_of_stack_entries_consumed,
                            short no_of_stack_entries_produced);
    static ParserToken *New(const char *op,
                            eTokenType token_type,
                            short token_priority,
                            short no_of_stack_entries_consumed,
                            short no_of_stack_entries_produced,
                            operationProc *opf);
    static ParserToken *New(const char *op, short token_priority, operationProc *opf);
    ParserToken *Init(eTokenType token_type, const char *op, int op_len);
    static ParserToken *Copy(ParserToken *base, long reference);
    ~ParserToken();
    static void Purge(ParserToken *token);
    ParserToken *Copy(long reference);

    void ParserContextProc(parserContextProc *pcf);
    bool HasParserContextProc();
    parserContextProc *ParserContextProc();

    UCHAR *Operation();
    int OperationLength();

    eTokenType TokenType();
    void TokenType(eTokenType tt);

    const char *Token();
    const char *TokenAsSds();
    bool Is(const char *aStr);
    int CompareToken(const char *);

    short Priority();
    void Priority(short token_priority);

    bool IsVolatile();
    bool HasExecutor();
    operationProc *Executor();
    int Execute(CStack *stackO);
};

class Sjiboleth
{
protected:
    rax *registry;

private:
    // Parser *parser;
    bool is_volatile;

protected:
    rxString default_operator;
    bool crlftab_as_operator;
    bool object_and_array_controls;

    ParserToken *GetTokenAt(list *list, int ix);
    ParserToken *NewToken(eTokenType token_type, const char *token, size_t len);
    ParserToken *FindToken(const char *token, size_t len);
    bool IsOperator(char aChar);
    bool IsNumber(char *aChar);
    bool IsSpace(char aChar);
    bool IsBracket(char *aChar, char **newPos);
    bool IsBracketOpen(char *aChar, char **newPos);
    char *GetFence(char *aChar);
    bool IsCsym(int c);
    ParserToken *ScanIdentifier(char *head, char **tail);
    ParserToken *ScanLiteral(char *head, char **tail);
    ParserToken *ScanOperator(char *head, char **tail);
    ParserToken *ScanBracket(char *head, char **tail);
    ParserToken *ScanNumber(char *head, char **tail);

    virtual bool RegisterDefaultSyntax();
    bool ResetSyntax();

    DECLARE_SJIBOLETH_HANDLER(executePlusMinus);
    DECLARE_SJIBOLETH_HANDLER(executeStore);
    DECLARE_SJIBOLETH_HANDLER(executeEquals);
    DECLARE_SJIBOLETH_HANDLER(executeOr);
    DECLARE_SJIBOLETH_HANDLER(executeAnd);
    DECLARE_SJIBOLETH_HANDLER(executeXor);
    DECLARE_SJIBOLETH_HANDLER(executeNotIn);
    DECLARE_SJIBOLETH_HANDLER(executeSelectFields);

public:
    ParserToken *LookupToken(rxString token);

public:
    Sjiboleth(const char *default_operator);
    Sjiboleth();
    virtual ~Sjiboleth();

    ParsedExpression *Parse(const char *query);

    bool RegisterSyntax(const char *op, short token_priority, int inE, int outE, operationProc *opf);
    bool RegisterSyntax(const char *op, short token_priority, int inE, int outE, operationProc *opf, parserContextProc *pcf);

    bool DeregisterSyntax(const char *op);

    bool hasDefaultOperator();

    const char *defaultOperator();

    friend class ParsedExpression;

    virtual SilNikParowy *GetEngine();
};

class QueryDialect : public Sjiboleth
{
protected:
    virtual bool RegisterDefaultSyntax();

public:
    virtual SilNikParowy *GetEngine();
};

class GremlinDialect : public Sjiboleth
{
public:
    virtual bool RegisterDefaultSyntax();

public:
    DECLARE_SJIBOLETH_HANDLER(executeMatch);
    DECLARE_SJIBOLETH_HANDLER(executeNomatch);
    DECLARE_SJIBOLETH_HANDLER(executeAllVertices);
    DECLARE_SJIBOLETH_HANDLER(executeGremlinParameters);
    DECLARE_SJIBOLETH_HANDLER(executeGremlinMatchInExclude);
    GremlinDialect();

    virtual SilNikParowy *GetEngine();
};

class JsonDialect : public Sjiboleth
{
public:
    virtual bool RegisterDefaultSyntax();
    JsonDialect();
    virtual SilNikParowy *GetEngine();
};

class TextDialect : public Sjiboleth
{
public:
    virtual bool RegisterDefaultSyntax();

    bool static FlushIndexables(rax *collector, rxString key, int key_type, CSimpleQueue *persist_q, bool use_bracket);

    TextDialect();
    virtual SilNikParowy *GetEngine();
};

class SilNikParowy;
class ParsedExpression
{
protected:
    Sjiboleth *dialect;
    GraphStack<ParserToken> *expression;
    list *errors;
    list *side_track;

    ParsedExpression *next;

public:
    UCHAR final_result_value_type;
    ParserToken *lastInstruction();
    friend class SilNikParowy;
    friend class SilNikParowy_Kontekst;
    /* Parser methods */
    ParserToken *tokenAt(list *list, int ix);
    ParserToken *peekParked();
    ParserToken *removeTokenAt(list *list, int ix);
    void stashToken(Sjiboleth *p, ParserToken *token, ParserToken *last_token);
    void emitFinal(ParserToken *token);
    void park(ParserToken *token);
    ParserToken *unpark();
    int sideTrackLength();
    bool hasParkedTokens();
    bool hasParkedToken(const char *token);
    void moveSideTrackToFinal();
    void flushSideTrack();

    void AddError(rxString msg);
    bool HasErrors();
    int writeErrors(RedisModuleCtx *ctx);

    void Write(RedisModuleCtx *ctx);

    void show(const char *query);
    void Show(const char *query);
    ParsedExpression(Sjiboleth *dialect);
    ~ParsedExpression();

    SilNikParowy *GetEngine();

    rxString ToString();

    ParsedExpression *Next();
    ParsedExpression *Next(ParsedExpression *next);
    GraphStack<ParserToken> *RPN();
};

class rxIndexEntry
{
public:
    const char *key;
    char key_type;
    double key_score;
    void *obj;

    rxIndexEntry *Init(char *key, char *key_type, double key_score)
    {
        this->key = key;
        this->key_type = key_type ? *key_type : '?';
        this->key_score = key_score;
        this->obj = NULL;
        return this;
    }

    rxIndexEntry *Init(char *key, char *key_type, double key_score, void *obj)
    {
        this->Init(key, key_type, key_score);
        this->obj = obj;
        return this;
    }

    static void *NewAsRobj(const char *entry, double key_score)
    {
        int entry_len = strlen(entry);
        void *o = rxMemAlloc(sizeof(rxIndexEntry) + entry_len + 2 + rxSizeofRobj());

        void *ie = o + rxSizeofRobj();

        char *key = (char *)(ie + sizeof(rxIndexEntry));
        strcpy(key, entry);
        char *tab = strchr((char *)key, '\t');
        *tab = 0x00;
        ((rxIndexEntry *)ie)->Init(key, tab +1, key_score);
        return rxInitObject(o, rxOBJ_INDEX_ENTRY, ie);
    }

    static rxIndexEntry *New(const char *entry, double key_score)
    {
        void *ie = rxMemAlloc(sizeof(rxIndexEntry) + strlen(entry) )+ 1 ;
        char *key = (char *)(ie +  sizeof(rxIndexEntry));
        strcpy(key, entry);
        char *tab = strchr((char *)key, '\t');
        *tab = 0x00;
        return ((rxIndexEntry *)ie)->Init(key, tab +1, key_score);
    }

    static rxIndexEntry *New(const char *entry, size_t len, double key_score, void *obj)
    {
        void *ie = rxMemAlloc(sizeof(rxIndexEntry) + strlen(entry) )+ 1 ;
        char *key = (char *)(ie +  sizeof(rxIndexEntry));
        strncpy(key, entry, len);
        char *tab = strchr((char *)key, '\t');
        if(tab)
           { *tab = 0x00;
               ++tab;
           }
        else
            *(key + len) = 0x00;
        return ((rxIndexEntry *)ie)->Init(key, tab, key_score, obj);
    }

    void Write(RedisModuleCtx * ctx){
        RedisModule_ReplyWithArray(ctx, 6);
        RedisModule_ReplyWithStringBuffer(ctx, "key", 3);
        RedisModule_ReplyWithStringBuffer(ctx, this->key, strlen(this->key));
        RedisModule_ReplyWithStringBuffer(ctx, "type", 4);
        RedisModule_ReplyWithStringBuffer(ctx, &this->key_type, 1);
        RedisModule_ReplyWithStringBuffer(ctx, "score", 5);
        char sc[32];
        snprintf(sc, sizeof(sc), "%f", this->key_score);
        RedisModule_ReplyWithSimpleString(ctx, sc);
    }
};
#endif