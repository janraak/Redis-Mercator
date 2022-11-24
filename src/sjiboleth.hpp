#ifndef __SJIBOLETH_HPP__
#define __SJIBOLETH_HPP__

#include "rxSuite.h"
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
    static ParserToken *Copy(ParserToken *base);
    ~ParserToken();
    static void Purge(ParserToken *token);
    ParserToken *Copy();

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
    int Execute(CParserToken *tO, CStack *stackO);
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

    bool static FlushIndexables(rax *collector, rxString key, int key_type, redisContext *index);

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

#endif