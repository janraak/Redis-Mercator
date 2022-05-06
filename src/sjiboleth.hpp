#ifndef __SJIBOLETH_HPP__
#define __SJIBOLETH_HPP__

#include "rxSuite.h"
#include "sjiboleth.h"

#include "graphstack.hpp"

// class Parser;
class ParsedExpression;
class ParserToken
{
protected:
    sds op;
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
    ParserToken(eTokenType token_type,
                const char *op,
                int op_len);
    ParserToken(const char *op,
                eTokenType token_type,
                short token_priority,
                short no_of_stack_entries_consumed,
                short no_of_stack_entries_produced);
    ParserToken(const char *op,
                eTokenType token_type,
                short token_priority,
                short no_of_stack_entries_consumed,
                short no_of_stack_entries_produced,
                operationProc *opf);
    ParserToken(const char *op, short token_priority, operationProc *opf);
    ~ParserToken();

    ParserToken *Copy();

    void ParserContextProc(parserContextProc *pcf);
    bool HasParserContextProc();
    parserContextProc *ParserContextProc();

    UCHAR *Operation();
    int OperationLength();

    eTokenType TokenType();
    void TokenType(eTokenType tt);

    const char *Token();
    sds TokenAsSds();
    bool Is(const char *aStr);
    int CompareToken(sds);

    short Priority();
    void Priority(short token_priority);

    bool IsVolatile();
    bool HasExecutor();
    operationProc *Executor();
    int Execute(CSilNikParowy *pO, CParsedExpression *eO, CParserToken *tO, CStack *stackO);
};

class Sjiboleth
{
protected:
    rax *registry;

private:
    // Parser *parser;
    bool is_volatile;

protected:
    sds default_operator;
    bool crlftab_as_operator;
    bool object_and_array_controls;

    ParserToken *getTokenAt(list *list, int ix);
    ParserToken *newToken(eTokenType token_type, const char *token, size_t len);
    bool isoperator(char aChar);
    bool isNumber(char *aChar);
    bool is_space(char aChar);
    bool isbracket(char aChar);
    bool iscsym(int c);
    ParserToken *scanIdentifier(char *head, char **tail);
    ParserToken *scanLiteral(char *head, char **tail);
    ParserToken *scanOperator(char *head, char **tail);
    ParserToken *scanBracket(char *head, char **tail);
    ParserToken *scanNumber(char *head, char **tail);

    virtual bool registerDefaultSyntax();
    bool resetSyntax();

    static int executePlusMinus(CSilNikParowy *pO, CParsedExpression *eO, CParserToken *tO, CStack *stackO);
    static int executeStore(CSilNikParowy *pO, CParsedExpression *eO, CParserToken *tO, CStack *stackO);
    static int executeEquals(CSilNikParowy *pO, CParsedExpression *eO, CParserToken *tO, CStack *stackO);
    static int executeOr(CSilNikParowy *pO, CParsedExpression *eO, CParserToken *tO, CStack *stackO);
    static int executeAnd(CSilNikParowy *pO, CParsedExpression *eO, CParserToken *tO, CStack *stackO);
    static int executeXor(CSilNikParowy *pO, CParsedExpression *eO, CParserToken *tO, CStack *stackO);
    static int executeNotIn(CSilNikParowy *pO, CParsedExpression *eO, CParserToken *tO, CStack *stackO);

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
};

class QueryDialect : public Sjiboleth
{
protected:
    virtual bool registerDefaultSyntax();

public:
};

class GremlinDialect : public Sjiboleth
{
public:
    virtual bool registerDefaultSyntax();

public:
    static int executeMatch(CSilNikParowy *pO, CParsedExpression *eO, CParserToken *tO, CStack *stackO);
    static int executeNomatch(CSilNikParowy *pO, CParsedExpression *eO, CParserToken *tO, CStack *stackO);
    static int executeAllVertices(CSilNikParowy *pO, CParsedExpression *eO, CParserToken *tO, CStack *stackO);
    static int executeGremlinParameters(CSilNikParowy *pO, CParsedExpression *eO, CParserToken *tO, CStack *stackO);
    static int executeGremlinMatchInExclude(CSilNikParowy *pO, CParsedExpression *eO, CParserToken *tO, CStack *stackO);
    GremlinDialect();
};

class JsonDialect : public Sjiboleth
{
public:
    virtual bool registerDefaultSyntax();
    JsonDialect();
};

class TextDialect : public Sjiboleth
{
public:
    virtual bool registerDefaultSyntax();

    TextDialect();
};

class SilNikParowy;
class ParsedExpression
{
protected:
    Sjiboleth *dialect;
    GraphStack<ParserToken> *expression;
    list *errors;
    list *side_track;

public:

    UCHAR final_result_value_type;
    ParserToken *lastInstruction();
    friend class SilNikParowy;
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

    void AddError(sds msg);
    bool HasErrors();
    int writeErrors(RedisModuleCtx *ctx);

    void show(const char *query);
    void Show(const char *query);
    ParsedExpression(Sjiboleth *dialect);

    sds ToString();
};

#endif