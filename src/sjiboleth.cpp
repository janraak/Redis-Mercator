#include "sjiboleth.hpp"
#include "sjiboleth-fablok.hpp"
#include "sjiboleth.h"

#include <cstring>

#include <setjmp.h>
#include <signal.h>
#include <threads.h>

#ifdef __cplusplus
extern "C"
{
#endif

#include "sdsWrapper.h"
#ifdef __cplusplus
}
#endif
#include "tls.hpp"
void *rxMercatorShared = NULL;

ParserToken::ParserToken()
{
    this->Init(eTokenType::_immediate_operator, NULL, 0, 0, Q_READONLY);
}

ParserToken *ParserToken::Init(eTokenType token_type,
                               const char *op,
                               int op_len,
                               int options, 
                            short read_or_write)
{
    this->op = NULL;
    this->op_len = 0;
    this->token_priority = priImmediate;
    this->pcf = NULL;
    this->is_volatile = false;
    this->copy_of = NULL;
    this->reference = 722765000;

    this->op = op;
    this->op_len = op_len;
    this->token_type = token_type;
    this->no_of_stack_entries_consumed = 0;
    this->no_of_stack_entries_produced = 1;
    this->objectExpression = NULL;
    this->bracketResult = NULL;
    this->options = options;
    this->read_or_write = read_or_write;

    this->n_calls = 0;
    this->n_errors = 0;
    this->us_calls = 0;
    this->us_errors = 0;
    this->n_setsize_in = 0;
    this->n_setsize_out = 0;
    return this;
}

ParserToken *ParserToken::New(eTokenType token_type,
                            const char *op,
                            int op_len, 
                            int options)
{
    auto *token = (ParserToken *)rxMemAlloc(sizeof(ParserToken) + op_len + 1);
    char *s = (char *)token + sizeof(ParserToken);
    strncpy(s, op, op_len);
    s[op_len] = 0x00;
    token->Init(token_type, s, op_len, options, Q_READONLY);
    return token;
}

ParserToken *ParserToken::New(const char *aOp,
                              eTokenType aToken_type,
                              short aToken_priority,
                              short aNo_of_stack_entries_consumed,
                              short aNo_of_stack_entries_produced,
                               int options, 
                            short read_or_write)
{
    auto *token = ParserToken::New(aToken_type, aOp, strlen(aOp), options);
    rxStringToUpper(token->op);
    token->token_priority = aToken_priority;
    token->no_of_stack_entries_consumed = aNo_of_stack_entries_consumed;
    token->no_of_stack_entries_produced = aNo_of_stack_entries_produced;
    token->read_or_write = read_or_write;
    return token;
}

ParserToken *ParserToken::Copy(ParserToken *base, long reference)
{
    auto *token = ParserToken::New(base->token_type, base->op, base->op_len, base->options);
    token->token_priority = base->token_priority;
    token->no_of_stack_entries_consumed = base->no_of_stack_entries_consumed;
    token->no_of_stack_entries_produced = base->no_of_stack_entries_produced;
    token->opf = base->opf;
    token->pcf = base->pcf;
    token->options = base->options;
    token->is_volatile = true;
    token->crlftab_as_operator = base->crlftab_as_operator;
    token->copy_of = base;
    token->reference = reference;
    token->read_or_write = base->read_or_write;
    return token;
}

void ParserToken::AggregateSizeSizes(long in, long out){
    auto t = this->copy_of ? this->copy_of : this;
    t->n_setsize_in += in;
    t->n_setsize_out += out;
}
void ParserToken::ObjectExpression(ParsedExpression *objectExpression)
{
    this->objectExpression = objectExpression;
}

ParsedExpression *ParserToken::ObjectExpression()
{
    return this->objectExpression;
}

ParserToken *ParserToken::BracketResult()
{
    return this->bracketResult;
}

void ParserToken::BracketResult(ParserToken *t)
{
    this->bracketResult = t;
}

ParserToken *ParserToken::Copy(long reference)
{
    ParserToken *out = ParserToken::New(this->Token(), this->TokenType(), this->Priority(), this->no_of_stack_entries_consumed, this->no_of_stack_entries_produced, this->read_or_write, this-> options);

    out->opf = this->opf;
    out->pcf = this->pcf;
    out->copy_of = this;
    out->reference = reference;
    return out;
}

ParserToken *ParserToken::New(const char *op,
                              eTokenType token_type,
                              short token_priority,
                              short no_of_stack_entries_consumed,
                              short no_of_stack_entries_produced,
                              operationProc *opf,
                               int options, 
                            short read_or_write)
{
    auto *token = ParserToken::New(op, token_type, token_priority, no_of_stack_entries_consumed, no_of_stack_entries_produced, options, read_or_write);
    token->opf = opf;
    return token;
}

void ParserToken::Purge(ParserToken *token)
{
    auto *t = token;
    if (t->copy_of)
    {
        auto *c = t->copy_of;
        if (t->opf != c->opf)
        {
            rxServerLog(rxLL_NOTICE, "Base Token %p %s opf:%p copy_of:%p reference:%ld", c, c->TokenAsSds(), c->opf, c->copy_of, c->reference);
            rxServerLog(rxLL_NOTICE, "opf mismatch Token %p::%p %s opf:%p::%p", t, c, t->TokenAsSds(), t->opf, c->opf);
        }
        if (c->copy_of)
        {
            rxServerLog(rxLL_NOTICE, "Base Token %p %s opf:%p copy_of:%p reference:%ld", c, c->TokenAsSds(), c->opf, c->copy_of, c->reference);
            rxServerLog(rxLL_NOTICE, "copy of link corrupt Token  %p %s opf:%p copy_of:%p", c, c->TokenAsSds(), c->opf, c->copy_of);
        }
    }

    if (t->copy_of != NULL)
        // if((void *)(token->op) == (void *)token + sizeof(ParserToken))
        rxMemFree(token);
}
void ParserToken::ParserContextProc(parserContextProc *pcf)
{
    this->pcf = pcf;
}

bool ParserToken::HasParserContextProc()
{
    return (this->pcf != NULL);
}

parserContextProc *ParserToken::ParserContextProc()
{
    return this->pcf;
}

ParserToken::~ParserToken()
{
    this->op = NULL;
    this->op_len = 0;
    this->token_priority = 0;
    this->opf = NULL;
    this->pcf = NULL;
}

UCHAR *ParserToken::Operation()
{
    return (UCHAR *)this->op;
}

int ParserToken::OperationLength()
{
    return this->op_len;
}

bool ParserToken::IsObjectExpression()
{
    return this->no_of_stack_entries_consumed == -1 && this->no_of_stack_entries_produced == -1;
}

bool ParserToken::IsNumber()
{
    auto c = this->Token();
    auto first = isdigit(*c);
    while (*c){
        if(!isdigit(*c))
            return false;
        c++;
    }
    return first;
}

bool ParserToken::IsAllSpaces()
{
    auto c = this->Token();
    if(!*c)
        return true;
    while (*c)
    {
        if(*c != ' ')
            return false;
        c++;
    }
    return true;
}

void ParserToken::TokenType(eTokenType tt)
{
    this->token_type = tt;
}

eTokenType ParserToken::TokenType()
{
    return this->token_type;
}

const char *ParserToken::Token()
{
    return this->op;
}

const char *ParserToken::TokenAsSds()
{
    return this->op;
}

operationProc *ParserToken::Executor()
{
    return this->opf;
}

bool ParserToken::HasExecutor()
{
    return this->opf != NULL;
}

int ParserToken::ExecuteExceptionReturn = 0;
jmp_buf ExecuteExceptionReturnMark; // Address for long jump to jump to

#ifdef __cplusplus
extern "C"
{
    void fphandler(int sig);
}
#endif

void fphandler(int sig)
{
    rxUNUSED(sig);
    longjmp(ExecuteExceptionReturnMark, -1);
}

int ParserToken::Execute(CStack *stackO)
{
    int rc = -1;
    // signal(SIGFPE, (void (*)(int))fphandler);
    // signal(SIGILL, (void (*)(int))fphandler);
    // signal(SIGSEGV, (void (*)(int))fphandler);

    // ParserToken::ExecuteExceptionReturn = 0;
    // ParserToken::ExecuteExceptionReturn = setjmp(ExecuteExceptionReturnMark);

    // if (ParserToken::ExecuteExceptionReturn == 0)
    // {
    //     ParserToken::ExecuteExceptionReturn = 0;
    auto *t = this;
    // rxServerLog(rxLL_NOTICE, "Token %p %s opf:%p copy_of:%p reference:%ld", t, t->TokenAsSds(), t->opf, t->copy_of, t->reference );
    if (t->copy_of)
    {
        auto *c = t->copy_of;
        if (t->opf != c->opf)
        {
            rxServerLog(rxLL_NOTICE, "Base Token %p %s opf:%p copy_of:%p reference:%ld", c, c->TokenAsSds(), c->opf, c->copy_of, c->reference);
            rxServerLog(rxLL_NOTICE, "opf mismatch Token %p::%p %s opf:%p::%p", t, c, t->TokenAsSds(), t->opf, c->opf);
        }
        if (c->copy_of)
        {
            rxServerLog(rxLL_NOTICE, "Base Token %p %s opf:%p copy_of:%p reference:%ld", c, c->TokenAsSds(), c->opf, c->copy_of, c->reference);
            rxServerLog(rxLL_NOTICE, "copy of link corrupt Token  %p %s opf:%p copy_of:%p", c, c->TokenAsSds(), c->opf, c->copy_of);
        }
        t = c;
    }
    rc = t->opf(this, stackO);
    //         goto done;
    //     }
    //     else
    //     {
    //         rxServerLog(rxLL_NOTICE, "Captured signal\n");
    //         rc = -1;
    //     }
    // done:
    //     signal(SIGFPE, SIG_DFL);
    //     signal(SIGILL, SIG_DFL);
    //     signal(SIGSEGV, SIG_DFL);
    return rc;
}

static void FreeSyntax(void *o)
{
    rxMemFree(o);
}

void ParserToken::Priority(short token_priority)
{
    this->token_priority = token_priority;
}

short ParserToken::Priority()
{
    return this->token_priority;
}

bool ParserToken::IsVolatile()
{
    return this->is_volatile;
}

    int ParserToken::Options(){
    return this->options;
}

int ParserToken::AddOptions(int object_expression_treatment){
    int o = this->options;
    this->options |= object_expression_treatment;
    if(object_expression_treatment == PARSER_OPTION_DELAY_OBJECT_EXPRESSION)
        this->token_type = _expression;
    return o;
}

int ParserToken::CompareToken(const char *aStr)
{
    return strcmp(this->op, aStr);
}

bool ParserToken::Is(const char *aStr)
{
    const char *tok = this->Token();
    if (tok == NULL)
        return false;
    return rxStringMatch(tok, aStr, 1);
    // while (*tok && *aStr)
    // {
    //     if (toupper(*tok) != toupper(*aStr))
    //         return false;
    //     ++tok;
    //     ++aStr;
    // }
    // return true;
}

mtx_t Master_Registry_lock;
int Master_Registry_lock_started =  mtx_init(&Master_Registry_lock, mtx_plain);

bool Sjiboleth::ResetSyntax()
{
    return false;
}

bool Sjiboleth::RegisterSyntax(const char *op, short token_priority, int inE, int outE, short read_or_write, operationProc *opf, int options)
{
    mtx_lock(&Master_Registry_lock);
    auto *t = ParserToken::New(op, _operator, token_priority, inE, outE, opf, options, read_or_write);
    void *old = NULL;
    raxRemove(this->registry, t->Operation(), t->OperationLength(), &old);
    raxInsert(this->registry, t->Operation(), t->OperationLength(), t, NULL);

    if (old != NULL)
        FreeSyntax(old);
    mtx_unlock(&Master_Registry_lock);
    return true;
}

bool Sjiboleth::RegisterSyntax(const char *op, short token_priority, int inE, int outE, short read_or_write, operationProc *opf, parserContextProc *pcf, int options)
{
    mtx_lock(&Master_Registry_lock);
    auto *t = ParserToken::New(op, _operator, token_priority, inE, outE, opf, options, read_or_write);
    void *old = NULL;
    raxRemove(this->registry, t->Operation(), t->OperationLength(), &old);
    t->ParserContextProc(pcf);
    raxInsert(this->registry, t->Operation(), t->OperationLength(), t, NULL);
    if (old != NULL)
        FreeSyntax(old);
    mtx_unlock(&Master_Registry_lock);
    return true;
}

bool Sjiboleth::RegisterSyntax(const char *op, short token_priority, int inE, int outE, short read_or_write, operationProc *opf)
{
    return Sjiboleth::RegisterSyntax(op, token_priority, inE, outE, read_or_write, opf, 0);
}

bool Sjiboleth::RegisterSyntax(const char *op, short token_priority, int inE, int outE, short read_or_write, operationProc *opf, parserContextProc *pcf)
{
    
    return Sjiboleth::RegisterSyntax(op, token_priority, inE, outE, read_or_write, opf, pcf, 0);
}

bool Sjiboleth::DeregisterSyntax(const char *)
{
    // void *old = NULL;
    // raxRemove(this->registry, (UCHAR *)op, strlen(op), &old);

    // //TODO: have a global syntax registry per dialect
    // //TODO: deregister safely for used tokens
    // // if(old != NULL)
    // //     FreeSyntax(old);
    return true;
}

ParserToken *Sjiboleth::LookupToken(rxString token)
{
    ParserToken *referal_token = (ParserToken *)raxFind(this->registry, (UCHAR *)token, strlen(token));
    if (referal_token != raxNotFound)
    {
        return referal_token;
    }
    return NULL;
}


long long sjiboleth_stats_cron_id = -1;
#define ms_5MINUTES (5 * 60 * 1000)
#define ms_1MINUTE (1 * 60 * 1000)

rax *Sjiboleth::Master_Registry = NULL;
static long long sjiboleth_stats_last_stats = 0;
int sjiboleth_stats_cron(struct aeEventLoop *, long long, void *clientData)
{
    if( (mstime() - sjiboleth_stats_last_stats) < ms_5MINUTES )
        return ms_1MINUTE;

    mtx_lock(&Master_Registry_lock);
    raxIterator dialectIterator;
    raxStart(&dialectIterator, clientData);
    raxSeek(&dialectIterator, "^", NULL, 0);
    while (raxNext(&dialectIterator))
    {
        rxString k = rxStringNewLen((const char *)dialectIterator.key, dialectIterator.key_len);
        rxServerLog(rxLL_NOTICE, "Sjiboleth Execution statistices for: %-16s  [%p]", k, clientData);
        rxStringFree(k);
        raxIterator tokenIterator;
        auto dialect = (Sjiboleth *)dialectIterator.data;
        auto tokens = dialect->Registry();
        if (tokens->numnodes)
        {
            raxStart(&tokenIterator, tokens);
            raxSeek(&tokenIterator, "^", NULL, 0);
                    rxServerLog(rxLL_NOTICE, "... %16s %10s %12s %10s %12s %12s %12s", 
                    "Function", 
                    "calls", 
                    "avg (us)", 
                    "errors", 
                    "avg (us)",
                    "avg in",
                    "avg out");
            while (raxNext(&tokenIterator))
            {
                auto t = (ParserToken *)tokenIterator.data;
                if ((t->n_calls + t->n_errors) > 0 )
                {
                    rxString i = rxStringNewLen((const char *)tokenIterator.key, tokenIterator.key_len);
                    rxServerLog(rxLL_NOTICE, "... %16s %10lld %12.2f %10lld %12.2f %12.2f %12.2f", 
                    i, 
                    t->n_calls, 
                    t->n_calls > 0 ? t->us_calls * 1.0 / t->n_calls : 0, 
                    t->n_errors, 
                    t->n_errors > 0 ? t->us_errors * 1.0 / t->n_errors : 0, 
                    t->n_calls > 0 ? t->n_setsize_in * 1.0 / t->n_calls : 0, 
                    t->n_calls > 0 ? t->n_setsize_out * 1.0 / t->n_calls : 0);
                    rxStringFree(i);
                }
            }
            raxStop(&tokenIterator);
        }
    }
    raxStop(&dialectIterator);
    mtx_unlock(&Master_Registry_lock);
    sjiboleth_stats_last_stats = mstime();

    return ms_1MINUTE;
}

rax *Sjiboleth::Registry(){
    return this->registry;
}
rax *Sjiboleth::Get_Master_Registry()
{
    if (Sjiboleth::Master_Registry == NULL){
        Sjiboleth::Master_Registry = raxNew();
    }
    // if(sjiboleth_stats_cron_id < 0){
    //     sjiboleth_stats_cron_id = rxCreateTimeEvent(1, (aeTimeProc *)sjiboleth_stats_cron, Sjiboleth::Master_Registry, NULL);
    //     rxServerLog(rxLL_NOTICE, "Sjiboleth stats cron started on %p => %lld", Sjiboleth::Master_Registry, sjiboleth_stats_cron_id);
    //     sjiboleth_stats_cron(NULL, 0, Sjiboleth::Master_Registry);
    // }
    return Sjiboleth::Master_Registry;
}

Sjiboleth::Sjiboleth()
{
    this->default_operator = rxStringNew("|");
    this->registry = raxNew();
    this->RegisterDefaultSyntax();
    this->object_and_array_controls = false;
    this->crlftab_as_operator = false;
}

Sjiboleth::Sjiboleth(const char *default_operator)
    : Sjiboleth()
{
    this->default_operator = rxStringNew(default_operator);
}

Sjiboleth::~Sjiboleth()
{
    // TODO: Separate syntax rules as globals!
    if (this->registry == NULL)
        return;
    // if (raxSize(this->registry) > 0)
    // {
    //     raxShow(this->registry);
    //     raxShow(FaBlok::Get_FaBlok_Registry());
    //     raxIterator ri;
    //     raxStart(&ri, this->registry);
    //     raxSeek(&ri, "$", NULL, 0);
    //     while (raxPrev(&ri))
    //     {
    //         void *old = NULL;
    //         raxRemove(this->registry, ri.key, ri.key_len, (void **)&old);
    //         auto *t = (ParserToken *)ri.data;
    //         if(t->Token() == ri.data + sizeof(ParserToken))
    //             rxMemFree(ri.data);
    //     }
    //     raxStop(&ri);
    // }
    // raxFree(this->registry);
    // this->registry = NULL;
}

void *_allocateDialect(void *)
{
    auto r = raxNew();
    sjiboleth_stats_cron_id = rxCreateTimeEvent(1, (aeTimeProc *)sjiboleth_stats_cron, r, NULL);
    sjiboleth_stats_cron(NULL, 0, r);
    return r;
}

Sjiboleth *Sjiboleth::Get(const char *dialect)
{
    // auto *master = tls_get<rax *>((const char *)"Sjiboleth", _allocateDialect, NULL);
    auto *master = Sjiboleth::Get_Master_Registry();
    size_t dialect_len = strlen(dialect);
    auto *parser = (Sjiboleth *)raxFind(master, (UCHAR *)dialect, dialect_len);
    if (parser == raxNotFound)
    {
        if (rxStringMatch(dialect, "QueryDialect", 1))
            parser = new QueryDialect();
        else if (rxStringMatch(dialect, "GremlinDialect", 1))
            parser = new GremlinDialect();
        else if (rxStringMatch(dialect, "JsonDialect", 1))
            parser = new JsonDialect();
        else if (rxStringMatch(dialect, "TextDialect", 1))
            parser = new TextDialect();
        raxInsert(master, (UCHAR *)dialect, dialect_len, parser, NULL);
    }
    return parser;
}

bool Sjiboleth::hasDefaultOperator()
{
    return this->default_operator && *this->default_operator != 0x00;
}

const char *Sjiboleth::defaultOperator()
{
    return this->default_operator;
}

SilNikParowy *Sjiboleth::GetEngine()
{
    // FOR NOW!
    return new SilNikParowy();
}

SilNikParowy *GremlinDialect::GetEngine()
{
    return new SilNikParowy();
}

SilNikParowy *QueryDialect::GetEngine()
{
    return new SilNikParowy();
}

SilNikParowy *JsonDialect::GetEngine()
{
    return new SilNikParowy();
}

SilNikParowy *TextDialect::GetEngine()
{
    return new SilNikParowy();
}

void *Sjiboleth::Startup(){
    if(!Sjiboleth::Master_Registry)
        Sjiboleth::Master_Registry = Sjiboleth_Master_Registry();
    Sjiboleth::Get("QueryDialect");
    Sjiboleth::Get("GremlinDialect");
    Sjiboleth::Get("JsonDialect");
    Sjiboleth::Get("TextDialect");
    sjiboleth_stats_cron_id = rxCreateTimeEvent(1, (aeTimeProc *)sjiboleth_stats_cron, Sjiboleth::Master_Registry, NULL);
    rxServerLog(rxLL_NOTICE, "Sjiboleth stats cron started on %p => %lld", Sjiboleth::Master_Registry, sjiboleth_stats_cron_id);
    sjiboleth_stats_cron(NULL, 0, Sjiboleth::Master_Registry);
    return Sjiboleth::Get_Master_Registry();
}

void *startUp = Sjiboleth::Startup();