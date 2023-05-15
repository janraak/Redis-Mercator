#include "sjiboleth.hpp"
#include "sjiboleth.h"
#include "sjiboleth-fablok.hpp"

#include <cstring>

#include <setjmp.h>
#include <signal.h>

#ifdef __cplusplus
extern "C"
{
#endif
    #include "sdsWrapper.h"
#ifdef __cplusplus
}
#endif
#include "tls.hpp"

void *rxMemAllocSession(size_t size, const char *tag);

ParserToken::ParserToken()
{
    this->Init(eTokenType::_immediate_operator, NULL, 0);
}

ParserToken *ParserToken::Init(eTokenType token_type,
                         const char *op,
                         int op_len)
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
    return this;
}

ParserToken *ParserToken::New(eTokenType token_type,
                         const char *op,
                         int op_len)
{
    auto *token = (ParserToken *)rxMemAlloc(sizeof(ParserToken) + op_len + 1);
    char *s = (char *)token + sizeof(ParserToken);
    strncpy(s, op, op_len);
    s[op_len] = 0x00;
    token->Init(token_type, s, op_len);
    return token;
}

ParserToken *ParserToken::New(const char *aOp,
                         eTokenType aToken_type,
                         short aToken_priority,
                         short aNo_of_stack_entries_consumed,
                         short aNo_of_stack_entries_produced)
{
    auto *token = ParserToken::New(aToken_type, aOp, strlen(aOp));
    rxStringToUpper(token->op);
    token->token_priority = aToken_priority;
    token->no_of_stack_entries_consumed = aNo_of_stack_entries_consumed;
    token->no_of_stack_entries_produced = aNo_of_stack_entries_produced;
    return token;
}

ParserToken *ParserToken::Copy(ParserToken *base, long reference)
{
    auto *token = ParserToken::New(base->token_type, base->op, base->op_len);
    token->token_priority = base->token_priority;
    token->no_of_stack_entries_consumed = base->no_of_stack_entries_consumed;
    token->no_of_stack_entries_produced = base->no_of_stack_entries_produced;
    token->opf= base->opf;
    token->pcf= base->pcf;
    token->is_volatile = true;
    token->crlftab_as_operator= base->crlftab_as_operator;
    token->copy_of = base;
    token->reference = reference;
    return token;
}

ParserToken *ParserToken::Copy(long reference)
{
    ParserToken* out = ParserToken::New(this->Token(), this->TokenType(), this->Priority(), this->no_of_stack_entries_consumed, this->no_of_stack_entries_produced);

    out->opf= this->opf;
    out->pcf=this->pcf;
    out->copy_of = this;
    out->reference = reference;
    return out;
}

ParserToken *ParserToken::New(const char *op,
                         eTokenType token_type,
                         short token_priority,
                         short no_of_stack_entries_consumed,
                         short no_of_stack_entries_produced,
                         operationProc *opf)
{
    auto *token =  ParserToken::New(op, token_type, token_priority, no_of_stack_entries_consumed, no_of_stack_entries_produced);
    token->opf = opf;
    return token;
}

void ParserToken::Purge(ParserToken *token){
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

    if(t->copy_of != NULL)
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
    if(t->copy_of){
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

int ParserToken::CompareToken(const char *aStr)
{
    return strcmp(this->op, aStr);
}

bool ParserToken::Is(const char *aStr)
{
    const char *tok = this->Token();
    if(tok == NULL)
        return false;
    while (*tok && *aStr)
    {
        if (toupper(*tok) != toupper(*aStr))
            return false;
        ++tok;
        ++aStr;
    }
    return true;
}

bool Sjiboleth::ResetSyntax()
{
    return false;
}

bool Sjiboleth::RegisterSyntax(const char *op, short token_priority, int inE, int outE, operationProc *opf)
{
    auto *t = ParserToken::New(op, _operator, token_priority, inE, outE, opf);
    void *old = NULL;
    raxRemove(this->registry, t->Operation(), t->OperationLength(), &old);
    raxInsert(this->registry, t->Operation(), t->OperationLength(), t, NULL);

    if(old != NULL)
        FreeSyntax(old);
    return true;
}

bool Sjiboleth::RegisterSyntax(const char *op, short token_priority, int inE, int outE, operationProc *opf, parserContextProc *pcf)
{
    auto *t = ParserToken::New(op, _operator, token_priority, inE, outE, opf);
    void *old = NULL;
    raxRemove(this->registry, t->Operation(), t->OperationLength(), &old);
    t->ParserContextProc(pcf);
    raxInsert(this->registry, t->Operation(), t->OperationLength(), t, NULL);
    if(old != NULL)
        FreeSyntax(old);
    return true;
}

bool Sjiboleth::DeregisterSyntax(const char *op)
{
    // void *old = NULL;
    // raxRemove(this->registry, (UCHAR *)op, strlen(op), &old);

    // //TODO: have a global syntax registry per dialect 
    // //TODO: deregister safely for used tokens
    // // if(old != NULL)
    // //     FreeSyntax(old);
    return true;
}


ParserToken *Sjiboleth::LookupToken(rxString token){
	ParserToken *referal_token = (ParserToken *)raxFind(this->registry, (UCHAR *)token, strlen(token));
	if (referal_token != raxNotFound){
        return referal_token;
    }
    return NULL;
}

rax *Sjiboleth::Master_Registry = NULL;
rax *Sjiboleth::Get_Master_Registry()
{
    if (Sjiboleth::Master_Registry == NULL)
        Sjiboleth::Master_Registry = raxNew();
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
    //TODO: Separate syntax rules as globals!
    if(this->registry == NULL)
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


void *_allocateDialect(void *){
    return raxNew();
}

Sjiboleth *Sjiboleth::Get(const char* dialect){
    auto *master = tls_get<rax *>((const char*)"Sjiboleth", _allocateDialect, NULL);
    // auto *master = Sjiboleth::Get_Master_Registry();
    size_t dialect_len = strlen(dialect);
    auto *parser = (Sjiboleth*)raxFind(master, (UCHAR *)dialect, dialect_len);
    if(parser == raxNotFound){
        if(rxStringMatch(dialect,"QueryDialect", 1))
            parser = new QueryDialect();
        else if(rxStringMatch(dialect,"GremlinDialect", 1))
            parser = new GremlinDialect();
        else if(rxStringMatch(dialect,"JsonDialect", 1))
            parser = new JsonDialect();
        else if(rxStringMatch(dialect,"TextDialect", 1))
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

SilNikParowy *Sjiboleth::GetEngine(){
    // FOR NOW!
    return new SilNikParowy();
}

SilNikParowy *GremlinDialect::GetEngine(){
    return new SilNikParowy();
}

SilNikParowy *QueryDialect::GetEngine(){
    return new SilNikParowy();
}


SilNikParowy *JsonDialect::GetEngine(){
    return new SilNikParowy();
}


SilNikParowy *TextDialect::GetEngine(){
    return new SilNikParowy();
}


