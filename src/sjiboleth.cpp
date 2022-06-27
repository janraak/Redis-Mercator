#include "sjiboleth.hpp"
#include "sjiboleth.h"
#include "sjiboleth-fablok.hpp"

#include <cstring>

#include <setjmp.h>
#include <signal.h>

ParserToken::ParserToken()
{
    this->token_type = _immediate_operator;
    this->op = sdsempty();
    this->op_len = 0;
    this->token_priority = priImmediate;
    this->opf = NULL;
    this->pcf = NULL;
    this->no_of_stack_entries_consumed = 1;
    this->no_of_stack_entries_produced = 1;
    this->is_volatile = false;
}

ParserToken::ParserToken(eTokenType token_type,
                         const char *op,
                         int op_len)
    : ParserToken()
{
    sds opje = sdsnewlen(op, op_len);
    this->op = strdup(opje);
    sdsfree(opje);
    this->op_len = op_len;
    this->token_type = token_type;
    this->no_of_stack_entries_consumed = 0;
    this->no_of_stack_entries_produced = 1;
}

ParserToken::ParserToken(const char *aOp,
                         eTokenType aToken_type,
                         short aToken_priority,
                         short aNo_of_stack_entries_consumed,
                         short aNo_of_stack_entries_produced)
    : ParserToken()
{
    this->op_len = strlen(aOp);
    this->op = sdsdup(sdsnewlen(aOp, this->op_len));
    sdstoupper(this->op);
    this->token_type = aToken_type;
    this->token_priority = aToken_priority;
    this->no_of_stack_entries_consumed = aNo_of_stack_entries_consumed;
    this->no_of_stack_entries_produced = aNo_of_stack_entries_produced;
}

ParserToken *ParserToken::Copy()
{
    ParserToken *out = new ParserToken(this->Token(), this->TokenType(), this->Priority(), this->no_of_stack_entries_consumed, this->no_of_stack_entries_produced);
    return out;
}

ParserToken::ParserToken(const char *op,
                         eTokenType token_type,
                         short token_priority,
                         short no_of_stack_entries_consumed,
                         short no_of_stack_entries_produced,
                         operationProc *opf)
    : ParserToken(op, token_type, token_priority, no_of_stack_entries_consumed, no_of_stack_entries_produced)
{
    this->opf = opf;
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
    sdsfree(this->op);
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

sds ParserToken::TokenAsSds()
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

int ParserToken::Execute(CParserToken *tO, CStack *stackO)
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
        rc = this->opf(tO, stackO);
//         goto done;
//     }
//     else
//     {
//         printf("Captured signal\n");
//         rc = -1;
//     }
// done:
//     signal(SIGFPE, SIG_DFL);
//     signal(SIGILL, SIG_DFL);
//     signal(SIGSEGV, SIG_DFL);
    return rc;
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

int ParserToken::CompareToken(sds aStr)
{
    return sdscmp(this->op, aStr);
}

bool ParserToken::Is(const char *aStr)
{
    const char *tok = this->Token();
    while (*tok && *aStr)
    {
        if (toupper(*tok) != toupper(*aStr))
            return false;
        ++tok;
        ++aStr;
    }
    return true;
}

bool Sjiboleth::resetSyntax()
{
    return false;
}

bool Sjiboleth::RegisterSyntax(const char *op, short token_priority, int inE, int outE, operationProc *opf)
{
    auto *t = new ParserToken(op, _operator, token_priority, inE, outE, opf);
    void *old;
    raxRemove(this->registry, t->Operation(), t->OperationLength(), &old);
    raxInsert(this->registry, t->Operation(), t->OperationLength(), t, NULL);
    return true;
}

bool Sjiboleth::RegisterSyntax(const char *op, short token_priority, int inE, int outE, operationProc *opf, parserContextProc *pcf)
{
    auto *t = new ParserToken(op, _operator, token_priority, inE, outE, opf);
    void *old;
    raxRemove(this->registry, t->Operation(), t->OperationLength(), &old);
    t->ParserContextProc(pcf);
    raxInsert(this->registry, t->Operation(), t->OperationLength(), t, NULL);
    return true;
}

bool Sjiboleth::DeregisterSyntax(const char *op)
{
    void *old;
    raxRemove(this->registry, (UCHAR *)op, strlen(op), &old);
    return true;
}


ParserToken *Sjiboleth::LookupToken(sds token){
	ParserToken *referal_token = (ParserToken *)raxFind(this->registry, (UCHAR *)token, sdslen(token));
	if (referal_token != raxNotFound){
        // if (referal_token->Priority() == priBreak)
        //     return NULL;
        return referal_token;
    }
    return NULL;
}

Sjiboleth::Sjiboleth()
{
    this->default_operator = sdsnew("|");
    this->registry = raxNew();
    this->registerDefaultSyntax();
    this->object_and_array_controls = false;
    this->crlftab_as_operator = false;
}

Sjiboleth::Sjiboleth(const char *default_operator)
    : Sjiboleth()
{
    this->default_operator = sdsnew(default_operator);
    this->registry = raxNew();
    this->registerDefaultSyntax();
}

Sjiboleth::~Sjiboleth()
{
    this->resetSyntax();
    raxFree(this->registry);
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


