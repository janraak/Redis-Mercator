#ifndef __sjiboleth_silnikparowy_CPP__
#define __sjiboleth_silnikparowy_CPP__

#ifdef __cplusplus
extern "C"
{
#endif
#include <cstdlib>

#include "../../deps/hiredis/hiredis.h"
#include "sdsWrapper.h"
#include "string.h"

#include "../../src/rax.h"
#include "rxSuiteHelpers.h"
    extern raxNode *raxNewNode(size_t children, int datafield);

#ifdef __cplusplus
}
#endif

#include <cstring>
extern std::string generate_uuid();
#include "client-pool.hpp"
#include "sjiboleth-fablok.hpp"
#include "sjiboleth-graph.hpp"

static void getRunningSha(SHA1_CTX *context, char *sha1)
{
    SHA1_CTX copy;
    memcpy(&copy, context, sizeof(SHA1_CTX));
    unsigned char digest[20];
    SHA1Final(digest, &copy);
    char *filler = sha1;
    for (int i = 0; i < 20; i++, filler += 2)
    {
        sprintf(filler, "%02x", digest[i]);
    }
    *filler = 0x00;
}
rax *SilNikParowy::Execute(ParsedExpression *e, SilNikParowy_Kontekst *stack, const char *key)
{
    stack->Reset();
    SHA1Init(&stack->fingerprint);
    SHA1Init(&stack->step_fingerprint);
    SHA1Update(&stack->fingerprint, (const unsigned char *)key, strlen(key));

    FaBlok *v = FaBlok::Get((char *)key, KeysetDescriptor_TYPE_KEYSET);
    v->AsTemp();
    // if(v->size <= 0 )
    v->LoadKey(0, v->setname);
    stack->Push(v);
    e->ClearErrors();
    SilNikParowy::Preload(e, stack);
    rax *result = SilNikParowy::Execute(e, stack);
    // delete v;
    return result;
}

rax *SilNikParowy::ExecuteWithSet(ParsedExpression *e, SilNikParowy_Kontekst *stack, FaBlok *triggers)
{
    stack->Reset();
    SHA1Init(&stack->fingerprint);
    SHA1Init(&stack->step_fingerprint);
    SHA1Update(&stack->fingerprint, (const unsigned char *)triggers->setname, strlen(triggers->setname));
    e->ClearErrors();
    SilNikParowy::Preload(e, stack);
    stack->Push(triggers);
    rax *result = SilNikParowy::Execute(e, stack);
    return result;
}

void SilNikParowy::Preload(ParsedExpression *e, SilNikParowy_Kontekst *)
{
    e->expression->StartHead();
    ParserToken *t;
    while ((t = e->expression->Next()) != NULL)
    {
        if (strlen(t->Token()) > 0)
        {
            switch (t->TokenType())
            {
            case _operand:
                FaBlok::Get(t->Token(), KeysetDescriptor_TYPE_SINGULAR);
                break;
            case _literal:
                FaBlok::Get(t->Token(), KeysetDescriptor_TYPE_SINGULAR);
                break;
            default:
                break;
            }
        }
        else
        {
            rxServerLogRaw(rxLL_WARNING,
                           rxStringNew("Odd token"));
        }
    }
}

// void *SilNikParowy::CurrentException = NULL;

// struct sigaction SilNikParowy::new_action;
// struct sigaction SilNikParowy::old_action;
//    jmp_buf SilNikParowy::env_buffer;

// __sighandler_t *SilNikParowy::UpstreamExceptionHandler = NULL;
// __sighandler_t *SilNikParowy::CurrentExceptionHandler = NULL;

// void SilNikParowy::ExceptionHandler(int signal, siginfo_t *info, void *secret)
// {
//     printf("ExceptionHandler \: %d\n", signal);
//     SilNikParowy::CurrentException = info;
//     longjmp(SilNikParowy::env_buffer, "SilNikParowy::ExceptionHandler");
// }

// bool SilNikParowy::ExceptionHandlerState = false;

// bool SilNikParowy::IsExceptionHandlerActive() { return false; }

// bool SilNikParowy::ExceptionHandler_Activate()
// {
//     if (SilNikParowy::CurrentExceptionHandler == NULL)
//     {
//         SilNikParowy::CurrentExceptionHandler = (__sighandler_t *)&SilNikParowy::ExceptionHandler;
//         new_action.sa_handler = SilNikParowy::ExceptionHandler;
//         sigemptyset(&SilNikParowy::new_action.sa_mask);
//         sigaddset(&SilNikParowy::old_action.sa_mask, SIGSEGV);
//         sigaddset(&SilNikParowy::old_action.sa_mask, SIGINT);
//         sigaddset(&SilNikParowy::old_action.sa_mask, SIGFPE);
//         sigaddset(&SilNikParowy::old_action.sa_mask, SIGTERM);
//         SilNikParowy::new_action.sa_flags = 0;
//         // SilNikParowy::new_action.sa_flags = SA_NODEFER | SA_RESETHAND | SA_SIGINFO;
//         sigaction(SIGSEGV, &SilNikParowy::new_action, &SilNikParowy::old_action);

//         sigaction(SIGFPE, &SilNikParowy::new_action, NULL);
//         sigaction(SIGTERM, &SilNikParowy::new_action, NULL);
//         sigaction(SIGINT, &SilNikParowy::new_action, NULL);
//     }
//     SilNikParowy::ExceptionHandlerState = true;
//     return SilNikParowy::ExceptionHandlerState;
// }
// bool SilNikParowy::ExceptionHandler_Deactivate()
// {
//     SilNikParowy::ExceptionHandlerState = false;
//     return SilNikParowy::ExceptionHandlerState;
// }
// void *SilNikParowy::GetException()
// {
//     return SilNikParowy::CurrentException;
// }

rax *SilNikParowy::Execute(ParsedExpression *e, SilNikParowy_Kontekst *stack, void *data)
{
    auto memoization_strategy = e->memoization_strategy;
    if (!stack->HasEntries())
    {
        SHA1Init(&stack->fingerprint);
        SHA1Init(&stack->step_fingerprint);
    }
    // SilNikParowy::ExceptionHandler_Activate();
    e->expression->StartHead();
    ParserToken *t;
    FaBlok *kd;
    // setjmp( SilNikParowy::env_buffer );
    // if(SilNikParowy::GetException() == NULL){
    int index_only_execution = 0;
    int stack_checkpoint = 0;
    while ((t = e->expression->Next()) != NULL)
    {
        kd == NULL;
        SHA1Update(&stack->fingerprint, (const unsigned char *)t->Operation(), strlen(t->Token()));
        SHA1Update(&stack->step_fingerprint, (const unsigned char *)t->Operation(), strlen(t->Token()));
        getRunningSha(&stack->fingerprint, stack->sha);
        getRunningSha(&stack->step_fingerprint, stack->step_sha);
        // rxServerLog(rxLL_NOTICE, "%d %p fingerprint: P=%s S=%s %s", (int)gettid(), stack, stack->sha, stack->step_sha, t->Operation());
        if (index_only_execution == _index_only_execution)
        {
            if (strcmp("~~~=~~~", (const char *)t->Operation()) != 0)
                t->TokenType(_literal);
        }
        if (t->IsObjectExpression())
        {
            t->TokenType(_operator);
        }

        switch (t->TokenType())
        {
        case _index_only_execution:
            index_only_execution = _index_only_execution;
            break;
        case _operand:
        case _literal:
            if (t->IsAllSpaces())
                continue;
            if (data && rxGetObjectType(data) == rxOBJ_HASH && !t->IsNumber()
                /*&& rxHashTypeExists(data, t->Token())*/)
            {
                rxString propertyValue = rxGetHashField(data, t->Token());
                switch (memoization_strategy)
                {
                case eMemoizationStrategy::PathMemoization:
                    if (propertyValue)
                        kd = FaBlok::Get(stack->sha, propertyValue, KeysetDescriptor_TYPE_SINGULAR);
                    else
                        kd = FaBlok::Get(stack->sha, (isdigit(t->Operation()[0] || t->TokenType() == _operand) ? "0" : t->Token()), KeysetDescriptor_TYPE_UNKNOWN);
                    break;
                case eMemoizationStrategy::TokenMemoization:
                    if (propertyValue)
                        kd = FaBlok::Get(propertyValue, KeysetDescriptor_TYPE_SINGULAR);
                    else
                        kd = FaBlok::Get((isdigit(t->Operation()[0] || t->TokenType() == _operand) ? "0" : t->Token()), KeysetDescriptor_TYPE_UNKNOWN);
                    break;
                case eMemoizationStrategy::StepMemoization:
                    if (propertyValue)
                        kd = FaBlok::Get(stack->sha, propertyValue, KeysetDescriptor_TYPE_SINGULAR);
                    else
                        kd = FaBlok::Get(stack->sha, (isdigit(t->Operation()[0] || t->TokenType() == _operand) ? "0" : t->Token()), KeysetDescriptor_TYPE_UNKNOWN);
                    break;
                case eMemoizationStrategy::NoMemoization:
                default:
                    if (propertyValue)
                        kd = FaBlok::Get(propertyValue, KeysetDescriptor_TYPE_SINGULAR);
                    else
                        kd = FaBlok::Get((isdigit(t->Operation()[0] || t->TokenType() == _operand) ? "0" : t->Token()), KeysetDescriptor_TYPE_UNKNOWN);
                    break;
                }
            }
            else
            {
                switch (memoization_strategy)
                {
                case eMemoizationStrategy::PathMemoization:
                    kd = FaBlok::Get(stack->sha, t->Token(), KeysetDescriptor_TYPE_SINGULAR);
                    break;
                case eMemoizationStrategy::TokenMemoization:
                    kd = FaBlok::Get(t->Token(), KeysetDescriptor_TYPE_SINGULAR);
                    break;
                case eMemoizationStrategy::StepMemoization:
                    kd = FaBlok::Get(stack->step_sha, t->Token(), KeysetDescriptor_TYPE_SINGULAR);
                    break;
                case eMemoizationStrategy::NoMemoization:
                default:
                    kd = FaBlok::Get(t->Token(), KeysetDescriptor_TYPE_SINGULAR);
                    break;
                }
            }
            kd->ref_count++;
            kd->reuse_count++;
            kd->IsValid();
            stack->Push(kd);
            break;
        case _expression:
        case _operator:
            if (t->IsObjectExpression() && ((t->Options() && PARSER_OPTION_DELAY_OBJECT_EXPRESSION) == PARSER_OPTION_DELAY_OBJECT_EXPRESSION))
            {
                auto tmp = generate_uuid();
                kd = FaBlok::Get(stack->sha, stack->sha, KeysetDescriptor_TYPE_OBJECT_EXPRESSION);
                kd->ObjectExpression(t);
                kd->ref_count++;
                kd->reuse_count++;
                stack->Push(kd);
            }
            else
            {
                if (t->HasExecutor())
                {
                    if (t->WillReturnResult())
                        switch (memoization_strategy)
                        {
                        case eMemoizationStrategy::PathMemoization:
                            kd = FaBlok_Get(stack->sha);
                            if (kd && stack->HasEntries())
                            {
                                kd->reuse_count++;
                                // rxServerLog(rxLL_NOTICE, "%d %p PathMemoization: P=%s S=%s %s %s", (int)gettid(), stack, stack->sha, stack->step_sha, t->Operation(), kd->setname);
                                while(stack->HasEntries()){
                                    auto e = stack->Pop();
                                    FaBlok::Delete(e);            
                                }
                                stack->Push(kd);
                                continue;
                            }
                            break;
                        case eMemoizationStrategy::StepMemoization:
                            kd = FaBlok_Get(stack->step_sha);
                            if (kd)
                            {
                                kd->reuse_count++;
                                // rxServerLog(rxLL_NOTICE, "%d %p Potential memoization to apply: chk:%d-%d  P=%s S=%s %s %s", (int)gettid(), stack, stack_checkpoint, stack->Size() - 1, stack->sha, stack->step_sha, t->Operation(), kd->setname);
                                // rxServerLog(rxLL_NOTICE, "%d %p StepMemoization: %s %s %s", (int)gettid(), stack, stack->step_sha, t->Operation(), kd->setname);
                                // int n = t->No_of_stack_entries_consumed();
                                // while (stack->HasEntries() && n > 0)
                                // {
                                //     auto e = stack->Pop();
                                //     FaBlok::Delete(e);
                                //     n--;
                                // }
                                // stack->Push(kd);
                                // continue;
                            }
                            break;
                        case eMemoizationStrategy::TokenMemoization:
                        case eMemoizationStrategy::NoMemoization:
                        default:
                            break;
                        }

                    int stack_sz = stack->Size();
                    long long start = ustime();
                    int rc = C_OK;
                    // if (kd)
                    // {
                    //     for (int n = 0; n < t->No_of_stack_entries_consumed(); ++n)
                    //     {
                    //         FaBlok *i = stack->Pop();
                    //         rxServerLog(rxLL_NOTICE, "discarded: %s", i->setname);
                    //     }
                    //     stack->Push(kd);
                    //     rxServerLog(rxLL_NOTICE, "memoized result: %s", kd->setname);
                    // }
                    // else
                    {
                        rc = t->Execute(stack);
                    }
                    long long laps = ustime() - start;

                    if (rc == C_ERR)
                    {
                        if (t->copy_of != NULL)
                        {
                            t->copy_of->us_errors += laps;
                            t->copy_of->n_errors++;
                        }
                        t->us_errors += laps;
                        t->n_errors++;
                        goto end_of_loop;
                    }
                    if (t->copy_of != NULL)
                    {
                        t->copy_of->us_calls += laps;
                        t->copy_of->n_calls++;
                        t->copy_of->stack_in += stack_sz;
                        t->copy_of->stack_out += stack->Size();
                        t->copy_of->stack_diff += (stack->Size() - stack_sz);
                    }
                    t->us_calls += laps;
                    t->n_calls++;
                    t->stack_in += stack_sz;
                    t->stack_out += stack->Size();
                    t->stack_diff += (stack->Size() - stack_sz);

                    if (t->WillReturnResult())
                    {
                        auto r = stack->Peek();
                        if (r && !r->IsValueType(KeysetDescriptor_TYPE_SINGULAR + KeysetDescriptor_TYPE_PARAMETER_LIST) && raxSize(r->keyset) > 0)
                            switch (memoization_strategy)
                            {
                            case eMemoizationStrategy::PathMemoization:
                            case eMemoizationStrategy::StepMemoization:
                                FaBlok_Set(stack->sha, r);
                                FaBlok_Set(stack->step_sha, r);
                            case eMemoizationStrategy::TokenMemoization:
                            case eMemoizationStrategy::NoMemoization:
                            default:
                                break;
                            }
                    }
                }
                else if (t->Priority() > priImmediate)
                {
                    char msg[256];
                    snprintf(msg, sizeof(msg), "%d Invalid operation %s\n", t->Priority(), t->Token());
                    e->AddError(msg);
                }
                SHA1Init(&stack->step_fingerprint);
                if (((t->Options() & Q_KEEP_STACK_POSITION)) != Q_KEEP_STACK_POSITION)
                    stack_checkpoint = stack->Size() - 1;
                break;
            default:
                break;
            }
        }
    }
    // }
end_of_loop:
    // SilNikParowy::ExceptionHandler_Deactivate();
    // rxServerLog(rxLL_NOTICE, "\n");
    rxString error;
    rax *rx = NULL;
    if (listLength(e->errors) > 0)
    {
        e->show(NULL);
        error = rxStringEmpty();
        listNode *ln;
        listIter *li = listGetIterator(e->errors, AL_START_HEAD);
        while ((ln = listNext(li)) != NULL)
        {
            if (strlen((char *)ln->value) > 0)
            {
                error = rxStringFormat("ERROR: %s%s\n", e->ToString(), (char *)ln->value);
                rxServerLog(rxLL_NOTICE, error);
            }
        }
        if (strlen(error) > 0)
        {
            return NULL;
        }
    }

    {
        ParserToken *t = NULL;
        FaBlok *r = NULL;
        switch (stack->Size())
        {
        case 0:
            t = e->lastInstruction();
            if (t != NULL && t->CompareToken(rxStringNew("=")) != 0)
            {
                e->AddError(rxStringNew("Invalid expression, no result yielded!"));
            }
            break;
        case 1:
            if (data)
                return NULL;
            r = stack->Pop();
            if (r->IsValueType(KeysetDescriptor_TYPE_MONITORED_SET))
                stack->RetainResult();
            if ((r->IsValueType(KeysetDescriptor_TYPE_KEYSET) || r->IsValueType(KeysetDescriptor_TYPE_SINGULAR)) && !r->HasKeySet())
            {
                stack->FetchKeySet(r);
            }
            e->final_result_value_type = r->ValueType();
            rx = r->AsRax();
            return rx;
        case 2:
        default:
        {
            r = stack->Pop();
            if (r->IsValueType(KeysetDescriptor_TYPE_MONITORED_SET))
                stack->RetainResult();
            // FaBlok *checkV = stack->Pop();
            // FaBlok *memo = (FaBlok *)stack->Recall("V");
            // if (memo != NULL && memo != checkV)
            // {
            //     error = rxStringNew("Invalid expression, ");
            //     error = rxStringFormat("%s %d  results yielded!\n", error, stack->Size());
            //     e->AddError(error);
            //     return NULL;
            // }
            if ((r->IsValueType(KeysetDescriptor_TYPE_KEYSET) || r->IsValueType(KeysetDescriptor_TYPE_SINGULAR)) && !r->HasKeySet())
            {
                r->FetchKeySet(stack->serviceConfig, rxStringEmpty(), r->setname, rxStringEmpty());
                // stack->Push(r);
            }
            if (r->IsValueType(KeysetDescriptor_TYPE_MONITORED_SET))
                stack->RetainResult();
            e->final_result_value_type = r->ValueType();
            rx = r->AsRax();

            while (stack->HasEntries())
            {
                stack->Pop();
            }

            return rx;
        }
        break;
            // default:
            //     error = rxStringNew("Invalid expression, ");
            //     error = rxStringFormat("%s %d  results yielded!\n", error, stack->Size());
            //     e->AddError(error);
            //     this->ClearStack();
            //     break;
        }
    }
    return NULL;
}

rax *SilNikParowy::Execute(ParsedExpression *e, SilNikParowy_Kontekst *stack)
{
    return Execute(e, stack, (void *)NULL);
}

SilNikParowy::SilNikParowy()
{
}

SilNikParowy::~SilNikParowy()
{
}
#endif