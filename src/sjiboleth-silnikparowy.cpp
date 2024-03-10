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

rax *SilNikParowy::Execute(ParsedExpression *e, SilNikParowy_Kontekst *stack, const char *key)
{
    stack->Reset();
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
    stack->Push(triggers);
    e->ClearErrors();
    SilNikParowy::Preload(e, stack);
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
    // SilNikParowy::ExceptionHandler_Activate();
    e->expression->StartHead();
    ParserToken *t;
    FaBlok *kd;
    // setjmp( SilNikParowy::env_buffer );
    // if(SilNikParowy::GetException() == NULL){
    int index_only_execution = 0;

    while ((t = e->expression->Next()) != NULL)
    {
        if (index_only_execution == _index_only_execution)
        {
            if (strcmp("~~~=~~~", (const char*)t->Operation()) != 0)
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
                if (propertyValue)
                    kd = FaBlok::Get(propertyValue, KeysetDescriptor_TYPE_SINGULAR);
                else
                    kd = FaBlok::Get((isdigit(t->Operation()[0] || t->TokenType() == _operand) ? "0" : t->Token()), KeysetDescriptor_TYPE_UNKNOWN);
            }
            else
                kd = FaBlok::Get(t->Token(), KeysetDescriptor_TYPE_SINGULAR);
            kd->IsValid();
            stack->Push(kd);
            break;
        case _expression:
        case _operator:
            if (t->IsObjectExpression() && ((t->Options() && PARSER_OPTION_DELAY_OBJECT_EXPRESSION) == PARSER_OPTION_DELAY_OBJECT_EXPRESSION))
            {
                auto tmp = generate_uuid();
                kd = FaBlok::Get(tmp.c_str(), KeysetDescriptor_TYPE_OBJECT_EXPRESSION);
                kd->ObjectExpression(t);
                stack->Push(kd);
            }
            else
            {
                if (t->HasExecutor())
                {
                    long long start = ustime();
                    int rc = t->Execute(stack);
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
                    }
                    t->us_calls += laps;
                    t->n_calls++;
                }
                else if (t->Priority() > priImmediate)
                {
                    char msg[256];
                    snprintf(msg, sizeof(msg), "%d Invalid operation %s\n", t->Priority(), t->Token());
                    e->AddError(msg);
                }
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
                rxServerLog(rxLL_NOTICE, "ERROR: %s\n", e->ToString());
                rxServerLog(rxLL_NOTICE, "ERROR: %s\n", (char *)ln->value);
                error = rxStringFormat("%s%s\n", error, (char *)ln->value);
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