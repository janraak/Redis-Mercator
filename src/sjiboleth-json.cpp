#include "sjiboleth.hpp"
#include <cstring>

#include "client-pool.hpp"
#include "sjiboleth-fablok.hpp"
#include "sjiboleth-graph.hpp"
#include "sjiboleth.h"
#include "sjiboleth.hpp"

static void Index_Text(FaBlok *v, SilNikParowy_Kontekst *text_kontekst, Sjiboleth *text_parser)
{
    auto *t = text_parser->Parse(v->setname);
    auto *sub = t;
    while (sub)
    {
        // printf("GNIRTS J: %s\n", sub->ToString());
        text_kontekst->Execute(sub);
        sub = sub->Next();
    }
    delete t;

    // sdsfree(text);
}

SJIBOLETH_HANDLER(IndexJson)
{
    STACK_CHECK(1);
    sds field_name;
    sds standing_field_name = sdsempty();
    if (stack->IsMemoized("@@field@@"))
        standing_field_name = sdsdup((sds)stack->Recall("@@field@@"));
    if (strncmp(":", (const char *)t->TokenAsSds(), 1) == 0)
    {
        FaBlok *p = stack->Pop_Last();
        field_name = sdsdup(p->setname);
    }
    else if (stack->IsMemoized("@@field@@")){
        field_name = sdsdup((sds)stack->Recall("@@field@@"));
    }
    else
    {
        field_name = sdsnew("*");
    }
    stack->Memoize("@@field@@", field_name);

    auto *text_kontekst = (SilNikParowy_Kontekst *)stack->Recall("@@SilNikParowy_Kontekst@@");
    auto *text_parser = (Sjiboleth *)stack->Recall("@@TEXT_PARSER@@");

    while (stack->HasEntries())
    {
        FaBlok *p = stack->Pop();
        if (p->IsParameterList())
        {
            while (p->parameter_list->HasEntries())
            {
                FaBlok *v = p->parameter_list->Pop();
                Index_Text(v, text_kontekst, text_parser);
            }
        }
        else
        {
            Index_Text(p, text_kontekst, text_parser);
        }
    }
    sdsfree(field_name);
    if(standing_field_name != sdsempty())
        stack->Memoize("@@field@@", standing_field_name);
}
END_SJIBOLETH_HANDLER(IndexJson)

/*
    executeGremlinParameters

    Collects 2 or more parameters.

    a: (%p0% , %p1%)
                v(familiy, raak)
    b: (%pl% , %pn%)
                v(birthyear, 1960, 1970)
*/
SJIBOLETH_HANDLER(executeJsonParameters)
{
    STACK_CHECK(2);

    FaBlok *second = stack->Pop();
    FaBlok *first = stack->Pop();
    if (first->IsParameterList())
    {
        first->parameter_list->Enqueue(second);
        PushResult(first, stack);
        return C_OK;
    }
    sds setname = sdscatprintf(sdsempty(), "P_%s_%lld", t->TokenAsSds(), ustime());
    FaBlok *pl = FaBlok::Get(setname, KeysetDescriptor_TYPE_PARAMETER_LIST);
    pl->AsTemp();
    pl->parameter_list = new GraphStack<FaBlok>();
    pl->parameter_list->Enqueue(first);
    pl->parameter_list->Enqueue(second);
    sdsfree(setname);
    PushResult(pl, stack);
}
END_SJIBOLETH_HANDLER(executeJsonParameters)

/*
 * GremlinScopeCheck
 *
 * Change token type and priority for 'subject' and 'object'
 * When in the context of a 'by' operation.
 */
SJIBOLETH_PARSER_CONTEXT_CHECKER(JsonCommaScopeCheck)
{
    rxUNUSED(head);
    rxUNUSED(expression);
    if (!HasParkedToken(expression, "{"))
    {
        sds referal = sdsnew("{{{");
        referal = sdscat(referal, ((ParserToken *)t)->TokenAsSds());
        t = lookupToken(pO, referal);
        sdsfree(referal);
    }
    else if (!HasParkedToken(expression, "["))
    {
        sds referal = sdsnew("!!!");
        referal = sdscat(referal, ((ParserToken *)t)->TokenAsSds());
        t = lookupToken(pO, referal);
        sdsfree(referal);
    }
}
END_SJIBOLETH_PARSER_CONTEXT_CHECKER(JsonCommaScopeCheck)

bool JsonDialect::registerDefaultSyntax()
{
    this->DeregisterSyntax("-");
    this->DeregisterSyntax("+");
    this->DeregisterSyntax("=");
    this->DeregisterSyntax("==");
    this->DeregisterSyntax(">=");
    this->DeregisterSyntax("<=");
    this->DeregisterSyntax("<");
    this->DeregisterSyntax(">");
    this->DeregisterSyntax("!=");
    this->DeregisterSyntax("|");
    this->DeregisterSyntax("&");
    this->DeregisterSyntax("|&");
    this->DeregisterSyntax("not");
    this->DeregisterSyntax("NOT");
    this->DeregisterSyntax("!");
	// Sjiboleth::registerDefaultSyntax();
    this->RegisterSyntax(",", 15, 0, 0, executeJsonParameters, JsonCommaScopeCheck);
    this->RegisterSyntax("!!!,", priBreak, 0, 0, IndexJson);
    this->RegisterSyntax("{{{,", 5, 0, 0, IndexJson);
    this->RegisterSyntax(":", 10, 0, 0, IndexJson);
    return true;
}

JsonDialect::JsonDialect()
    : Sjiboleth()
{
    this->default_operator = sdsempty();
    this->registerDefaultSyntax();
}
