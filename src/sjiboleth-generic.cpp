#include "sjiboleth-fablok.hpp"
#include "sjiboleth.h"
#include "sjiboleth.hpp"

SJIBOLETH_HANDLER_STUB(Sjiboleth::executePlusMinus)
END_SJIBOLETH_HANDLER(executePlusMinus)

SJIBOLETH_HANDLER_STUB(Sjiboleth::executeStore)
END_SJIBOLETH_HANDLER(Sjiboleth::executeStore)

SJIBOLETH_HANDLER(Sjiboleth::executeEquals)    
    STACK_CHECK(2);
    CFaBlok *out = stack->GetOperationPair(t->TokenAsSds(), QE_LOAD_NONE);
    FetchKeySet(stackO, out, GetLeft(out), GetRight(out), t);
    PushResult(out, stack);
END_SJIBOLETH_HANDLER(Sjiboleth::executeEquals)

SJIBOLETH_HANDLER(Sjiboleth::executeOr)
    STACK_CHECK(2);
    CFaBlok *out = stack->GetOperationPair(t->TokenAsSds(), QE_LOAD_LEFT_AND_RIGHT | QE_CREATE_SET | QE_SWAP_LARGEST_FIRST);
    // propagate_set_type(out, out->left, out->right);

    CopyKeySet(GetRight(out), out);
    MergeInto(GetLeft(out), out);
    PushResult(out, stack);
END_SJIBOLETH_HANDLER(Sjiboleth::executeOr)

SJIBOLETH_HANDLER(Sjiboleth::executeAnd)
    STACK_CHECK(2);
    CFaBlok *out = stack->GetOperationPair(t->TokenAsSds(), QE_LOAD_LEFT_AND_RIGHT | QE_CREATE_SET | QE_SWAP_LARGEST_FIRST);
    MergeFrom(out, GetLeft(out), GetRight(out));
    PushResult(out, stack);
END_SJIBOLETH_HANDLER(Sjiboleth::executeAnd)

SJIBOLETH_HANDLER(Sjiboleth::executeXor)    
    STACK_CHECK(2);
    CFaBlok *out = stack->GetOperationPair(t->TokenAsSds(), QE_LOAD_LEFT_AND_RIGHT | QE_CREATE_SET | QE_SWAP_LARGEST_FIRST);
    MergeDisjunct(out, GetLeft(out), GetRight(out));
    PushResult(out, stack);
END_SJIBOLETH_HANDLER(Sjiboleth::executeXor)

SJIBOLETH_HANDLER(Sjiboleth::executeNotIn)
    STACK_CHECK(2);
    CFaBlok *out = stack->GetOperationPair(t->TokenAsSds(), QE_LOAD_LEFT_AND_RIGHT | QE_CREATE_SET | QE_SWAP_LARGEST_FIRST);
    CopyNotIn(out, GetLeft(out), GetRight(out));
    PushResult(out, stack);
END_SJIBOLETH_HANDLER(Sjiboleth::executeNotIn)

SJIBOLETH_HANDLER(Sjiboleth::executeSelectFields)
{
    rxUNUSED(t);
    STACK_CHECK(1);
    FaBlok *pl = stack->Pop();
    stack->fieldSelector = new GraphStack<const char>();
    if (pl->IsParameterList())
    {
        while (pl->parameter_list->HasEntries())
        {
            FaBlok *f = pl->parameter_list->Dequeue();
            stack->fieldSelector->Enqueue(f->setname);
            FaBlok::Delete(f);
        }
    }
    else
        stack->fieldSelector->Enqueue(pl->setname);
    FaBlok::Delete(pl);
}
END_SJIBOLETH_HANDLER(Sjiboleth::executeSelectFields)

extern int executeGremlinComparePropertyToRangeValue(CParserToken *tO, CSilNikParowy_Kontekst *stackO);
extern int executeGremlinComparePropertyToValue(CParserToken *tO, CSilNikParowy_Kontekst *stackO);

bool Sjiboleth::RegisterDefaultSyntax()
{
    this->RegisterSyntax("-", 40, 2, 1, Q_READONLY, &Sjiboleth::executePlusMinus);
    this->RegisterSyntax("+", 40, 2, 1, Q_READONLY, &Sjiboleth::executePlusMinus);
    this->RegisterSyntax("=", 5, 2, 1, Q_READONLY, &Sjiboleth::executeStore);
    this->RegisterSyntax("==", 30, 2, 1, Q_READONLY, &Sjiboleth::executeEquals);
    this->RegisterSyntax(">=", 30, 2, 1, Q_READONLY, &Sjiboleth::executeEquals);
    this->RegisterSyntax("<=", 30, 2, 1, Q_READONLY, &Sjiboleth::executeEquals);
    this->RegisterSyntax("<", 30, 2, 1, Q_READONLY, &Sjiboleth::executeEquals);
    this->RegisterSyntax(">", 30, 2, 1, Q_READONLY, &Sjiboleth::executeEquals);
    this->RegisterSyntax("!=", 30, 2, 1, Q_READONLY, &Sjiboleth::executeEquals);
    this->RegisterSyntax("|", 20, 2, 1, Q_READONLY, &Sjiboleth::executeOr);
    this->RegisterSyntax("or", 20, 2, 1, Q_READONLY, &Sjiboleth::executeOr);
    this->RegisterSyntax("&", 10, 2, 1, Q_READONLY, &Sjiboleth::executeAnd);
    this->RegisterSyntax("and", 10, 2, 1, Q_READONLY, &Sjiboleth::executeAnd);
    this->RegisterSyntax("|&", 10, 2, 1, Q_READONLY, &Sjiboleth::executeXor);
    this->RegisterSyntax("xor", 10, 2, 1, Q_READONLY, &Sjiboleth::executeXor);
    this->RegisterSyntax("not", 10, 2, 1, Q_READONLY, &Sjiboleth::executeNotIn);
    this->RegisterSyntax("!", 10, 2, 1, Q_READONLY, &Sjiboleth::executeNotIn);
    this->RegisterSyntax("between", pri200, 2, 1, Q_READONLY, &executeGremlinComparePropertyToRangeValue);
    this->RegisterSyntax("contains", pri200, 2, 1, Q_READONLY, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax("select", pri200 * 10, 1, 0, Q_READONLY, &Sjiboleth::executeSelectFields);
    this->RegisterSyntax(",", pri500 * 10, 2, 1, Q_READONLY, &GremlinDialect::executeGremlinParameters);
    return false;
}
