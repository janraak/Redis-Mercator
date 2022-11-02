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

bool Sjiboleth::RegisterDefaultSyntax()
{
    this->RegisterSyntax("-", 30, 2, 1, &Sjiboleth::executePlusMinus);
    this->RegisterSyntax("+", 30, 2, 1, &Sjiboleth::executePlusMinus);
    this->RegisterSyntax("=", 5, 2, 1, &Sjiboleth::executeStore);
    this->RegisterSyntax("==", 20, 2, 1, &Sjiboleth::executeEquals);
    this->RegisterSyntax(">=", 20, 2, 1, &Sjiboleth::executeEquals);
    this->RegisterSyntax("<=", 20, 2, 1, &Sjiboleth::executeEquals);
    this->RegisterSyntax("<", 20, 2, 1, &Sjiboleth::executeEquals);
    this->RegisterSyntax(">", 20, 2, 1, &Sjiboleth::executeEquals);
    this->RegisterSyntax("!=", 20, 2, 1, &Sjiboleth::executeEquals);
    this->RegisterSyntax("|", 20, 2, 1, &Sjiboleth::executeOr);
    this->RegisterSyntax("&", 10, 2, 1, &Sjiboleth::executeAnd);
    this->RegisterSyntax("|&", 10, 2, 1, &Sjiboleth::executeXor);
    this->RegisterSyntax("not", 10, 2, 1, &Sjiboleth::executeNotIn);
    this->RegisterSyntax("!", 10, 2, 1, &Sjiboleth::executeNotIn);
    return false;
}
