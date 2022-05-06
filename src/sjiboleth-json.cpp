#include "sjiboleth.hpp"

bool JsonDialect::registerDefaultSyntax()
{
    this->RegisterSyntax(",", 5, 0, 0, NULL);
    this->RegisterSyntax(":", 30, 0, 0, NULL);
    return Sjiboleth::registerDefaultSyntax();
}


JsonDialect::JsonDialect()
:Sjiboleth()
{
    this->default_operator = sdsempty();
    this->registerDefaultSyntax();
}

