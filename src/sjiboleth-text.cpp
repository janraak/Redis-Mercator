#include "sjiboleth.hpp"


bool TextDialect::registerDefaultSyntax()
{
	this->RegisterSyntax("=", 5, 0, 0, NULL);
	this->RegisterSyntax("-", 5, 0, 0, NULL);
	this->RegisterSyntax(".", 5, 0, 0, NULL);
	this->RegisterSyntax("@", 5, 0, 0, NULL);
	this->RegisterSyntax(",", 5, 0, 0, NULL);
	this->RegisterSyntax(";", 5, 0, 0, NULL);
	this->RegisterSyntax(":", 5, 0, 0, NULL);
	this->RegisterSyntax("\t", 5, 0, 0, NULL);
	this->RegisterSyntax("\n", 5, 0, 0, NULL);
	this->RegisterSyntax("`", 5, 0, 0, NULL);
	this->RegisterSyntax("'", 5, 0, 0, NULL);

    return Sjiboleth::registerDefaultSyntax();
}



TextDialect::TextDialect()
:Sjiboleth()
{
    this->default_operator = sdsempty();
    this->registerDefaultSyntax();
}
