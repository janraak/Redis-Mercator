#include <cstring>

#include "client-pool.hpp"
#include "sjiboleth-fablok.hpp"
#include "sjiboleth-graph.hpp"
#include "sjiboleth.h"
#include "sjiboleth.hpp"

typedef struct
{
	double sum_w;
	int tally;
} Indexable;

char *toLowerCase(char *pString)
{
	char *start = (char *)pString;
	while (*pString)
	{
		*pString = tolower(*pString);
		++pString;
	}
	return start;
}

bool TextDialect::FlushIndexables(rax *collector, sds key, char *key_type, redisContext *index)
{
	raxIterator indexablesIterator;
	raxStart(&indexablesIterator, collector);
	raxSeek(&indexablesIterator, "^", NULL, 0);
	redisReply *rcc = (redisReply *)redisCommand(index, "MULTI", "");
	freeReplyObject(rcc);
	rcc = (redisReply *)redisCommand(index, "RXBEGIN %s", key);
	freeReplyObject(rcc);

	while (raxNext(&indexablesIterator))
	{
		sds avp = sdsnewlen(indexablesIterator.key, indexablesIterator.key_len);
		int segments = 0;
		sds *parts = sdssplitlen(avp, indexablesIterator.key_len, "/", 1, &segments);
		auto *indexable = (Indexable *)indexablesIterator.data;
		rcc = (redisReply *)redisCommand(index, "RXADD %s %s %s %s %f 0", key, key_type, parts[0], parts[1], indexable->sum_w / (indexable->tally * indexable->tally));
		if (rcc)
		{
			if (index->err)
			{
				printf("Error: %s on %s\n", index->errstr, avp);
			}
			freeReplyObject(rcc);
		}
		sdsfreesplitres(parts, segments);
		sdsfree(avp);
	}
	raxStop(&indexablesIterator);

	rcc = (redisReply *)redisCommand(index, "RXCOMMIT %s", key);
	freeReplyObject(rcc);
	rcc = (redisReply *)redisCommand(index, "EXEC");
	freeReplyObject(rcc);
	return true;
}

bool static CollectIndexables(rax *collector, sds field_name, sds field_value, double w)
{
	sds key = sdsdup(field_name);
	key = sdscat(key, "/");
	key = sdscat(key, field_value);
	toLowerCase(key);
	auto *indexable = (Indexable *)raxFind(collector, (UCHAR *)key, sdslen(key));
	if (indexable == raxNotFound)
	{
		indexable = new Indexable();
		indexable->sum_w = 0.0;
		indexable->tally = 0;
	}
	indexable->sum_w += w;
	indexable->tally++;
	Indexable *prev_indexable = NULL;
	raxInsert(collector, (UCHAR *)key, sdslen(key), indexable, (void **)&prev_indexable);
	sdsfree(key);
	return true;
}

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

bool isPointerBad(void *p)
{
	if (p == NULL)
		return false;
	// if (p == 0x1){
	// 	return true;
	// }
	int fh = open((const char *)p, 0, 0);
	int e = errno;

	if (-1 == fh && e == EFAULT)
	{
		printf("bad pointer: %p\n", p);
		return true;
	}
	else if (fh != -1)
	{
		close(fh);
	}

	//    printf( "good pointer: %p\n", p );
	return false;
}

// SJIBOLETH_HANDLER(IndexText)
    int IndexText(CParserToken *tO, CSilNikParowy_Kontekst *stackO) 
    {                                                        
        // UNWRAP_SJIBOLETH_HANDLER_PARAMETERS();
		    auto *t = (ParserToken *)tO;                    \
    auto *stack = (SilNikParowy_Kontekst *)stackO;  

	if (stack->HasEntries())
	{
		auto *collector = (rax *)stack->Recall("@@collector@@");
		sds field_name;
		if (strncmp(":", (const char *)t->TokenAsSds(), 1) == 0)
		{
			FaBlok *p = stack->Pop_Last();
			field_name = sdsdup(p->setname);
		}
		else if (stack->IsMemoized("@@field@@"))
		{
			field_name = sdsdup((sds)stack->Recall("@@field@@"));
		}
		else
		{
			field_name = sdsnew("*");
		}
		int l = stack->Size();
		double w = 1.0 / l;
		while (stack->HasEntries())
		{
			FaBlok *p = stack->Pop();
			if (isPointerBad(p->parameter_list))
			{
				printf("%s\n", stack->expression->ToString());
			}
			// else if (p->parameter_list == 0x1)
			// {
			// 	p->parameter_list == = NULL;
			// }
			else if (p->IsParameterList())
			{
				w = 1.0 / p->parameter_list->Size();
				while (p->parameter_list->HasEntries())
				{
					FaBlok *v = p->parameter_list->Pop();
					CollectIndexables(collector, field_name, v->setname, w);
				}
			}
			else
			{
				if (p->setname == NULL || isPointerBad(p->setname))
				{
					printf("%s\n", stack->expression->ToString());
				}
				else
					CollectIndexables(collector, field_name, p->setname, w);
			}
		}
		sdsfree(field_name);
	}
        return C_OK;                                \
}
// END_SJIBOLETH_HANDLER(IndexText)

/*
	executeGremlinParameters

	Collects 2 or more parameters.

	a: (%p0% , %p1%)
				v(familiy, raak)
	b: (%pl% , %pn%)
				v(birthyear, 1960, 1970)
*/
SJIBOLETH_HANDLER(executeTextParameters)
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
END_SJIBOLETH_HANDLER(executeTextParameters)

/*
 * GremlinScopeCheck
 *
 * Change token type and priority for 'subject' and 'object'
 * When in the context of a 'by' operation.
 */
SJIBOLETH_PARSER_CONTEXT_CHECKER(TextCommaScopeCheck)
{
	rxUNUSED(head);
	rxUNUSED(expression);
	if (!HasParkedToken(expression, ":"))
	{
		sds referal = sdsnew("!!!");
		referal = sdscat(referal, ((ParserToken *)t)->TokenAsSds());
		t = lookupToken(pO, referal);
		sdsfree(referal);
	}
}
END_SJIBOLETH_PARSER_CONTEXT_CHECKER(TextCommaScopeCheck)
SJIBOLETH_PARSER_CONTEXT_CHECKER(TextColonScopeCheck)
{
	rxUNUSED(head);
	rxUNUSED(expression);
	// auto *e = (ParsedExpression *)expression;
	// printf(":check: %s\n", e->ToString());
	char *s = head + 1;
	char *es = s + 8;
	while (s <= es)
	{
		switch (*s)
		{
		case 0xe2: // Unicode
			if ((*(s + 1) == 0x80 && *(s + 2) == 0x9c) || (*(s + 1) == 0x80 && *(s + 2) == 0x98) || (*(s + 1) == 0x20 && *(s + 2) == 0x39))
			{
				sds referal = sdsnew("!!!");
				referal = sdscat(referal, ((ParserToken *)t)->TokenAsSds());
				t = lookupToken(pO, referal);
				sdsfree(referal);
			}
			break;
		case '"':
		// case '\'':
		case 0x60:
		{
			sds referal = sdsnew("!!!");
			referal = sdscat(referal, ((ParserToken *)t)->TokenAsSds());
			t = lookupToken(pO, referal);
			sdsfree(referal);
			break;
		}
		break;
		}
		++s;
	}
}
END_SJIBOLETH_PARSER_CONTEXT_CHECKER(TextColonScopeCheck)

SJIBOLETH_PARSER_CONTEXT_CHECKER(TextBulletScopeCheck)
{
	rxUNUSED(head);
	rxUNUSED(expression);
	rxUNUSED(pO);
	sds referal = sdsnew("!!!");
	referal = sdscat(referal, ((ParserToken *)t)->TokenAsSds());
	t = lookupToken(pO, referal);
	sdsfree(referal);
}
END_SJIBOLETH_PARSER_CONTEXT_CHECKER(TextBulletScopeCheck)

SJIBOLETH_PARSER_CONTEXT_CHECKER(TextDashScopeCheck)
{
	rxUNUSED(head);
	rxUNUSED(expression);
	rxUNUSED(pO);
	if (isdigit(*(head - 1)))
	{
		sds referal = sdsnew("!!!");
		referal = sdscat(referal, ((ParserToken *)t)->TokenAsSds());
		t = lookupToken(pO, referal);
		sdsfree(referal);
	}
}
END_SJIBOLETH_PARSER_CONTEXT_CHECKER(TextDashScopeCheck)

bool TextDialect::registerDefaultSyntax()
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
	this->RegisterSyntax("=", 10, 0, 0, NULL);
	// this->RegisterSyntax("-", 10, 0, 0, NULL, TextDashScopeCheck);
	// this->RegisterSyntax("!!!-", priIgnore, 0, 0, NULL);
	this->RegisterSyntax(".", priBreak, 0, 0, IndexText);
	this->RegisterSyntax("@", -1, 0, 0, IndexText);
	this->RegisterSyntax(",", 15, 0, 0, executeTextParameters, TextCommaScopeCheck);
	this->RegisterSyntax("!!!,", priBreak, 0, 0, IndexText);
	this->RegisterSyntax("*", 10, 0, 0, NULL, TextBulletScopeCheck);
	this->RegisterSyntax("!!!*", priBreak, 0, 0, NULL);
	this->RegisterSyntax(";", 10, 0, 0, NULL, TextBulletScopeCheck);
	this->RegisterSyntax("!!!;", priBreak, 0, 0, IndexText);
	this->RegisterSyntax(":", 10, 0, 0, IndexText, TextColonScopeCheck);
	this->RegisterSyntax("!!!:", priIgnore, 0, 0, NULL);
	this->RegisterSyntax("\t", priBreak, 0, 0, NULL);
	this->RegisterSyntax("\n", priBreak, 0, 0, NULL);
	this->RegisterSyntax("`", priIgnore, 0, 0, NULL);
	this->RegisterSyntax("'", priIgnore, 0, 0, NULL);
	this->RegisterSyntax("~~~=~~~", 5, 0, 0, IndexText);
	return true;
}

TextDialect::TextDialect()
	: Sjiboleth()
{
	this->default_operator = sdsempty();
	this->registerDefaultSyntax();
}
