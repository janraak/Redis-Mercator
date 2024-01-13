#include <cstring>

#include "client-pool.hpp"
#include "graphstackentry_abbreviated.hpp"
#include "sjiboleth-fablok.hpp"
#include "sjiboleth-graph.hpp"
#include "sjiboleth.h"
#include "sjiboleth.hpp"

#define STASH_STRING 1
#define STASH_ROBJ 2

// extern void *rxStashCommand(SimpleQueue *ctx, const char *command, int argc, ...);
// extern void *rxStashCommand2(SimpleQueue *ctx, const char *command, int argt, int argc, void **args);

typedef struct
{
	double sum_w;
	int tally;
} Indexable;

char *KEYTYPE_TAGS[] = {"S", "L", "C", "Z", "H", "M", "X"};

bool TextDialect::FlushIndexables(rax *collector, rxString key, int key_type, CSimpleQueue *, bool use_bracket)
{
	redisNodeInfo *index_config = rxIndexNode();
	auto *client = RedisClientPool<redisContext>::Acquire(index_config->host_reference, "_CLIENT", "TextDialect::FlushIndexables");

	// SimpleQueue *persist_q = (SimpleQueue *)oPersist_q;
	raxIterator indexablesIterator;
	raxStart(&indexablesIterator, collector);
	raxSeek(&indexablesIterator, "^", NULL, 0);
	// void *args[] = {(void *)key};
	// rxStashCommand2(persist_q, "MULTI", STASH_STRING, 0, NULL);
	auto *rcc = (redisReply *)redisCommand(client, "MULTI", NULL);
	if (rcc != NULL)
		freeReplyObject(rcc);

	if (use_bracket)
	{
		// rxStashCommand2(persist_q, "RXBEGIN", STASH_STRING, 1, args);
		rcc = (redisReply *)redisCommand(client, "RXBEGIN %s", key);
		if (rcc != NULL)
			freeReplyObject(rcc);
	}
	while (raxNext(&indexablesIterator))
	{
		rxString avp = rxStringNewLen((const char *)indexablesIterator.key, indexablesIterator.key_len);
		int segments = 0;
		rxString *parts = rxStringSplitLen(avp, indexablesIterator.key_len, "/", 1, &segments);
		auto *indexable = (Indexable *)indexablesIterator.data;
		rxString score = rxStringFormat("%f", rxGetIndexScoringMethod() == UnweightedIndexScoring
											? indexable->sum_w 
											: indexable->sum_w / (indexable->tally * indexable->tally)
										);
		// void *add_args[] = {(void *)key, (void *)KEYTYPE_TAGS[key_type], (void *)parts[0], (void *)parts[1], (void *)score, (void *)"0"};
		// rxStashCommand2(persist_q, "RXADD", STASH_STRING, 6, add_args);
		rcc = (redisReply *)redisCommand(client, "RXADD %s %s %s %s %s %s", key, KEYTYPE_TAGS[key_type], parts[0], parts[1], score, "0");
		if (rcc != NULL)
			freeReplyObject(rcc);
		rxStringFreeSplitRes(parts, segments);
		rxStringFree(avp);
		rxStringFree(score);
	}
	raxStop(&indexablesIterator);

	if (use_bracket)
	{
		// rxStashCommand2(persist_q, "RXCOMMIT", STASH_STRING, 1, args);
		rcc = (redisReply *)redisCommand(client, "RXCOMMIT %s", key);
		if (rcc != NULL)
			freeReplyObject(rcc);
	}
	// rxStashCommand2(persist_q, "EXEC", STASH_STRING, 0, NULL);
	rcc = (redisReply *)redisCommand(client, "EXEC", NULL);
	if (rcc != NULL)
		freeReplyObject(rcc);
	RedisClientPool<redisContext>::Release(client, "TextDialect::FlushIndexables");

	return true;
}

bool static CollectIndexables(rax *collector, rxString field_name, const char *field_value, double w)
{
	if (collector == NULL)
		return false;
	rxString key = rxStringDup(field_name);
	key = rxStringFormat("%s%s", key, "/");
	key = rxStringFormat("%s%s", key, field_value);
	rxStringToUpper(key);
	auto *indexable = (Indexable *)raxFind(collector, (UCHAR *)key, strlen(key));
	if (indexable == raxNotFound)
	{
		indexable = new Indexable();
		indexable->sum_w = 0.0;
		indexable->tally = 0;
	}
	indexable->sum_w += w;
	indexable->tally++;
	Indexable *prev_indexable = NULL;
	raxInsert(collector, (UCHAR *)key, strlen(key), indexable, (void **)&prev_indexable);
	rxStringFree(key);
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
		rxServerLog(rxLL_NOTICE, "bad pointer: %p\n", p);
		return true;
	}
	else if (fh != -1)
	{
		close(fh);
	}

	//    rxServerLog(rxLL_NOTICE,  "good pointer: %p\n", p );
	return false;
}

// SJIBOLETH_HANDLER(IndexText)
int IndexText(CParserToken *tO, CSilNikParowy_Kontekst *stackO)
{
	auto *t = (ParserToken *)tO;
	auto *stack = (SilNikParowy_Kontekst *)stackO;

	if (stack->HasEntries())
	{
		auto *collector = (rax *)stack->Recall("@@collector@@");
		rxString field_name;
		if (strncmp(":", (const char *)t->TokenAsSds(), 1) == 0
		|| strncmp("=", (const char *)t->TokenAsSds(), 1) == 0)
		{
			FaBlok *p = stack->Pop_Last();
			field_name = rxStringNew(p->setname);
		}
		else if (stack->IsMemoized("@@field@@"))
		{
			field_name = rxStringDup((rxString)stack->Recall("@@field@@"));
		}
		else
		{
			field_name = rxStringNew("*");
		}
		int l = stack->Size();
		double w = 1.0 / l;
		while (stack->HasEntries())
		{
			FaBlok *p = stack->Pop();
			if (isPointerBad(p->parameter_list))
			{
				rxServerLog(rxLL_NOTICE, "%s\n", stack->expression->ToString());
			}
			else if (p->IsParameterList())
			{
				w = 1.0 / p->parameter_list->Size();
				while (p->parameter_list->HasEntries())
				{
					FaBlok *v = p->parameter_list->Pop();
					CollectIndexables(collector, field_name, v->setname, w);
					rxRaxFree(v->keyset);
					FaBlok::Delete(v);
				}
			}
			else
			{
				if (p->setname == NULL)
				{
					rxServerLog(rxLL_NOTICE, "%s\n", stack->expression->ToString());
				}
				else
					CollectIndexables(collector, field_name, p->setname, w);
			}
			FaBlok::Delete(p);
		}
		rxStringFree(field_name);
	}
	return C_OK;
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
	rxString setname = rxStringFormat("P_%s_%lld", t->TokenAsSds(), ustime());
	FaBlok *pl = FaBlok::Get(setname, KeysetDescriptor_TYPE_PARAMETER_LIST);
	pl->AsTemp();
	pl->parameter_list = new GraphStack<FaBlok>();
	pl->parameter_list->Enqueue(first);
	pl->parameter_list->Enqueue(second);
	rxStringFree(setname);
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
		rxString referal = rxStringNew("!!!");
		referal = rxStringFormat("%s%s", referal, ((ParserToken *)t)->TokenAsSds());
		t = lookupToken(pO, referal);
		if (t != NULL)
			t = (CParserToken *)((ParserToken *)t)->Copy(722764002);
		rxStringFree(referal);
	}
}
END_SJIBOLETH_PARSER_CONTEXT_CHECKER(TextCommaScopeCheck)

SJIBOLETH_PARSER_CONTEXT_CHECKER(TextColonScopeCheck)
{
	rxUNUSED(head);
	rxUNUSED(expression);
	// auto *e = (ParsedExpression *)expression;
	// rxServerLog(rxLL_NOTICE, ":check: %s\n", e->ToString());
	char *s = head + 1;
	char *es = s + 8;
	while (s <= es)
	{
		auto ucs = (unsigned char) *s;
		auto ucs1 = (unsigned char) *(s + 1);
		auto ucs2 = (unsigned char) *(s + 2);
		switch (ucs)
		{
		case 0xe2: // Unicode
			if ((ucs1 == 0x80 && ucs2 == 0x9c) || (ucs1 == 0x80 && ucs2 == 0x98) || (ucs1 == 0x20 && ucs2 == 0x39))
			{
				rxString referal = rxStringNew("!!!");
				referal = rxStringFormat("%s%s", referal, ((ParserToken *)t)->TokenAsSds());
				t = lookupToken(pO, referal);
				if (t != NULL)
					t = (CParserToken *)((ParserToken *)t)->Copy(722764003);
				rxStringFree(referal);
			}
			break;
		case '"':
		// case '\'':
		case 0x60:
		{
			rxString referal = rxStringNew("!!!");
			referal = rxStringFormat("%s%s", referal, ((ParserToken *)t)->TokenAsSds());
			t = lookupToken(pO, referal);
			if (t != NULL)
				t = (CParserToken *)((ParserToken *)t)->Copy(722764004);
			rxStringFree(referal);
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
	rxString referal = rxStringNew("!!!");
	referal = rxStringFormat("%s%s", referal, ((ParserToken *)t)->TokenAsSds());
	t = lookupToken(pO, referal);
	if (t != NULL)
		t = (CParserToken *)((ParserToken *)t)->Copy(722764001);
	rxStringFree(referal);
}
END_SJIBOLETH_PARSER_CONTEXT_CHECKER(TextBulletScopeCheck)

SJIBOLETH_PARSER_CONTEXT_CHECKER(TextDashScopeCheck)
{
	rxUNUSED(head);
	rxUNUSED(expression);
	rxUNUSED(pO);
	if (isdigit(*(head - 1)))
	{
		rxString referal = rxStringNew("!!!");
		referal = rxStringFormat("%s%s", referal, ((ParserToken *)t)->TokenAsSds());
		t = lookupToken(pO, referal);
		if (t != NULL)
			t = (CParserToken *)((ParserToken *)t)->Copy(722774040);
		rxStringFree(referal);
	}
}
END_SJIBOLETH_PARSER_CONTEXT_CHECKER(TextDashScopeCheck)

bool TextDialect::RegisterDefaultSyntax()
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
	// Sjiboleth::RegisterDefaultSyntax();
	this->RegisterSyntax("=", 10, 0, 0,  Q_READONLY, IndexText);
	// this->RegisterSyntax("-", 10, 0, 0, Q_READONLY, NULL, TextDashScopeCheck);
	// this->RegisterSyntax("!!!-", priIgnore, 0, 0, Q_READONLY, Q_READONLY, NULL);
	this->RegisterSyntax(".", priBreak, 0, 0, Q_READONLY, IndexText);
	this->RegisterSyntax("@", priIgnore, 0, 0, Q_READONLY, IndexText);
	this->RegisterSyntax(",", 15, 0, 0, Q_READONLY, executeTextParameters, TextCommaScopeCheck);
	this->RegisterSyntax("!!!,", priBreak, 0, 0, Q_READONLY, IndexText);
	this->RegisterSyntax("*", 10, 0, 0, Q_READONLY, NULL, TextBulletScopeCheck);
	this->RegisterSyntax("!!!*", priBreak, 0, 0, Q_READONLY, NULL);
	this->RegisterSyntax(";", 10, 0, 0, Q_READONLY, NULL, TextBulletScopeCheck);
	this->RegisterSyntax("!!!;", priBreak, 0, 0, Q_READONLY, IndexText);
	this->RegisterSyntax(":", 10, 0, 0, Q_READONLY, IndexText, TextColonScopeCheck);
	this->RegisterSyntax("!!!:", priIgnore, 0, 0, Q_READONLY, NULL);
	this->RegisterSyntax("\t", priBreak, 0, 0, Q_READONLY, NULL);
	this->RegisterSyntax("\n", priBreak, 0, 0, Q_READONLY, NULL);
	this->RegisterSyntax("`", priIgnore, 0, 0, Q_READONLY, NULL);
	this->RegisterSyntax("'", priIgnore, 0, 0, Q_READONLY, NULL);
	this->RegisterSyntax("~~~=~~~", 5, 0, 0, Q_READONLY, IndexText);
	return true;
}

TextDialect::TextDialect()
	: Sjiboleth()
{
	this->default_operator = rxStringEmpty();
	this->RegisterDefaultSyntax();
}
