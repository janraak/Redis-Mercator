#include "parser.h"
#include <ctype.h>
#include <stdio.h>
#include <string.h>

Token *scanLiteral(char *head, char **tail, newTokenProc new_token_provider);
Token *scanIdentifier(char *head, char **tail, newTokenProc new_token_provider, int crlftab_as_operator);
Token *scanNumber(char *head, char **tail, newTokenProc new_token_provider);
Token *scanOperator(char *head, char **tail, newTokenProc new_token_provider, int crlftab_as_operator);
Token *scanBracket(char *head, char **tail, newTokenProc new_token_provider);
void stashToken(Parser *p, list *output, list *side_track, Token *token, Token *last_token, newTokenProc new_token_provider);
void flushSideTrack(list *output, list *side_track);

#define semicolon sdsnew(";")

int isbracket(char aChar)
{
	switch (aChar)
	{
	case '(':
	case ')':
	case '{':
	case '}':
	case '[':
	case ']':
	case ';':
		return 1;
	default:
		return 0;
	}
}

int is_space(char aChar, int crlftab_as_operator)
{
	switch (aChar)
	{
	case ' ':
	case '\v':
	case '\f':
	case '\r':
		return 1;
	case '\n':
	case '\t':
		return !crlftab_as_operator;
	default:
		return 0;
	}
}

int isNumber(char *aChar)
{
	return isdigit(*aChar) || ((*aChar == '+' || *aChar == '-') && isdigit(*(aChar + 1)));
}

int isoperator(char aChar, int crlftab_as_operator)
{
	switch (aChar)
	{
	case '.':
	// case ';':
	case ':':
	case ',':
		return 1;
	case '<':
	case '=':
	case '>':
	case '!':
	case '+':
	case '-':
	case '^':
	case '/':
	case '*':
	case '\\':
	case '?':
	case '|':
	case '&':
	case '%':
		return ~crlftab_as_operator;
	case '@':
		return crlftab_as_operator;
	case '\n':
	case '\t':
		return crlftab_as_operator;
	default:
		return 0;
	}
}

Token *getTokenAt(list *list, int ix)
{
	listNode *node = listIndex(list, ix);
	if (node)
		return node->value;
	return NULL;
}

void setShowDebugInfo(Parser *p, int info)
{
	p->show_debug_info = info;
}

Token *removeTokenAt(list *list, int ix)
{
	listNode *node = listIndex(list, ix);
	if (node)
	{
		Token *t = node->value;
		listDelNode(list, node);
		return t;
	}
	return NULL;
}

extern dict *executeQuery(Parser *p);

Parser *newParser(const char *dialect)
{
	Parser *p = zmalloc(sizeof(Parser));
	setDefaultOperator(p, (char *)"||");
	p->rpn = NULL;
	p->errors = NULL;
	// p->final_result_set_iterator = NULL;
	p->show_debug_info = 0;
	p->touchesAndPaths = NULL;
	p->execute_query = (execute_query_proc)executeQuery;
	p->module_contex = NULL;
	p->index_context = NULL;
	p->matchIncludes = NULL;
	p->matchExcludes = NULL;
	if (strcmp("gremlin", dialect) == 0)
	{
		p->new_token_provider = newGremlinToken;
		p->dialect = gremlin_parser_procs;
	}
	else if (strcmp("sparql", dialect) == 0)
	{
		p->new_token_provider = newSparQlToken;
		p->dialect = sparql_parser_procs;
	}
	else if (strcmp("text", dialect) == 0)
	{
		p->new_token_provider = newSentenceToken;
		p->dialect = sentence_parser_procs;
	}
	else if (strcmp("graph", dialect) == 0)
	{
		p->new_token_provider = newJsonToken;
		p->dialect = json_graph_parser_procs;
	}
	else if (strcmp("json", dialect) == 0)
	{
		p->new_token_provider = newJsonToken;
		p->dialect = json_parser_procs;
	}
	else
	{
		p->new_token_provider = newQueryToken;
		p->dialect = query_parser_procs;
	}
	return p;
}

void resetParser(Parser *p)
{
	/*
	dict *touchesAndPaths;
	*/
    if(p->matchIncludes){
        raxFree(p->matchIncludes);
        p->matchIncludes = NULL;
    }
    if(    p->matchExcludes){
        raxFree(p->matchExcludes);
        p->matchExcludes = NULL;
    }
	if(p->index_context){
		redisFree(p->index_context);
		p->index_context = NULL;
	}
	// if (p->final_result_set_iterator)
	// {
	// 	dictReleaseIterator(p->final_result_set_iterator);
	// 	p->final_result_set_iterator = NULL;
	// }
	if (p->rpn){
		listRelease(p->rpn);
	}
	if (p->errors){
		listRelease(p->errors);
	}
	p->rpn = NULL;
	if (p->touchesAndPaths)
		dictRelease(p->touchesAndPaths);
	p->touchesAndPaths = NULL;
}

void freeParser(Parser *p)
{
	resetParser(p);
	zfree(p);
}

void parse(Parser *p, const char *query, newTokenProc new_token_provider)
{
	new_token_provider = p->new_token_provider;
	////printf("command to parse: %lx %s\n", (long) query, query);
	list *output = listCreate();
	list *side_track = listCreate();
	list *errors = listCreate();
	char *head = (char *)query;
	while (*head && is_space(*head, p->crlftab_as_operator))
		++head;
	// ////printf("%s\n\n", head);
	Token *last_token = NULL;
	while (*head)
	{
		char *tail;
		while (is_space(*head, p->crlftab_as_operator))
			head++;

		Token *token = NULL;
		/*if (isalpha(*head)) {
			token = scanIdentifier(head, &tail, new_token_provider, p->crlftab_as_operator);
			head = tail;
			if(token->token_type == _operator){
				if (side_track->len > 0) {
					Token *head_token = getTokenAt(side_track, 0);
					if (head_token->token_type == _operator && head_token->token_priority >= token->token_priority) {
						listAddNodeTail(output, head_token);
						removeTokenAt(side_track, 0);
					}
				}
				listAddNodeHead(side_track, token);
			}else
			stashToken(p, output, side_track, token, last_token, new_token_provider);

		}
		else */
		if (*head == '"' || *head == '\'')
		{
			token = scanLiteral(head, &tail, new_token_provider);
			head = tail + 1;
			stashToken(p, output, side_track, token, last_token, new_token_provider);
		}
		else if (isNumber(head))
		{
			token = scanNumber(head, &tail, new_token_provider);
			head = tail;
			stashToken(p, output, side_track, token, last_token, new_token_provider);
		}
		else if (isoperator(*head, p->crlftab_as_operator))
		{
			token = scanOperator(head, &tail, new_token_provider, p->crlftab_as_operator);
			head = tail;
			if (side_track->len > 0)
			{
				Token *head_token = getTokenAt(side_track, 0);
				if (head_token->token_type == _operator && head_token->token_priority >= token->token_priority)
				{
					listAddNodeTail(output, head_token);
					removeTokenAt(side_track, 0);
				}
			}
			if (token->token_type == _immediate_operator)
			{
				flushSideTrack(output, side_track);
				listAddNodeTail(output, token);
			}
			else
				listAddNodeHead(side_track, token);
		}
		else if (isbracket(*head))
		{
			token = scanBracket(head, &tail, new_token_provider);
			head = tail;
			if (token->token_type == _close_bracket)
			{
				if (side_track->len > 0)
				{
					while (side_track->len > 0)
					{
						Token *head_token = getTokenAt(side_track, 0);
						if (head_token->token_type == _open_bracket)
						{
							removeTokenAt(side_track, 0);
							break;
						}
						else
						{
							listAddNodeTail(output, head_token);
							removeTokenAt(side_track, 0);
						}
					}
				}
				else
					listAddNodeTail(errors, sdsnew("unbalanced brackers"));
				if (sdscmp((sds)token->token, semicolon) == 0)
					listAddNodeTail(output, token);
				if (p->dialect.object_and_array_controls == 1)
				{
					listAddNodeTail(output, token);
				}
			}
			else
			{
				if (p->dialect.object_and_array_controls == 1)
				{
					listAddNodeTail(output, token);
				}
				if (last_token &&
					(last_token->token_type == _operand || last_token->token_type == _close_bracket))
				{
					while (side_track->len > 0)
					{
						Token *head_token = getTokenAt(side_track, 0);
						removeTokenAt(side_track, 0);
						listAddNodeTail(output, head_token);
					}
					if (p->default_operator && *p->default_operator)
					{
						char *dummy;
						Token *default_token = scanOperator(p->default_operator, &dummy, new_token_provider, p->crlftab_as_operator);
						listAddNodeHead(side_track, default_token);
					}
				}
				listAddNodeHead(side_track, token);
			}
		}
		else if (is_space(*head, p->crlftab_as_operator))
		{
			while (*head && is_space(*head, p->crlftab_as_operator))
				++head;
			token = last_token;
		}
		else
		{
			token = scanIdentifier(head, &tail, new_token_provider, p->crlftab_as_operator);
			head = tail;
			switch (token->token_type)
			{
			case _operator:
				if (side_track->len > 0)
				{
					Token *head_token = getTokenAt(side_track, 0);
					if (head_token->token_type == _operator && head_token->token_priority >= token->token_priority)
					{
						listAddNodeTail(output, head_token);
						removeTokenAt(side_track, 0);
					}
				}
				listAddNodeHead(side_track, token);
				break;
			default:
				stashToken(p, output, side_track, token, last_token, new_token_provider);
				break;
			}
		};
		// if (token)
		// 	//printf("%d %d token: %d %s\n", (int)token->token_type, (int)token->token_priority, (int)strlen(token->token), token->token);
		last_token = token;
		// //printf("parsed: %s\n", query);
	}
	flushSideTrack(output, side_track);
	// //printf(head);
	p->rpn = output;
	p->errors = errors;
	if (p->show_debug_info)
	{
		printf("rpn stack: %ld entries\n", output->len);
		printf("side_track: %ld entries\n", side_track->len);
	}
	listRelease(side_track);
	if (p->show_debug_info)
	{
		printf("parsed: %s %ld tokens\n", query, output->len);
		for (long unsigned int j = 0; j < output->len; j++)
		{
			Token *t = getTokenAt(output, j);
			printf("parse: %ld %d %s\n", j, t->token_type, t->token);
		}
	}
}

void parseQuery(Parser *p, const char *query)
{
	p->crlftab_as_operator = 0;
	p->dialect = query_parser_procs;
	if (strncmp("g:", query, 2) == 0)
		parse(p, query + 2, &newGremlinToken);
	else
		parse(p, query, &newQueryToken);
}

void parseJson(Parser *p, const char *query)
{
	setDefaultOperator(p, NULL);
	p->crlftab_as_operator = 0;
	p->dialect = json_parser_procs;
	parse(p, query, &newJsonToken);
}

void parseGraph(Parser *p, const char *query)
{
	setDefaultOperator(p, NULL);
	p->crlftab_as_operator = 0;
	p->dialect = json_graph_parser_procs;
	parse(p, query, &newJsonToken);
}

void parseSentence(Parser *p, const char *query)
{
	setDefaultOperator(p, NULL);
	p->crlftab_as_operator = 1;
	p->dialect = sentence_parser_procs;
	parse(p, query, &newSentenceToken);
}
void parseSparQl(Parser *p, const char *query)
{
	setDefaultOperator(p, NULL);
	p->crlftab_as_operator = 1;
	p->dialect = sparql_parser_procs;
	parse(p, query, &newSparQlToken);
}

void parseGremlin(Parser *p, const char *query)
{
	setDefaultOperator(p, NULL);
	p->crlftab_as_operator = 1;
	p->dialect = gremlin_parser_procs;
	parse(p, query, &newGremlinToken);
}

void stashToken(Parser *p, list *output, list *side_track, Token *token, Token *last_token, newTokenProc new_token_provider)
{
	if (last_token && last_token->token_type == _operand && (p->default_operator && *p->default_operator))
	{
		listAddNodeTail(output, token);
		char *dummy;
		Token *default_token = scanOperator(p->default_operator, &dummy, new_token_provider, p->crlftab_as_operator);
		listAddNodeTail(side_track, default_token);
	}
	else
		listAddNodeTail(output, token);
}

void setDefaultOperator(Parser *p, char *op)
{
	p->default_operator = sdsnew(op);
}

int iscsym(int c)
{
	if (isalnum(c))
		return 1;
	if (c == '_' || c == '@' || c == '$')
		return 1;
	return 0;
}

Token *scanIdentifier(char *head, char **tail, newTokenProc new_token_provider, int crlftab_as_operator)
{
	*tail = head + 1;
	// int last_was_literal_fence = 0;
	while (**tail && !(/*(**tail == '"' || **tail == '\'') || */ isNumber(*tail) || isoperator(**tail, crlftab_as_operator) || isbracket(**tail) || is_space(**tail, crlftab_as_operator)))
	{

		// last_was_literal_fence = (**tail == '"' || **tail == '\'');

		++*tail;
	}
	return new_token_provider(_operand, head, *tail - head);
}

Token *scanLiteral(char *head, char **tail, newTokenProc new_token_provider)
{
	char fence = *head;
	++head;
	*tail = head;
	while (**tail && **tail != fence)
		++*tail;
	return new_token_provider(_literal, head, *tail - head);
}

Token *scanOperator(char *head, char **tail, newTokenProc new_token_provider, int crlftab_as_operator)
{
	*tail = head + 1;
	while (isoperator(**tail, crlftab_as_operator))
		++*tail;
	Token *t = new_token_provider(_operator, head, *tail - head);
	while (t->token_priority == -1 && (*tail - 1) > head)
	{
		sdsfree((sds)t->token);
		zfree(t);
		--*tail;
		t = new_token_provider(_operator, head, *tail - head);
	}
	return t;
}

Token *scanBracket(char *head, char **tail, newTokenProc new_token_provider)
{
	*tail = head + 1;
	switch (*head)
	{
	case '(':
	case '{':
	case '[':
		return new_token_provider(_open_bracket, head, 1);
	default:
		return new_token_provider(_close_bracket, head, 1);
	}
}

Token *scanNumber(char *head, char **tail, newTokenProc new_token_provider)
{
	*tail = head + 1;
	int no_of_decimal_points = 0;
	while (**tail)
	{
		if (isdigit(**tail))
			++*tail;
		else if (**tail == '.' && !no_of_decimal_points)
		{
			++*tail;
			no_of_decimal_points++;
		}
		else
			return new_token_provider(_operand, head, *tail - head);
	}
	return new_token_provider(_operand, head, *tail - head);
}

void flushSideTrack(list *output, list *side_track)
{
	while (side_track->len)
	{
		Token *head_token = getTokenAt(side_track, 0);
		removeTokenAt(side_track, 0);
		listAddNodeTail(output, head_token);
	}
}


void claimParsers();
void releaseParsers();
