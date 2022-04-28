

#ifndef __RXQ_PARSER_H__
#define __RXQ_PARSER_H__

#include "redismodule.h"
#include "rxSuite.h"
#include "token.h"
#include "adlist.h"
#include "dict.h"
#include "rax.h"
#include "../../deps/hiredis/hiredis.h"

#define UCHAR unsigned char

// typedef dict* (*execute_query_proc)(int token_type, char *token, size_t len);
typedef dict *(*execute_query_proc)(void *p);
typedef struct
{
	sds default_operator;
	list *rpn;
	list *errors;
	redisContext *index_context;
	RedisModuleCtx *module_contex;
	// dictIterator *final_result_set_iterator;
	char *index_entry;
	int final_result_value_type;
	int crlftab_as_operator;
	newTokenProc new_token_provider;
	execute_query_proc execute_query;
	ParserDialect dialect;
	int show_debug_info;
	dict *touchesAndPaths;
	rax *matchIncludes;
	rax *matchExcludes;
} Parser;

Parser *newParser(const char *dialect);
void setDefaultOperator(Parser *p, char *op);
void setShowDebugInfo(Parser *p, int info);
void parseQuery(Parser *p, const char *query);
void parseJson(Parser *p, const char *query);
void parseGraph(Parser *p, const char *query);
void parseSentence(Parser *p, const char *query);
void parseSparQl(Parser *p, const char *query);
void parseGremlin(Parser *p, const char *query);
void freeParser(Parser *p);
void resetParser(Parser *p);

void claimParsers();
void releaseParsers();

Token *getTokenAt(list *list, int ix);
Token *removeTokenAt(list *list, int ix);

#endif