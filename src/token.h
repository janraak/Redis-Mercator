#pragma once

#ifndef __RXQ_TOKEN_H__
#define __RXQ_TOKEN_H__

#undef _DEFAULT_SOURCE
#ifdef __cplusplus
extern "C" {
#endif
#include "rxSuite.h"

#include "/usr/include/arm-linux-gnueabihf/bits/types/siginfo_t.h"
#include <sched.h>
#include <signal.h>

#include "dict.h"

#ifndef __cplusplus
	#include "server.h"
#else
	// #include "../sds.h"
#endif

#ifdef __cplusplus
}
#endif

#ifndef eTokenType_TYPE

enum TokenType
{
	_operand = 1,
	_literal = 2,
	_operator = 3,
	_open_bracket = 4,
	_close_bracket = 5,
	_key_set = 6,
	_immediate_operator = 7
};
#define eTokenType_TYPE
#endif

typedef struct
{
	int token_priority;
	int token_type;
	const char* token;
	dict *keyset;
} Token;

typedef Token * (*newTokenProc)(int token_type, char *token, size_t len);
typedef Token * (*triageTokenProc)(int token_type, char *token, size_t len);
typedef int (*isoperatorProc)(char aChar, int crlftab_as_operator);

typedef struct
{
	newTokenProc new_token_provider;
	triageTokenProc triage_token;
	isoperatorProc is_operator;
	int object_and_array_controls;
} ParserDialect;

extern ParserDialect json_parser_procs;
extern ParserDialect json_graph_parser_procs;
extern ParserDialect query_parser_procs;
extern ParserDialect sentence_parser_procs;
extern ParserDialect sparql_parser_procs;
extern ParserDialect gremlin_parser_procs;

Token *newJsonToken(int token_type, char *token, size_t len);
Token *newQueryToken(int token_type, char *token, size_t len );
Token *newSentenceToken(int token_type, char *token, size_t len );
Token *newSparQlToken(int token_type, char *token, size_t len);
Token *newGremlinToken(int token_type, char *token, size_t len);
#endif