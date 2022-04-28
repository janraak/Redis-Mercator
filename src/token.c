
#include "token.h"
#include <sys/time.h>

int pri5 = 5;
int pri10 = 10;
int pri20 = 20;
int pri30 = 30;
int pri50 = 50;
int pri60 = 60;
int pri6 = 6;
int pri70 = 70;
int pri100 = 100;
int pri200 = 200;
int pri500 = 500;
int priIgnore = -1;
int priImmediate = 0;

uint64_t dictSdsHash(const void *key)
{
	return dictGenHashFunction((unsigned char *)key, sdslen((char *)key));
}

int dictSdsKeyCompare(void *privdata, const void *key1,
					  const void *key2)
{
	int l1, l2;
	DICT_NOTUSED(privdata);

	l1 = sdslen((sds)key1);
	l2 = sdslen((sds)key2);
	if (l1 != l2)
		return 0;
	return memcmp(key1, key2, l1) == 0;
}

void dictSdsDestructor(void *privdata, void *val)
{
	DICT_NOTUSED(privdata);

	sdsfree(val);
}

void *dictSdsDup(void *privdata, const void *key)
{
	DICT_NOTUSED(privdata);
	return sdsdup((char *const)key);
}

/* Db->dict, keys are sds strings, vals are Redis objects. */
dictType tokenDictType = {
	dictSdsHash,	   /* hash function */
	dictSdsDup,		   /* key dup */
	NULL,			   /* val dup */
	dictSdsKeyCompare, /* key compare */
	dictSdsDestructor, /* key destructor */
	NULL			   /* val destructor */
};

/* Db->dict, keys are sds strings, vals are Redis objects. */
dictType tokenDictType2 = {
	dictSdsHash,	   /* hash function */
	dictSdsDup,		   /* key dup */
	NULL,			   /* val dup */
	dictSdsKeyCompare, /* key compare */
	dictSdsDestructor, /* key destructor */
	NULL  /* val destructor */
};

static dict *OperatorMap = NULL;
static dict *JsonOperatorMap = NULL;
static dict *SentenceOperatorMap = NULL;
static dict *SparQlOperatorMap = NULL;
static dict *GremlinOperatorMap = NULL;

void InitTokenStatic();
void InitJsonTokenStatic();
void InitSentenceTokenStatic();
void InitSparQlTokenStatic();
void InitGremlinTokenStatic();

Token *newToken(int token_type, char *token, size_t len, dict *operator_map)
{
	Token *t = zmalloc(sizeof(Token));
	t->token_type = token_type;
	t->token = sdsdup(sdsnewlen(token, len));
	t->keyset = NULL;
	dictEntry *o;
	switch (token_type)
	{
	case _operand:
		o = dictFind(operator_map, (void *)t->token);
		t->token_priority = o ? *(int *)o->v.val : 100;
		t->token_type = o ? _operator : token_type;
		break;
	case _literal:
		t->token_priority = 100;
		break;
	case _open_bracket:
		t->token_priority = 50;
		break;
	case _close_bracket:
		t->token_priority = 50;
		break;
	case _operator:
	default:
		o = dictFind(operator_map, (void *)t->token);
		if (o)
		{
			t->token_priority = *(int *)o->v.val;
			if (t->token_priority == priImmediate)
				t->token_type = _immediate_operator;
		}
		else
			t->token_priority = -1;
		break;
	}
	return t;
}

Token *newJsonToken(int token_type, char *token, size_t len)
{
	if (!JsonOperatorMap)
		InitJsonTokenStatic();
	return newToken(token_type, token, len, JsonOperatorMap);
}

Token *newQueryToken(int token_type, char *token, size_t len)
{
	if (!OperatorMap)
		InitTokenStatic();

	return newToken(token_type, token, len, OperatorMap);
}

Token *newSentenceToken(int token_type, char *token, size_t len)
{
	if (!SentenceOperatorMap)
		InitSentenceTokenStatic();

	return newToken(token_type, token, len, SentenceOperatorMap);
}

Token *newSparQlToken(int token_type, char *token, size_t len)
{
	if (!SparQlOperatorMap)
		InitSparQlTokenStatic();

	return newToken(token_type, token, len, SparQlOperatorMap);
}

Token *newGremlinToken(int token_type, char *token, size_t len)
{
	if (!GremlinOperatorMap)
		InitGremlinTokenStatic();

	return newToken(token_type, token, len, GremlinOperatorMap);
}

void InitTokenStatic()
{
	OperatorMap = dictCreate(&tokenDictType2, (void *)NULL);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("=")), &pri5);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("+=")), &pri5);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("-=")), &pri5);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("*=")), &pri5);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("/=")), &pri5);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("+")), &pri30);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("-")), &pri30);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("++")), &pri30);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("--")), &pri30);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("*")), &pri50);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("/")), &pri50);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("%")), &pri50);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("^^")), &pri60);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("<")), &pri20);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("<=")), &pri20);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew(">=")), &pri20);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew(">")), &pri20);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("==")), &pri20);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("!=")), &pri20);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("<>")), &pri20);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("!")), &pri10);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("||")), &pri10);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("&&")), &pri10);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("|")), &pri10);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("&")), &pri10);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("|&")), &pri10);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew(">>")), &pri50);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("<<")), &pri50);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew(".")), &pri100);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew(",")), &pri200);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew(");")), &pri6);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew(":")), &pri200);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("?")), &pri200);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("in")), &pri10);
	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("not")), &pri10);
}

void InitSentenceTokenStatic()
{
	SentenceOperatorMap = dictCreate(&tokenDictType2, (void *)NULL);
	dictAdd(SentenceOperatorMap, (void *)sdsdup(sdsnew("=")), &priImmediate);
	dictAdd(SentenceOperatorMap, (void *)sdsdup(sdsnew("-")), &priImmediate);
	dictAdd(SentenceOperatorMap, (void *)sdsdup(sdsnew(".")), &priImmediate);
	dictAdd(SentenceOperatorMap, (void *)sdsdup(sdsnew("@")), &priImmediate);
	dictAdd(SentenceOperatorMap, (void *)sdsdup(sdsnew(",")), &priImmediate);
	dictAdd(SentenceOperatorMap, (void *)sdsdup(sdsnew(";")), &priImmediate);
	dictAdd(SentenceOperatorMap, (void *)sdsdup(sdsnew(":")), &priImmediate);
	dictAdd(SentenceOperatorMap, (void *)sdsdup(sdsnew("\t")), &priImmediate);
	dictAdd(SentenceOperatorMap, (void *)sdsdup(sdsnew("\n")), &priImmediate);
	dictAdd(SentenceOperatorMap, (void *)sdsdup(sdsnew("`")), &priImmediate);
	dictAdd(SentenceOperatorMap, (void *)sdsdup(sdsnew("'")), &priImmediate);
}

void InitSparQlTokenStatic()
{
	SparQlOperatorMap = dictCreate(&tokenDictType2, (void *)NULL);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("=")), &pri5);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("+=")), &pri5);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("-=")), &pri5);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("*=")), &pri5);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("/=")), &pri5);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("+")), &pri30);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("-")), &pri30);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("++")), &pri30);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("--")), &pri30);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("*")), &pri50);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("/")), &pri50);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("%")), &pri50);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("^^")), &pri60);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("<")), &pri20);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("<=")), &pri20);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew(">=")), &pri20);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew(">")), &pri20);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("==")), &pri20);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("!=")), &pri20);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("<>")), &pri20);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("!")), &pri10);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("||")), &pri10);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("&&")), &pri10);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("|")), &pri10);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("&")), &pri10);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("|&")), &pri10);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew(">>")), &pri50);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("<<")), &pri50);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew(".")), &pri100);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew(",")), &pri200);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew(");")), &pri6);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew(":")), &pri200);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("?")), &pri200);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("in")), &pri10);
	dictAdd(SparQlOperatorMap, (void *)sdsdup(sdsnew("not")), &pri10);
}

void InitGremlinTokenStatic()
{
	GremlinOperatorMap = dictCreate(&tokenDictType2, (void *)NULL);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("=")), &pri5);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("+=")), &pri5);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("-=")), &pri5);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("*=")), &pri5);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("/=")), &pri5);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("+")), &pri30);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("-")), &pri30);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("++")), &pri30);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("--")), &pri30);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("*")), &pri50);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("/")), &pri50);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("%")), &pri50);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("^^")), &pri60);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("<")), &pri20);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("<=")), &pri20);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew(">=")), &pri20);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew(">")), &pri20);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("==")), &pri20);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("!=")), &pri20);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("<>")), &pri20);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("!")), &pri10);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("||")), &pri10);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("&&")), &pri10);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("|")), &pri10);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("&")), &pri10);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("|&")), &pri10);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew(">>")), &pri50);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("<<")), &pri50);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew(".")), &pri100);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew(",")), &pri200);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew(");")), &pri6);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew(":")), &pri200);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("?")), &pri200);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("g")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("v")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("V")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("E")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("e")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("has")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("label")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("hasLabel")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("missing")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("missingLabel")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("match")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("nomatch")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("out")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("in")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("inout")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("hasout")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("hasin")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("hasinout")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("as")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("select")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("where")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("groupCount")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("count")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("by")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("eq")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("neq")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("property")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("addVertex")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("addV")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("addEdge")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("addE")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("to")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("incl")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("include")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("excl")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("exclude")), &pri500);
	// dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("object")), &pri500);
	dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("from")), &pri500);
	// dictAdd(GremlinOperatorMap, (void *)sdsdup(sdsnew("subject")), &pri500);
}

// void InitTokenStatic()
// {
// 	OperatorMap = dictCreate(&tokenDictType2, (void *)NULL);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("=")), &pri5);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("+=")), &pri5);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("-=")), &pri5);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("*=")), &pri5);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("/=")), &pri5);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("+")), &pri30);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("-")), &pri30);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("++")), &pri30);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("--")), &pri30);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("*")), &pri50);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("/")), &pri50);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("%")), &pri50);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("^^")), &pri60);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("<")), &pri20);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("<=")), &pri20);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew(">=")), &pri20);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew(">")), &pri20);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("==")), &pri20);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("!=")), &pri20);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("<>")), &pri20);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("!")), &pri70);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("||")), &pri10);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("&&")), &pri10);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("|")), &pri30);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("&")), &pri30);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("|&")), &pri30);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew(">>")), &pri50);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("<<")), &pri50);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew(".")), &pri100);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew(",")), &pri200);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew(");")), &pri6);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew(":")), &pri200);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("?")), &pri200);
// 	dictAdd(OperatorMap, (void *)sdsdup(sdsnew("in")), &pri10);
// }

void InitJsonTokenStatic()
{
	JsonOperatorMap = dictCreate(&tokenDictType2, (void *)NULL);
	dictAdd(JsonOperatorMap, (void *)sdsdup(sdsnew(",")), &pri5);
	dictAdd(JsonOperatorMap, (void *)sdsdup(sdsnew(":")), &pri30);
}

/* Low level logging. To use only for very big messages, otherwise
 * serverLog() is to prefer. */
void serverLogRaw(int level, const char *msg)
{
	printf("%d %s", level, msg);
}

/* _serverAssert is needed by dict */
void _serverAssert(const char *estr, const char *file, int line)
{
	printf("=== ASSERTION FAILED ===");
	printf("==> %s:%d '%s' is not true", file, line, estr);
	*((char *)-1) = 'x';
}

void serverLog(int level, const char *fmt, ...)
{
	va_list ap;
	char msg[LOG_MAX_LEN];

	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	serverLogRaw(level, msg);
}

/* Return the UNIX time in microseconds */
long long ustime(void)
{
	struct timeval tv;
	long long ust;

	gettimeofday(&tv, NULL);
	ust = ((long long)tv.tv_sec) * 1000000;
	ust += tv.tv_usec;
	return ust;
}

/* Return the UNIX time in milliseconds */
mstime_t mstime(void)
{
	return ustime() / 1000;
}

ParserDialect json_parser_procs = {newJsonToken, NULL, NULL, 0};
ParserDialect json_graph_parser_procs = {newJsonToken, NULL, NULL, 1};
ParserDialect query_parser_procs = {newQueryToken, NULL, NULL, 0};
ParserDialect sentence_parser_procs = {newSentenceToken, NULL, NULL, 0};
ParserDialect sparql_parser_procs = {newSparQlToken, NULL, NULL, 0};
ParserDialect gremlin_parser_procs = {newGremlinToken, NULL, NULL, 0};

void claimParsers()
{
	rxSuiteShared *rxShared = getRxSuite();
	rxShared->parserClaimCount++;
}
void releaseParsers()
{
	rxSuiteShared *rxShared = getRxSuite();
	if ((rxShared->parserClaimCount--) == 0)
	{

		if (OperatorMap)
		{
			dictRelease(OperatorMap);
			OperatorMap = NULL;
		}
		if (JsonOperatorMap)
		{
			dictRelease(JsonOperatorMap);
			JsonOperatorMap = NULL;
		}
		if (SentenceOperatorMap)
		{
			dictRelease(SentenceOperatorMap);
			SentenceOperatorMap = NULL;
		}
		if (SparQlOperatorMap)
		{
			dictRelease(SparQlOperatorMap);
			SparQlOperatorMap = NULL;
		}
		if (GremlinOperatorMap)
		{
			dictRelease(GremlinOperatorMap);
			GremlinOperatorMap = NULL;
		}
	}
}
