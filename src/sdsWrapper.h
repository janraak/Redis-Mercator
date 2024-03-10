/* * sdsWrapper
 *
 * sds is not always compiled by C++
 * therefor this wrapper has been added.
 */
#ifndef SDSWRAPPER
#define SDSWRAPPER
#include <stddef.h> /* atof */
#include <stdlib.h> /* atof */

#define rxLL_DEBUG 0
#define rxLL_VERBOSE 1
#define rxLL_NOTICE 2
#define rxLL_WARNING 3

#define MATCH_IGNORE_CASE 1
#define MATCH_USER_CASE 0

typedef const char *rxString;
typedef const char *rxRedisModuleString;

rxString rxStringNew(const char *s);
rxString rxStringNewLen(const char *s, int l);
rxString rxStringEmpty();
rxString rxStringDup(rxString s);

void rxStringFree(rxString s);
char *rxCopyString(char *t, size_t tl, const char*s, size_t sl);

rxString rxStringTrim(rxString s, const char *cset);

rxString rxStringFormat(const char *fmt, ...);
rxString rxStringAppend(const char *current, const char *extra, char sep);

rxString *rxStringSplitLen(const char *s, ssize_t len, const char *sep, int seplen, int *count);
rxString *rxStringFreeSplitRes(rxString *tokens, int count);
// rxString rxStringMapChars(rxString s, const char *from, const char *to, size_t setlen);
// rxString rxStringLenMapChars(rxString s, size_t sl, const char *from, const char *to, size_t setlen);

int rxStringGetSdsHdrSize();
void * rxStringGetSdsHdr(void *address, int sz);
rxString rxSdsAttachlen(void *address, const void *init, size_t initlen, size_t *total_size);
int rxStringCharCount(const char *s, char c);

/* Apply tolower() to every character of the string 's'. */
void rxStringToLower(const char *s);
/* Apply toupper() to every character of the string 's'. */
void rxStringToUpper(const char *s);

int rxStringMatchLen(const char *p, int plen, const char *s, int slen, int nocase);
int rxStringMatch(const char *p, const char *s, int nocase);


void rxServerLogRaw(int level, const char *msg);
void rxServerAssert(const char *estr, const char *file, int line);
void rxServerLog(int level, const char *fmt, ...);
void rxServerLogHexDump(int level, void *value, size_t len, const char *fmt, ...);
void *rxMemAlloc(size_t size);
void rxMemFree(void *ptr);
void rxMemFreeX(void *ptr);
size_t rxMemAllocSize(void *ptr);

const char *rxStringBuildRedisCommand(int argc, rxRedisModuleString **argv);

char **breakupPointer(char *pointer, char sep, char **colons, int max);
int  toInt(char **dots, int start, int end);

char **breakupPointer(char *pointer, char sep, char **colons, int max);
int  toInt(char **dots, int start, int end);

#endif