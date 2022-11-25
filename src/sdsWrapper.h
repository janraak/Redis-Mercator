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

typedef const char *rxString;

rxString rxStringNew(const char *s);
rxString rxStringNewLen(const char *s, int l);
rxString rxStringEmpty();
rxString rxStringDup(rxString s);

void rxStringFree(rxString s);

rxString rxStringTrim(rxString s, const char *cset);

rxString rxStringFormat(const char *fmt, ...);

rxString *rxStringSplitLen(const char *s, ssize_t len, const char *sep, int seplen, int *count);
void rxStringFreeSplitRes(rxString *tokens, int count);
rxString rxStringMapChars(rxString s, const char *from, const char *to, size_t setlen);

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

void *rxMemAlloc(size_t size);
void rxMemFree(void *ptr);

#endif