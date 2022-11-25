#include <stdlib.h>
#include <string.h>

#include "../../src/sds.h"
#include "ctype.h"
#include "stddef.h"
#include "stdint.h"
#include "stdlib.h"
#include "util.h"
#include <stdarg.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <zmalloc.h>

#include "sdsWrapper.h"

rxString rxStringNew(const char *s)
{
    return sdsnew(s);
}
rxString rxStringNewLen(const char *s, int l)
{
    return sdsnewlen(s, l);
}

rxString rxStringEmpty()
{
    return sdsempty();
}

rxString rxStringDup(rxString s)
{
    return sdsdup((sds)s);
}

void rxStringFree(rxString s)
{
    sdsfree((sds)s);
}

rxString *rxStringSplitLen(const char *s, ssize_t len, const char *sep, int seplen, int *count)
{
    return (const char **)sdssplitlen(s, len, sep, seplen, count);
}

void rxStringFreeSplitRes(rxString *tokens, int count)
{
    sdsfreesplitres((sds *)tokens, count);
}

rxString rxStringMapChars(rxString s, const char *from, const char *to, size_t setlen)
{
    return sdsmapchars((sds)s, from, to, setlen);
}

rxString rxStringFormat(const char *fmt, ...)
{
    sds s = sdsempty();
    va_list ap;
    char *t;
    va_start(ap, fmt);
    t = sdscatvprintf(s, fmt, ap);
    va_end(ap);
    return t;
}

rxString rxStringTrim(rxString s, const char *cset)
{
    return sdstrim((sds)s, cset);
}

rxString rxSdsAttachlen(void *address, const void *init, size_t initlen, size_t *total_size)
{
    struct sdshdr32 *s_preamble = (struct sdshdr32 *)address;
    char *s;
    char type = SDS_TYPE_32;
    int hdrlen = sizeof(struct sdshdr32);

    s = (char *)((void *)s_preamble + hdrlen);
    memcpy(s, init, initlen);
    s[initlen] = 0x00;

    s_preamble->len = initlen;
    s_preamble->alloc = initlen;
    s_preamble->flags = type;
    *total_size = hdrlen + initlen + 1;
    return (rxString)s;
}

void *rxStringGetSdsHdr(void *address, int sz)
{
    return (struct sdshdr32 *)(address + sz);
}

int rxStringGetSdsHdrSize()
{
    return sizeof(struct sdshdr32);
}

int rxStringCharCount(const char *s, char c)
{
    int tally = 0;
    char *t = (char *)s;
    while (*t)
    {
        if (*t == toupper(c))
            ++tally;
        ++t;
    }
    return tally;
}

/* Apply tolower() to every character of the string 's'. */
void rxStringToLower(const char *cs)
{
    char *s = (char *)cs;
    int len = strlen(s), j;
    for (j = 0; j < len; j++)
        s[j] = tolower(s[j]);
}

/* Apply toupper() to every character of the string 's'. */
void rxStringToUpper(const char *cs)
{
    char *s = (char *)cs;
    int len = strlen(s), j;
    for (j = 0; j < len; j++)
        s[j] = toupper(s[j]);
}

int rxStringMatchLen(const char *p, int plen, const char *s, int slen, int nocase)
{
    return stringmatchlen(p, plen, s, slen, nocase);
}

int rxStringMatch(const char *p, const char *s, int nocase)
{
    return stringmatch(p, s, nocase);
}

extern void serverLogRaw(int level, const char *msg);
void rxServerLogRaw(int level, const char *msg)
{
    serverLogRaw(level, msg);
}

extern void _serverAssert(const char *estr, const char *file, int line);
void rxServerAssert(const char *estr, const char *file, int line)
{
    _serverAssert(estr, file, line);
}

void serverLog(int level, const char *fmt, ...)
{
    va_list ap;
    char msg[2048];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    serverLogRaw(level, msg);
}
void rxServerLog(int level, const char *fmt, ...)
{
    va_list ap;
    char msg[2048];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    serverLogRaw(level, msg);
}

void *rxMemAlloc(size_t size)
{
    return zmalloc(size);
}

void rxMemFree(void *ptr)
{
    zfree(ptr);
}
