
#include <cstdarg>
#include <iostream>
 
#include <string>

using namespace std;

#ifdef __cplusplus
extern "C"
{
#endif
#include "sdsWrapper.h"
#include <stdlib.h>
#include <string.h>

#include "sha1.h"

#ifdef __cplusplus
}
#endif

using namespace std;

rxString getSha1(const char *codes, ...)
{
    int argc = strlen(codes);
    std::va_list args;
    va_start(args, argc);

    SHA1_CTX ctx;
    unsigned char hash[20];
    int i;

    SHA1Init(&ctx);
    for (; *codes; ++codes)
    {
        switch (*codes)
        {
        case 's':
        {
            rxString s = va_arg(args, rxString);
            if (s)
                SHA1Update(&ctx, (const unsigned char *)s, strlen(s));
        }
        break;
        case 'i':
        {
            int i = va_arg(args, int);
            SHA1Update(&ctx, (const unsigned char *)&i, sizeof(int));
        }
        break;
        case 'f':
        {
            int f = va_arg(args, double);
            SHA1Update(&ctx, (const unsigned char *)&f, sizeof(double));
        }
        break;
        }
    }
    SHA1Final(hash, &ctx);

    char *sha1 = (char *)rxMemAlloc(41);
    char *filler = sha1;
    for (i = 0; i < 20; i++, filler += 2)
    {
        sprintf(filler, "%02x", hash[i]);
    }
    *filler = 0x00;
    return rxStringNew(sha1);
}

void freeSha1(rxString sha1)
{
    rxStringFree(sha1);
}
