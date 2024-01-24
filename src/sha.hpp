#ifndef __SHA_HPP__
#define __SHA_HPP__

#include "sdsWrapper.h"

#ifdef __cplusplus
extern "C"
{
#endif

#ifdef __cplusplus
}
#endif

    rxString getSha1(const char *codes, ...);
    void freeSha1(rxString sha1);

#endif