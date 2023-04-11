#ifndef __RXLITERALS_H__
#define __RXLITERALS_H__

#include <stdint.h>
#if UINTPTR_MAX == 0xffffffff
/* 32-bit */

#define RXINDEX_FORMAT_001 "rxIndexer LD_LIBRARY_PATH[%d]=%s"

#elif UINTPTR_MAX == 0xffffffffffffffff
/* 64-bit */

#define RXINDEX_FORMAT_001 "rxIndexer LD_LIBRARY_PATH[%ld]=%s"

#else
/* wtf */
#endif


#endif