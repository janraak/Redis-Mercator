#ifndef __RXSESSIONMEMORY_HPP__
#define __RXSESSIONMEMORY_HPP__



#define rxMemAllocSession(size, tag) rxMemAlloc(size)
#define rxMemFreeSession(ptr) rxMemFreeX(ptr)

// #include "rxSuite.h"
// #include "rxSuiteHelpers.h"

// void *rxMemAllocSession(size_t size, const char *tag);

// #ifdef __cplusplus
// extern "C"
// {
// #include "sdsWrapper.h"
// #include <stddef.h> /* atof */
// #include <stdlib.h> /* atof */

// #endif
// void rxMemFreeSession(void *ptr);

// #ifdef __cplusplus
// }
// #endif


#endif