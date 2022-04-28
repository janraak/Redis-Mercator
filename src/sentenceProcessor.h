

#ifndef __SENTENCEPROCESSOR_H__
#define __SENTENCEPROCESSOR_H__

#include "parser.h"

int getSentenceEnd(Parser *p, int start);
int getSubSentenceEnd(Parser *p, int start);
int getClassifierEnd(Parser *p, int start);
int getListEnd(Parser *p, int start);
const char *getSentenceToken(Parser *p, int n);

#endif