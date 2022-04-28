

#include "sentenceProcessor.h"

int getSentenceEnd(Parser *p, int start) {
    long unsigned int j = start;
    if(j >= p->rpn->len) return 0;
	for(; j < p->rpn->len; j++){
		Token *t = getTokenAt(p->rpn, j);
        if(t->token_type == _immediate_operator && strcmp(".", t->token) == 0)
            return j;
        if(t->token_type == _immediate_operator && strcmp("\n", t->token) == 0)
            return j;
	}
            return j;
}

int getSubSentenceEnd(Parser *p, int start) {
    long unsigned int j = start;
    if(j >= p->rpn->len) return 0;
	for(; j < p->rpn->len; j++){
		Token *t = getTokenAt(p->rpn, j);
        if(t->token_type == _immediate_operator ){
            if(strcmp(".", t->token) == 0)
            return j;
            if(strcmp(",", t->token) == 0)
                return j;
            if(strcmp("\n", t->token) == 0)
                return j;
            if(strcmp("\t", t->token) == 0)
                return j;
        }
	}
    return j;
}

int getClassifierEnd(Parser *p, int start) {
    long unsigned int j = start;
    if(j >= p->rpn->len) return 0;
	for(; j < p->rpn->len; j++){
		Token *t = getTokenAt(p->rpn, j);
        if(t->token_type == _immediate_operator){
            if( strcmp(".", t->token) == 0)
                return 0;
            if( strcmp("\n", t->token) == 0)
                return 0;
            if(strcmp(":", t->token) == 0)
                return j;
        }
	}
    return 0;
}

int getListEnd(Parser *p, int start) {
    long unsigned int j = start;
    if(j >= p->rpn->len) return 0;
    int listLength = 0;
    while(j < p->rpn->len){
		Token *t = getTokenAt(p->rpn, j);
        //printf("getListEnd #100# start=%d j=%ld, token=%s\n", start, j, t->token);
        // if(t->token_type == _immediate_operator){
        //     //printf("getListEnd #110# start=%d j=%ld, token=%s matched:%d\n", start, j, t->token,(strcmp(".", t->token) == 0) || (strcmp("\n", t->token) == 0));
            if((strcmp(".", t->token) == 0)
            || (strcmp("\n", t->token) == 0)){
                if(listLength) ++listLength;
                //printf("getListEnd #120# start=%d j=%ld, token=%s listLength=%d\n", start, j, t->token, listLength);
                break;
            }
        // }
        if((j + 1) < p->rpn->len){
    		Token *n = getTokenAt(p->rpn, j + 1);
            //printf("getListEnd #150# start=%d j=%ld, token=%s\n", start, j, n->token);
            // if(t->token_type == _immediate_operator){
            // //printf("getListEnd #160# start=%d j=%ld, token=%s matched:%d\n", start, j, n->token, (strcmp(",", n->token) == 0) || (strcmp("\t", n->token) == 0));
                if((strcmp(",", n->token) == 0)
                || (strcmp("\t", n->token) == 0)){
                    j++;
                    ++listLength;
                    //printf("getListEnd #170# start=%d j=%ld, token=%s listLength=%d\n", start, j, t->token, listLength);
                }
            // }
        }   j++;
    }
    //printf("getListEnd #999# start=%d j=%ld, listLength=%d\n", start, j, listLength);
    return j;
}
const char *getSentenceToken(Parser *p, int n) {
    if((long unsigned)n >= p->rpn->len) return NULL;
    return getTokenAt(p->rpn, n)->token;
}
