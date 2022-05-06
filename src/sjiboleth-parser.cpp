#include "sjiboleth.h"
#include "sjiboleth.hpp"
#include "stdio.h"
#include <cstring>

#define semicolon sdsnew(";")

ParserToken *Sjiboleth::newToken(eTokenType token_type, const char *token, size_t len)
{
    ParserToken *t = NULL;
    if (token_type != _literal)
    {
        t = (ParserToken *)raxFind(this->registry, (UCHAR *)token, len);
        if (t != raxNotFound)
        {
            return t;
        }
    }
    t = new ParserToken(token_type, token, len);
    // t->keyset = NULL;
    switch (token_type)
    {
    case _operand:
        // o = dictFind(operator_map, (void *)t->token);
        // t->priority = o ? *(int *)o->v.val : 100;
        // t->TokenType() = o ? _operator : token_type;
        break;
    case _literal:
        t->Priority(100);
        break;
    case _open_bracket:
        t->Priority(50);
        break;
    case _close_bracket:
        t->Priority(50);
        break;
    case _operator:
    default:
        // o = dictFind(operator_map, (void *)t->token);
        // if (o)
        // {
        // 	t->priority = *(int *)o->v.val;
        // 	if (t->priority == priImmediate)
        // 		t->TokenType() = _immediate_operator;
        // }
        // else
        t->Priority(-1);
        break;
    }
    return t;
}

ParsedExpression *Sjiboleth::Parse(const char *query)
{
    auto *expression = new ParsedExpression(this);
    char *head = (char *)query;
    while (*head && is_space(*head))
        ++head;
    ParserToken *last_token = NULL;
    while (*head)
    {
        char *tail;
        while (is_space(*head))
            head++;

        ParserToken *token = NULL;
        if (*head == '"' || *head == '\'')
        {
            token = scanLiteral(head, &tail);
            head = tail + 1;
            expression->stashToken(this, token, last_token);
        }
        else if (isNumber(head))
        {
            token = scanNumber(head, &tail);
            head = tail;
            expression->stashToken(this, token, last_token);
        }
        else if (isoperator(*head))
        {
            token = scanOperator(head, &tail);
            head = tail;
            if (expression->hasParkedTokens())
            {
                ParserToken *head_token = expression->peekParked();
                if (head_token->TokenType() == _operator && head_token->Priority() >= token->Priority())
                {
                    expression->moveSideTrackToFinal();
                }
            }
            if (token->TokenType() == _immediate_operator)
            {
                expression->flushSideTrack();
                expression->emitFinal(token);
            }
            else
                expression->park(token);
        }
        else if (isbracket(*head))
        {
            token = scanBracket(head, &tail);
            head = tail;
            if (token->TokenType() == _close_bracket)
            {
                if (expression->hasParkedTokens())
                {
                    while (expression->hasParkedTokens())
                    {
                        ParserToken *head_token = expression->peekParked();
                        if (head_token->TokenType() == _open_bracket)
                        {
                            expression->unpark();
                            break;
                        }
                        else
                        {
                            expression->emitFinal(head_token);
                            expression->unpark();
                        }
                    }
                }
                else
                    expression->AddError(sdsnew("unbalanced brackets"));
                if (token->Is(";"))
                    expression->emitFinal(token);
                if (this->object_and_array_controls)
                {
                    expression->emitFinal(token);
                }
            }
            else
            {
                if (this->object_and_array_controls)
                {
                    expression->emitFinal(token);
                }
                if (last_token &&
                    (last_token->TokenType() == _operand || last_token->TokenType() == _close_bracket))
                {
                    while (expression->hasParkedTokens())
                    {
                        expression->moveSideTrackToFinal();
                    }
                    if (this->hasDefaultOperator())
                    {
                        char *dummy;
                        ParserToken *default_token = scanOperator((char *)this->defaultOperator(), &dummy);
                        expression->park(default_token);
                    }
                }
                expression->park(token);
            }
        }
        else if (is_space(*head))
        {
            while (*head && is_space(*head))
                ++head;
            token = last_token;
        }
        else
        {
            token = scanIdentifier(head, &tail);
            if(token->HasParserContextProc()){
                parserContextProc *pcp = token->ParserContextProc();
                token = (ParserToken *)pcp((CSjiboleth *)this, (CParserToken *) token, head, (CParsedExpression *)expression);
            }
            head = tail;
            switch (token->TokenType())
            {
            case _operator:
                if (expression->hasParkedTokens())
                {
                    ParserToken *head_token = expression->peekParked();
                    if (head_token->TokenType() == _operator && head_token->Priority() >= token->Priority())
                    {
                        expression->emitFinal(head_token);
                        expression->unpark();
                    }
                }
                expression->park(token);
                break;
            default:
                expression->stashToken(this, token, last_token);
                break;
            }
        };
        last_token = token;
    }
    expression->flushSideTrack();
    // //printf(head);
    expression->show(query);
    return expression;
}

void ParsedExpression::show(const char *query)
{
    return;
    // if (p->show_debug_info)
    // {
    printf("expression: %d entries\n", this->expression->Size());
    printf("side_track: %ld entries\n", this->side_track->len);
    // }
    // listRelease(side_track);
    // if (p->show_debug_info)
    // {
    printf("parsed: %s %d tokens\n", query, this->expression->Size());
    this->expression->StartHead();
    ParserToken *t;
    int j = 0;
    while ((t = this->expression->Next()) != NULL)
    {
        printf("Parse: %d %d %s\n", j, t->TokenType(), t->Token());
        ++j;
    }
    // }
}

void ParsedExpression::Show(const char *query)
{
    // if (p->show_debug_info)
    // {
    printf("expression: %d entries\n", this->expression->Size());
    printf("side_track: %ld entries\n", this->side_track->len);
    // }
    // listRelease(side_track);
    // if (p->show_debug_info)
    // {
    printf("parsed: %s %d tokens\n", query, this->expression->Size());
    this->expression->StartHead();
    ParserToken *t;
    int j = 0;
    while ((t = this->expression->Next()) != NULL)
    {
        printf("Parse: %d %d %s\n", j, t->TokenType(), t->Token());
        ++j;
    }
    // }
}

sds ParsedExpression::ToString()
{
    sds result = sdsempty();
    this->expression->StartHead();
    ParserToken *t;
    while ((t = this->expression->Next()) != NULL)
    {
        result = sdscatprintf(result, " %s", t->Token());
    }
    return result;
}

ParserToken *Sjiboleth::getTokenAt(list *list, int ix)
{
    listNode *node = listIndex(list, ix);
    if (node)
        return (ParserToken *)node->value;
    return NULL;
}

ParserToken *ParsedExpression::unpark()
{
    auto *token = this->removeTokenAt(side_track, 0);
    return token;
}

void ParsedExpression::moveSideTrackToFinal()
{
    auto *head_token = this->removeTokenAt(side_track, 0);
    this->expression->Add(head_token);
}

ParserToken *ParsedExpression::tokenAt(list *list, int ix)
{
    listNode *node = listIndex(list, ix);
    if (node)
        return (ParserToken *)node->value;
    return NULL;
}

ParserToken *ParsedExpression::lastInstruction()
{
    return this->expression->Last();
}

void ParsedExpression::emitFinal(ParserToken *token)
{
    if (/*token->TokenType() == _operator && */ token->Priority() == priIgnore)
        return;
    this->expression->Add(token);
}
void ParsedExpression::park(ParserToken *token)
{
    listAddNodeHead(this->side_track, token);
}

int ParsedExpression::sideTrackLength()
{
    return listLength(side_track);
}

bool ParsedExpression::hasParkedTokens()
{
    return listLength(side_track) > 0;
}

bool ParsedExpression::hasParkedToken(const char *token){
    listIter *li = listGetIterator(this->side_track, AL_START_HEAD);
    listNode *ln;
    while((ln = listNext(li)) != NULL) {
        ParserToken *t = (ParserToken *)ln->value;
        if(t->Is(token)){
            listReleaseIterator(li);
            return true;
        }
    }
    listReleaseIterator(li);
    return false;
}

bool Sjiboleth::isbracket(char aChar)
{
    switch (aChar)
    {
    case '(':
    case ')':
    case '{':
    case '}':
    case '[':
    case ']':
    case ';':
        return true;
    default:
        return false;
    }
}

bool Sjiboleth::is_space(char aChar)
{
    switch (aChar)
    {
    case ' ':
    case '\v':
    case '\f':
    case '\r':
        return true;
    case '\n':
    case '\t':
        return !this->crlftab_as_operator;
    default:
        return false;
    }
}

bool Sjiboleth::isNumber(char *aChar)
{
    return isdigit(*aChar) || ((*aChar == '+' || *aChar == '-') && isdigit(*(aChar + 1)));
}

bool Sjiboleth::isoperator(char aChar)
{
    switch (aChar)
    {
    case '.':
    // case ';':
    case ':':
    case ',':
        return true;
    case '<':
    case '=':
    case '>':
    case '!':
    case '+':
    case '-':
    case '^':
    case '/':
    case '*':
    case '\\':
    case '?':
    case '|':
    case '&':
    case '%':
        return !this->crlftab_as_operator;
    case '@':
        return this->crlftab_as_operator;
    case '\n':
    case '\t':
        return this->crlftab_as_operator;
    default:
        return false;
    }
}

bool Sjiboleth::iscsym(int c)
{
    if (isalnum(c))
        return true;
    if (c == '_' || c == '@' || c == '$')
        return true;
    return false;
}

ParserToken *Sjiboleth::scanIdentifier(char *head, char **tail)
{
    *tail = head + 1;
    // int last_was_literal_fence = 0;
    while (**tail && !(/*(**tail == '"' || **tail == '\'') || isNumber(*tail) ||*/  isoperator(**tail) || isbracket(**tail) || is_space(**tail)))
    {

        // last_was_literal_fence = (**tail == '"' || **tail == '\'');

        ++*tail;
    }
    sds op = sdsnewlen(head, (int)(*tail - head));
    sdstoupper(op);
    ParserToken *t = (ParserToken *)raxFind(this->registry, (UCHAR *)op, (int)(*tail - head));
    sdsfree(op);
    if (t != raxNotFound)
    {
        return t;
    }
    return newToken(_operand, head, *tail - head);
}

ParserToken *Sjiboleth::scanLiteral(char *head, char **tail)
{
    char fence = *head;
    ++head;
    *tail = head;
    while (**tail && **tail != fence)
        ++*tail;
    return newToken(_literal, head, *tail - head);
}

ParserToken *Sjiboleth::scanOperator(char *head, char **tail)
{
    *tail = head + 1;
    while (isoperator(**tail))
        *tail += 1;
    ParserToken *t = newToken(_operator, head, *tail - head);
    return t;
}

ParserToken *Sjiboleth::scanBracket(char *head, char **tail)
{
    *tail = head + 1;
    switch (*head)
    {
    case '(':
    case '{':
    case '[':
        return newToken(_open_bracket, head, 1);
    default:
        return newToken(_close_bracket, head, 1);
    }
}

ParserToken *Sjiboleth::scanNumber(char *head, char **tail)
{
    *tail = head + 1;
    int no_of_decimal_points = 0;
    while (**tail)
    {
        if (isdigit(**tail))
            ++*tail;
        else if (**tail == '.' && !no_of_decimal_points)
        {
            ++*tail;
            no_of_decimal_points++;
        }
        else
        {
            return newToken(_operand, head, *tail - head);
        }
    }
    return newToken(_operand, head, *tail - head);
}

void ParsedExpression::stashToken(Sjiboleth *p, ParserToken *token, ParserToken *last_token)
{
    if (last_token && last_token->TokenType() == _operand && p->hasDefaultOperator())
    {
        this->emitFinal(token);
        char *dummy;
        ParserToken *default_token = p->scanOperator((char *)p->defaultOperator(), &dummy);
        this->park(default_token);
    }
    else
        this->emitFinal(token);
}

ParserToken *ParsedExpression::removeTokenAt(list *list, int ix)
{
    listNode *node = listIndex(list, ix);
    if (node)
    {
        ParserToken *t = (ParserToken *)node->value;
        listDelNode(list, node);
        return t;
    }
    return NULL;
}

void ParsedExpression::flushSideTrack()
{
    while (this->side_track->len)
    {
        this->moveSideTrackToFinal();
    }
}

ParsedExpression::ParsedExpression(Sjiboleth *dialect)
{
    this->dialect = dialect;
    this->expression = new GraphStack<ParserToken>();
    this->side_track = listCreate();
    this->errors = listCreate();
};

void ParsedExpression::AddError(sds msg)
{
    listAddNodeTail(this->errors, msg);
}

bool ParsedExpression::HasErrors(){
    return listLength(this->errors) > 0;
}

int ParsedExpression::writeErrors(RedisModuleCtx *ctx){
    RedisModule_ReplyWithArray(ctx, listLength(this->errors));
    listIter *li = listGetIterator(this->errors, 0);
    listNode *ln;
    while ((ln = listNext(li)) != NULL){
        RedisModule_ReplyWithSimpleString(ctx, (char *)ln->value);
    }
    listReleaseIterator(li);
    return C_OK;
}

ParserToken *ParsedExpression::peekParked()
{
    return this->tokenAt(this->side_track, 0);
}
