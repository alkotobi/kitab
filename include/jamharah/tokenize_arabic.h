#ifndef JAMHARAH_TOKENIZE_ARABIC_H
#define JAMHARAH_TOKENIZE_ARABIC_H

#include <stddef.h>
#include <stdint.h>

typedef struct {
    const char *word;
    size_t      length;
    uint32_t    position;
} jh_token;

size_t jh_tokenize_arabic_utf8_normalized(const char *text, size_t text_len,
                                          jh_token *tokens, size_t max_tokens,
                                          char *workspace, size_t workspace_cap);

size_t jh_tokenize_arabic_utf8_raw(const char *text, size_t text_len,
                                   jh_token *tokens, size_t max_tokens,
                                   char *workspace, size_t workspace_cap);

size_t jh_tokenize_arabic_utf8(const char *text, size_t text_len,
                               jh_token *tokens, size_t max_tokens,
                               char *workspace, size_t workspace_cap);

size_t jh_normalize_and_tokenize_arabic_utf8(const char *text, size_t text_len,
                                             jh_token *tokens, size_t max_tokens,
                                             char *workspace, size_t workspace_cap);

#endif
