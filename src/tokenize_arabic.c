#include "jamharah/tokenize_arabic.h"
#include "jamharah/normalize_arabic.h"
#include <string.h>
#include <stdint.h>
static int jh_is_delim(unsigned char c) {
    if (c <= 0x20) return 1;
    switch (c) {
        case ',':
        case '.':
        case ';':
        case ':':
        case '!':
        case '?':
        case '"':
        case '\'':
        case '(':
        case ')':
        case '[':
        case ']':
        case '{':
        case '}':
        case '-':
        case '_':
        case '/':
        case '\\':
        case '+':
        case '=':
        case '*':
        case '&':
        case '%':
        case '$':
        case '#':
        case '@':
        case '<':
        case '>':
            return 1;
        default:
            return 0;
    }
}

static size_t jh_tokenize_buffer(char *buf, size_t buf_len,
                                 jh_token *tokens, size_t max_tokens)
{
    size_t i = 0;
    size_t token_count = 0;
    uint32_t pos = 0;

    while (i < buf_len) {
        while (i < buf_len && jh_is_delim((unsigned char)buf[i])) {
            i++;
        }
        if (i >= buf_len) {
            break;
        }
        if (token_count >= max_tokens) {
            return (size_t)-1;
        }
        size_t start = i;
        while (i < buf_len && !jh_is_delim((unsigned char)buf[i])) {
            i++;
        }
        size_t len = i - start;
        if (len == 0) {
            continue;
        }
        tokens[token_count].word = buf + start;
        tokens[token_count].length = len;
        tokens[token_count].position = pos;
        token_count++;
        pos++;
    }

    return token_count;
}

static int jh_utf8_decode_tok(const char *s, size_t len, size_t *i, uint32_t *out_cp) {
    if (*i >= len) return 0;
    unsigned char c0 = (unsigned char)s[*i];
    if (c0 < 0x80) {
        *out_cp = c0;
        (*i)++;
        return 1;
    } else if ((c0 & 0xE0) == 0xC0 && *i + 1 < len) {
        unsigned char c1 = (unsigned char)s[*i + 1];
        if ((c1 & 0xC0) != 0x80) return 0;
        *out_cp = ((uint32_t)(c0 & 0x1F) << 6) | (uint32_t)(c1 & 0x3F);
        *i += 2;
        return 1;
    } else if ((c0 & 0xF0) == 0xE0 && *i + 2 < len) {
        unsigned char c1 = (unsigned char)s[*i + 1];
        unsigned char c2 = (unsigned char)s[*i + 2];
        if ((c1 & 0xC0) != 0x80 || (c2 & 0xC0) != 0x80) return 0;
        *out_cp = ((uint32_t)(c0 & 0x0F) << 12)
                | ((uint32_t)(c1 & 0x3F) << 6)
                |  (uint32_t)(c2 & 0x3F);
        *i += 3;
        return 1;
    } else if ((c0 & 0xF8) == 0xF0 && *i + 3 < len) {
        unsigned char c1 = (unsigned char)s[*i + 1];
        unsigned char c2 = (unsigned char)s[*i + 2];
        unsigned char c3 = (unsigned char)s[*i + 3];
        if ((c1 & 0xC0) != 0x80 || (c2 & 0xC0) != 0x80 || (c3 & 0xC0) != 0x80) return 0;
        *out_cp = ((uint32_t)(c0 & 0x07) << 18)
                | ((uint32_t)(c1 & 0x3F) << 12)
                | ((uint32_t)(c2 & 0x3F) << 6)
                |  (uint32_t)(c3 & 0x3F);
        *i += 4;
        return 1;
    }
    return 0;
}

static int jh_utf8_encode_tok(uint32_t cp, char *out, size_t out_cap, size_t *out_len) {
    if (cp < 0x80) {
        if (*out_len + 1 > out_cap) return 0;
        out[*out_len] = (char)cp;
        (*out_len)++;
    } else if (cp < 0x800) {
        if (*out_len + 2 > out_cap) return 0;
        out[*out_len + 0] = (char)(0xC0 | (cp >> 6));
        out[*out_len + 1] = (char)(0x80 | (cp & 0x3F));
        *out_len += 2;
    } else if (cp < 0x10000) {
        if (*out_len + 3 > out_cap) return 0;
        out[*out_len + 0] = (char)(0xE0 | (cp >> 12));
        out[*out_len + 1] = (char)(0x80 | ((cp >> 6) & 0x3F));
        out[*out_len + 2] = (char)(0x80 | (cp & 0x3F));
        *out_len += 3;
    } else {
        if (*out_len + 4 > out_cap) return 0;
        out[*out_len + 0] = (char)(0xF0 | (cp >> 18));
        out[*out_len + 1] = (char)(0x80 | ((cp >> 12) & 0x3F));
        out[*out_len + 2] = (char)(0x80 | ((cp >> 6) & 0x3F));
        out[*out_len + 3] = (char)(0x80 | (cp & 0x3F));
        *out_len += 4;
    }
    return 1;
}

static int jh_is_arabic_diacritic_tok(uint32_t cp) {
    if (cp >= 0x064B && cp <= 0x065F) return 1;
    if (cp >= 0x06D6 && cp <= 0x06ED) return 1;
    return 0;
}

static uint32_t jh_normalize_arabic_cp_tok(uint32_t cp) {
    if (jh_is_arabic_diacritic_tok(cp)) {
        return 0;
    }
    if (cp == 0x0622 || cp == 0x0623 || cp == 0x0625 || cp == 0x0671) {
        return 0x0627;
    }
    if (cp == 0x0649) {
        return 0x064A;
    }
    if (cp == 0x0629) {
        return 0x0647;
    }
    return cp;
}

size_t jh_tokenize_arabic_utf8_normalized(const char *text, size_t text_len,
                                          jh_token *tokens, size_t max_tokens,
                                          char *workspace, size_t workspace_cap)
{
    size_t norm_len = jh_normalize_arabic_utf8(text, text_len,
                                               workspace, workspace_cap);
    if (norm_len == (size_t)-1) {
        return (size_t)-1;
    }
    return jh_tokenize_buffer(workspace, norm_len, tokens, max_tokens);
}

size_t jh_tokenize_arabic_utf8_raw(const char *text, size_t text_len,
                                   jh_token *tokens, size_t max_tokens,
                                   char *workspace, size_t workspace_cap)
{
    if (text_len > workspace_cap) {
        return (size_t)-1;
    }
    if (text_len > 0) {
        memcpy(workspace, text, text_len);
    }
    return jh_tokenize_buffer(workspace, text_len, tokens, max_tokens);
}

size_t jh_tokenize_arabic_utf8(const char *text, size_t text_len,
                               jh_token *tokens, size_t max_tokens,
                               char *workspace, size_t workspace_cap)
{
    return jh_tokenize_arabic_utf8_normalized(text, text_len,
                                              tokens, max_tokens,
                                              workspace, workspace_cap);
}

size_t jh_normalize_and_tokenize_arabic_utf8(const char *text, size_t text_len,
                                             jh_token *tokens, size_t max_tokens,
                                             char *workspace, size_t workspace_cap)
{
    size_t i = 0;
    size_t out_len = 0;
    size_t token_count = 0;
    uint32_t pos = 0;
    size_t token_start = 0;
    int in_token = 0;

    while (i < text_len) {
        uint32_t cp;
        if (!jh_utf8_decode_tok(text, text_len, &i, &cp)) {
            return (size_t)-1;
        }
        cp = jh_normalize_arabic_cp_tok(cp);
        if (cp == 0) {
            continue;
        }
        int is_delim_code = 0;
        if (cp < 0x80 && jh_is_delim((unsigned char)cp)) {
            is_delim_code = 1;
        }
        if (is_delim_code) {
            if (in_token) {
                tokens[token_count].length = out_len - token_start;
                token_count++;
                pos++;
                in_token = 0;
                if (token_count > max_tokens) {
                    return (size_t)-1;
                }
            }
            continue;
        }
        if (!in_token) {
            if (token_count >= max_tokens) {
                return (size_t)-1;
            }
            token_start = out_len;
            tokens[token_count].word = workspace + token_start;
            tokens[token_count].position = pos;
            in_token = 1;
        }
        if (cp < 0x80) {
            if (out_len + 1 > workspace_cap) {
                return (size_t)-1;
            }
            workspace[out_len] = (char)cp;
            out_len += 1;
        } else {
            if (!jh_utf8_encode_tok(cp, workspace, workspace_cap, &out_len)) {
                return (size_t)-1;
            }
        }
    }
    if (in_token) {
        tokens[token_count].length = out_len - token_start;
        token_count++;
    }
    return token_count;
}
