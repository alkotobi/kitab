#include "jamharah/normalize_arabic.h"
#include <stdint.h>

static int jh_utf8_decode(const char *s, size_t len, size_t *i, uint32_t *out_cp) {
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

static int jh_utf8_encode(uint32_t cp, char *out, size_t out_cap, size_t *out_len) {
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

static int jh_is_arabic_diacritic(uint32_t cp) {
    if (cp >= 0x064B && cp <= 0x065F) return 1;
    if (cp >= 0x06D6 && cp <= 0x06ED) return 1;
    return 0;
}

static uint32_t jh_normalize_arabic_cp(uint32_t cp) {
    if (jh_is_arabic_diacritic(cp)) {
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

size_t jh_normalize_arabic_utf8(const char *in, size_t in_len, char *out, size_t out_cap) {
    size_t i = 0;
    size_t out_len = 0;
    while (i < in_len) {
        uint32_t cp;
        if (!jh_utf8_decode(in, in_len, &i, &cp)) {
            return (size_t)-1;
        }
        cp = jh_normalize_arabic_cp(cp);
        if (cp == 0) {
            continue;
        }
        if (!jh_utf8_encode(cp, out, out_cap, &out_len)) {
            return (size_t)-1;
        }
    }
    return out_len;
}

