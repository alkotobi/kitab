#include "jamharah/tokenize_arabic.h"
#include <stddef.h>
#include <string.h>

static int jh_has_prefix(const char *word, size_t len, const char *prefix, size_t *out_skip, size_t min_len) {
    size_t plen = strlen(prefix);
    if (len < plen + min_len) {
        return 0;
    }
    if (memcmp(word, prefix, plen) != 0) {
        return 0;
    }
    *out_skip = plen;
    return 1;
}

static int jh_has_suffix(const char *word, size_t len, const char *suffix, size_t *out_trim, size_t min_len) {
    size_t slen = strlen(suffix);
    if (len < slen + min_len) {
        return 0;
    }
    if (memcmp(word + len - slen, suffix, slen) != 0) {
        return 0;
    }
    *out_trim = slen;
    return 1;
}

void jh_light_stem_arabic_tokens(jh_token *tokens, size_t token_count) {
    size_t i;
    static const char *prefixes[] = {
        "وال",
        "فال",
        "بال",
        "كال",
        "لل",
        "ال",
        "و",
        "ف",
        "ب",
        "ك",
        "ل",
        "س"
    };
    static const size_t prefix_count = sizeof(prefixes) / sizeof(prefixes[0]);
    static const char *suffixes[] = {
        "هما",
        "كما",
        "كم",
        "هم",
        "هن",
        "كن",
        "ها",
        "نا",
        "ات",
        "ون",
        "ين",
        "ان",
        "ة",
        "ه",
        "ي"
    };
    static const size_t suffix_count = sizeof(suffixes) / sizeof(suffixes[0]);
    const size_t min_bytes = 4;

    for (i = 0; i < token_count; ++i) {
        const char *w = tokens[i].word;
        size_t len = tokens[i].length;
        size_t start = 0;
        size_t end = len;
        size_t k;

        if (len <= min_bytes) {
            continue;
        }

        for (k = 0; k < prefix_count; ++k) {
            size_t skip = 0;
            if (jh_has_prefix(w + start, end - start, prefixes[k], &skip, min_bytes)) {
                start += skip;
                break;
            }
        }

        for (k = 0; k < suffix_count; ++k) {
            size_t trim = 0;
            if (jh_has_suffix(w + start, end - start, suffixes[k], &trim, min_bytes)) {
                end -= trim;
                break;
            }
        }

        if (end > start) {
            tokens[i].word = w + start;
            tokens[i].length = end - start;
        }
    }
}

