#ifndef JAMHARAH_NORMALIZE_ARABIC_H
#define JAMHARAH_NORMALIZE_ARABIC_H

#include <stddef.h>

size_t jh_normalize_arabic_utf8(const char *in, size_t in_len, char *out, size_t out_cap);

#endif

