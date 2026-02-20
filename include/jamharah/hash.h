#ifndef JAMHARAH_HASH_H
#define JAMHARAH_HASH_H

#include <stddef.h>
#include <stdint.h>

uint64_t jh_hash_utf8_64(const char *data, size_t len, uint64_t seed);

#endif
