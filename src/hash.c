#include "jamharah/hash.h"

uint64_t jh_hash_utf8_64(const char *data, size_t len, uint64_t seed) {
    const uint64_t fnv_offset = 14695981039346656037ULL;
    const uint64_t fnv_prime = 1099511628211ULL;
    uint64_t h = fnv_offset ^ seed;
    for (size_t i = 0; i < len; i++) {
        h ^= (unsigned char)data[i];
        h *= fnv_prime;
    }
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return h;
}

