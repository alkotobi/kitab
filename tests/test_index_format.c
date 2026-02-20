#include "jamharah/index_format.h"
#include "jamharah/normalize_arabic.h"
#include "jamharah/tokenize_arabic.h"
#include "jamharah/hash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* test_build_simple_postings fills a buffer with a tiny postings list used by several tests. */
static void test_build_simple_postings(jh_u8 *buf, size_t *out_size) {
    jh_u32 doc_count = 2;
    jh_u32 *p = (jh_u32 *)buf;
    size_t idx = 0;

    p[idx++] = 2;

    p[idx++] = 3;
    p[idx++] = 2;
    p[idx++] = 1;
    p[idx++] = 2;

    p[idx++] = 7;
    p[idx++] = 1;
    p[idx++] = 5;

    *out_size = idx * sizeof(jh_u32);
}

/* test_postings_cursor_basic verifies that the postings cursor decodes doc ids and positions. */
static int test_postings_cursor_basic(void) {
    jh_u8 buf[64];
    size_t size = 0;
    jh_postings_cursor cur;
    jh_posting_entry e;
    jh_u32 pos_buf[8];
    int rc;

    test_build_simple_postings(buf, &size);

    rc = jh_postings_cursor_init(&cur, buf, size);
    if (rc != 0) {
        fprintf(stderr, "cursor_init rc=%d\n", rc);
        return 1;
    }

    rc = jh_postings_cursor_next(&cur, &e, pos_buf, 8);
    if (rc != 0) {
        fprintf(stderr, "cursor_next first rc=%d\n", rc);
        return 1;
    }
    if (e.page_id != 3 || e.term_freq != 2) {
        fprintf(stderr, "first doc mismatch\n");
        return 1;
    }
    if (pos_buf[0] != 1 || pos_buf[1] != 3) {
        fprintf(stderr, "first positions mismatch\n");
        return 1;
    }

    rc = jh_postings_cursor_next(&cur, &e, pos_buf, 8);
    if (rc != 0) {
        fprintf(stderr, "cursor_next second rc=%d\n", rc);
        return 1;
    }
    if (e.page_id != 10 || e.term_freq != 1) {
        fprintf(stderr, "second doc mismatch\n");
        return 1;
    }
    if (pos_buf[0] != 5) {
        fprintf(stderr, "second positions mismatch\n");
        return 1;
    }

    rc = jh_postings_cursor_next(&cur, &e, pos_buf, 8);
    if (rc != 1) {
        fprintf(stderr, "expected end rc=1 got=%d\n", rc);
        return 1;
    }

    return 0;
}

/* test_postings_list_parse_basic verifies the materialized postings list representation. */
static int test_postings_list_parse_basic(void) {
    jh_u8 buf[64];
    size_t size = 0;
    jh_postings_list list;
    int rc;

    test_build_simple_postings(buf, &size);

    rc = jh_postings_list_parse(buf, size, &list);
    if (rc != 0) {
        fprintf(stderr, "list_parse rc=%d\n", rc);
        return 1;
    }
    if (list.entry_count != 2) {
        fprintf(stderr, "entry_count=%u\n", (unsigned)list.entry_count);
        jh_postings_list_free(&list);
        return 1;
    }
    if (list.entries[0].page_id != 3 || list.entries[0].term_freq != 2) {
        fprintf(stderr, "entry0 mismatch\n");
        jh_postings_list_free(&list);
        return 1;
    }
    if (list.entries[1].page_id != 10 || list.entries[1].term_freq != 1) {
        fprintf(stderr, "entry1 mismatch\n");
        jh_postings_list_free(&list);
        return 1;
    }
    if (list.positions_storage[0] != 1 || list.positions_storage[1] != 3 || list.positions_storage[2] != 5) {
        fprintf(stderr, "positions mismatch\n");
        jh_postings_list_free(&list);
        return 1;
    }

    jh_postings_list_free(&list);
    return 0;
}

/* test_build_and_postings builds two compatible postings buffers for AND and phrase tests. */
static void test_build_and_postings(jh_u8 *a_buf, size_t *a_size, jh_u8 *b_buf, size_t *b_size) {
    jh_u32 *p;
    size_t idx;

    p = (jh_u32 *)a_buf;
    idx = 0;
    p[idx++] = 2;      /* doc_count */
    p[idx++] = 3;      /* doc_delta=3 -> doc_id=3 */
    p[idx++] = 1;      /* term_freq=1 */
    p[idx++] = 2;      /* position: 2 */
    p[idx++] = 17;     /* doc_delta=17 -> doc_id=20 */
    p[idx++] = 1;      /* term_freq=1 */
    p[idx++] = 5;      /* position: 5 */
    *a_size = idx * sizeof(jh_u32);

    p = (jh_u32 *)b_buf;
    idx = 0;
    p[idx++] = 2;      /* doc_count */
    p[idx++] = 3;      /* doc_delta=3 -> doc_id=3 */
    p[idx++] = 1;      /* term_freq=1 */
    p[idx++] = 3;      /* position: 3 */
    p[idx++] = 27;     /* doc_delta=27 -> doc_id=30 */
    p[idx++] = 1;      /* term_freq=1 */
    p[idx++] = 6;      /* position: 6 */
    *b_size = idx * sizeof(jh_u32);
}

/* test_postings_and_cursor_basic checks that the AND cursor returns only shared docs. */
static int test_postings_and_cursor_basic(void) {
    jh_u8 a_buf[64];
    jh_u8 b_buf[64];
    size_t a_size = 0;
    size_t b_size = 0;
    jh_postings_cursor cur_a;
    jh_postings_cursor cur_b;
    jh_u32 pos_a[8];
    jh_u32 pos_b[8];
    jh_postings_and_cursor ac;
    jh_posting_entry e;
    int rc;

    test_build_and_postings(a_buf, &a_size, b_buf, &b_size);

    rc = jh_postings_cursor_init(&cur_a, a_buf, a_size);
    if (rc != 0) {
        fprintf(stderr, "cursor_init a rc=%d\n", rc);
        return 1;
    }
    rc = jh_postings_cursor_init(&cur_b, b_buf, b_size);
    if (rc != 0) {
        fprintf(stderr, "cursor_init b rc=%d\n", rc);
        return 1;
    }
    rc = jh_postings_and_cursor_init(&ac, &cur_a, &cur_b, pos_a, 8, pos_b, 8);
    if (rc != 0) {
        fprintf(stderr, "and_cursor_init rc=%d\n", rc);
        return 1;
    }

    rc = jh_postings_and_cursor_next(&ac, &e);
    if (rc != 0) {
        fprintf(stderr, "and_cursor_next rc=%d\n", rc);
        return 1;
    }
    if (e.page_id != 3) {
        fprintf(stderr, "and_cursor page_id=%u\n", (unsigned)e.page_id);
        return 1;
    }

    rc = jh_postings_and_cursor_next(&ac, &e);
    if (rc != 1) {
        fprintf(stderr, "and_cursor expected end rc=%d\n", rc);
        return 1;
    }

    return 0;
}

/* test_postings_phrase_and_cursor_basic checks phrase detection between two term streams. */
static int test_postings_phrase_and_cursor_basic(void) {
    jh_u8 a_buf[64];
    jh_u8 b_buf[64];
    size_t a_size = 0;
    size_t b_size = 0;
    jh_postings_cursor cur_a;
    jh_postings_cursor cur_b;
    jh_u32 pos_a[8];
    jh_u32 pos_b[8];
    jh_postings_phrase_and_cursor pc;
    jh_posting_entry e;
    int rc;

    test_build_and_postings(a_buf, &a_size, b_buf, &b_size);

    rc = jh_postings_cursor_init(&cur_a, a_buf, a_size);
    if (rc != 0) {
        fprintf(stderr, "cursor_init a rc=%d\n", rc);
        return 1;
    }
    rc = jh_postings_cursor_init(&cur_b, b_buf, b_size);
    if (rc != 0) {
        fprintf(stderr, "cursor_init b rc=%d\n", rc);
        return 1;
    }
    rc = jh_postings_phrase_and_cursor_init(&pc, &cur_a, &cur_b, pos_a, 8, pos_b, 8);
    if (rc != 0) {
        fprintf(stderr, "phrase_and_cursor_init rc=%d\n", rc);
        return 1;
    }

    rc = jh_postings_phrase_and_cursor_next(&pc, &e);
    if (rc != 0) {
        fprintf(stderr, "phrase_and_cursor_next rc=%d\n", rc);
        return 1;
    }
    if (e.page_id != 3 || e.term_freq != 1) {
        fprintf(stderr, "phrase_and_cursor mismatch page_id=%u term_freq=%u\n",
                (unsigned)e.page_id, (unsigned)e.term_freq);
        return 1;
    }

    rc = jh_postings_phrase_and_cursor_next(&pc, &e);
    if (rc != 1) {
        fprintf(stderr, "phrase_and_cursor expected end rc=%d\n", rc);
        return 1;
    }

    return 0;
}

static int test_normalize_arabic_basic(void) {
    const char in[] =
        "\xD8\xA2\xD8\xA3\xD8\xA5\xD8\xA7"
        "\xD9\x89"
        "\xD8\xA9"
        "\xD9\x8E";
    const char expected[] =
        "\xD8\xA7\xD8\xA7\xD8\xA7\xD8\xA7"
        "\xD9\x8A"
        "\xD9\x87";
    char out[64];
    size_t out_len = jh_normalize_arabic_utf8(in, strlen(in), out, sizeof(out));
    if (out_len == (size_t)-1) {
        fprintf(stderr, "normalize_arabic_utf8 returned error\n");
        return 1;
    }
    if (out_len != strlen(expected)) {
        fprintf(stderr, "normalize_arabic_utf8 length mismatch\n");
        return 1;
    }
    if (memcmp(out, expected, out_len) != 0) {
        fprintf(stderr, "normalize_arabic_utf8 content mismatch\n");
        return 1;
    }
    return 0;
}

static int test_tokenize_arabic_basic(void) {
    const char in[] =
        "\xD8\xA2\xD8\xAD\xD9\x85\xD8\xAF"  /* آحمد (Alef with madda) */
        " "
        "\xD9\x8A\xD8\xB3";                /* يس */
    jh_token tokens[4];
    char workspace[128];
    size_t n = jh_tokenize_arabic_utf8_normalized(in, strlen(in),
                                                  tokens, 4,
                                                  workspace, sizeof(workspace));
    if (n == (size_t)-1) {
        fprintf(stderr, "tokenize_arabic_utf8 returned error\n");
        return 1;
    }
    if (n != 2) {
        fprintf(stderr, "token count=%zu expected 2\n", n);
        return 1;
    }
    if (tokens[0].position != 0 || tokens[1].position != 1) {
        fprintf(stderr, "token positions mismatch\n");
        return 1;
    }

    n = jh_tokenize_arabic_utf8_raw(in, strlen(in),
                                    tokens, 4,
                                    workspace, sizeof(workspace));
    if (n == (size_t)-1) {
        fprintf(stderr, "tokenize_arabic_utf8_raw returned error\n");
        return 1;
    }
    if (n != 2) {
        fprintf(stderr, "raw token count=%zu expected 2\n", n);
        return 1;
    }
    if (tokens[0].position != 0 || tokens[1].position != 1) {
        fprintf(stderr, "raw token positions mismatch\n");
        return 1;
    }

    n = jh_normalize_and_tokenize_arabic_utf8(in, strlen(in),
                                              tokens, 4,
                                              workspace, sizeof(workspace));
    if (n == (size_t)-1) {
        fprintf(stderr, "normalize_and_tokenize_arabic_utf8 returned error\n");
        return 1;
    }
    if (n != 2) {
        fprintf(stderr, "normalize_and_tokenize token count=%zu expected 2\n", n);
        return 1;
    }
    if (tokens[0].position != 0 || tokens[1].position != 1) {
        fprintf(stderr, "normalize_and_tokenize positions mismatch\n");
        return 1;
    }
    return 0;
}

static int test_tokenize_arabic_page_from_sqlite(void) {
    const char page_text[] =
        "بسم الله الرحمن الرحيم\n"
        "الطريقُ المستقيمُ فِي نظم علاماتِ الترقيم\n"
        "1 - يَقولُ رَاجِي رَحْمةِ السَّمِيع ... ذو العَجْز مَحْمُودٌ أبُو سَريع\n"
        "2 - الحَمْدُ للهِ الذِي بالقلم ... قدْ عَلمَ الإنسَانَ مَا لمْ يَعْلم\n"
        "3 - وَأفضَلُ الصَّلاةِ وَالتسْلِيم ... عَلى النبيِّ المُصْطفى الكريم\n"
        "4 - وَبَعْدُ فالترْقِيمُ ذو فوَائِدِ ... لِكاتِبٍ وَقارئٍ وَناقِدِ\n"
        "5 - مَوَاقِعُ الفصْل بهِ تنكشِفُ ... ويُدْركُ القارئُ أيْنَ يَقِفُ\n"
        "6 - كأنهُ إشَارَةُ المُرُور ... تؤْذِنُ بالوُقوفِ وَالعُبُور\n"
        "7 - يَمِيزُ أجْزَاءَ الكلام مَبنى ... وَينجَلِي بهِ اكتِمَالُ المَعْنى\n"
        "8 - يُقرِّبُ المَعْنى إلى الأذهَان ... وَيكشِفُ الغُمُوضَ في المَعَانِي\n"
        "9 - وَقدْ رَأيْتُ نظمَهُ للطالبِ ... نظمًا بَدِيعًا سَائِغًا للشَّاربِ\n"
        "10 - وَرَبُّنا المَسْئُولُ فِي الرِّعَايَة ... وَالمُسْتعَانُ فِي بُلوغ الغايَة\n"
        "11 - سَألتهُ الصَّوابَ وَالتوْفِيقا ... مُذللا لِعَبْدِهِ الطريقا\n";
    jh_token tokens[1024];
    char workspace[8192];
    size_t n = jh_normalize_and_tokenize_arabic_utf8(page_text, strlen(page_text),
                                                     tokens, 1024,
                                                     workspace, sizeof(workspace));
    if (n == (size_t)-1) {
        fprintf(stderr, "normalize_and_tokenize_arabic_utf8(page) returned error\n");
        return 1;
    }
    if (n < 10) {
        fprintf(stderr, "page token count=%zu unexpectedly small\n", n);
        return 1;
    }
    if (tokens[0].position != 0) {
        fprintf(stderr, "page first token position=%u expected 0\n",
                (unsigned)tokens[0].position);
        return 1;
    }
    return 0;
}

static int test_hash_utf8_basic(void) {
    const char *s1 = "abc";
    const char *s2 = "abd";
    const char *s3 = "بسم";
    uint64_t h1 = jh_hash_utf8_64(s1, strlen(s1), 0);
    uint64_t h2 = jh_hash_utf8_64(s2, strlen(s2), 0);
    uint64_t h3 = jh_hash_utf8_64(s1, strlen(s1), 1);
    uint64_t h4 = jh_hash_utf8_64(s3, strlen(s3), 0);
    if (h1 == 0 || h2 == 0 || h3 == 0 || h4 == 0) {
        fprintf(stderr, "hash returned zero\n");
        return 1;
    }
    if (h1 == h2) {
        fprintf(stderr, "hash collision between abc and abd\n");
        return 1;
    }
    if (h1 == h3) {
        fprintf(stderr, "hash seed did not change value\n");
        return 1;
    }
    return 0;
}

/* main runs all index_format tests and returns non-zero on failure. */
int main(void) {
    if (test_postings_cursor_basic() != 0) {
        return 1;
    }
    if (test_postings_list_parse_basic() != 0) {
        return 1;
    }
    if (test_postings_and_cursor_basic() != 0) {
        return 1;
    }
    if (test_postings_phrase_and_cursor_basic() != 0) {
        return 1;
    }
    if (test_normalize_arabic_basic() != 0) {
        return 1;
    }
    if (test_tokenize_arabic_basic() != 0) {
        return 1;
    }
    if (test_tokenize_arabic_page_from_sqlite() != 0) {
        return 1;
    }
    if (test_hash_utf8_basic() != 0) {
        return 1;
    }
    return 0;
}
