#include "jamharah/index_format.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void jh_die_words(const char *msg) {
    fprintf(stderr, "[build_words_index] %s\n", msg);
    fflush(stderr);
    exit(1);
}

static jh_u32 jh_read_u32_le(const jh_u8 *p) {
    return (jh_u32)p[0]
        | ((jh_u32)p[1] << 8)
        | ((jh_u32)p[2] << 16)
        | ((jh_u32)p[3] << 24);
}

static void jh_build_words_index(const char *occ_path, const char *postings_path, const char *out_path) {
    FILE *occ_fp = fopen(occ_path, "rb");
    FILE *out_fp;
    FILE *pf;
    jh_postings_file_header ph;
    jh_word_dict_header wh;
    jh_occurrence_record occ;
    int have_occ = 0;
    int have_word = 0;
    jh_u64 current_word_hash = 0;
    jh_u64 postings_count = 0;
    jh_u32 doc_count = 0;
    jh_u32 prev_page_id = 0;
    int have_doc = 0;
    jh_u64 base_offset;
    jh_u64 current_offset = 0;
    jh_u64 entry_count = 0;
    jh_u8 len_buf[4];
    jh_u32 block_size = 0;

    if (jh_read_postings_file_header(postings_path, &ph) != 0) {
        jh_die_words("jh_read_postings_file_header failed");
    }

    base_offset = ph.blocks_data_offset;

    if (!occ_fp) {
        jh_die_words("open occurrences.sorted.tmp failed");
    }

    pf = fopen(postings_path, "rb");
    if (!pf) {
        fclose(occ_fp);
        jh_die_words("open postings.bin failed");
    }
    if (fseek(pf, (long)base_offset, SEEK_SET) != 0) {
        fclose(occ_fp);
        fclose(pf);
        jh_die_words("seek postings.bin to data offset failed");
    }
    current_offset = base_offset;

    out_fp = fopen(out_path, "wb+");
    if (!out_fp) {
        fclose(occ_fp);
        jh_die_words("open words.idx failed");
    }

    memset(&wh, 0, sizeof(wh));
    memcpy(wh.magic, "WDIX", 4);
    wh.version = 1;
    wh.reserved = 0;
    wh.entry_count = 0;

    if (fwrite(&wh, 1, sizeof(wh), out_fp) != sizeof(wh)) {
        fclose(occ_fp);
        fclose(pf);
        fclose(out_fp);
        jh_die_words("write words.idx header failed");
    }

    while (1) {
        if (!have_occ) {
            size_t n = fread(&occ, sizeof(jh_occurrence_record), 1, occ_fp);
            if (n != 1) {
                break;
            }
            have_occ = 1;
        }

        if (!have_word) {
            current_word_hash = occ.word_hash;
            postings_count = 0;
            doc_count = 0;
            have_doc = 0;
            prev_page_id = 0;
            have_word = 1;
        }

        if (occ.word_hash != current_word_hash) {
            jh_word_dict_entry e;
            e.word_hash = current_word_hash;
            e.postings_offset = current_offset;
            e.postings_count = postings_count;
            if (fwrite(&e, 1, sizeof(e), out_fp) != sizeof(e)) {
                fclose(occ_fp);
                fclose(pf);
                fclose(out_fp);
                jh_die_words("write words.idx entry failed");
            }
            entry_count += 1;
            if (fread(len_buf, 1, 4, pf) != 4) {
                fclose(occ_fp);
                fclose(pf);
                fclose(out_fp);
                jh_die_words("read postings block length failed");
            }
            block_size = jh_read_u32_le(len_buf);
            current_offset += 4 + (jh_u64)block_size;
            if (fseek(pf, (long)block_size, SEEK_CUR) != 0) {
                fclose(occ_fp);
                fclose(pf);
                fclose(out_fp);
                jh_die_words("seek postings block data failed");
            }
            have_word = 0;
            have_doc = 0;
            continue;
        }

        if (!have_doc || occ.page_id != prev_page_id) {
            doc_count += 1;
            prev_page_id = occ.page_id;
            have_doc = 1;
        }

        postings_count += 1;
        have_occ = 0;
    }

    if (have_word) {
        jh_word_dict_entry e;
        e.word_hash = current_word_hash;
        e.postings_offset = current_offset;
        e.postings_count = postings_count;
        if (fwrite(&e, 1, sizeof(e), out_fp) != sizeof(e)) {
            fclose(occ_fp);
            fclose(pf);
            fclose(out_fp);
            jh_die_words("write words.idx entry failed (final word)");
        }
        entry_count += 1;
        if (fread(len_buf, 1, 4, pf) != 4) {
            fclose(occ_fp);
            fclose(pf);
            fclose(out_fp);
            jh_die_words("read final postings block length failed");
        }
        block_size = jh_read_u32_le(len_buf);
        current_offset += 4 + (jh_u64)block_size;
        if (fseek(pf, (long)block_size, SEEK_CUR) != 0) {
            fclose(occ_fp);
            fclose(pf);
            fclose(out_fp);
            jh_die_words("seek final postings block data failed");
        }
    }

    fclose(occ_fp);
    fclose(pf);

    if (fseek(out_fp, 0, SEEK_SET) != 0) {
        fclose(out_fp);
        jh_die_words("seek words.idx header failed");
    }

    memset(&wh, 0, sizeof(wh));
    memcpy(wh.magic, "WDIX", 4);
    wh.version = 1;
    wh.reserved = 0;
    wh.entry_count = entry_count;

    if (fwrite(&wh, 1, sizeof(wh), out_fp) != sizeof(wh)) {
        fclose(out_fp);
        jh_die_words("rewrite words.idx header failed");
    }

    fclose(out_fp);
}

int main(int argc, char **argv) {
    const char *occ_path = "occurrences.sorted.tmp";
    const char *postings_path = "postings.bin";
    const char *out_path = "words.idx";
    if (argc > 1) {
        occ_path = argv[1];
    }
    if (argc > 2) {
        postings_path = argv[2];
    }
    if (argc > 3) {
        out_path = argv[3];
    }
    jh_build_words_index(occ_path, postings_path, out_path);
    return 0;
}
