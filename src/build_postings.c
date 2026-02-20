#include "jamharah/index_format.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef JH_HAVE_ZSTD
#include <zstd.h>
#endif

static void jh_die_post(const char *msg) {
    fprintf(stderr, "[build_postings] %s\n", msg);
    fflush(stderr);
    exit(1);
}

static void jh_write_u32_le(jh_u8 *p, jh_u32 v) {
    p[0] = (jh_u8)(v & 0xffu);
    p[1] = (jh_u8)((v >> 8) & 0xffu);
    p[2] = (jh_u8)((v >> 16) & 0xffu);
    p[3] = (jh_u8)((v >> 24) & 0xffu);
}

static void ensure_cap(jh_u8 **buf, size_t *cap, size_t need) {
    if (need <= *cap) {
        return;
    }
    size_t nc = *cap ? *cap * 2 : 1024;
    while (nc < need) {
        nc *= 2;
    }
    jh_u8 *nb = (jh_u8 *)realloc(*buf, nc);
    if (!nb) {
        free(*buf);
        jh_die_post("out of memory in ensure_cap");
    }
    *buf = nb;
    *cap = nc;
}

static void jh_build_postings(const char *occ_path, const char *out_path) {
    FILE *occ_fp = fopen(occ_path, "rb");
    FILE *out_fp;
    jh_postings_file_header hdr;
    jh_occurrence_record occ;
    int have_occ = 0;
    int have_word = 0;
    jh_u64 current_word_hash = 0;
    jh_u64 total_postings = 0;
    jh_u8 *wbuf = NULL;
    size_t wcap = 0;
    size_t wlen = 0;
    size_t doc_count_offset = 0;
    jh_u32 doc_count = 0;
    jh_u32 last_page_id = 0;
    jh_u32 current_page_id = 0;
    jh_u32 term_freq = 0;
    size_t term_freq_offset = 0;
    jh_u32 last_position = 0;
    int have_doc = 0;
    jh_u8 *cbuf = NULL;
    size_t ccap = 0;
    int used_zstd = 0;

    if (!occ_fp) {
        jh_die_post("open occurrences file failed");
    }

    out_fp = fopen(out_path, "wb+");
    if (!out_fp) {
        fclose(occ_fp);
        jh_die_post("open postings.bin failed");
    }

    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, "PSTB", 4);
    hdr.version = 1;
    hdr.flags = 0;
    hdr.total_postings = 0;
    hdr.block_count = 0;
    hdr.block_index_offset = 0;
    hdr.blocks_data_offset = (jh_u64)sizeof(jh_postings_file_header);

    if (fwrite(&hdr, 1, sizeof(hdr), out_fp) != sizeof(hdr)) {
        fclose(occ_fp);
        fclose(out_fp);
        jh_die_post("write postings header failed");
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
            wlen = 0;
            ensure_cap(&wbuf, &wcap, 4);
            doc_count_offset = 0;
            jh_write_u32_le(wbuf + doc_count_offset, 0);
            wlen = 4;
            doc_count = 0;
            last_page_id = 0;
            have_doc = 0;
            have_word = 1;
        }

        if (occ.word_hash != current_word_hash) {
            if (have_doc) {
                jh_write_u32_le(wbuf + term_freq_offset, term_freq);
            }
            jh_write_u32_le(wbuf + doc_count_offset, doc_count);
            if (wlen > 0) {
                jh_u8 len_hdr[4];
                size_t csize;
#ifdef JH_HAVE_ZSTD
                size_t bound = ZSTD_compressBound(wlen);
                if (bound > ccap) {
                    jh_u8 *nb = (jh_u8 *)realloc(cbuf, bound);
                    if (!nb) {
                        free(wbuf);
                        free(cbuf);
                        fclose(occ_fp);
                        fclose(out_fp);
                        jh_die_post("out of memory in compress bound realloc");
                    }
                    cbuf = nb;
                    ccap = bound;
                }
                csize = ZSTD_compress(cbuf, ccap, wbuf, wlen, 3);
                if (ZSTD_isError(csize)) {
                    free(wbuf);
                    free(cbuf);
                    fclose(occ_fp);
                    fclose(out_fp);
                    jh_die_post("ZSTD_compress failed");
                }
                used_zstd = 1;
#else
                if (wlen > ccap) {
                    jh_u8 *nb = (jh_u8 *)realloc(cbuf, wlen);
                    if (!nb) {
                        free(wbuf);
                        free(cbuf);
                        fclose(occ_fp);
                        fclose(out_fp);
                        jh_die_post("out of memory in uncompressed buffer realloc");
                    }
                    cbuf = nb;
                    ccap = wlen;
                }
                memcpy(cbuf, wbuf, wlen);
                csize = wlen;
#endif
                jh_write_u32_le(len_hdr, (jh_u32)csize);
                if (fwrite(len_hdr, 1, 4, out_fp) != 4) {
                    free(wbuf);
                    free(cbuf);
                    fclose(occ_fp);
                    fclose(out_fp);
                    jh_die_post("write postings block length failed");
                }
                if (fwrite(cbuf, 1, csize, out_fp) != csize) {
                    free(wbuf);
                    free(cbuf);
                    fclose(occ_fp);
                    fclose(out_fp);
                    jh_die_post("write postings block data failed");
                }
            }
            have_word = 0;
            have_doc = 0;
            continue;
        }

        if (!have_doc || occ.page_id != current_page_id) {
            if (have_doc) {
                jh_write_u32_le(wbuf + term_freq_offset, term_freq);
            }
            doc_count += 1;
            current_page_id = occ.page_id;
            jh_u32 doc_delta = have_doc ? (current_page_id - last_page_id) : current_page_id - last_page_id;
            ensure_cap(&wbuf, &wcap, wlen + 8);
            jh_write_u32_le(wbuf + wlen, doc_delta);
            wlen += 4;
            term_freq_offset = wlen;
            jh_write_u32_le(wbuf + wlen, 0);
            wlen += 4;
            term_freq = 0;
            last_position = 0;
            have_doc = 1;
            last_page_id = current_page_id;
        }

        {
            jh_u32 delta_pos = occ.position - last_position;
            ensure_cap(&wbuf, &wcap, wlen + 4);
            jh_write_u32_le(wbuf + wlen, delta_pos);
            wlen += 4;
            last_position = occ.position;
            term_freq += 1;
            total_postings += 1;
        }

        have_occ = 0;
    }

    if (have_word) {
        if (have_doc) {
            jh_write_u32_le(wbuf + term_freq_offset, term_freq);
        }
        jh_write_u32_le(wbuf + doc_count_offset, doc_count);
        if (wlen > 0) {
            jh_u8 len_hdr[4];
            size_t csize;
#ifdef JH_HAVE_ZSTD
            size_t bound = ZSTD_compressBound(wlen);
            if (bound > ccap) {
                jh_u8 *nb = (jh_u8 *)realloc(cbuf, bound);
                if (!nb) {
                    free(wbuf);
                    free(cbuf);
                    fclose(occ_fp);
                    fclose(out_fp);
                    jh_die_post("out of memory in compress bound realloc (final)");
                }
                cbuf = nb;
                ccap = bound;
            }
            csize = ZSTD_compress(cbuf, ccap, wbuf, wlen, 3);
            if (ZSTD_isError(csize)) {
                free(wbuf);
                free(cbuf);
                fclose(occ_fp);
                fclose(out_fp);
                jh_die_post("ZSTD_compress failed (final)");
            }
            used_zstd = 1;
#else
            if (wlen > ccap) {
                jh_u8 *nb = (jh_u8 *)realloc(cbuf, wlen);
                if (!nb) {
                    free(wbuf);
                    free(cbuf);
                    fclose(occ_fp);
                    fclose(out_fp);
                    jh_die_post("out of memory in uncompressed buffer realloc (final)");
                }
                cbuf = nb;
                ccap = wlen;
            }
            memcpy(cbuf, wbuf, wlen);
            csize = wlen;
#endif
            jh_write_u32_le(len_hdr, (jh_u32)csize);
            if (fwrite(len_hdr, 1, 4, out_fp) != 4) {
                free(wbuf);
                free(cbuf);
                fclose(occ_fp);
                fclose(out_fp);
                jh_die_post("write postings block length failed (final)");
            }
            if (fwrite(cbuf, 1, csize, out_fp) != csize) {
                free(wbuf);
                free(cbuf);
                fclose(occ_fp);
                fclose(out_fp);
                jh_die_post("write postings block data failed (final)");
            }
        }
    }

    free(wbuf);
    free(cbuf);
    fclose(occ_fp);

    if (fseek(out_fp, 0, SEEK_SET) != 0) {
        fclose(out_fp);
        jh_die_post("seek postings header failed");
    }

    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, "PSTB", 4);
    hdr.version = 1;
    hdr.flags = used_zstd ? 1u : 0u;
    hdr.total_postings = total_postings;
    hdr.block_count = 0;
    hdr.block_index_offset = 0;
    hdr.blocks_data_offset = (jh_u64)sizeof(jh_postings_file_header);

    if (fwrite(&hdr, 1, sizeof(hdr), out_fp) != sizeof(hdr)) {
        fclose(out_fp);
        jh_die_post("rewrite postings header failed");
    }

    fclose(out_fp);
}

int main(int argc, char **argv) {
    const char *occ_path = "occurrences.sorted.tmp";
    const char *out_path = "postings.bin";
    if (argc > 1) {
        occ_path = argv[1];
    }
    if (argc > 2) {
        out_path = argv[2];
    }
    jh_build_postings(occ_path, out_path);
    return 0;
}
