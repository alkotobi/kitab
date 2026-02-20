#include "jamharah/index_format.h"
#include "jamharah/tokenize_arabic.h"
#include "jamharah/hash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void jh_die_occ(const char *msg) {
    fprintf(stderr, "[build_occurrences] %s\n", msg);
    fflush(stderr);
    exit(1);
}

static void jh_build_occurrences(const char *books_path, const char *pages_idx_path, const char *out_path) {
    jh_books_file_header books_hdr;
    FILE *books_fp;
    jh_block_index_entry *blocks;
    jh_pages_index_header pages_hdr;
    FILE *pages_fp;
    jh_page_index_entry *pages;
    FILE *out_fp;
    jh_token *tokens = NULL;
    size_t tokens_cap = 0;
    char *page_buf = NULL;
    size_t page_cap = 0;
    char *workspace = NULL;
    size_t workspace_cap = 0;
    jh_u32 i;

    if (jh_read_books_file_header(books_path, &books_hdr) != 0) {
        jh_die_occ("jh_read_books_file_header failed");
    }
    books_fp = fopen(books_path, "rb");
    if (!books_fp) {
        jh_die_occ("open books.bin failed");
    }
    if (fseek(books_fp, (long)books_hdr.index_offset, SEEK_SET) != 0) {
        fclose(books_fp);
        jh_die_occ("seek books.bin index failed");
    }
    if (books_hdr.block_count == 0) {
        fclose(books_fp);
        jh_die_occ("books.bin has zero blocks");
    }
    if (books_hdr.block_count > (jh_u64)(SIZE_MAX / sizeof(jh_block_index_entry))) {
        fclose(books_fp);
        jh_die_occ("block_count too large");
    }
    blocks = (jh_block_index_entry *)malloc(sizeof(jh_block_index_entry) * (size_t)books_hdr.block_count);
    if (!blocks) {
        fclose(books_fp);
        jh_die_occ("alloc blocks failed");
    }
    if (fread(blocks, sizeof(jh_block_index_entry), (size_t)books_hdr.block_count, books_fp) != books_hdr.block_count) {
        free(blocks);
        fclose(books_fp);
        jh_die_occ("read blocks index failed");
    }

    if (jh_read_pages_index_header(pages_idx_path, &pages_hdr) != 0) {
        free(blocks);
        fclose(books_fp);
        jh_die_occ("jh_read_pages_index_header failed");
    }
    pages_fp = fopen(pages_idx_path, "rb");
    if (!pages_fp) {
        free(blocks);
        fclose(books_fp);
        jh_die_occ("open pages.idx failed");
    }
    if (fseek(pages_fp, (long)sizeof(jh_pages_index_header), SEEK_SET) != 0) {
        fclose(pages_fp);
        free(blocks);
        fclose(books_fp);
        jh_die_occ("seek pages.idx failed");
    }
    if (pages_hdr.page_count == 0) {
        fclose(pages_fp);
        free(blocks);
        fclose(books_fp);
        jh_die_occ("pages.idx has zero pages");
    }
    if (pages_hdr.page_count > (jh_u32)(SIZE_MAX / sizeof(jh_page_index_entry))) {
        fclose(pages_fp);
        free(blocks);
        fclose(books_fp);
        jh_die_occ("page_count too large");
    }
    pages = (jh_page_index_entry *)malloc(sizeof(jh_page_index_entry) * (size_t)pages_hdr.page_count);
    if (!pages) {
        fclose(pages_fp);
        free(blocks);
        fclose(books_fp);
        jh_die_occ("alloc pages failed");
    }
    if (fread(pages, sizeof(jh_page_index_entry), (size_t)pages_hdr.page_count, pages_fp) != pages_hdr.page_count) {
        free(pages);
        fclose(pages_fp);
        free(blocks);
        fclose(books_fp);
        jh_die_occ("read pages entries failed");
    }
    fclose(pages_fp);

    out_fp = fopen(out_path, "wb");
    if (!out_fp) {
        free(pages);
        free(blocks);
        fclose(books_fp);
        jh_die_occ("open occurrences.tmp failed");
    }

    for (i = 0; i < pages_hdr.page_count; ++i) {
        jh_page_index_entry *pe = &pages[i];
        jh_block_index_entry *blk;
        jh_u64 file_offset;
        size_t len;
        size_t nread;
        size_t tok_count;
        size_t t;

        if (pe->block_id >= books_hdr.block_count) {
            free(page_buf);
            free(workspace);
            fclose(out_fp);
            free(pages);
            free(blocks);
            fclose(books_fp);
            jh_die_occ("page block_id out of range");
        }
        if (pe->length == 0) {
            continue;
        }

        len = pe->length;
        if (len > page_cap) {
            char *nb = (char *)realloc(page_buf, len);
            if (!nb) {
                free(page_buf);
                free(workspace);
                fclose(out_fp);
                free(pages);
                free(blocks);
                fclose(books_fp);
                jh_die_occ("alloc page buffer failed");
            }
            page_buf = nb;
            page_cap = len;
        }
        if (len > tokens_cap) {
            size_t new_cap = len;
            jh_token *nt = (jh_token *)realloc(tokens, sizeof(jh_token) * new_cap);
            if (!nt) {
                free(tokens);
                free(page_buf);
                free(workspace);
                fclose(out_fp);
                free(pages);
                free(blocks);
                fclose(books_fp);
                jh_die_occ("alloc tokens failed");
            }
            tokens = nt;
            tokens_cap = new_cap;
        }
        if (len > workspace_cap) {
            char *nw = (char *)realloc(workspace, len);
            if (!nw) {
                free(page_buf);
                free(workspace);
                fclose(out_fp);
                free(pages);
                free(blocks);
                fclose(books_fp);
                jh_die_occ("alloc workspace failed");
            }
            workspace = nw;
            workspace_cap = len;
        }

        blk = &blocks[pe->block_id];
        file_offset = blk->compressed_offset + (jh_u64)pe->offset_in_block;
        if (fseek(books_fp, (long)file_offset, SEEK_SET) != 0) {
            free(page_buf);
            free(workspace);
            fclose(out_fp);
            free(pages);
            free(blocks);
            fclose(books_fp);
            jh_die_occ("seek page text failed");
        }
        nread = fread(page_buf, 1, len, books_fp);
        if (nread != len) {
            free(page_buf);
            free(workspace);
            fclose(out_fp);
            free(pages);
            free(blocks);
            fclose(books_fp);
            jh_die_occ("read page text failed");
        }

        tok_count = jh_normalize_and_tokenize_arabic_utf8(page_buf, len,
                                                          tokens, tokens_cap,
                                                          workspace, workspace_cap);
        if (tok_count == (size_t)-1) {
            free(tokens);
            free(page_buf);
            free(workspace);
            fclose(out_fp);
            free(pages);
            free(blocks);
            fclose(books_fp);
            jh_die_occ("tokenization failed");
        }

        for (t = 0; t < tok_count; ++t) {
            jh_occurrence_record rec;
            rec.word_hash = jh_hash_utf8_64(tokens[t].word, tokens[t].length, 0);
            rec.page_id = pe->page_id;
            rec.position = tokens[t].position;
            if (fwrite(&rec, 1, sizeof(rec), out_fp) != sizeof(rec)) {
                free(tokens);
                free(page_buf);
                free(workspace);
                fclose(out_fp);
                free(pages);
                free(blocks);
                fclose(books_fp);
                jh_die_occ("write occurrence failed");
            }
        }
    }

    free(tokens);
    free(page_buf);
    free(workspace);
    fclose(out_fp);
    free(pages);
    free(blocks);
    fclose(books_fp);
}

int main(int argc, char **argv) {
    const char *books_path = "books.bin";
    const char *pages_idx_path = "pages.idx";
    const char *out_path = "occurrences.tmp";
    if (argc > 1) {
        books_path = argv[1];
    }
    if (argc > 2) {
        pages_idx_path = argv[2];
    }
    if (argc > 3) {
        out_path = argv[3];
    }
    jh_build_occurrences(books_path, pages_idx_path, out_path);
    return 0;
}
