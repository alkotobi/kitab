#include "jamharah/index_format.h"
#include "jamharah/tokenize_arabic.h"
#include "jamharah/hash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

typedef struct {
    jh_u64 hash1;
    jh_u64 hash2;
    int used;
} jh_vocab_entry;

typedef struct {
    const jh_books_file_header *books_hdr;
    const jh_block_index_entry *blocks;
    const jh_pages_index_header *pages_hdr;
    const jh_page_index_entry *pages;
    const char *books_path;
    FILE *out_fp;
    pthread_mutex_t *out_mutex;
    jh_vocab_entry *vocab;
    size_t vocab_cap;
    pthread_mutex_t *vocab_mutex;
    jh_u32 start_page;
    jh_u32 end_page;
} jh_occ_worker_ctx;

static void jh_die_occ(const char *msg) {
    fprintf(stderr, "[build_occurrences] %s\n", msg);
    fflush(stderr);
    exit(1);
}

static unsigned int jh_detect_thread_count(jh_u32 page_count) {
    const char *env = getenv("JH_OCC_THREADS");
    unsigned long v;
    unsigned int tc;
    long ncpu;
    if (page_count == 0) {
        return 0;
    }
    if (env && *env) {
        char *endp = NULL;
        v = strtoul(env, &endp, 10);
        if (endp && *endp == 0 && v > 0) {
            if (v > (unsigned long)page_count) {
                v = (unsigned long)page_count;
            }
            if (v > 32UL) {
                v = 32UL;
            }
            return (unsigned int)v;
        }
    }
    ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    if (ncpu <= 0) {
        tc = 4;
    } else if (ncpu > 32) {
        tc = 32;
    } else {
        tc = (unsigned int)ncpu;
    }
    if (tc > page_count) {
        tc = (unsigned int)page_count;
    }
    if (tc == 0) {
        tc = 1;
    }
    return tc;
}

static void *jh_occ_worker(void *arg) {
    jh_occ_worker_ctx *ctx = (jh_occ_worker_ctx *)arg;
    FILE *books_fp;
    jh_token *tokens = NULL;
    size_t tokens_cap = 0;
    char *page_buf = NULL;
    size_t page_cap = 0;
    char *workspace = NULL;
    size_t workspace_cap = 0;
    jh_u32 i;

    books_fp = fopen(ctx->books_path, "rb");
    if (!books_fp) {
        jh_die_occ("open books.bin failed in worker");
    }

    for (i = ctx->start_page; i < ctx->end_page; ++i) {
        jh_page_index_entry *pe = &ctx->pages[i];
        jh_block_index_entry *blk;
        jh_u64 file_offset;
        size_t len;
        size_t nread;
        size_t tok_count;
        size_t t;

        if (pe->block_id >= ctx->books_hdr->block_count) {
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
                fclose(books_fp);
                jh_die_occ("alloc tokens failed");
            }
            tokens = nt;
            tokens_cap = new_cap;
        }
        if (len > workspace_cap) {
            char *nw = (char *)realloc(workspace, len);
            if (!nw) {
                fclose(books_fp);
                jh_die_occ("alloc workspace failed");
            }
            workspace = nw;
            workspace_cap = len;
        }

        blk = (jh_block_index_entry *)&ctx->blocks[pe->block_id];
        file_offset = blk->compressed_offset + (jh_u64)pe->offset_in_block;
        if (fseek(books_fp, (long)file_offset, SEEK_SET) != 0) {
            fclose(books_fp);
            jh_die_occ("seek page text failed");
        }
        nread = fread(page_buf, 1, len, books_fp);
        if (nread != len) {
            fclose(books_fp);
            jh_die_occ("read page text failed");
        }

        tok_count = jh_normalize_and_tokenize_arabic_utf8(page_buf, len,
                                                          tokens, tokens_cap,
                                                          workspace, workspace_cap);
        if (tok_count == (size_t)-1) {
            fclose(books_fp);
            jh_die_occ("tokenization failed");
        }

        for (t = 0; t < tok_count; ++t) {
            jh_occurrence_record rec;
            jh_u64 h1 = jh_hash_utf8_64(tokens[t].word, tokens[t].length, 0);
            jh_u64 h2 = jh_hash_utf8_64(tokens[t].word, tokens[t].length, (jh_u64)0x9e3779b97f4a7c15ULL);
            size_t idx = (size_t)(h1 & (jh_u64)(ctx->vocab_cap - 1));
            size_t probed = 0;

            pthread_mutex_lock(ctx->vocab_mutex);
            for (;;) {
                jh_vocab_entry *ve = &ctx->vocab[idx];
                if (!ve->used) {
                    ve->hash1 = h1;
                    ve->hash2 = h2;
                    ve->used = 1;
                    break;
                }
                if (ve->hash1 == h1) {
                    if (ve->hash2 == h2) {
                        break;
                    }
                    pthread_mutex_unlock(ctx->vocab_mutex);
                    fclose(books_fp);
                    jh_die_occ("hash collision detected");
                }
                idx = (idx + 1) & (ctx->vocab_cap - 1);
                probed += 1;
                if (probed >= ctx->vocab_cap) {
                    pthread_mutex_unlock(ctx->vocab_mutex);
                    fclose(books_fp);
                    jh_die_occ("vocab table full");
                }
            }
            pthread_mutex_unlock(ctx->vocab_mutex);

            rec.word_hash = h1;
            rec.page_id = pe->page_id;
            rec.position = tokens[t].position;

            pthread_mutex_lock(ctx->out_mutex);
            if (fwrite(&rec, 1, sizeof(rec), ctx->out_fp) != sizeof(rec)) {
                pthread_mutex_unlock(ctx->out_mutex);
                fclose(books_fp);
                jh_die_occ("write occurrence failed");
            }
            pthread_mutex_unlock(ctx->out_mutex);
        }
    }

    free(tokens);
    free(page_buf);
    free(workspace);
    fclose(books_fp);
    return NULL;
}

static void jh_build_occurrences(const char *books_path, const char *pages_idx_path, const char *out_path) {
    jh_books_file_header books_hdr;
    FILE *books_fp;
    jh_block_index_entry *blocks;
    jh_pages_index_header pages_hdr;
    FILE *pages_fp;
    jh_page_index_entry *pages;
    FILE *out_fp;
    jh_vocab_entry *vocab = NULL;
    size_t vocab_cap = 0;
    pthread_mutex_t out_mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t vocab_mutex = PTHREAD_MUTEX_INITIALIZER;
    unsigned int thread_count;
    pthread_t *threads = NULL;
    jh_occ_worker_ctx *ctxs = NULL;
    unsigned int ti;

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

    vocab_cap = 1u << 20;
    vocab = (jh_vocab_entry *)calloc(vocab_cap, sizeof(jh_vocab_entry));
    if (!vocab) {
        fclose(out_fp);
        free(pages);
        free(blocks);
        fclose(books_fp);
        jh_die_occ("alloc vocab failed");
    }

    thread_count = jh_detect_thread_count(pages_hdr.page_count);
    if (thread_count == 0) {
        free(vocab);
        fclose(out_fp);
        free(pages);
        free(blocks);
        fclose(books_fp);
        jh_die_occ("no pages to index");
    }

    threads = (pthread_t *)malloc(sizeof(pthread_t) * thread_count);
    ctxs = (jh_occ_worker_ctx *)malloc(sizeof(jh_occ_worker_ctx) * thread_count);
    if (!threads || !ctxs) {
        free(threads);
        free(ctxs);
        free(vocab);
        fclose(out_fp);
        free(pages);
        free(blocks);
        fclose(books_fp);
        jh_die_occ("alloc threads failed");
    }

    for (ti = 0; ti < thread_count; ++ti) {
        jh_u32 start = (jh_u32)((pages_hdr.page_count * ti) / thread_count);
        jh_u32 end = (jh_u32)((pages_hdr.page_count * (ti + 1)) / thread_count);
        ctxs[ti].books_hdr = &books_hdr;
        ctxs[ti].blocks = blocks;
        ctxs[ti].pages_hdr = &pages_hdr;
        ctxs[ti].pages = pages;
        ctxs[ti].books_path = books_path;
        ctxs[ti].out_fp = out_fp;
        ctxs[ti].out_mutex = &out_mutex;
        ctxs[ti].vocab = vocab;
        ctxs[ti].vocab_cap = vocab_cap;
        ctxs[ti].vocab_mutex = &vocab_mutex;
        ctxs[ti].start_page = start;
        ctxs[ti].end_page = end;
        if (pthread_create(&threads[ti], NULL, jh_occ_worker, &ctxs[ti]) != 0) {
            jh_die_occ("pthread_create failed");
        }
    }

    for (ti = 0; ti < thread_count; ++ti) {
        if (pthread_join(threads[ti], NULL) != 0) {
            jh_die_occ("pthread_join failed");
        }
    }

    free(threads);
    free(ctxs);
    free(vocab);
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
