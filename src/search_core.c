#include "jamharah/index_format.h"
#include "jamharah/tokenize_arabic.h"
#include "jamharah/hash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void jh_die_search(const char *msg) {
    fprintf(stderr, "[search_core] %s\n", msg);
    fflush(stderr);
    exit(1);
}

static void jh_search_core_run(const char *words_idx_path, const char *postings_path, const char *query) {
    size_t qlen = strlen(query);
    size_t workspace_cap = qlen ? qlen * 4 : 16;
    char *workspace = (char *)malloc(workspace_cap);
    size_t tokens_cap = qlen ? qlen : 16;
    jh_token *tokens = (jh_token *)malloc(sizeof(jh_token) * tokens_cap);
    size_t tok_count;
    jh_u64 *hashes;
    size_t i;
    jh_postings_list *lists;
    jh_word_dict_entry e;
    jh_ranked_hit *hits = NULL;
    size_t hit_count = 0;
    jh_u32 *phrase_pages = NULL;
    size_t phrase_page_count = 0;

    if (!workspace || !tokens) {
        free(workspace);
        free(tokens);
        jh_die_search("alloc query buffers failed");
    }

    tok_count = jh_normalize_and_tokenize_arabic_utf8(query, qlen, tokens, tokens_cap, workspace, workspace_cap);
    if (tok_count == (size_t)-1) {
        free(workspace);
        free(tokens);
        jh_die_search("query tokenization failed");
    }
    if (tok_count == 0) {
        free(workspace);
        free(tokens);
        printf("no tokens\n");
        return;
    }

    hashes = (jh_u64 *)malloc(sizeof(jh_u64) * tok_count);
    lists = (jh_postings_list *)calloc(tok_count, sizeof(jh_postings_list));
    if (!hashes || !lists) {
        free(workspace);
        free(tokens);
        free(hashes);
        free(lists);
        jh_die_search("alloc hashes or lists failed");
    }

    for (i = 0; i < tok_count; ++i) {
        hashes[i] = jh_hash_utf8_64(tokens[i].word, tokens[i].length, 0);
    }

    if (tok_count >= 2) {
        if (jh_phrase_search(words_idx_path, postings_path, hashes, tok_count, &phrase_pages, &phrase_page_count) != 0) {
            free(workspace);
            free(tokens);
            free(hashes);
            free(lists);
            jh_die_search("phrase_search failed");
        }
    }

    for (i = 0; i < tok_count; ++i) {
        if (jh_word_dict_lookup(words_idx_path, hashes[i], &e) != 0) {
            continue;
        }
        {
            FILE *pf = fopen(postings_path, "rb");
            if (!pf) {
                continue;
            }
            fclose(pf);
            if (e.postings_count == 0) {
                continue;
            }
            if (jh_postings_list_read(postings_path, e.postings_offset, &lists[i]) != 0) {
                continue;
            }
        }
    }

    if (jh_rank_results(lists, tok_count, phrase_pages, phrase_page_count, &hits, &hit_count) != 0) {
        size_t k;
        for (k = 0; k < tok_count; ++k) {
            jh_postings_list_free(&lists[k]);
        }
        free(workspace);
        free(tokens);
        free(hashes);
        free(lists);
        free(phrase_pages);
        jh_die_search("rank_results failed");
    }

    {
        size_t k;
        for (k = 0; k < tok_count; ++k) {
            jh_postings_list_free(&lists[k]);
        }
    }
    free(workspace);
    free(tokens);
    free(hashes);
    free(lists);
    free(phrase_pages);

    if (!hits || hit_count == 0) {
        printf("no results\n");
        free(hits);
        return;
    }

    for (i = 0; i < hit_count; ++i) {
        printf("%u %.6f\n", hits[i].page_id, hits[i].score);
    }
    free(hits);
}

static void jh_search_core_run_multi(const char **words_idx_paths, const char **postings_paths, size_t cat_count, const char *query) {
    size_t qlen = strlen(query);
    size_t workspace_cap = qlen ? qlen * 4 : 16;
    char *workspace = (char *)malloc(workspace_cap);
    size_t tokens_cap = qlen ? qlen : 16;
    jh_token *tokens = (jh_token *)malloc(sizeof(jh_token) * tokens_cap);
    size_t tok_count;
    jh_u64 *hashes;
    size_t i;
    jh_u32 *pages = NULL;
    jh_u32 *cats = NULL;
    size_t count = 0;

    if (!workspace || !tokens) {
        free(workspace);
        free(tokens);
        jh_die_search("alloc query buffers failed");
    }

    tok_count = jh_normalize_and_tokenize_arabic_utf8(query, qlen, tokens, tokens_cap, workspace, workspace_cap);
    if (tok_count == (size_t)-1) {
        free(workspace);
        free(tokens);
        jh_die_search("query tokenization failed");
    }
    if (tok_count == 0) {
        free(workspace);
        free(tokens);
        printf("no tokens\n");
        return;
    }

    hashes = (jh_u64 *)malloc(sizeof(jh_u64) * tok_count);
    if (!hashes) {
        free(workspace);
        free(tokens);
        jh_die_search("alloc hashes failed");
    }

    for (i = 0; i < tok_count; ++i) {
        hashes[i] = jh_hash_utf8_64(tokens[i].word, tokens[i].length, 0);
    }

    if (tok_count < 2) {
        free(workspace);
        free(tokens);
        free(hashes);
        printf("need at least two tokens\n");
        return;
    }

    {
        int rc = jh_phrase_search_multi(words_idx_paths, postings_paths, cat_count, hashes, tok_count, &pages, &cats, &count);
        free(workspace);
        free(tokens);
        free(hashes);
        if (rc != 0) {
            free(pages);
            free(cats);
            jh_die_search("phrase_search_multi failed");
        }
    }

    if (!pages || !cats || count == 0) {
        free(pages);
        free(cats);
        printf("no results\n");
        return;
    }

    for (i = 0; i < count; ++i) {
        printf("%u %u\n", cats[i], pages[i]);
    }

    free(pages);
    free(cats);
}

int main(int argc, char **argv) {
    char buf[4096];

    if (argc <= 2) {
        const char *words_idx_path = "words.idx";
        const char *postings_path = "postings.bin";

        if (argc > 1) {
            words_idx_path = argv[1];
        }
        if (argc > 2) {
            postings_path = argv[2];
        }

        if (!fgets(buf, sizeof(buf), stdin)) {
            return 0;
        }
        {
            size_t len = strlen(buf);
            if (len > 0 && (buf[len - 1] == '\n' || buf[len - 1] == '\r')) {
                buf[len - 1] = 0;
            }
        }
        jh_search_core_run(words_idx_path, postings_path, buf);
        return 0;
    } else {
        int arg_count = argc - 1;
        size_t cat_count;
        const char **words_idx_paths;
        const char **postings_paths;
        size_t i;

        if (arg_count % 2 != 0) {
            fprintf(stderr, "usage: %s [words.idx postings.bin]...\n", argv[0]);
            return 1;
        }

        cat_count = (size_t)(arg_count / 2);
        words_idx_paths = (const char **)malloc(sizeof(char *) * cat_count);
        postings_paths = (const char **)malloc(sizeof(char *) * cat_count);
        if (!words_idx_paths || !postings_paths) {
            free(words_idx_paths);
            free(postings_paths);
            jh_die_search("alloc category path arrays failed");
        }

        for (i = 0; i < cat_count; ++i) {
            words_idx_paths[i] = argv[1 + (int)(i * 2)];
            postings_paths[i] = argv[1 + (int)(i * 2) + 1];
        }

        if (!fgets(buf, sizeof(buf), stdin)) {
            free(words_idx_paths);
            free(postings_paths);
            return 0;
        }
        {
            size_t len = strlen(buf);
            if (len > 0 && (buf[len - 1] == '\n' || buf[len - 1] == '\r')) {
                buf[len - 1] = 0;
            }
        }

        jh_search_core_run_multi(words_idx_paths, postings_paths, cat_count, buf);

        free(words_idx_paths);
        free(postings_paths);
        return 0;
    }
}
