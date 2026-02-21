#include "jamharah/index_format.h"
#include "jamharah/tokenize_arabic.h"
#include "jamharah/hash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void jh_die_snip(const char *msg) {
    fprintf(stderr, "[search_snippets] %s\n", msg);
    fflush(stderr);
    exit(1);
}

static void jh_run_search_and_snippets(const char *books_path,
                                       const char *pages_idx_path,
                                       const char *words_idx_path,
                                       const char *postings_path,
                                       const char *query,
                                       size_t offset,
                                       size_t limit,
                                       int exact_only) {
    size_t qlen = strlen(query);
    size_t workspace_cap = qlen ? qlen * 4 : 16;
    char *workspace = (char *)malloc(workspace_cap);
    size_t tokens_cap = qlen ? qlen : 16;
    jh_token *tokens = (jh_token *)malloc(sizeof(jh_token) * tokens_cap);
    size_t tok_count;
    jh_u64 *hashes;
    size_t term_count = 0;
    size_t i;
    jh_postings_list *lists = NULL;
    jh_word_dict_entry e;
    jh_ranked_hit *hits = NULL;
    size_t hit_count = 0;
    jh_u32 *phrase_pages = NULL;
    size_t phrase_page_count = 0;
    int require_all_terms = 1;
    int has_or_token = 0;

    if (!workspace || !tokens) {
        free(workspace);
        free(tokens);
        jh_die_snip("alloc query buffers failed");
    }

    tok_count = jh_normalize_and_tokenize_arabic_utf8(query, qlen,
                                                      tokens, tokens_cap,
                                                      workspace, workspace_cap);
    if (tok_count == (size_t)-1) {
        free(workspace);
        free(tokens);
        jh_die_snip("query tokenization failed");
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
        jh_die_snip("alloc hashes or lists failed");
    }

    for (i = 0; i < tok_count; ++i) {
        if (tokens[i].length == 2 &&
            tokens[i].word[0] == 'O' &&
            tokens[i].word[1] == 'R') {
            has_or_token = 1;
            continue;
        }
        hashes[term_count] = jh_hash_utf8_64(tokens[i].word, tokens[i].length, 0);
        term_count++;
    }

    if (term_count == 0) {
        free(workspace);
        free(tokens);
        free(hashes);
        free(lists);
        printf("no tokens\n");
        return;
    }

    require_all_terms = has_or_token ? 0 : 1;

    if (term_count >= 2 && !has_or_token) {
        if (jh_phrase_search(words_idx_path, postings_path,
                             hashes, term_count,
                             &phrase_pages, &phrase_page_count) != 0) {
            free(workspace);
            free(tokens);
            free(hashes);
            free(lists);
            jh_die_snip("phrase_search failed");
        }
    }

    for (i = 0; i < term_count; ++i) {
        if (jh_word_dict_lookup(words_idx_path, hashes[i], &e) != 0) {
            continue;
        }
        if (e.postings_count == 0) {
            continue;
        }
        if (jh_postings_list_read(postings_path, e.postings_offset, &lists[i]) != 0) {
            continue;
        }
    }

    if (jh_rank_results(lists, term_count, require_all_terms,
                        phrase_pages, phrase_page_count,
                        &hits, &hit_count) != 0) {
        size_t k;
        for (k = 0; k < term_count; ++k) {
            jh_postings_list_free(&lists[k]);
        }
        free(workspace);
        free(tokens);
        free(hashes);
        free(lists);
        free(phrase_pages);
        jh_die_snip("rank_results failed");
    }

    {
        size_t k;
        for (k = 0; k < term_count; ++k) {
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

    {
        size_t h;
        size_t start_index = 0;
        size_t end_index = hit_count;
        jh_pages_index_header ph;
        jh_page_index_entry *pages = NULL;
        jh_page_index_entry *page_by_id = NULL;
        FILE *pf;

        if (jh_read_pages_index_header(pages_idx_path, &ph) != 0) {
            free(hits);
            jh_die_snip("jh_read_pages_index_header failed");
        }
        pf = fopen(pages_idx_path, "rb");
        if (!pf) {
            free(hits);
            jh_die_snip("open pages.idx failed");
        }
        if (fseek(pf, (long)sizeof(jh_pages_index_header), SEEK_SET) != 0) {
            fclose(pf);
            free(hits);
            jh_die_snip("seek pages.idx failed");
        }
        pages = (jh_page_index_entry *)malloc(sizeof(jh_page_index_entry) * ph.page_count);
        if (!pages) {
            fclose(pf);
            free(hits);
            jh_die_snip("alloc page entries failed");
        }
        if (fread(pages, sizeof(jh_page_index_entry), ph.page_count, pf) != ph.page_count) {
            fclose(pf);
            free(pages);
            free(hits);
            jh_die_snip("read page entries failed");
        }
        fclose(pf);

        page_by_id = (jh_page_index_entry *)malloc(sizeof(jh_page_index_entry) * ph.page_count);
        if (!page_by_id) {
            free(pages);
            free(hits);
            jh_die_snip("alloc page_by_id failed");
        }
        {
            jh_u32 pi;
            for (pi = 0; pi < ph.page_count; ++pi) {
                jh_page_index_entry *e = &pages[pi];
                if (e->page_id >= ph.page_count) {
                    free(page_by_id);
                    free(pages);
                    free(hits);
                    jh_die_snip("page_id out of range");
                }
                page_by_id[e->page_id] = *e;
            }
        }
        free(pages);

        if (offset >= hit_count) {
            free(page_by_id);
            free(hits);
            return;
        }
        start_index = offset;
        if (limit > 0 && start_index + limit < end_index) {
            end_index = start_index + limit;
        }

        for (h = start_index; h < end_index; ++h) {
            jh_u32 page_id = hits[h].page_id;
            double score = hits[h].score;
            char *page_text = NULL;
            jh_u32 page_len = 0;
            size_t qlen2 = strlen(query);
            const char *found = NULL;
            size_t pos = 0;
            size_t head_len;
            size_t tail_len;
            size_t start;
            size_t end;
            jh_u32 book_id = 0;
            jh_u32 page_number = 0;

            if (page_id < ph.page_count && page_by_id) {
                jh_page_index_entry *pe = &page_by_id[page_id];
                book_id = pe->book_id;
                page_number = pe->page_number;
            }

            if (jh_load_page_text(books_path, pages_idx_path,
                                  page_id, &page_text, &page_len) != 0) {
                printf("book %u page %u id %u score %.6f (failed to load text)\n",
                       book_id, page_number, page_id, score);
                continue;
            }

            while (pos + qlen2 <= page_len) {
                if (memcmp(page_text + pos, query, qlen2) == 0) {
                    found = page_text + pos;
                    break;
                }
                pos++;
            }

            if (!found) {
                int boundary_found = 0;
                jh_u32 next_page_id = page_id + 1;
                if (next_page_id < ph.page_count && page_by_id) {
                    jh_page_index_entry *pe1 = &page_by_id[page_id];
                    jh_page_index_entry *pe2 = &page_by_id[next_page_id];
                    if (pe1->book_id == pe2->book_id) {
                        char *next_page_text = NULL;
                        jh_u32 next_page_len = 0;
                        if (jh_load_page_text(books_path, pages_idx_path,
                                              next_page_id, &next_page_text, &next_page_len) == 0) {
                            size_t tail_bytes = page_len > 200 ? 200 : page_len;
                            size_t head_bytes = next_page_len > 200 ? 200 : next_page_len;
                            size_t combo_len = tail_bytes + head_bytes;
                            char *combo = (char *)malloc(combo_len);
                            if (combo) {
                                size_t cpos = 0;
                                memcpy(combo, page_text + (page_len - tail_bytes), tail_bytes);
                                memcpy(combo + tail_bytes, next_page_text, head_bytes);
                                while (cpos + qlen2 <= combo_len) {
                                    if (memcmp(combo + cpos, query, qlen2) == 0) {
                                        const char *f2 = combo + cpos;
                                        size_t start2 = (size_t)(f2 - combo);
                                        size_t head_len2 = start2 > 40 ? 40 : start2;
                                        size_t tail_len2 = (combo_len - (start2 + qlen2)) > 40 ?
                                                           40 : (combo_len - (start2 + qlen2));
                                        printf("book %u pages %u-%u ids %u-%u score %.6f\n",
                                               book_id, pe1->page_number, pe2->page_number,
                                               page_id, next_page_id, score);
                                        printf("  ...%.*s«", (int)head_len2, combo + start2 - head_len2);
                                        printf("%.*s", (int)qlen2, combo + start2);
                                        printf("»%.*s...\n", (int)tail_len2, combo + start2 + qlen2);
                                        boundary_found = 1;
                                        break;
                                    }
                                    cpos++;
                                }
                                free(combo);
                            }
                            free(next_page_text);
                        }
                    }
                }

                if (!boundary_found) {
                    if (!exact_only) {
                        size_t page_snippet_len = page_len > 80 ? 80 : page_len;
                        char *q_ws = NULL;
                        jh_token *q_tokens = NULL;
                        size_t q_ws_cap = qlen2 ? qlen2 * 4 : 16;
                        size_t q_tok_cap = qlen2 ? qlen2 : 16;
                        char *p_ws = NULL;
                        jh_token *p_tokens = NULL;
                        size_t p_ws_cap = page_len ? page_len * 4 : 16;
                        size_t p_tok_cap = page_len ? page_len : 16;
                        size_t q_tok_count = 0;
                        size_t p_tok_count = 0;
                        int root_found = 0;
                        size_t root_start = 0;
                        size_t root_len = 0;
                        size_t norm_len = 0;

                        q_ws = (char *)malloc(q_ws_cap);
                        q_tokens = (jh_token *)malloc(sizeof(jh_token) * q_tok_cap);
                        p_ws = (char *)malloc(p_ws_cap);
                        p_tokens = (jh_token *)malloc(sizeof(jh_token) * p_tok_cap);

                        if (q_ws && q_tokens) {
                            q_tok_count = jh_normalize_and_tokenize_arabic_utf8(query, qlen2,
                                                                                q_tokens, q_tok_cap,
                                                                                q_ws, q_ws_cap);
                        }
                        if (p_ws && p_tokens) {
                            p_tok_count = jh_normalize_and_tokenize_arabic_utf8(page_text, page_len,
                                                                                p_tokens, p_tok_cap,
                                                                                p_ws, p_ws_cap);
                        }

                        if (q_tok_count != (size_t)-1 && p_tok_count != (size_t)-1 &&
                            q_tok_count > 0 && p_tok_count > 0 &&
                            q_ws && p_ws && q_tokens && p_tokens) {
                            size_t ti;
                            size_t qi;
                            norm_len = (size_t)((p_tokens[p_tok_count - 1].word - p_ws) +
                                                p_tokens[p_tok_count - 1].length);
                            for (ti = 0; ti < p_tok_count && !root_found; ++ti) {
                                for (qi = 0; qi < q_tok_count; ++qi) {
                                    if (q_tokens[qi].length == p_tokens[ti].length &&
                                        memcmp(q_tokens[qi].word, p_tokens[ti].word, q_tokens[qi].length) == 0) {
                                        root_found = 1;
                                        root_start = (size_t)(p_tokens[ti].word - p_ws);
                                        root_len = p_tokens[ti].length;
                                        break;
                                    }
                                }
                            }
                        }

                        printf("book %u page %u id %u score %.6f\n",
                               book_id, page_number, page_id, score);

                        if (root_found && norm_len > 0 && root_len > 0 && root_start < norm_len) {
                            size_t head_len_r = root_start > 40 ? 40 : root_start;
                            size_t tail_len_r = (norm_len - (root_start + root_len)) > 40 ?
                                                40 : (norm_len - (root_start + root_len));
                            printf("  ...%.*s«", (int)head_len_r, p_ws + root_start - head_len_r);
                            printf("%.*s", (int)root_len, p_ws + root_start);
                            printf("»%.*s...\n", (int)tail_len_r, p_ws + root_start + root_len);
                        } else {
                            printf("  ...%.*s...\n", (int)page_snippet_len, page_text);
                        }

                        free(q_ws);
                        free(q_tokens);
                        free(p_ws);
                        free(p_tokens);
                    }
                }
                free(page_text);
                continue;
            }

            start = (size_t)(found - page_text);
            head_len = start > 40 ? 40 : start;
            tail_len = (page_len - (start + qlen2)) > 40 ?
                       40 : (page_len - (start + qlen2));
            end = start + qlen2 + tail_len;

            printf("book %u page %u id %u score %.6f\n",
                   book_id, page_number, page_id, score);
            printf("  ...%.*s«", (int)head_len, page_text + start - head_len);
            printf("%.*s", (int)qlen2, page_text + start);
            printf("»%.*s...\n", (int)tail_len, page_text + start + qlen2);

            free(page_text);
        }
        free(page_by_id);
        free(hits);
    }
}

int main(int argc, char **argv) {
    const char *books_path = "books.bin";
    const char *pages_idx_path = "pages.idx";
    const char *words_idx_path = "words.idx";
    const char *postings_path = "postings.bin";
    char buf[4096];
    size_t offset = 0;
    size_t limit = 0;
    int exact_only = 0;
    char *endp;

    if (argc >= 2) {
        books_path = argv[1];
    }
    if (argc >= 3) {
        pages_idx_path = argv[2];
    }
    if (argc >= 4) {
        words_idx_path = argv[3];
    }
    if (argc >= 5) {
        postings_path = argv[4];
    }
    if (argc >= 6) {
        unsigned long long v = strtoull(argv[5], &endp, 10);
        if (endp && *endp == 0) {
            offset = (size_t)v;
        }
    }
    if (argc >= 7) {
        unsigned long long v = strtoull(argv[6], &endp, 10);
        if (endp && *endp == 0) {
            limit = (size_t)v;
        }
    }
    if (argc >= 8) {
        if (strcmp(argv[7], "--exact") == 0) {
            exact_only = 1;
        }
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

    jh_run_search_and_snippets(books_path,
                               pages_idx_path,
                               words_idx_path,
                               postings_path,
                               buf,
                               offset,
                               limit,
                               exact_only);
    return 0;
}
