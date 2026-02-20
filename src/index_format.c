#include "jamharah/index_format.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* jh_read_header loads a fixed-size header from a file and validates its magic tag. */
static int jh_read_header(const char *path, void *header, size_t header_size, const char expected_magic[4]) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        return -1;
    }
    size_t n = fread(header, 1, header_size, f);
    fclose(f);
    if (n != header_size) {
        return -2;
    }
    if (expected_magic) {
        if (memcmp(((char *)header), expected_magic, 4) != 0) {
            return -3;
        }
    }
    return 0;
}

/* jh_read_books_file_header reads and validates the header of books.bin. */
int jh_read_books_file_header(const char *path, jh_books_file_header *out) {
    static const char magic[4] = { 'B', 'K', 'S', 'B' };
    return jh_read_header(path, out, sizeof(jh_books_file_header), magic);
}

/* jh_read_books_index_header reads and validates the header of books.idx. */
int jh_read_books_index_header(const char *path, jh_books_index_header *out) {
    static const char magic[4] = { 'B', 'K', 'I', 'X' };
    return jh_read_header(path, out, sizeof(jh_books_index_header), magic);
}

/* jh_read_pages_index_header reads and validates the header of pages.idx. */
int jh_read_pages_index_header(const char *path, jh_pages_index_header *out) {
    static const char magic[4] = { 'P', 'G', 'I', 'X' };
    return jh_read_header(path, out, sizeof(jh_pages_index_header), magic);
}

/* jh_read_chapters_index_header reads and validates the header of chapters.idx. */
int jh_read_chapters_index_header(const char *path, jh_chapters_index_header *out) {
    static const char magic[4] = { 'C', 'H', 'I', 'X' };
    return jh_read_header(path, out, sizeof(jh_chapters_index_header), magic);
}

/* jh_read_titles_file_header reads and validates the header of titles.bin. */
int jh_read_titles_file_header(const char *path, jh_titles_file_header *out) {
    static const char magic[4] = { 'T', 'T', 'L', 'B' };
    return jh_read_header(path, out, sizeof(jh_titles_file_header), magic);
}

/* jh_read_words_index_header reads and validates the header of words.idx. */
int jh_read_words_index_header(const char *path, jh_words_index_header *out) {
    static const char magic[4] = { 'W', 'D', 'I', 'X' };
    return jh_read_header(path, out, sizeof(jh_words_index_header), magic);
}

/* jh_read_postings_file_header reads and validates the header of postings.bin. */
int jh_read_postings_file_header(const char *path, jh_postings_file_header *out) {
    static const char magic[4] = { 'P', 'S', 'T', 'B' };
    return jh_read_header(path, out, sizeof(jh_postings_file_header), magic);
}

/* jh_read_u32_le decodes a little-endian 32-bit unsigned integer from a byte buffer. */
static jh_u32 jh_read_u32_le(const jh_u8 *p) {
    return (jh_u32)p[0]
         | ((jh_u32)p[1] << 8)
         | ((jh_u32)p[2] << 16)
         | ((jh_u32)p[3] << 24);
}

/* jh_postings_list_parse materializes an in-memory postings list from a compressed buffer. */
int jh_postings_list_parse(const jh_u8 *data, size_t data_size, jh_postings_list *out) {
    jh_u32 doc_count;
    size_t offset;
    jh_u32 i;
    jh_u32 total_positions;
    jh_u32 current_page_id;
    jh_posting_entry *entries;
    jh_u32 *positions_storage;
    jh_u32 *pos_out;

    if (!data || !out) {
        return -1;
    }
    memset(out, 0, sizeof(*out));
    if (data_size < 4) {
        return -2;
    }

    offset = 0;
    doc_count = jh_read_u32_le(data + offset);
    offset += 4;

    total_positions = 0;
    current_page_id = 0;

    for (i = 0; i < doc_count; ++i) {
        jh_u32 doc_delta;
        jh_u32 term_freq;
        jh_u32 j;

        if (data_size - offset < 8) {
            return -2;
        }

        doc_delta = jh_read_u32_le(data + offset);
        offset += 4;
        term_freq = jh_read_u32_le(data + offset);
        offset += 4;

        if (data_size - offset < (size_t)term_freq * 4) {
            return -2;
        }

        total_positions += term_freq;
        offset += (size_t)term_freq * 4;

        current_page_id += doc_delta;
        (void)current_page_id;

        for (j = 0; j < term_freq; ++j) {
        }
    }

    entries = (jh_posting_entry *)malloc(sizeof(jh_posting_entry) * doc_count);
    if (!entries) {
        return -3;
    }
    positions_storage = (jh_u32 *)malloc(sizeof(jh_u32) * total_positions);
    if (!positions_storage) {
        free(entries);
        return -3;
    }

    offset = 4;
    current_page_id = 0;
    pos_out = positions_storage;

    for (i = 0; i < doc_count; ++i) {
        jh_u32 doc_delta;
        jh_u32 term_freq;
        jh_u32 j;
        jh_u32 pos;

        doc_delta = jh_read_u32_le(data + offset);
        offset += 4;
        term_freq = jh_read_u32_le(data + offset);
        offset += 4;

        current_page_id += doc_delta;
        entries[i].page_id = current_page_id;
        entries[i].term_freq = term_freq;
        entries[i].positions = pos_out;

        pos = 0;
        for (j = 0; j < term_freq; ++j) {
            jh_u32 d = jh_read_u32_le(data + offset);
            offset += 4;
            pos += d;
            *pos_out++ = pos;
        }
    }

    out->entries = entries;
    out->entry_count = doc_count;
    out->positions_storage = positions_storage;
    out->positions_count = total_positions;

    return 0;
}

/* jh_postings_list_free releases memory owned by a postings list structure. */
void jh_postings_list_free(jh_postings_list *list) {
    if (!list) {
        return;
    }
    if (list->entries) {
        free(list->entries);
    }
    if (list->positions_storage) {
        free(list->positions_storage);
    }
    memset(list, 0, sizeof(*list));
}

/* jh_postings_list_intersect computes the document-level AND of two postings lists. */
int jh_postings_list_intersect(const jh_postings_list *a, const jh_postings_list *b, jh_postings_list *out) {
    jh_u32 i;
    jh_u32 j;
    jh_u32 k;
    jh_posting_entry *entries;
    jh_u32 max_entries;

    if (!a || !b || !out) {
        return -1;
    }

    memset(out, 0, sizeof(*out));

    if (!a->entries || !b->entries) {
        return 0;
    }

    max_entries = a->entry_count < b->entry_count ? a->entry_count : b->entry_count;
    entries = (jh_posting_entry *)malloc(sizeof(jh_posting_entry) * max_entries);
    if (!entries) {
        return -2;
    }

    i = 0;
    j = 0;
    k = 0;

    while (i < a->entry_count && j < b->entry_count) {
        jh_u32 da = a->entries[i].page_id;
        jh_u32 db = b->entries[j].page_id;
        if (da == db) {
            entries[k].page_id = da;
            entries[k].term_freq = a->entries[i].term_freq + b->entries[j].term_freq;
            entries[k].positions = NULL;
            k += 1;
            i += 1;
            j += 1;
        } else if (da < db) {
            i += 1;
        } else {
            j += 1;
        }
    }

    out->entries = entries;
    out->entry_count = k;
    out->positions_storage = NULL;
    out->positions_count = 0;

    return 0;
}

/* jh_postings_cursor_init prepares a streaming cursor over an encoded postings buffer. */
int jh_postings_cursor_init(jh_postings_cursor *cur, const jh_u8 *data, size_t size) {
    if (!cur || !data) {
        return -1;
    }
    if (size < 4) {
        return -2;
    }
    cur->data = data;
    cur->size = size;
    cur->offset = 4;
    cur->doc_count = jh_read_u32_le(data);
    cur->index = 0;
    cur->current_page_id = 0;
    return 0;
}

/* jh_postings_cursor_next decodes the next posting into caller-provided buffers. */
int jh_postings_cursor_next(jh_postings_cursor *cur, jh_posting_entry *out, jh_u32 *pos_buf, jh_u32 pos_buf_cap) {
    jh_u32 doc_delta;
    jh_u32 term_freq;
    jh_u32 j;
    jh_u32 pos;

    if (!cur || !out || !pos_buf) {
        return -1;
    }

    if (cur->index >= cur->doc_count) {
        return 1;
    }

    if (cur->size - cur->offset < 8) {
        return -2;
    }

    doc_delta = jh_read_u32_le(cur->data + cur->offset);
    cur->offset += 4;
    term_freq = jh_read_u32_le(cur->data + cur->offset);
    cur->offset += 4;

    if (term_freq > pos_buf_cap) {
        return -3;
    }
    if (cur->size - cur->offset < (size_t)term_freq * 4) {
        return -2;
    }

    cur->current_page_id += doc_delta;
    out->page_id = cur->current_page_id;
    out->term_freq = term_freq;
    out->positions = pos_buf;

    pos = 0;
    for (j = 0; j < term_freq; ++j) {
        jh_u32 d = jh_read_u32_le(cur->data + cur->offset);
        cur->offset += 4;
        pos += d;
        pos_buf[j] = pos;
    }

    cur->index += 1;

    return 0;
}

/* jh_postings_and_cursor_init creates a streaming AND view over two postings cursors. */
int jh_postings_and_cursor_init(jh_postings_and_cursor *ac, jh_postings_cursor *a, jh_postings_cursor *b, jh_u32 *buf_a, jh_u32 cap_a, jh_u32 *buf_b, jh_u32 cap_b) {
    int rc;

    if (!ac || !a || !b || !buf_a || !buf_b) {
        return -1;
    }

    ac->a = a;
    ac->b = b;
    ac->buf_a = buf_a;
    ac->cap_a = cap_a;
    ac->buf_b = buf_b;
    ac->cap_b = cap_b;
    ac->a_valid = 0;
    ac->b_valid = 0;

    rc = jh_postings_cursor_next(a, &ac->cur_a, buf_a, cap_a);
    if (rc == 0) {
        ac->a_valid = 1;
    } else if (rc == 1) {
        ac->a_valid = 0;
    } else {
        return rc;
    }

    rc = jh_postings_cursor_next(b, &ac->cur_b, buf_b, cap_b);
    if (rc == 0) {
        ac->b_valid = 1;
    } else if (rc == 1) {
        ac->b_valid = 0;
    } else {
        return rc;
    }

    return 0;
}

/* jh_postings_and_cursor_next advances to the next document present in both inputs. */
int jh_postings_and_cursor_next(jh_postings_and_cursor *ac, jh_posting_entry *out) {
    if (!ac || !out) {
        return -1;
    }

    while (ac->a_valid && ac->b_valid) {
        jh_u32 da = ac->cur_a.page_id;
        jh_u32 db = ac->cur_b.page_id;

        if (da == db) {
            out->page_id = da;
            out->term_freq = ac->cur_a.term_freq + ac->cur_b.term_freq;
            out->positions = NULL;

            int rc_a = jh_postings_cursor_next(ac->a, &ac->cur_a, ac->buf_a, ac->cap_a);
            if (rc_a == 0) {
                ac->a_valid = 1;
            } else if (rc_a == 1) {
                ac->a_valid = 0;
            } else {
                return rc_a;
            }

            int rc_b = jh_postings_cursor_next(ac->b, &ac->cur_b, ac->buf_b, ac->cap_b);
            if (rc_b == 0) {
                ac->b_valid = 1;
            } else if (rc_b == 1) {
                ac->b_valid = 0;
            } else {
                return rc_b;
            }

            return 0;
        } else if (da < db) {
            int rc_a = jh_postings_cursor_next(ac->a, &ac->cur_a, ac->buf_a, ac->cap_a);
            if (rc_a == 0) {
                ac->a_valid = 1;
            } else if (rc_a == 1) {
                ac->a_valid = 0;
            } else {
                return rc_a;
            }
        } else {
            int rc_b = jh_postings_cursor_next(ac->b, &ac->cur_b, ac->buf_b, ac->cap_b);
            if (rc_b == 0) {
                ac->b_valid = 1;
            } else if (rc_b == 1) {
                ac->b_valid = 0;
            } else {
                return rc_b;
            }
        }
    }

    return 1;
}

/* jh_postings_or_cursor_init creates a streaming OR view over two postings cursors. */
int jh_postings_or_cursor_init(jh_postings_or_cursor *oc, jh_postings_cursor *a, jh_postings_cursor *b, jh_u32 *buf_a, jh_u32 cap_a, jh_u32 *buf_b, jh_u32 cap_b) {
    int rc;

    if (!oc || !a || !b || !buf_a || !buf_b) {
        return -1;
    }

    oc->a = a;
    oc->b = b;
    oc->buf_a = buf_a;
    oc->cap_a = cap_a;
    oc->buf_b = buf_b;
    oc->cap_b = cap_b;
    oc->a_valid = 0;
    oc->b_valid = 0;

    rc = jh_postings_cursor_next(a, &oc->cur_a, buf_a, cap_a);
    if (rc == 0) {
        oc->a_valid = 1;
    } else if (rc == 1) {
        oc->a_valid = 0;
    } else {
        return rc;
    }

    rc = jh_postings_cursor_next(b, &oc->cur_b, buf_b, cap_b);
    if (rc == 0) {
        oc->b_valid = 1;
    } else if (rc == 1) {
        oc->b_valid = 0;
    } else {
        return rc;
    }

    return 0;
}

/* jh_postings_or_cursor_next advances to the next document present in either input. */
int jh_postings_or_cursor_next(jh_postings_or_cursor *oc, jh_posting_entry *out) {
    if (!oc || !out) {
        return -1;
    }

    while (oc->a_valid || oc->b_valid) {
        if (oc->a_valid && !oc->b_valid) {
            out->page_id = oc->cur_a.page_id;
            out->term_freq = oc->cur_a.term_freq;
            out->positions = NULL;

            int rc_a = jh_postings_cursor_next(oc->a, &oc->cur_a, oc->buf_a, oc->cap_a);
            if (rc_a == 0) {
                oc->a_valid = 1;
            } else if (rc_a == 1) {
                oc->a_valid = 0;
            } else {
                return rc_a;
            }

            return 0;
        }

        if (!oc->a_valid && oc->b_valid) {
            out->page_id = oc->cur_b.page_id;
            out->term_freq = oc->cur_b.term_freq;
            out->positions = NULL;

            int rc_b = jh_postings_cursor_next(oc->b, &oc->cur_b, oc->buf_b, oc->cap_b);
            if (rc_b == 0) {
                oc->b_valid = 1;
            } else if (rc_b == 1) {
                oc->b_valid = 0;
            } else {
                return rc_b;
            }

            return 0;
        }

        if (oc->a_valid && oc->b_valid) {
            jh_u32 da = oc->cur_a.page_id;
            jh_u32 db = oc->cur_b.page_id;

            if (da == db) {
                out->page_id = da;
                out->term_freq = oc->cur_a.term_freq + oc->cur_b.term_freq;
                out->positions = NULL;

                int rc_a = jh_postings_cursor_next(oc->a, &oc->cur_a, oc->buf_a, oc->cap_a);
                if (rc_a == 0) {
                    oc->a_valid = 1;
                } else if (rc_a == 1) {
                    oc->a_valid = 0;
                } else {
                    return rc_a;
                }

                int rc_b = jh_postings_cursor_next(oc->b, &oc->cur_b, oc->buf_b, oc->cap_b);
                if (rc_b == 0) {
                    oc->b_valid = 1;
                } else if (rc_b == 1) {
                    oc->b_valid = 0;
                } else {
                    return rc_b;
                }

                return 0;
            } else if (da < db) {
                out->page_id = da;
                out->term_freq = oc->cur_a.term_freq;
                out->positions = NULL;

                int rc_a = jh_postings_cursor_next(oc->a, &oc->cur_a, oc->buf_a, oc->cap_a);
                if (rc_a == 0) {
                    oc->a_valid = 1;
                } else if (rc_a == 1) {
                    oc->a_valid = 0;
                } else {
                    return rc_a;
                }

                return 0;
            } else {
                out->page_id = db;
                out->term_freq = oc->cur_b.term_freq;
                out->positions = NULL;

                int rc_b = jh_postings_cursor_next(oc->b, &oc->cur_b, oc->buf_b, oc->cap_b);
                if (rc_b == 0) {
                    oc->b_valid = 1;
                } else if (rc_b == 1) {
                    oc->b_valid = 0;
                } else {
                    return rc_b;
                }

                return 0;
            }
        }
    }

    return 1;
}

/* jh_phrase_adjacent_count counts phrase matches where term B follows term A by one position. */
static jh_u32 jh_phrase_adjacent_count(const jh_posting_entry *a, const jh_posting_entry *b) {
    jh_u32 i = 0;
    jh_u32 j = 0;
    jh_u32 count = 0;
    const jh_u32 *pa = a->positions;
    const jh_u32 *pb = b->positions;
    jh_u32 na = a->term_freq;
    jh_u32 nb = b->term_freq;

    while (i < na && j < nb) {
        jh_u32 va = pa[i] + 1;
        jh_u32 vb = pb[j];
        if (va == vb) {
            count += 1;
            i += 1;
            j += 1;
        } else if (va < vb) {
            i += 1;
        } else {
            j += 1;
        }
    }

    return count;
}

/* jh_postings_phrase_and_cursor_init creates a streaming phrase-AND view over two cursors. */
int jh_postings_phrase_and_cursor_init(jh_postings_phrase_and_cursor *pc, jh_postings_cursor *a, jh_postings_cursor *b, jh_u32 *buf_a, jh_u32 cap_a, jh_u32 *buf_b, jh_u32 cap_b) {
    int rc;

    if (!pc || !a || !b || !buf_a || !buf_b) {
        return -1;
    }

    pc->a = a;
    pc->b = b;
    pc->buf_a = buf_a;
    pc->cap_a = cap_a;
    pc->buf_b = buf_b;
    pc->cap_b = cap_b;
    pc->a_valid = 0;
    pc->b_valid = 0;

    rc = jh_postings_cursor_next(a, &pc->cur_a, buf_a, cap_a);
    if (rc == 0) {
        pc->a_valid = 1;
    } else if (rc == 1) {
        pc->a_valid = 0;
    } else {
        return rc;
    }

    rc = jh_postings_cursor_next(b, &pc->cur_b, buf_b, cap_b);
    if (rc == 0) {
        pc->b_valid = 1;
    } else if (rc == 1) {
        pc->b_valid = 0;
    } else {
        return rc;
    }

    return 0;
}

/* jh_postings_phrase_and_cursor_next yields documents where the second term follows the first as a phrase. */
int jh_postings_phrase_and_cursor_next(jh_postings_phrase_and_cursor *pc, jh_posting_entry *out) {
    if (!pc || !out) {
        return -1;
    }

    while (pc->a_valid && pc->b_valid) {
        jh_u32 da = pc->cur_a.page_id;
        jh_u32 db = pc->cur_b.page_id;

        if (da == db) {
            jh_u32 count = jh_phrase_adjacent_count(&pc->cur_a, &pc->cur_b);

            int rc_a = jh_postings_cursor_next(pc->a, &pc->cur_a, pc->buf_a, pc->cap_a);
            if (rc_a == 0) {
                pc->a_valid = 1;
            } else if (rc_a == 1) {
                pc->a_valid = 0;
            } else {
                return rc_a;
            }

            int rc_b = jh_postings_cursor_next(pc->b, &pc->cur_b, pc->buf_b, pc->cap_b);
            if (rc_b == 0) {
                pc->b_valid = 1;
            } else if (rc_b == 1) {
                pc->b_valid = 0;
            } else {
                return rc_b;
            }

            if (count > 0) {
                out->page_id = da;
                out->term_freq = count;
                out->positions = NULL;
                return 0;
            }
        } else if (da < db) {
            int rc_a = jh_postings_cursor_next(pc->a, &pc->cur_a, pc->buf_a, pc->cap_a);
            if (rc_a == 0) {
                pc->a_valid = 1;
            } else if (rc_a == 1) {
                pc->a_valid = 0;
            } else {
                return rc_a;
            }
        } else {
            int rc_b = jh_postings_cursor_next(pc->b, &pc->cur_b, pc->buf_b, pc->cap_b);
            if (rc_b == 0) {
                pc->b_valid = 1;
            } else if (rc_b == 1) {
                pc->b_valid = 0;
            } else {
                return rc_b;
            }
        }
    }

    return 1;
}
