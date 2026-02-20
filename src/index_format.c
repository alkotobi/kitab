#include "jamharah/index_format.h"
#include "jamharah/hash.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#ifdef JH_HAVE_ZSTD
#include <zstd.h>
#endif

static int jh_decompress_block_if_needed(const jh_postings_file_header *hdr,
                                         const jh_u8 *comp_buf,
                                         size_t comp_size,
                                         jh_u8 **out_plain,
                                         size_t *out_size) {
#ifdef JH_HAVE_ZSTD
    if (hdr->flags & 1u) {
        unsigned long long content_size = ZSTD_getFrameContentSize(comp_buf, comp_size);
        if (content_size == ZSTD_CONTENTSIZE_ERROR || content_size == ZSTD_CONTENTSIZE_UNKNOWN) {
            return -1;
        }
        if (content_size > SIZE_MAX) {
            return -2;
        }
        *out_plain = (jh_u8 *)malloc((size_t)content_size);
        if (!*out_plain) {
            return -3;
        }
        size_t dsize = ZSTD_decompress(*out_plain, (size_t)content_size, comp_buf, comp_size);
        if (ZSTD_isError(dsize) || dsize != content_size) {
            free(*out_plain);
            *out_plain = NULL;
            return -4;
        }
        *out_size = (size_t)content_size;
        return 0;
    }
#else
    (void)hdr;
#endif
    *out_plain = (jh_u8 *)malloc(comp_size);
    if (!*out_plain) {
        return -5;
    }
    memcpy(*out_plain, comp_buf, comp_size);
    *out_size = comp_size;
    return 0;
}

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

int jh_load_page_text(const char *books_path, const char *pages_idx_path, jh_u32 page_id, char **out_text, jh_u32 *out_len) {
    jh_books_file_header books_hdr;
    jh_pages_index_header pages_hdr;
    FILE *books_fp;
    FILE *pages_fp;
    jh_page_index_entry pe;
    jh_block_index_entry block_entry;
    char *buf = NULL;
    size_t len;
    jh_u64 file_offset;
    size_t nread;
    jh_u64 lo;
    jh_u64 hi;

    if (!books_path || !pages_idx_path || !out_text || !out_len) {
        return -1;
    }

    if (jh_read_books_file_header(books_path, &books_hdr) != 0) {
        return -2;
    }
    books_fp = fopen(books_path, "rb");
    if (!books_fp) {
        return -3;
    }
    if (books_hdr.block_count == 0) {
        fclose(books_fp);
        return -5;
    }

    if (jh_read_pages_index_header(pages_idx_path, &pages_hdr) != 0) {
        fclose(books_fp);
        return -9;
    }
    pages_fp = fopen(pages_idx_path, "rb");
    if (!pages_fp) {
        fclose(books_fp);
        return -10;
    }
    if (pages_hdr.page_count == 0) {
        fclose(pages_fp);
        fclose(books_fp);
        return -12;
    }
    lo = 0;
    hi = pages_hdr.page_count;
    while (lo < hi) {
        jh_u64 mid = lo + (hi - lo) / 2;
        long offset = (long)(sizeof(jh_pages_index_header) + mid * (jh_u64)sizeof(jh_page_index_entry));
        if (fseek(pages_fp, offset, SEEK_SET) != 0) {
            fclose(pages_fp);
            fclose(books_fp);
            return -13;
        }
        if (fread(&pe, 1, sizeof(pe), pages_fp) != sizeof(pe)) {
            fclose(pages_fp);
            fclose(books_fp);
            return -14;
        }
        if (pe.page_id == page_id) {
            break;
        } else if (pe.page_id < page_id) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    fclose(pages_fp);
    if (lo >= pages_hdr.page_count || pe.page_id != page_id) {
        fclose(books_fp);
        return -16;
    }
    if (pe.block_id >= books_hdr.block_count || pe.length == 0) {
        fclose(books_fp);
        return -17;
    }

    len = pe.length;
    buf = (char *)malloc(len + 1);
    if (!buf) {
        fclose(books_fp);
        return -18;
    }

    file_offset = books_hdr.index_offset + (jh_u64)pe.block_id * (jh_u64)sizeof(jh_block_index_entry);
    if (fseek(books_fp, (long)file_offset, SEEK_SET) != 0) {
        free(buf);
        fclose(books_fp);
        return -19;
    }
    if (fread(&block_entry, 1, sizeof(block_entry), books_fp) != sizeof(block_entry)) {
        free(buf);
        fclose(books_fp);
        return -20;
    }
    file_offset = block_entry.compressed_offset + (jh_u64)pe.offset_in_block;
    if (fseek(books_fp, (long)file_offset, SEEK_SET) != 0) {
        free(buf);
        fclose(books_fp);
        return -21;
    }
    nread = fread(buf, 1, len, books_fp);
    if (nread != len) {
        free(buf);
        fclose(books_fp);
        return -22;
    }
    fclose(books_fp);

    buf[len] = 0;
    *out_text = buf;
    *out_len = (jh_u32)len;
    return 0;
}

typedef struct {
    jh_u64 path_hash;
    jh_u64 word_hash;
    jh_word_dict_entry entry;
    int valid;
    jh_u64 age;
} jh_word_dict_cache_entry;

#define JH_WORD_DICT_CACHE_CAP 64

static jh_word_dict_cache_entry jh_word_dict_cache[JH_WORD_DICT_CACHE_CAP];
static jh_u64 jh_word_dict_cache_clock = 1;

static jh_u64 jh_word_dict_path_hash(const char *path) {
    size_t len = 0;
    const char *p = path;
    while (*p) {
        p++;
        len++;
    }
    return jh_hash_utf8_64(path, len, 0);
}

int jh_word_dict_lookup(const char *path, jh_u64 word_hash, jh_word_dict_entry *out) {
    FILE *f;
    jh_word_dict_header hdr;
    jh_word_dict_entry entry;
    jh_u64 lo;
    jh_u64 hi;
    jh_u64 path_hash;
    size_t i;
    size_t victim = 0;
    jh_u64 victim_age = 0;

    if (!path || !out) {
        return -1;
    }

    path_hash = jh_word_dict_path_hash(path);

    for (i = 0; i < JH_WORD_DICT_CACHE_CAP; ++i) {
        jh_word_dict_cache_entry *ce = &jh_word_dict_cache[i];
        if (ce->valid && ce->path_hash == path_hash && ce->word_hash == word_hash) {
            *out = ce->entry;
            return 0;
        }
    }

    for (i = 0; i < JH_WORD_DICT_CACHE_CAP; ++i) {
        jh_word_dict_cache_entry *ce = &jh_word_dict_cache[i];
        if (!ce->valid) {
            victim = i;
            victim_age = 0;
            break;
        }
        if (victim_age == 0 || ce->age < victim_age) {
            victim = i;
            victim_age = ce->age;
        }
    }

    f = fopen(path, "rb");
    if (!f) {
        return -2;
    }
    if (fread(&hdr, 1, sizeof(hdr), f) != sizeof(hdr)) {
        fclose(f);
        return -3;
    }
    if (memcmp(hdr.magic, "WDIX", 4) != 0 || hdr.version != 1) {
        fclose(f);
        return -4;
    }

    lo = 0;
    hi = hdr.entry_count;
    while (lo < hi) {
        jh_u64 mid = lo + (hi - lo) / 2;
        long offset = (long)(sizeof(hdr) + mid * (jh_u64)sizeof(jh_word_dict_entry));
        if (fseek(f, offset, SEEK_SET) != 0) {
            fclose(f);
            return -5;
        }
        if (fread(&entry, 1, sizeof(entry), f) != sizeof(entry)) {
            fclose(f);
            return -6;
        }
        if (entry.word_hash == word_hash) {
            *out = entry;
            fclose(f);
            {
                jh_word_dict_cache_entry *ce = &jh_word_dict_cache[victim];
                ce->path_hash = path_hash;
                ce->word_hash = word_hash;
                ce->entry = entry;
                ce->valid = 1;
                ce->age = jh_word_dict_cache_clock++;
            }
            return 0;
        } else if (entry.word_hash < word_hash) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    fclose(f);
    return 1;
}

int jh_anno_open(const char *path, jh_anno_file_view *out) {
    FILE *f;
    long len;
    jh_u8 *buf;
    jh_anno_header hdr;

    if (!path || !out) {
        return -1;
    }
    f = fopen(path, "rb");
    if (!f) {
        return -2;
    }
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return -3;
    }
    len = ftell(f);
    if (len <= 0) {
        fclose(f);
        return -4;
    }
    if (fseek(f, 0, SEEK_SET) != 0) {
        fclose(f);
        return -5;
    }
    buf = (jh_u8 *)malloc((size_t)len);
    if (!buf) {
        fclose(f);
        return -6;
    }
    if (fread(buf, 1, (size_t)len, f) != (size_t)len) {
        free(buf);
        fclose(f);
        return -7;
    }
    fclose(f);

    if ((size_t)len < sizeof(jh_anno_header)) {
        free(buf);
        return -8;
    }
    memcpy(&hdr, buf, sizeof(jh_anno_header));
    if (memcmp(hdr.magic, JH_ANNO_MAGIC, 4) != 0) {
        free(buf);
        return -9;
    }
    if (hdr.version != JH_ANNO_VERSION) {
        free(buf);
        return -10;
    }

    out->data = buf;
    out->size = (size_t)len;
    out->header = hdr;
    out->comments = NULL;
    out->formatting = NULL;
    out->highlights = NULL;

    if (hdr.comments_count && hdr.comments_offset) {
        jh_u64 end = hdr.comments_offset + hdr.comments_count * (jh_u64)sizeof(jh_anno_comment_disk);
        if (end <= (jh_u64)out->size) {
            out->comments = (const jh_anno_comment_disk *)(buf + (size_t)hdr.comments_offset);
        }
    }
    if (hdr.formatting_count && hdr.formatting_offset) {
        jh_u64 end = hdr.formatting_offset + hdr.formatting_count * (jh_u64)sizeof(jh_anno_formatting_disk);
        if (end <= (jh_u64)out->size) {
            out->formatting = (const jh_anno_formatting_disk *)(buf + (size_t)hdr.formatting_offset);
        }
    }
    if (hdr.highlights_count && hdr.highlights_offset) {
        jh_u64 end = hdr.highlights_offset + hdr.highlights_count * (jh_u64)sizeof(jh_anno_highlight_disk);
        if (end <= (jh_u64)out->size) {
            out->highlights = (const jh_anno_highlight_disk *)(buf + (size_t)hdr.highlights_offset);
        }
    }

    return 0;
}

void jh_anno_close(jh_anno_file_view *view) {
    if (!view) {
        return;
    }
    if (view->data) {
        free((void *)view->data);
    }
    memset(view, 0, sizeof(*view));
}

static void jh_anno_find_page_range_comments(const jh_anno_comment_disk *arr, size_t n, jh_u32 page_id, size_t *first_index, size_t *count) {
    size_t lo = 0;
    size_t hi = n;
    size_t first = n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (arr[mid].page_id >= page_id) {
            hi = mid;
            if (arr[mid].page_id == page_id) {
                first = mid;
            }
        } else {
            lo = mid + 1;
        }
    }
    if (first == n) {
        *first_index = 0;
        *count = 0;
        return;
    }
    size_t i = first;
    while (i < n && arr[i].page_id == page_id) {
        i++;
    }
    *first_index = first;
    *count = i - first;
}

void jh_anno_find_comments_for_page(const jh_anno_file_view *view, jh_u32 page_id, size_t *first_index, size_t *count) {
    if (!view || !view->comments || !first_index || !count) {
        if (first_index) {
            *first_index = 0;
        }
        if (count) {
            *count = 0;
        }
        return;
    }
    jh_anno_find_page_range_comments(view->comments, (size_t)view->header.comments_count, page_id, first_index, count);
}

static void jh_anno_find_page_range_formatting(const jh_anno_formatting_disk *arr, size_t n, jh_u32 page_id, size_t *first_index, size_t *count) {
    size_t lo = 0;
    size_t hi = n;
    size_t first = n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (arr[mid].page_id >= page_id) {
            hi = mid;
            if (arr[mid].page_id == page_id) {
                first = mid;
            }
        } else {
            lo = mid + 1;
        }
    }
    if (first == n) {
        *first_index = 0;
        *count = 0;
        return;
    }
    size_t i = first;
    while (i < n && arr[i].page_id == page_id) {
        i++;
    }
    *first_index = first;
    *count = i - first;
}

void jh_anno_find_formatting_for_page(const jh_anno_file_view *view, jh_u32 page_id, size_t *first_index, size_t *count) {
    if (!view || !view->formatting || !first_index || !count) {
        if (first_index) {
            *first_index = 0;
        }
        if (count) {
            *count = 0;
        }
        return;
    }
    jh_anno_find_page_range_formatting(view->formatting, (size_t)view->header.formatting_count, page_id, first_index, count);
}

static void jh_anno_find_page_range_highlights(const jh_anno_highlight_disk *arr, size_t n, jh_u32 page_id, size_t *first_index, size_t *count) {
    size_t lo = 0;
    size_t hi = n;
    size_t first = n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (arr[mid].page_id >= page_id) {
            hi = mid;
            if (arr[mid].page_id == page_id) {
                first = mid;
            }
        } else {
            lo = mid + 1;
        }
    }
    if (first == n) {
        *first_index = 0;
        *count = 0;
        return;
    }
    size_t i = first;
    while (i < n && arr[i].page_id == page_id) {
        i++;
    }
    *first_index = first;
    *count = i - first;
}

void jh_anno_find_highlights_for_page(const jh_anno_file_view *view, jh_u32 page_id, size_t *first_index, size_t *count) {
    if (!view || !view->highlights || !first_index || !count) {
        if (first_index) {
            *first_index = 0;
        }
        if (count) {
            *count = 0;
        }
        return;
    }
    jh_anno_find_page_range_highlights(view->highlights, (size_t)view->header.highlights_count, page_id, first_index, count);
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

int jh_postings_list_read(const char *path, jh_u64 offset, jh_postings_list *out) {
    FILE *f;
    jh_postings_file_header hdr;
    jh_u8 len_buf[4];
    jh_u32 block_size;
    jh_u8 *comp_buf = NULL;
    jh_u8 *plain_buf = NULL;
    size_t plain_size = 0;
    int rc;

    if (!path || !out) {
        return -1;
    }

    if (jh_read_postings_file_header(path, &hdr) != 0) {
        return -2;
    }

    f = fopen(path, "rb");
    if (!f) {
        return -3;
    }
    if (fseek(f, (long)offset, SEEK_SET) != 0) {
        fclose(f);
        return -4;
    }
    if (fread(len_buf, 1, 4, f) != 4) {
        fclose(f);
        return -5;
    }
    block_size = jh_read_u32_le(len_buf);
    if (block_size == 0) {
        fclose(f);
        return -6;
    }

    comp_buf = (jh_u8 *)malloc(block_size);
    if (!comp_buf) {
        fclose(f);
        return -7;
    }
    if (fread(comp_buf, 1, block_size, f) != block_size) {
        free(comp_buf);
        fclose(f);
        return -8;
    }
    fclose(f);

    rc = jh_decompress_block_if_needed(&hdr, comp_buf, block_size, &plain_buf, &plain_size);
    free(comp_buf);
    if (rc != 0) {
        return -9;
    }

    rc = jh_postings_list_parse(plain_buf, plain_size, out);
    free(plain_buf);
    if (rc != 0) {
        return -10;
    }
    return 0;
}

int jh_postings_block_read(const char *path, jh_u64 offset, jh_u8 **out_buf, size_t *out_size) {
    FILE *f;
    jh_postings_file_header hdr;
    jh_u8 len_buf[4];
    jh_u32 block_size;
    jh_u8 *comp_buf = NULL;
    jh_u8 *plain_buf = NULL;
    size_t plain_size = 0;
    int rc;

    if (!path || !out_buf || !out_size) {
        return -1;
    }

    if (jh_read_postings_file_header(path, &hdr) != 0) {
        return -2;
    }

    f = fopen(path, "rb");
    if (!f) {
        return -3;
    }
    if (fseek(f, (long)offset, SEEK_SET) != 0) {
        fclose(f);
        return -4;
    }
    if (fread(len_buf, 1, 4, f) != 4) {
        fclose(f);
        return -5;
    }
    block_size = jh_read_u32_le(len_buf);
    if (block_size == 0) {
        fclose(f);
        return -6;
    }

    comp_buf = (jh_u8 *)malloc(block_size);
    if (!comp_buf) {
        fclose(f);
        return -7;
    }
    if (fread(comp_buf, 1, block_size, f) != block_size) {
        free(comp_buf);
        fclose(f);
        return -8;
    }
    fclose(f);

    rc = jh_decompress_block_if_needed(&hdr, comp_buf, block_size, &plain_buf, &plain_size);
    free(comp_buf);
    if (rc != 0) {
        return -9;
    }

    *out_buf = plain_buf;
    *out_size = plain_size;
    return 0;
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

static jh_posting_entry *jh_find_posting_in_list(const jh_postings_list *pl, jh_u32 page_id) {
    jh_u32 lo = 0;
    jh_u32 hi;
    if (!pl || !pl->entries) {
        return NULL;
    }
    hi = pl->entry_count;
    while (lo < hi) {
        jh_u32 mid = lo + (hi - lo) / 2;
        jh_u32 v = pl->entries[mid].page_id;
        if (v == page_id) {
            return &pl->entries[mid];
        } else if (v < page_id) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return NULL;
}

static int jh_phrase_matches_doc(const jh_posting_entry **entries, size_t term_count) {
    const jh_posting_entry *base;
    jh_u32 i;
    if (!entries || term_count == 0) {
        return 0;
    }
    base = entries[0];
    for (i = 0; i < base->term_freq; ++i) {
        jh_u32 pos0 = base->positions[i];
        size_t t;
        int ok = 1;
        for (t = 1; t < term_count; ++t) {
            const jh_posting_entry *pe = entries[t];
            const jh_u32 *pb = pe->positions;
            jh_u32 nb = pe->term_freq;
            jh_u32 want = pos0 + (jh_u32)t;
            jh_u32 lo = 0;
            jh_u32 hi = nb;
            int found = 0;
            while (lo < hi) {
                jh_u32 mid = lo + (hi - lo) / 2;
                jh_u32 v = pb[mid];
                if (v == want) {
                    found = 1;
                    break;
                } else if (v < want) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            if (!found) {
                ok = 0;
                break;
            }
        }
        if (ok) {
            return 1;
        }
    }
    return 0;
}

int jh_phrase_search(const char *words_idx_path, const char *postings_path, const jh_u64 *hashes, size_t hash_count, jh_u32 **out_pages, size_t *out_page_count) {
    jh_postings_list *lists = NULL;
    size_t i;
    size_t result_cap = 0;
    size_t result_count = 0;
    jh_u32 *result_pages = NULL;

    if (!words_idx_path || !postings_path || !hashes || hash_count == 0 || !out_pages || !out_page_count) {
        return -1;
    }

    if (hash_count == 1) {
        return -2;
    }

    lists = (jh_postings_list *)calloc(hash_count, sizeof(jh_postings_list));
    if (!lists) {
        free(lists);
        return -3;
    }

    for (i = 0; i < hash_count; ++i) {
        jh_word_dict_entry e;
        if (jh_word_dict_lookup(words_idx_path, hashes[i], &e) != 0 || e.postings_count == 0) {
            size_t k;
            for (k = 0; k < i; ++k) {
                jh_postings_list_free(&lists[k]);
            }
            free(lists);
            *out_pages = NULL;
            *out_page_count = 0;
            return 0;
        }
        if (jh_postings_list_read(postings_path, e.postings_offset, &lists[i]) != 0) {
            size_t k;
            for (k = 0; k <= i; ++k) {
                jh_postings_list_free(&lists[k]);
            }
            free(lists);
            return -4;
        }
    }

    {
        size_t base_idx = 0;
        jh_u32 d;
        const jh_postings_list *base = &lists[0];
        if (hash_count > 1) {
            for (i = 1; i < hash_count; ++i) {
                if (lists[i].entry_count < base->entry_count) {
                    base = &lists[i];
                    base_idx = i;
                }
            }
        }
        for (i = 0; i < base->entry_count; ++i) {
            const jh_posting_entry *base_entry = &base->entries[i];
            const jh_posting_entry *entries[32];
            size_t t;
            if (hash_count > 32) {
                break;
            }
            d = base_entry->page_id;
            entries[base_idx] = base_entry;
            for (t = 0; t < hash_count; ++t) {
                if (t == base_idx) {
                    continue;
                }
                entries[t] = jh_find_posting_in_list(&lists[t], d);
                if (!entries[t]) {
                    break;
                }
            }
            if (t != hash_count) {
                continue;
            }
            {
                const jh_posting_entry *ordered[32];
                size_t k;
                for (k = 0; k < hash_count; ++k) {
                    ordered[k] = entries[k];
                }
                if (jh_phrase_matches_doc(ordered, hash_count)) {
                    if (result_count == result_cap) {
                        size_t new_cap = result_cap ? result_cap * 2 : 16;
                        jh_u32 *np = (jh_u32 *)realloc(result_pages, new_cap * sizeof(jh_u32));
                        if (!np) {
                            size_t m;
                            for (m = 0; m < hash_count; ++m) {
                                jh_postings_list_free(&lists[m]);
                            }
                            free(lists);
                            free(result_pages);
                            return -5;
                        }
                        result_pages = np;
                        result_cap = new_cap;
                    }
                    result_pages[result_count++] = d;
                }
            }
        }
    }

    for (i = 0; i < hash_count; ++i) {
        jh_postings_list_free(&lists[i]);
    }
    free(lists);

    *out_pages = result_pages;
    *out_page_count = result_count;
    return 0;
}

int jh_phrase_search_multi(const char **words_idx_paths, const char **postings_paths, size_t cat_count, const jh_u64 *hashes, size_t hash_count, jh_u32 **out_pages, jh_u32 **out_categories, size_t *out_count) {
    size_t i;
    jh_u32 *all_pages = NULL;
    jh_u32 *all_cats = NULL;
    size_t total = 0;
    size_t cap = 0;

    if (!words_idx_paths || !postings_paths || !hashes || hash_count == 0 || !out_pages || !out_categories || !out_count) {
        return -1;
    }
    if (cat_count == 0) {
        *out_pages = NULL;
        *out_categories = NULL;
        *out_count = 0;
        return 0;
    }

    for (i = 0; i < cat_count; ++i) {
        jh_u32 *pages = NULL;
        size_t page_count = 0;
        int rc;

        if (!words_idx_paths[i] || !postings_paths[i]) {
            free(all_pages);
            free(all_cats);
            return -2;
        }

        rc = jh_phrase_search(words_idx_paths[i], postings_paths[i], hashes, hash_count, &pages, &page_count);
        if (rc != 0) {
            free(all_pages);
            free(all_cats);
            free(pages);
            return rc;
        }

        if (!pages || page_count == 0) {
            free(pages);
            continue;
        }

        if (total + page_count > cap) {
            size_t new_cap = cap ? cap * 2 : 16;
            while (new_cap < total + page_count) {
                new_cap *= 2;
            }
            jh_u32 *np_pages = (jh_u32 *)realloc(all_pages, new_cap * sizeof(jh_u32));
            jh_u32 *np_cats = (jh_u32 *)realloc(all_cats, new_cap * sizeof(jh_u32));
            if (!np_pages || !np_cats) {
                if (np_pages && np_pages != all_pages) {
                    free(np_pages);
                }
                if (np_cats && np_cats != all_cats) {
                    free(np_cats);
                }
                free(all_pages);
                free(all_cats);
                free(pages);
                return -3;
            }
            all_pages = np_pages;
            all_cats = np_cats;
            cap = new_cap;
        }

        {
            size_t j;
            for (j = 0; j < page_count; ++j) {
                all_pages[total + j] = pages[j];
                all_cats[total + j] = (jh_u32)i;
            }
            total += page_count;
        }

        free(pages);
    }

    if (total == 0) {
        free(all_pages);
        free(all_cats);
        *out_pages = NULL;
        *out_categories = NULL;
        *out_count = 0;
        return 0;
    }

    *out_pages = all_pages;
    *out_categories = all_cats;
    *out_count = total;
    return 0;
}

static int jh_u32_cmp(const void *a, const void *b) {
    jh_u32 va = *(const jh_u32 *)a;
    jh_u32 vb = *(const jh_u32 *)b;
    if (va < vb) return -1;
    if (va > vb) return 1;
    return 0;
}

static int jh_ranked_hit_cmp_desc(const void *a, const void *b) {
    const jh_ranked_hit *ha = (const jh_ranked_hit *)a;
    const jh_ranked_hit *hb = (const jh_ranked_hit *)b;
    if (ha->score > hb->score) return -1;
    if (ha->score < hb->score) return 1;
    if (ha->page_id < hb->page_id) return -1;
    if (ha->page_id > hb->page_id) return 1;
    return 0;
}

int jh_rank_results(const jh_postings_list *lists, size_t list_count, const jh_u32 *phrase_pages, size_t phrase_page_count, jh_ranked_hit **out_hits, size_t *out_hit_count) {
    size_t i;
    size_t total_docs = 0;
    jh_u32 *pages;
    size_t page_count = 0;
    jh_ranked_hit *hits = NULL;
    size_t hits_count = 0;
    const double freq_weight = 1.0;
    const double prox_weight = 2.0;
    const double phrase_weight = 5.0;
    jh_u32 *phrase_sorted = NULL;

    if (!out_hits || !out_hit_count) {
        return -1;
    }

    *out_hits = NULL;
    *out_hit_count = 0;

    if (!lists || list_count == 0) {
        return 0;
    }

    for (i = 0; i < list_count; ++i) {
        total_docs += lists[i].entry_count;
    }
    if (total_docs == 0) {
        return 0;
    }

    pages = (jh_u32 *)malloc(sizeof(jh_u32) * total_docs);
    if (!pages) {
        return -2;
    }

    for (i = 0; i < list_count; ++i) {
        jh_u32 j;
        for (j = 0; j < lists[i].entry_count; ++j) {
            pages[page_count++] = lists[i].entries[j].page_id;
        }
    }

    qsort(pages, page_count, sizeof(jh_u32), jh_u32_cmp);

    {
        size_t w = 0;
        size_t r;
        for (r = 0; r < page_count; ++r) {
            if (w == 0 || pages[r] != pages[w - 1]) {
                pages[w++] = pages[r];
            }
        }
        page_count = w;
    }

    if (phrase_pages && phrase_page_count > 0) {
        phrase_sorted = (jh_u32 *)malloc(sizeof(jh_u32) * phrase_page_count);
        if (!phrase_sorted) {
            free(pages);
            return -3;
        }
        memcpy(phrase_sorted, phrase_pages, sizeof(jh_u32) * phrase_page_count);
        qsort(phrase_sorted, phrase_page_count, sizeof(jh_u32), jh_u32_cmp);
    }

    hits = (jh_ranked_hit *)malloc(sizeof(jh_ranked_hit) * page_count);
    if (!hits) {
        free(pages);
        free(phrase_sorted);
        return -4;
    }

    for (i = 0; i < page_count; ++i) {
        jh_u32 d = pages[i];
        double freq_score = 0.0;
        double prox_score = 0.0;
        double phrase_score = 0.0;
        size_t t;

        for (t = 0; t < list_count; ++t) {
            jh_posting_entry *pe = jh_find_posting_in_list(&lists[t], d);
            if (pe) {
                freq_score += (double)pe->term_freq;
            }
        }

        if (list_count >= 2) {
            for (t = 0; t + 1 < list_count; ++t) {
                jh_posting_entry *a = jh_find_posting_in_list(&lists[t], d);
                jh_posting_entry *b = jh_find_posting_in_list(&lists[t + 1], d);
                if (a && b && a->term_freq > 0 && b->term_freq > 0) {
                    const jh_u32 *pa = a->positions;
                    const jh_u32 *pb = b->positions;
                    jh_u32 na = a->term_freq;
                    jh_u32 nb = b->term_freq;
                    jh_u32 ia = 0;
                    jh_u32 ib = 0;
                    jh_u32 best = (jh_u32)-1;
                    while (ia < na && ib < nb) {
                        jh_u32 va = pa[ia];
                        jh_u32 vb = pb[ib];
                        jh_u32 diff = va > vb ? va - vb : vb - va;
                        if (diff < best) {
                            best = diff;
                        }
                        if (va < vb) {
                            ia += 1;
                        } else {
                            ib += 1;
                        }
                    }
                    if (best != (jh_u32)-1) {
                        prox_score += 1.0 / (1.0 + (double)best);
                    }
                }
            }
        }

        if (phrase_sorted && phrase_page_count > 0) {
            size_t lo = 0;
            size_t hi = phrase_page_count;
            while (lo < hi) {
                size_t mid = lo + (hi - lo) / 2;
                jh_u32 v = phrase_sorted[mid];
                if (v == d) {
                    phrase_score = phrase_weight;
                    break;
                } else if (v < d) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
        }

        if (freq_score > 0.0 || prox_score > 0.0 || phrase_score > 0.0) {
            hits[hits_count].page_id = d;
            hits[hits_count].score = freq_weight * freq_score + prox_weight * prox_score + phrase_score;
            hits_count += 1;
        }
    }

    free(pages);
    free(phrase_sorted);

    if (hits_count == 0) {
        free(hits);
        *out_hits = NULL;
        *out_hit_count = 0;
        return 0;
    }

    qsort(hits, hits_count, sizeof(jh_ranked_hit), jh_ranked_hit_cmp_desc);

    *out_hits = hits;
    *out_hit_count = hits_count;
    return 0;
}
