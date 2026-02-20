/* jamharah index_format.h defines all on-disk index layouts and core postings APIs. */
#ifndef JAMHARAH_INDEX_FORMAT_H
#define JAMHARAH_INDEX_FORMAT_H
 
#include <stdint.h>
#include <stddef.h>
 
typedef uint8_t jh_u8;
typedef uint16_t jh_u16;
typedef uint32_t jh_u32;
typedef uint64_t jh_u64;

#define JH_ANNO_MAGIC "ANNO"
#define JH_ANNO_VERSION 1
 
#pragma pack(push,1)
 
/* jh_books_file_header describes the layout of the compressed books.bin text file. */
typedef struct {
    char magic[4];
    jh_u32 version;
    jh_u32 compression;
    jh_u32 block_size;
    jh_u32 reserved;
    jh_u64 block_count;
    jh_u64 index_offset;
} jh_books_file_header;

/* jh_block_index_entry describes a single compressed text block inside books.bin. */
typedef struct {
    jh_u64 uncompressed_offset;
    jh_u32 uncompressed_size;
    jh_u64 compressed_offset;
    jh_u32 compressed_size;
} jh_block_index_entry;

/* jh_books_index_header is the header for the per-book index file books.idx. */
typedef struct {
    char magic[4];
    jh_u32 version;
    jh_u32 book_count;
    jh_u32 reserved;
    jh_u64 reserved2;
} jh_books_index_header;

/* jh_book_index_entry summarizes one logical book and its page/chapter ranges. */
typedef struct {
    jh_u32 book_id;
    jh_u32 first_chapter_id;
    jh_u32 chapter_count;
    jh_u32 first_page_id;
    jh_u32 page_count;
    jh_u32 title_index;
    jh_u64 text_start_offset;
    jh_u64 text_end_offset;
} jh_book_index_entry;

/* jh_pages_index_header is the header for the per-page index file pages.idx. */
typedef struct {
    char magic[4];
    jh_u32 version;
    jh_u32 page_count;
    jh_u32 reserved;
    jh_u64 reserved2;
} jh_pages_index_header;

/* jh_page_index_entry maps a logical page to its position inside books.bin. */
typedef struct {
    jh_u32 page_id;
    jh_u32 book_id;
    jh_u32 chapter_id;
    jh_u32 page_number;
    jh_u32 block_id;
    jh_u32 reserved;
    jh_u32 offset_in_block;
    jh_u32 length;
} jh_page_index_entry;

/* jh_chapters_index_header is the header for the chapter index file chapters.idx. */
typedef struct {
    char magic[4];
    jh_u32 version;
    jh_u32 chapter_count;
    jh_u32 reserved;
    jh_u64 reserved2;
} jh_chapters_index_header;

/* jh_chapter_index_entry describes a chapter and its span of pages. */
typedef struct {
    jh_u32 chapter_id;
    jh_u32 book_id;
    jh_u32 chapter_number;
    jh_u32 first_page_id;
    jh_u32 page_count;
    jh_u32 title_index;
    jh_u32 reserved1;
    jh_u32 reserved2;
} jh_chapter_index_entry;

/* jh_titles_file_header is the header for the packed titles file titles.bin. */
typedef struct {
    char magic[4];
    jh_u32 version;
    jh_u32 title_count;
    jh_u32 reserved;
    jh_u64 strings_offset;
} jh_titles_file_header;

/* jh_title_entry points to a single UTF-8 title string inside titles.bin. */
typedef struct {
    jh_u64 offset;
    jh_u32 length;
    jh_u32 flags;
} jh_title_entry;

/* jh_words_index_header is the header for the dictionary index file words.idx. */
typedef struct {
    char magic[4];
    jh_u32 version;
    jh_u32 word_count;
    jh_u32 reserved;
    jh_u32 reserved2;
    jh_u64 words_bin_size;
    jh_u64 postings_file_size;
} jh_words_index_header;

/* jh_word_index_entry stores statistics and postings location for one word. */
typedef struct {
    jh_u32 word_id;
    jh_u32 df;
    jh_u32 cf;
    jh_u32 postings_count;
    jh_u32 postings_block_id;
    jh_u32 postings_offset_in_block;
    jh_u32 postings_length_in_block;
    jh_u32 word_string_offset;
    jh_u32 word_string_length;
    jh_u32 flags;
} jh_word_index_entry;

typedef struct {
    jh_u64 word_hash;
    jh_u32 page_id;
    jh_u32 position;
} jh_occurrence_record;

/* jh_postings_file_header is the header for the postings data file postings.bin. */
typedef struct {
    char magic[4];
    jh_u32 version;
    jh_u32 flags;
    jh_u32 reserved;
    jh_u32 reserved2;
    jh_u64 total_postings;
    jh_u64 block_count;
    jh_u64 block_index_offset;
    jh_u64 blocks_data_offset;
} jh_postings_file_header;

/* jh_postings_block_index_entry locates a compressed postings block in postings.bin. */
typedef struct {
    jh_u64 first_word_id;
    jh_u64 last_word_id;
    jh_u64 uncompressed_size;
    jh_u64 compressed_offset;
    jh_u64 compressed_size;
} jh_postings_block_index_entry;

typedef struct {
    char magic[4];
    jh_u32 version;
    jh_u32 reserved;
    jh_u64 entry_count;
} jh_word_dict_header;

typedef struct {
    jh_u64 word_hash;
    jh_u64 postings_offset;
    jh_u64 postings_count;
} jh_word_dict_entry;

typedef struct {
    char magic[4];
    jh_u32 version;
    jh_u64 corpus_version;
    jh_u64 comments_count;
    jh_u64 formatting_count;
    jh_u64 highlights_count;
    jh_u64 comments_offset;
    jh_u64 formatting_offset;
    jh_u64 highlights_offset;
    jh_u64 reserved[4];
} jh_anno_header;

typedef struct {
    jh_u32 page_id;
    jh_u32 anchor_start;
    jh_u32 anchor_end;
    jh_u16 flags;
    jh_u16 kind;
    jh_u32 author_id;
    jh_u64 created_at_unix;
    jh_u64 updated_at_unix;
    jh_u64 text_offset;
    jh_u32 text_length;
    jh_u32 reserved;
} jh_anno_comment_disk;

typedef struct {
    jh_u32 page_id;
    jh_u32 anchor_start;
    jh_u32 anchor_end;
    jh_u16 flags;
    jh_u16 style_id;
    jh_u16 layer;
    jh_u16 priority;
    jh_u32 reserved;
} jh_anno_formatting_disk;

typedef struct {
    jh_u32 page_id;
    jh_u32 anchor_start;
    jh_u32 anchor_end;
    jh_u16 flags;
    jh_u16 color_id;
    jh_u16 category_id;
    jh_u16 reserved16;
    jh_u32 author_id;
    jh_u64 created_at_unix;
    jh_u64 updated_at_unix;
    jh_u64 comment_ref_offset;
} jh_anno_highlight_disk;

int jh_word_dict_lookup(const char *path, jh_u64 word_hash, jh_word_dict_entry *out);

#pragma pack(pop)
 
/* jh_read_* helpers load and validate file headers from disk by magic tag. */
int jh_read_books_file_header(const char *path, jh_books_file_header *out);
int jh_read_books_index_header(const char *path, jh_books_index_header *out);
int jh_read_pages_index_header(const char *path, jh_pages_index_header *out);
int jh_read_chapters_index_header(const char *path, jh_chapters_index_header *out);
int jh_read_titles_file_header(const char *path, jh_titles_file_header *out);
int jh_read_words_index_header(const char *path, jh_words_index_header *out);
int jh_read_postings_file_header(const char *path, jh_postings_file_header *out);
int jh_load_page_text(const char *books_path, const char *pages_idx_path, jh_u32 page_id, char **out_text, jh_u32 *out_len);
 
/* jh_posting_entry holds a single document id and its term positions. */
typedef struct {
    jh_u32 page_id;
    jh_u32 term_freq;
    jh_u32 *positions;
} jh_posting_entry;

/* jh_postings_list is a fully materialized list of postings in memory. */
typedef struct {
    jh_posting_entry *entries;
    jh_u32 entry_count;
    jh_u32 *positions_storage;
    jh_u32 positions_count;
} jh_postings_list;

typedef struct {
    jh_u32 page_id;
    double score;
} jh_ranked_hit;

/* jh_postings_list_parse decodes an encoded postings buffer into an in-memory list. */
int jh_postings_list_parse(const jh_u8 *data, size_t data_size, jh_postings_list *out);
void jh_postings_list_free(jh_postings_list *list);
int jh_postings_list_read(const char *path, jh_u64 offset, jh_postings_list *out);
int jh_postings_block_read(const char *path, jh_u64 offset, jh_u8 **out_buf, size_t *out_size);
 
/* jh_postings_list_intersect computes document-level AND between two postings lists. */
int jh_postings_list_intersect(const jh_postings_list *a, const jh_postings_list *b, jh_postings_list *out);
 
/* jh_postings_cursor streams postings from an encoded buffer without allocations. */
typedef struct {
    const jh_u8 *data;
    size_t size;
    size_t offset;
    jh_u32 doc_count;
    jh_u32 index;
    jh_u32 current_page_id;
} jh_postings_cursor;

/* jh_postings_cursor_init prepares a cursor for iteration over a postings buffer. */
int jh_postings_cursor_init(jh_postings_cursor *cur, const jh_u8 *data, size_t size);
/* jh_postings_cursor_next yields the next posting into caller-provided storage. */
int jh_postings_cursor_next(jh_postings_cursor *cur, jh_posting_entry *out, jh_u32 *pos_buf, jh_u32 pos_buf_cap);
 
/* jh_postings_and_cursor walks the intersection of two postings cursors. */
typedef struct {
    jh_postings_cursor *a;
    jh_postings_cursor *b;
    jh_posting_entry cur_a;
    jh_posting_entry cur_b;
    jh_u32 *buf_a;
    jh_u32 cap_a;
    jh_u32 *buf_b;
    jh_u32 cap_b;
    int a_valid;
    int b_valid;
} jh_postings_and_cursor;

/* jh_postings_and_cursor_init sets up a streaming AND view over two cursors. */
int jh_postings_and_cursor_init(jh_postings_and_cursor *ac, jh_postings_cursor *a, jh_postings_cursor *b, jh_u32 *buf_a, jh_u32 cap_a, jh_u32 *buf_b, jh_u32 cap_b);
/* jh_postings_and_cursor_next returns the next doc that appears in both inputs. */
int jh_postings_and_cursor_next(jh_postings_and_cursor *ac, jh_posting_entry *out);
 
/* jh_postings_or_cursor walks the union of two postings cursors. */
typedef struct {
    jh_postings_cursor *a;
    jh_postings_cursor *b;
    jh_posting_entry cur_a;
    jh_posting_entry cur_b;
    jh_u32 *buf_a;
    jh_u32 cap_a;
    jh_u32 *buf_b;
    jh_u32 cap_b;
    int a_valid;
    int b_valid;
} jh_postings_or_cursor;

/* jh_postings_or_cursor_init sets up a streaming OR view over two cursors. */
int jh_postings_or_cursor_init(jh_postings_or_cursor *oc, jh_postings_cursor *a, jh_postings_cursor *b, jh_u32 *buf_a, jh_u32 cap_a, jh_u32 *buf_b, jh_u32 cap_b);
/* jh_postings_or_cursor_next returns the next doc that appears in either input. */
int jh_postings_or_cursor_next(jh_postings_or_cursor *oc, jh_posting_entry *out);
 
/* jh_postings_phrase_and_cursor enforces a two-term phrase across two cursors. */
typedef struct {
    jh_postings_cursor *a;
    jh_postings_cursor *b;
    jh_posting_entry cur_a;
    jh_posting_entry cur_b;
    jh_u32 *buf_a;
    jh_u32 cap_a;
    jh_u32 *buf_b;
    jh_u32 cap_b;
    int a_valid;
    int b_valid;
} jh_postings_phrase_and_cursor;

/* jh_postings_phrase_and_cursor_init sets up phrase-AND streaming between two terms. */
int jh_postings_phrase_and_cursor_init(jh_postings_phrase_and_cursor *pc, jh_postings_cursor *a, jh_postings_cursor *b, jh_u32 *buf_a, jh_u32 cap_a, jh_u32 *buf_b, jh_u32 cap_b);
/* jh_postings_phrase_and_cursor_next returns docs where term B follows term A by one. */
int jh_postings_phrase_and_cursor_next(jh_postings_phrase_and_cursor *pc, jh_posting_entry *out);

int jh_phrase_search(const char *words_idx_path, const char *postings_path, const jh_u64 *hashes, size_t hash_count, jh_u32 **out_pages, size_t *out_page_count);
int jh_phrase_search_multi(const char **words_idx_paths, const char **postings_paths, size_t cat_count, const jh_u64 *hashes, size_t hash_count, jh_u32 **out_pages, jh_u32 **out_categories, size_t *out_count);
int jh_rank_results(const jh_postings_list *lists, size_t list_count, int require_all_terms, const jh_u32 *phrase_pages, size_t phrase_page_count, jh_ranked_hit **out_hits, size_t *out_hit_count);

typedef struct {
    const jh_u8 *data;
    size_t size;
    jh_anno_header header;
    const jh_anno_comment_disk *comments;
    const jh_anno_formatting_disk *formatting;
    const jh_anno_highlight_disk *highlights;
} jh_anno_file_view;

int jh_anno_open(const char *path, jh_anno_file_view *out);
void jh_anno_close(jh_anno_file_view *view);
void jh_anno_find_comments_for_page(const jh_anno_file_view *view, jh_u32 page_id, size_t *first_index, size_t *count);
void jh_anno_find_formatting_for_page(const jh_anno_file_view *view, jh_u32 page_id, size_t *first_index, size_t *count);
void jh_anno_find_highlights_for_page(const jh_anno_file_view *view, jh_u32 page_id, size_t *first_index, size_t *count);
 
#endif
