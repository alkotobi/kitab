#include "jamharah/index_format.h"
#include <sqlite3.h>
#include <dirent.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* jh_page_tmp holds temporary page metadata while building the index from SQLite. */
typedef struct {
    jh_u32 book_id;
    jh_u32 page_number;
    jh_u64 text_offset;
    jh_u32 text_length;
    jh_u32 chapter_id;
} jh_page_tmp;

/* jh_chapter_tmp holds temporary chapter metadata derived from title rows. */
typedef struct {
    jh_u32 chapter_id;
    jh_u32 book_id;
    jh_u32 chapter_number;
    jh_u32 title_index;
    jh_u32 first_page_id;
    jh_u32 page_count;
    jh_u32 start_page_number;
} jh_chapter_tmp;

/* jh_book_tmp holds per-book summary information while exporting all data. */
typedef struct {
    jh_u32 book_id;
    jh_u32 first_page_id;
    jh_u32 page_count;
    jh_u32 first_chapter_id;
    jh_u32 chapter_count;
    jh_u64 text_start_offset;
    jh_u64 text_end_offset;
    jh_u32 title_index;
} jh_book_tmp;

/* jh_title_tmp stores raw title strings before they are packed into titles.bin. */
typedef struct {
    char *text;
    jh_u32 flags;
} jh_title_tmp;

static jh_page_tmp *g_pages = NULL;
static size_t g_pages_count = 0;
static size_t g_pages_cap = 0;

static jh_chapter_tmp *g_chapters = NULL;
static size_t g_chapters_count = 0;
static size_t g_chapters_cap = 0;

static jh_book_tmp *g_books = NULL;
static size_t g_books_count = 0;
static size_t g_books_cap = 0;

static jh_title_tmp *g_titles = NULL;
static size_t g_titles_count = 0;
static size_t g_titles_cap = 0;

static jh_block_index_entry *g_blocks = NULL;
static size_t g_blocks_count = 0;
static size_t g_blocks_cap = 0;

static FILE *g_books_fp = NULL;
static jh_u8 *g_block_buf = NULL;
static jh_u32 g_block_size = 1u << 16;
static jh_u32 g_block_fill = 0;
static jh_u64 g_block_uncompressed_start = 0;
static jh_u64 g_block_file_offset = 0;
static jh_u64 g_uncompressed_offset = 0;

/* jh_die prints an error message and terminates the process. */
static void jh_die(const char *msg) {
    fprintf(stderr, "error: %s\n", msg);
    exit(1);
}

/* jh_xrealloc grows a dynamic array, exiting on allocation failure. */
static void *jh_xrealloc(void *ptr, size_t new_count, size_t elem_size) {
    size_t bytes = new_count * elem_size;
    void *p = realloc(ptr, bytes);
    if (!p) {
        jh_die("out of memory");
    }
    return p;
}

/* jh_pages_push appends a page descriptor to the global page array. */
static void jh_pages_push(const jh_page_tmp *p) {
    if (g_pages_count == g_pages_cap) {
        size_t nc = g_pages_cap ? g_pages_cap * 2 : 1024;
        g_pages = (jh_page_tmp *)jh_xrealloc(g_pages, nc, sizeof(jh_page_tmp));
        g_pages_cap = nc;
    }
    g_pages[g_pages_count++] = *p;
}

/* jh_chapters_push_empty reserves and returns a new chapter slot. */
static jh_chapter_tmp *jh_chapters_push_empty(void) {
    if (g_chapters_count == g_chapters_cap) {
        size_t nc = g_chapters_cap ? g_chapters_cap * 2 : 128;
        g_chapters = (jh_chapter_tmp *)jh_xrealloc(g_chapters, nc, sizeof(jh_chapter_tmp));
        g_chapters_cap = nc;
    }
    return &g_chapters[g_chapters_count++];
}

/* jh_books_push_empty reserves and returns a new book slot. */
static jh_book_tmp *jh_books_push_empty(void) {
    if (g_books_count == g_books_cap) {
        size_t nc = g_books_cap ? g_books_cap * 2 : 64;
        g_books = (jh_book_tmp *)jh_xrealloc(g_books, nc, sizeof(jh_book_tmp));
        g_books_cap = nc;
    }
    return &g_books[g_books_count++];
}

/* jh_titles_push stores a title string and returns its index in the title table. */
static jh_u32 jh_titles_push(const char *text, jh_u32 flags) {
    if (g_titles_count == g_titles_cap) {
        size_t nc = g_titles_cap ? g_titles_cap * 2 : 128;
        g_titles = (jh_title_tmp *)jh_xrealloc(g_titles, nc, sizeof(jh_title_tmp));
        g_titles_cap = nc;
    }
    size_t len = strlen(text);
    char *buf = (char *)malloc(len + 1);
    if (!buf) {
        jh_die("out of memory");
    }
    memcpy(buf, text, len);
    buf[len] = 0;
    g_titles[g_titles_count].text = buf;
    g_titles[g_titles_count].flags = flags;
    return (jh_u32)g_titles_count++;
}

/* jh_blocks_push appends a block index entry for books.bin. */
static void jh_blocks_push(const jh_block_index_entry *b) {
    if (g_blocks_count == g_blocks_cap) {
        size_t nc = g_blocks_cap ? g_blocks_cap * 2 : 256;
        g_blocks = (jh_block_index_entry *)jh_xrealloc(g_blocks, nc, sizeof(jh_block_index_entry));
        g_blocks_cap = nc;
    }
    g_blocks[g_blocks_count++] = *b;
}

/* jh_flush_block writes the current text block to disk and records its index entry. */
static void jh_flush_block(void) {
    if (g_block_fill == 0) {
        return;
    }
    if (!g_books_fp) {
        jh_die("books file not open");
    }
    if (fseek(g_books_fp, (long)g_block_file_offset, SEEK_SET) != 0) {
        jh_die("fseek failed");
    }
    if (fwrite(g_block_buf, 1, g_block_fill, g_books_fp) != g_block_fill) {
        jh_die("write books block failed");
    }
    jh_block_index_entry e;
    e.uncompressed_offset = g_block_uncompressed_start;
    e.uncompressed_size = g_block_fill;
    e.compressed_offset = g_block_file_offset;
    e.compressed_size = g_block_fill;
    jh_blocks_push(&e);
    g_block_fill = 0;
}

/* jh_append_books_text appends UTF-8 text to books.bin using fixed-size blocks. */
static void jh_append_books_text(const char *data, size_t len) {
    size_t pos = 0;
    while (pos < len) {
        if (g_block_fill == 0) {
            g_block_uncompressed_start = g_uncompressed_offset;
            long cur = ftell(g_books_fp);
            if (cur < 0) {
                jh_die("ftell failed");
            }
            g_block_file_offset = (jh_u64)cur;
        }
        size_t space = g_block_size - g_block_fill;
        size_t chunk = len - pos;
        if (chunk > space) {
            chunk = space;
        }
        memcpy(g_block_buf + g_block_fill, data + pos, chunk);
        g_block_fill += (jh_u32)chunk;
        pos += chunk;
        g_uncompressed_offset += (jh_u64)chunk;
        if (g_block_fill == g_block_size) {
            jh_flush_block();
        }
    }
}

/* jh_is_number_name checks for NNN.sqlite filenames and extracts the numeric book id. */
static int jh_is_number_name(const char *name, jh_u32 *out_id) {
    size_t len = strlen(name);
    if (len < 8) {
        return 0;
    }
    if (len < 8 || strcmp(name + len - 7, ".sqlite") != 0) {
        return 0;
    }
    size_t i;
    jh_u32 v = 0;
    for (i = 0; i < len - 7; ++i) {
        if (!isdigit((unsigned char)name[i])) {
            return 0;
        }
        v = v * 10u + (jh_u32)(name[i] - '0');
    }
    *out_id = v;
    return 1;
}

/* jh_book_file pairs a discovered SQLite path with its numeric book id. */
typedef struct {
    char *path;
    jh_u32 book_id;
} jh_book_file;

/* jh_book_file_cmp orders book files by id for deterministic processing. */
static int jh_book_file_cmp(const void *a, const void *b) {
    const jh_book_file *aa = (const jh_book_file *)a;
    const jh_book_file *bb = (const jh_book_file *)b;
    if (aa->book_id < bb->book_id) return -1;
    if (aa->book_id > bb->book_id) return 1;
    return 0;
}

/* jh_scan_books_dir finds all NNN.sqlite files under the books directory. */
static jh_book_file *jh_scan_books_dir(const char *dir_path, size_t *out_count) {
    DIR *dir = opendir(dir_path);
    struct dirent *de;
    jh_book_file *files = NULL;
    size_t count = 0;
    size_t cap = 0;
    if (!dir) {
        jh_die("cannot open books directory");
    }
    while ((de = readdir(dir)) != NULL) {
        jh_u32 id = 0;
        if (!jh_is_number_name(de->d_name, &id)) {
            continue;
        }
        if (count == cap) {
            size_t nc = cap ? cap * 2 : 16;
            files = (jh_book_file *)jh_xrealloc(files, nc, sizeof(jh_book_file));
            cap = nc;
        }
        size_t len_dir = strlen(dir_path);
        size_t len_name = strlen(de->d_name);
        size_t total = len_dir + 1 + len_name + 1;
        char *path = (char *)malloc(total);
        if (!path) {
            jh_die("out of memory");
        }
        memcpy(path, dir_path, len_dir);
        path[len_dir] = '/';
        memcpy(path + len_dir + 1, de->d_name, len_name);
        path[len_dir + 1 + len_name] = 0;
        files[count].path = path;
        files[count].book_id = id;
        count += 1;
    }
    closedir(dir);
    qsort(files, count, sizeof(jh_book_file), jh_book_file_cmp);
    *out_count = count;
    return files;
}

/* jh_load_titles_for_book reads title rows and creates chapter placeholders. */
static void jh_load_titles_for_book(sqlite3 *db, jh_u32 book_id, jh_book_tmp *book) {
    const char *sql = "SELECT tit, lvl, sub, id FROM title ORDER BY id";
    sqlite3_stmt *st = NULL;
    jh_u32 first_chapter_id = (jh_u32)g_chapters_count;
    jh_u32 chapter_number = 0;
    int rc = sqlite3_prepare_v2(db, sql, -1, &st, NULL);
    if (rc != SQLITE_OK) {
        return;
    }
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
        const unsigned char *tit = sqlite3_column_text(st, 0);
        int tit_len = sqlite3_column_bytes(st, 0);
        int id_val = sqlite3_column_int(st, 3);
        if (!tit || tit_len <= 0) {
            continue;
        }
        char *buf = (char *)malloc((size_t)tit_len + 1);
        if (!buf) {
            jh_die("out of memory");
        }
        memcpy(buf, tit, (size_t)tit_len);
        buf[tit_len] = 0;
        jh_u32 title_index = jh_titles_push(buf, 0);
        free(buf);
        jh_chapter_tmp *ch = jh_chapters_push_empty();
        ch->chapter_id = (jh_u32)(g_chapters_count - 1);
        ch->book_id = book_id;
        ch->chapter_number = ++chapter_number;
        ch->title_index = title_index;
        ch->first_page_id = 0;
        ch->page_count = 0;
        ch->start_page_number = (jh_u32)id_val;
    }
    sqlite3_finalize(st);
    book->first_chapter_id = first_chapter_id;
    book->chapter_count = (jh_u32)g_chapters_count - first_chapter_id;
}

/* jh_process_book_rows groups nass text by page and streams it into books.bin. */
static void jh_process_book_rows(sqlite3 *db, jh_u32 book_id, jh_book_tmp *book) {
    const char *sql = "SELECT nass, id, page, part FROM book ORDER BY page, id";
    sqlite3_stmt *st = NULL;
    int rc = sqlite3_prepare_v2(db, sql, -1, &st, NULL);
    jh_u32 first_page_id = (jh_u32)g_pages_count;
    jh_u32 current_page = 0;
    int have_page = 0;
    char *page_buf = NULL;
    size_t page_len = 0;
    size_t page_cap = 0;
    if (rc != SQLITE_OK) {
        return;
    }
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
        const unsigned char *nass = sqlite3_column_text(st, 0);
        int nass_len = sqlite3_column_bytes(st, 0);
        int page = sqlite3_column_int(st, 2);
        if (!nass || nass_len <= 0) {
            continue;
        }
        if (!have_page || (jh_u32)page != current_page) {
            if (have_page && page_len > 0) {
                jh_page_tmp p;
                p.book_id = book_id;
                p.page_number = current_page;
                p.text_offset = g_uncompressed_offset;
                p.text_length = (jh_u32)page_len;
                p.chapter_id = 0;
                jh_append_books_text(page_buf, page_len);
                jh_pages_push(&p);
                page_len = 0;
            }
            current_page = (jh_u32)page;
            have_page = 1;
        }
        if (page_len + (size_t)nass_len + 1 > page_cap) {
            size_t nc = page_cap ? page_cap * 2 : 1024;
            while (nc < page_len + (size_t)nass_len + 1) {
                nc *= 2;
            }
            page_buf = (char *)jh_xrealloc(page_buf, nc, 1);
            page_cap = nc;
        }
        memcpy(page_buf + page_len, nass, (size_t)nass_len);
        page_len += (size_t)nass_len;
        page_buf[page_len] = '\n';
        page_len += 1;
    }
    if (have_page && page_len > 0) {
        jh_page_tmp p;
        p.book_id = book_id;
        p.page_number = current_page;
        p.text_offset = g_uncompressed_offset;
        p.text_length = (jh_u32)page_len;
        p.chapter_id = 0;
        jh_append_books_text(page_buf, page_len);
        jh_pages_push(&p);
    }
    free(page_buf);
    sqlite3_finalize(st);
    book->first_page_id = first_page_id;
    book->page_count = (jh_u32)g_pages_count - first_page_id;
    if (book->page_count > 0) {
        jh_page_tmp *first = &g_pages[book->first_page_id];
        jh_page_tmp *last = &g_pages[book->first_page_id + book->page_count - 1];
        book->text_start_offset = first->text_offset;
        book->text_end_offset = last->text_offset + last->text_length;
    } else {
        book->text_start_offset = 0;
        book->text_end_offset = 0;
    }
}

/* jh_assign_chapters maps pages into chapter ranges based on starting page numbers. */
static void jh_assign_chapters(void) {
    size_t bi;
    for (bi = 0; bi < g_books_count; ++bi) {
        jh_book_tmp *b = &g_books[bi];
        if (b->chapter_count == 0 || b->page_count == 0) {
            continue;
        }
        jh_u32 c_first = b->first_chapter_id;
        jh_u32 c_last = c_first + b->chapter_count;
        jh_u32 p_first = b->first_page_id;
        jh_u32 p_last = p_first + b->page_count;
        jh_u32 ci;
        for (ci = c_first; ci < c_last; ++ci) {
            jh_chapter_tmp *ch = &g_chapters[ci];
            jh_u32 next_start = 0xffffffffu;
            jh_u32 cj;
            for (cj = ci + 1; cj < c_last; ++cj) {
                if (g_chapters[cj].start_page_number > ch->start_page_number) {
                    next_start = g_chapters[cj].start_page_number;
                    break;
                }
            }
            jh_u32 first_page_id = 0xffffffffu;
            jh_u32 page_count = 0;
            jh_u32 pi;
            for (pi = p_first; pi < p_last; ++pi) {
                jh_page_tmp *pg = &g_pages[pi];
                if (pg->page_number < ch->start_page_number) {
                    continue;
                }
                if (pg->page_number >= next_start) {
                    break;
                }
                if (first_page_id == 0xffffffffu) {
                    first_page_id = (jh_u32)pi;
                }
                pg->chapter_id = ch->chapter_id;
                page_count += 1;
            }
            if (first_page_id == 0xffffffffu) {
                ch->first_page_id = 0;
                ch->page_count = 0;
            } else {
                ch->first_page_id = first_page_id;
                ch->page_count = page_count;
            }
        }
    }
}

/* jh_write_books_bin opens books.bin and writes an initial header and state. */
static void jh_write_books_bin(const char *path) {
    jh_books_file_header hdr;
    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, "BKSB", 4);
    hdr.version = 1;
    hdr.compression = 0;
    hdr.block_size = g_block_size;
    hdr.block_count = 0;
    hdr.index_offset = 0;
    g_books_fp = fopen(path, "wb+");
    if (!g_books_fp) {
        jh_die("cannot open books.bin");
    }
    if (fwrite(&hdr, 1, sizeof(hdr), g_books_fp) != sizeof(hdr)) {
        jh_die("write header failed");
    }
    g_block_buf = (jh_u8 *)malloc(g_block_size);
    if (!g_block_buf) {
        jh_die("out of memory");
    }
    g_block_fill = 0;
    g_uncompressed_offset = 0;
    g_block_uncompressed_start = 0;
    long pos = ftell(g_books_fp);
    if (pos < 0) {
        jh_die("ftell failed");
    }
    g_block_file_offset = (jh_u64)pos;
}

/* jh_finalize_books_bin flushes remaining blocks and writes the final header and index. */
static void jh_finalize_books_bin(const char *path) {
    jh_books_file_header hdr;
    jh_u64 index_offset;
    jh_flush_block();
    long pos = ftell(g_books_fp);
    if (pos < 0) {
        jh_die("ftell failed");
    }
    index_offset = (jh_u64)pos;
    if (g_blocks_count > 0) {
        if (fwrite(g_blocks, sizeof(jh_block_index_entry), g_blocks_count, g_books_fp) != g_blocks_count) {
            jh_die("write block index failed");
        }
    }
    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, "BKSB", 4);
    hdr.version = 1;
    hdr.compression = 0;
    hdr.block_size = g_block_size;
    hdr.block_count = (jh_u64)g_blocks_count;
    hdr.index_offset = index_offset;
    if (fseek(g_books_fp, 0, SEEK_SET) != 0) {
        jh_die("fseek header failed");
    }
    if (fwrite(hdr.magic, 1, sizeof(hdr), g_books_fp) != sizeof(hdr)) {
        jh_die("rewrite header failed");
    }
    fclose(g_books_fp);
    g_books_fp = NULL;
    free(g_block_buf);
    g_block_buf = NULL;
}

/* jh_build_and_write_pages_idx writes pages.idx using collected page metadata. */
static void jh_build_and_write_pages_idx(const char *path) {
    FILE *fp = fopen(path, "wb");
    jh_pages_index_header hdr;
    jh_u32 i;
    jh_u32 b_index = 0;
    if (!fp) {
        jh_die("cannot open pages.idx");
    }
    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, "PGIX", 4);
    hdr.version = 1;
    hdr.page_count = (jh_u32)g_pages_count;
    if (fwrite(&hdr, 1, sizeof(hdr), fp) != sizeof(hdr)) {
        jh_die("write pages header failed");
    }
    for (i = 0; i < g_pages_count; ++i) {
        jh_page_tmp *pt = &g_pages[i];
        while (b_index + 1 < g_blocks_count) {
            jh_block_index_entry *b = &g_blocks[b_index];
            jh_block_index_entry *bn = &g_blocks[b_index + 1];
            jh_u64 end = b->uncompressed_offset + b->uncompressed_size;
            if (pt->text_offset >= end && pt->text_offset >= bn->uncompressed_offset) {
                b_index += 1;
            } else {
                break;
            }
        }
        jh_block_index_entry *blk = &g_blocks[b_index];
        jh_page_index_entry e;
        memset(&e, 0, sizeof(e));
        e.page_id = i;
        e.book_id = pt->book_id;
        e.chapter_id = pt->chapter_id;
        e.page_number = pt->page_number;
        e.block_id = b_index;
        e.offset_in_block = (jh_u32)(pt->text_offset - blk->uncompressed_offset);
        e.length = pt->text_length;
        if (fwrite(&e, 1, sizeof(e), fp) != sizeof(e)) {
            jh_die("write pages entry failed");
        }
    }
    fclose(fp);
}

/* jh_build_and_write_books_idx writes books.idx summarizing per-book ranges. */
static void jh_build_and_write_books_idx(const char *path) {
    FILE *fp = fopen(path, "wb");
    jh_books_index_header hdr;
    jh_u32 i;
    if (!fp) {
        jh_die("cannot open books.idx");
    }
    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, "BKIX", 4);
    hdr.version = 1;
    hdr.book_count = (jh_u32)g_books_count;
    if (fwrite(&hdr, 1, sizeof(hdr), fp) != sizeof(hdr)) {
        jh_die("write books header failed");
    }
    for (i = 0; i < g_books_count; ++i) {
        jh_book_tmp *bt = &g_books[i];
        jh_book_index_entry e;
        memset(&e, 0, sizeof(e));
        e.book_id = bt->book_id;
        e.first_chapter_id = bt->first_chapter_id;
        e.chapter_count = bt->chapter_count;
        e.first_page_id = bt->first_page_id;
        e.page_count = bt->page_count;
        e.title_index = bt->title_index;
        e.text_start_offset = bt->text_start_offset;
        e.text_end_offset = bt->text_end_offset;
        if (fwrite(&e, 1, sizeof(e), fp) != sizeof(e)) {
            jh_die("write books entry failed");
        }
    }
    fclose(fp);
}

/* jh_build_and_write_chapters_idx writes chapters.idx describing chapter ranges. */
static void jh_build_and_write_chapters_idx(const char *path) {
    FILE *fp = fopen(path, "wb");
    jh_chapters_index_header hdr;
    jh_u32 i;
    if (!fp) {
        jh_die("cannot open chapters.idx");
    }
    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, "CHIX", 4);
    hdr.version = 1;
    hdr.chapter_count = (jh_u32)g_chapters_count;
    if (fwrite(&hdr, 1, sizeof(hdr), fp) != sizeof(hdr)) {
        jh_die("write chapters header failed");
    }
    for (i = 0; i < g_chapters_count; ++i) {
        jh_chapter_tmp *ct = &g_chapters[i];
        jh_chapter_index_entry e;
        memset(&e, 0, sizeof(e));
        e.chapter_id = ct->chapter_id;
        e.book_id = ct->book_id;
        e.chapter_number = ct->chapter_number;
        e.first_page_id = ct->first_page_id;
        e.page_count = ct->page_count;
        e.title_index = ct->title_index;
        if (fwrite(&e, 1, sizeof(e), fp) != sizeof(e)) {
            jh_die("write chapters entry failed");
        }
    }
    fclose(fp);
}

/* jh_build_and_write_titles_bin packs all chapter titles into titles.bin. */
static void jh_build_and_write_titles_bin(const char *path) {
    FILE *fp = fopen(path, "wb");
    jh_titles_file_header hdr;
    jh_title_entry *entries;
    jh_u64 strings_offset;
    jh_u64 current_offset = 0;
    size_t i;
    if (!fp) {
        jh_die("cannot open titles.bin");
    }
    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, "TTLB", 4);
    hdr.version = 1;
    hdr.title_count = (jh_u32)g_titles_count;
    strings_offset = sizeof(jh_titles_file_header) + sizeof(jh_title_entry) * (jh_u64)g_titles_count;
    hdr.strings_offset = strings_offset;
    entries = (jh_title_entry *)malloc(sizeof(jh_title_entry) * g_titles_count);
    if (!entries) {
        jh_die("out of memory");
    }
    for (i = 0; i < g_titles_count; ++i) {
        size_t len = strlen(g_titles[i].text);
        entries[i].offset = current_offset;
        entries[i].length = (jh_u32)len;
        entries[i].flags = g_titles[i].flags;
        current_offset += (jh_u64)len;
    }
    if (fwrite(&hdr, 1, sizeof(hdr), fp) != sizeof(hdr)) {
        jh_die("write titles header failed");
    }
    if (g_titles_count > 0) {
        if (fwrite(entries, sizeof(jh_title_entry), g_titles_count, fp) != g_titles_count) {
            jh_die("write title entries failed");
        }
    }
    for (i = 0; i < g_titles_count; ++i) {
        size_t len = strlen(g_titles[i].text);
        if (len == 0) {
            continue;
        }
        if (fwrite(g_titles[i].text, 1, len, fp) != len) {
            jh_die("write title string failed");
        }
    }
    free(entries);
    fclose(fp);
}

/* main exports all SQLite book databases under the books directory to static binary files. */
int main(int argc, char **argv) {
    const char *books_dir = "books";
    size_t file_count = 0;
    jh_book_file *files;
    size_t i;
    if (argc > 1) {
        books_dir = argv[1];
    }
    if (sqlite3_initialize() != SQLITE_OK) {
        jh_die("sqlite3_initialize failed");
    }
    files = jh_scan_books_dir(books_dir, &file_count);
    jh_write_books_bin("books.bin");
    for (i = 0; i < file_count; ++i) {
        sqlite3 *db = NULL;
        jh_book_tmp *book;
        int rc = sqlite3_open_v2(files[i].path, &db, SQLITE_OPEN_READONLY, NULL);
        if (rc != SQLITE_OK) {
            sqlite3_close(db);
            continue;
        }
        book = jh_books_push_empty();
        book->book_id = files[i].book_id;
        book->first_page_id = 0;
        book->page_count = 0;
        book->first_chapter_id = 0;
        book->chapter_count = 0;
        book->text_start_offset = 0;
        book->text_end_offset = 0;
        book->title_index = 0;
        jh_load_titles_for_book(db, files[i].book_id, book);
        jh_process_book_rows(db, files[i].book_id, book);
        sqlite3_close(db);
    }
    jh_assign_chapters();
    jh_finalize_books_bin("books.bin");
    jh_build_and_write_pages_idx("pages.idx");
    jh_build_and_write_books_idx("books.idx");
    jh_build_and_write_chapters_idx("chapters.idx");
    jh_build_and_write_titles_bin("titles.bin");
    for (i = 0; i < file_count; ++i) {
        free(files[i].path);
    }
    free(files);
    sqlite3_shutdown();
    return 0;
}
