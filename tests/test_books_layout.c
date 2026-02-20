#include "jamharah/index_format.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static void die(const char *msg) {
    fprintf(stderr, "[books_layout] ERROR: %s\n", msg);
    fflush(stderr);
    exit(1);
}

static void mkdir_if_not_exists(const char *path) {
    struct stat st;
    if (stat(path, &st) == 0) {
        if (!S_ISDIR(st.st_mode)) {
            die("path exists and is not a directory");
        }
        return;
    }
    if (mkdir(path, 0700) != 0) {
        perror("mkdir");
        die("mkdir failed");
    }
}

static void run_exporter(const char *books_subdir) {
    char cmd[512];
    printf("[books_layout] Running build_from_sqlite on '%s'\n", books_subdir);
    fflush(stdout);
    snprintf(cmd, sizeof(cmd), "../build_from_sqlite %s", books_subdir);
    int rc = system(cmd);
    if (rc != 0) {
        fprintf(stderr, "[books_layout] build_from_sqlite returned %d\n", rc);
        die("exporter failed");
    }
    printf("[books_layout] build_from_sqlite completed successfully\n");
}

static void check_books_header(const char *path, jh_books_file_header *hdr_out) {
    jh_books_file_header hdr;
    int rc = jh_read_books_file_header(path, &hdr);
    if (rc != 0) {
        die("jh_read_books_file_header failed");
    }
    if (memcmp(hdr.magic, "BKSB", 4) != 0) {
        die("books.bin magic mismatch");
    }
    if (hdr.version != 1) {
        die("books.bin version mismatch");
    }
    if (hdr.block_size == 0 || hdr.block_count == 0) {
        die("books.bin blocks not recorded");
    }
    if (hdr.index_offset == 0) {
        die("books.bin index_offset is zero");
    }
    printf("[books_layout] books.bin: block_size=%u block_count=%llu\n",
           (unsigned)hdr.block_size,
           (unsigned long long)hdr.block_count);
    *hdr_out = hdr;
    printf("[books_layout] check_books_header passed\n");
}

static void check_books_index(const char *path) {
    jh_books_index_header hdr;
    FILE *f;
    jh_book_index_entry *entries;
    jh_u32 i;

    if (jh_read_books_index_header(path, &hdr) != 0) {
        die("jh_read_books_index_header failed");
    }
    if (memcmp(hdr.magic, "BKIX", 4) != 0) {
        die("books.idx magic mismatch");
    }
    if (hdr.version != 1) {
        die("books.idx version mismatch");
    }
    if (hdr.book_count == 0) {
        die("books.idx has zero books");
    }

    printf("[books_layout] books.idx: book_count=%u\n", (unsigned)hdr.book_count);

    f = fopen(path, "rb");
    if (!f) {
        die("open books.idx failed");
    }
    if (fseek(f, (long)sizeof(jh_books_index_header), SEEK_SET) != 0) {
        fclose(f);
        die("seek books.idx failed");
    }
    entries = (jh_book_index_entry *)malloc(sizeof(jh_book_index_entry) * hdr.book_count);
    if (!entries) {
        fclose(f);
        die("alloc book entries failed");
    }
    if (fread(entries, sizeof(jh_book_index_entry), hdr.book_count, f) != hdr.book_count) {
        fclose(f);
        free(entries);
        die("read book entries failed");
    }
    fclose(f);

    for (i = 0; i < hdr.book_count; ++i) {
        jh_book_index_entry *e = &entries[i];
        if (e->book_id == 0) {
            die("book_id == 0");
        }
        if (e->text_start_offset >= e->text_end_offset) {
            die("book text offsets invalid");
        }
    }

    free(entries);
    printf("[books_layout] check_books_index passed\n");
}

static void check_pages_index(const char *path, const jh_books_file_header *books_hdr) {
    jh_pages_index_header hdr;
    FILE *f;
    jh_page_index_entry *entries;
    jh_u32 i;
    jh_u32 page_count;
    jh_block_index_entry *blocks;
    FILE *bf;

    if (jh_read_pages_index_header(path, &hdr) != 0) {
        die("jh_read_pages_index_header failed");
    }
    if (memcmp(hdr.magic, "PGIX", 4) != 0) {
        die("pages.idx magic mismatch");
    }
    if (hdr.version != 1) {
        die("pages.idx version mismatch");
    }
    if (hdr.page_count == 0) {
        die("pages.idx has zero pages");
    }

    page_count = hdr.page_count;
    printf("[books_layout] pages.idx: page_count=%u\n", (unsigned)page_count);

    f = fopen(path, "rb");
    if (!f) {
        die("open pages.idx failed");
    }
    if (fseek(f, (long)sizeof(jh_pages_index_header), SEEK_SET) != 0) {
        fclose(f);
        die("seek pages.idx failed");
    }
    entries = (jh_page_index_entry *)malloc(sizeof(jh_page_index_entry) * page_count);
    if (!entries) {
        fclose(f);
        die("alloc page entries failed");
    }
    if (fread(entries, sizeof(jh_page_index_entry), page_count, f) != page_count) {
        fclose(f);
        free(entries);
        die("read page entries failed");
    }
    fclose(f);

    bf = fopen("books.bin", "rb");
    if (!bf) {
        free(entries);
        die("open books.bin failed");
    }
    blocks = (jh_block_index_entry *)malloc(sizeof(jh_block_index_entry) * books_hdr->block_count);
    if (!blocks) {
        fclose(bf);
        free(entries);
        die("alloc block index failed");
    }
    if (fseek(bf, (long)books_hdr->index_offset, SEEK_SET) != 0) {
        fclose(bf);
        free(blocks);
        free(entries);
        die("seek books.bin index failed");
    }
    if (fread(blocks, sizeof(jh_block_index_entry), (size_t)books_hdr->block_count, bf) != books_hdr->block_count) {
        fclose(bf);
        free(blocks);
        free(entries);
        die("read books block index failed");
    }
    fclose(bf);

    for (i = 0; i < page_count; ++i) {
        jh_page_index_entry *e = &entries[i];
        if (e->book_id == 0) {
            die("page book_id == 0");
        }
        if (e->block_id >= books_hdr->block_count) {
            die("page block_id out of range");
        }
        if (e->length == 0) {
            die("page length == 0");
        }
    }

    free(entries);
    free(blocks);
    printf("[books_layout] check_pages_index passed\n");
}

static void check_chapters_index(const char *path) {
    jh_chapters_index_header hdr;
    FILE *f;
    jh_chapter_index_entry *entries;
    jh_u32 i;

    if (jh_read_chapters_index_header(path, &hdr) != 0) {
        die("jh_read_chapters_index_header failed");
    }
    if (memcmp(hdr.magic, "CHIX", 4) != 0) {
        die("chapters.idx magic mismatch");
    }
    if (hdr.version != 1) {
        die("chapters.idx version mismatch");
    }

    printf("[books_layout] chapters.idx: chapter_count=%u\n", (unsigned)hdr.chapter_count);

    if (hdr.chapter_count == 0) {
        return;
    }

    f = fopen(path, "rb");
    if (!f) {
        die("open chapters.idx failed");
    }
    if (fseek(f, (long)sizeof(jh_chapters_index_header), SEEK_SET) != 0) {
        fclose(f);
        die("seek chapters.idx failed");
    }
    entries = (jh_chapter_index_entry *)malloc(sizeof(jh_chapter_index_entry) * hdr.chapter_count);
    if (!entries) {
        fclose(f);
        die("alloc chapter entries failed");
    }
    if (fread(entries, sizeof(jh_chapter_index_entry), hdr.chapter_count, f) != hdr.chapter_count) {
        fclose(f);
        free(entries);
        die("read chapter entries failed");
    }
    fclose(f);

    for (i = 0; i < hdr.chapter_count; ++i) {
        jh_chapter_index_entry *e = &entries[i];
        if (e->book_id == 0) {
            free(entries);
            die("chapter book_id == 0");
        }
    }

    free(entries);
    printf("[books_layout] check_chapters_index passed\n");
}

static void check_titles_bin(const char *path) {
    jh_titles_file_header hdr;
    FILE *f;
    jh_title_entry *entries;
    jh_u32 i;

    if (jh_read_titles_file_header(path, &hdr) != 0) {
        die("jh_read_titles_file_header failed");
    }
    if (memcmp(hdr.magic, "TTLB", 4) != 0) {
        die("titles.bin magic mismatch");
    }
    if (hdr.version != 1) {
        die("titles.bin version mismatch");
    }

    printf("[books_layout] titles.bin: title_count=%u\n", (unsigned)hdr.title_count);

    if (hdr.title_count == 0) {
        return;
    }

    f = fopen(path, "rb");
    if (!f) {
        die("open titles.bin failed");
    }
    if (fseek(f, (long)sizeof(jh_titles_file_header), SEEK_SET) != 0) {
        fclose(f);
        die("seek titles.bin failed");
    }

    entries = (jh_title_entry *)malloc(sizeof(jh_title_entry) * hdr.title_count);
    if (!entries) {
        fclose(f);
        die("alloc title entries failed");
    }
    if (fread(entries, sizeof(jh_title_entry), hdr.title_count, f) != hdr.title_count) {
        fclose(f);
        free(entries);
        die("read title entries failed");
    }

    for (i = 0; i < hdr.title_count; ++i) {
        jh_title_entry *e = &entries[i];
        char buf[256];
        size_t to_read;

        if (fseek(f, (long)hdr.strings_offset + (long)e->offset, SEEK_SET) != 0) {
            fclose(f);
            free(entries);
            die("seek title string failed");
        }
        to_read = e->length;
        if (to_read >= sizeof(buf)) {
            to_read = sizeof(buf) - 1;
        }
        if (to_read > 0) {
            if (fread(buf, 1, to_read, f) != to_read) {
                fclose(f);
                free(entries);
                die("read title string failed");
            }
            buf[to_read] = 0;
            if (i < 3) {
                printf("[books_layout] title[%u]=%s\n", (unsigned)i, buf);
            }
        }
    }

    free(entries);
    fclose(f);
    printf("[books_layout] check_titles_bin passed\n");
}

static void run_occurrence_tools(void) {
    char cmd[256];
    int rc;
    snprintf(cmd, sizeof(cmd), "../build_occurrences");
    rc = system(cmd);
    if (rc != 0) {
        fprintf(stderr, "[books_layout] build_occurrences returned %d\n", rc);
        die("build_occurrences failed");
    }
    printf("[books_layout] build_occurrences completed successfully\n");
    snprintf(cmd, sizeof(cmd), "../sort_occurrences");
    rc = system(cmd);
    if (rc != 0) {
        fprintf(stderr, "[books_layout] sort_occurrences returned %d\n", rc);
        die("sort_occurrences failed");
    }
    printf("[books_layout] sort_occurrences completed successfully\n");
    snprintf(cmd, sizeof(cmd), "../build_postings");
    rc = system(cmd);
    if (rc != 0) {
        fprintf(stderr, "[books_layout] build_postings returned %d\n", rc);
        die("build_postings failed");
    }
    printf("[books_layout] build_postings completed successfully\n");
    snprintf(cmd, sizeof(cmd), "../build_words_index");
    rc = system(cmd);
    if (rc != 0) {
        fprintf(stderr, "[books_layout] build_words_index returned %d\n", rc);
        die("build_words_index failed");
    }
    printf("[books_layout] build_words_index completed successfully\n");
}

static void check_occurrences_sorted(const char *pages_idx_path, const char *occ_path) {
    jh_pages_index_header ph;
    FILE *pf;
    FILE *of;
    jh_occurrence_record prev;
    jh_occurrence_record cur;
    int have_prev = 0;
    if (jh_read_pages_index_header(pages_idx_path, &ph) != 0) {
        die("jh_read_pages_index_header failed in check_occurrences_sorted");
    }
    pf = fopen(pages_idx_path, "rb");
    if (!pf) {
        die("open pages.idx failed in check_occurrences_sorted");
    }
    fclose(pf);
    of = fopen(occ_path, "rb");
    if (!of) {
        die("open occurrences file failed");
    }
    for (;;) {
        size_t n = fread(&cur, sizeof(jh_occurrence_record), 1, of);
        if (n == 0) {
            break;
        }
        if (cur.page_id >= ph.page_count) {
            fclose(of);
            die("occurrence page_id out of range");
        }
        if (have_prev) {
            if (prev.word_hash > cur.word_hash) {
                fclose(of);
                die("occurrences not sorted by word_hash");
            }
            if (prev.word_hash == cur.word_hash && prev.page_id > cur.page_id) {
                fclose(of);
                die("occurrences not sorted by page_id");
            }
            if (prev.word_hash == cur.word_hash && prev.page_id == cur.page_id && prev.position > cur.position) {
                fclose(of);
                die("occurrences not sorted by position");
            }
        }
        prev = cur;
        have_prev = 1;
    }
    fclose(of);
    printf("[books_layout] occurrences sorted check passed\n");
}

static void check_postings_bin(const char *occ_path, const char *postings_path) {
    jh_postings_file_header hdr;
    FILE *f;
    jh_occurrence_record rec;
    jh_u64 count = 0;
    if (jh_read_postings_file_header(postings_path, &hdr) != 0) {
        die("jh_read_postings_file_header failed");
    }
    if (memcmp(hdr.magic, "PSTB", 4) != 0) {
        die("postings.bin magic mismatch");
    }
    if (hdr.version != 1) {
        die("postings.bin version mismatch");
    }
    f = fopen(occ_path, "rb");
    if (!f) {
        die("open occurrences.sorted.tmp failed");
    }
    for (;;) {
        size_t n = fread(&rec, sizeof(jh_occurrence_record), 1, f);
        if (n == 0) {
            break;
        }
        count += 1;
    }
    fclose(f);
    if (hdr.total_postings != count) {
        die("postings.bin total_postings mismatch");
    }
    printf("[books_layout] postings.bin header and counts check passed\n");
}

static void check_words_index(const char *occ_path, const char *dict_path) {
    jh_word_dict_header wh;
    FILE *df;
    FILE *of;
    jh_word_dict_entry prev;
    jh_word_dict_entry cur;
    int have_prev = 0;
    jh_occurrence_record occ;
    jh_u64 distinct_words = 0;
    jh_u64 prev_hash = 0;

    df = fopen(dict_path, "rb");
    if (!df) {
        die("open words.idx failed");
    }
    if (fread(&wh, 1, sizeof(wh), df) != sizeof(wh)) {
        fclose(df);
        die("read words.idx header failed");
    }
    if (memcmp(wh.magic, "WDIX", 4) != 0) {
        fclose(df);
        die("words.idx magic mismatch");
    }
    if (wh.version != 1) {
        fclose(df);
        die("words.idx version mismatch");
    }

    while (1) {
        size_t n = fread(&cur, 1, sizeof(cur), df);
        if (n == 0) {
            break;
        }
        if (n != sizeof(cur)) {
            fclose(df);
            die("partial words.idx entry read");
        }
        if (have_prev) {
            if (prev.word_hash > cur.word_hash) {
                fclose(df);
                die("words.idx not sorted by word_hash");
            }
        }
        prev = cur;
        have_prev = 1;
    }
    fclose(df);

    of = fopen(occ_path, "rb");
    if (!of) {
        die("open occurrences.sorted.tmp failed in check_words_index");
    }
    while (1) {
        size_t n = fread(&occ, sizeof(jh_occurrence_record), 1, of);
        if (n == 0) {
            break;
        }
        if (!distinct_words || occ.word_hash != prev_hash) {
            distinct_words += 1;
            prev_hash = occ.word_hash;
        }
    }
    fclose(of);

    if (wh.entry_count != distinct_words) {
        die("words.idx entry_count mismatch");
    }

    printf("[books_layout] words.idx header and sorting check passed\n");
}

int main(void) {
    const char *run_dir = "books_layout_run";
    jh_books_file_header books_hdr;

    printf("[books_layout] Starting real-books layout test\n");
    fflush(stdout);

    mkdir_if_not_exists(run_dir);

    if (chdir(run_dir) != 0) {
        perror("chdir");
        die("chdir run_dir failed");
    }

    run_exporter("../../books");

    printf("[books_layout] Checking books.bin header\n");
    check_books_header("books.bin", &books_hdr);
    printf("[books_layout] Checking books.idx\n");
    check_books_index("books.idx");
    printf("[books_layout] Checking pages.idx\n");
    check_pages_index("pages.idx", &books_hdr);
    printf("[books_layout] Checking chapters.idx\n");
    check_chapters_index("chapters.idx");
    printf("[books_layout] Checking titles.bin\n");
    check_titles_bin("titles.bin");
    printf("[books_layout] Building and checking occurrences\n");
    run_occurrence_tools();
    check_occurrences_sorted("pages.idx", "occurrences.sorted.tmp");
    printf("[books_layout] Checking postings.bin\n");
    check_postings_bin("occurrences.sorted.tmp", "postings.bin");
    printf("[books_layout] Checking words.idx\n");
    check_words_index("occurrences.sorted.tmp", "words.idx");

    printf("[books_layout] All real-books checks passed\n");
    return 0;
}
