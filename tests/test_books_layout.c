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

    printf("[books_layout] All real-books checks passed\n");
    return 0;
}
