#include "jamharah/index_format.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

static void jh_die_sort(const char *msg) {
    fprintf(stderr, "[sort_occurrences] %s\n", msg);
    fflush(stderr);
    exit(1);
}

static int jh_occurrence_cmp(const void *a, const void *b) {
    const jh_occurrence_record *ra = (const jh_occurrence_record *)a;
    const jh_occurrence_record *rb = (const jh_occurrence_record *)b;
    if (ra->word_hash < rb->word_hash) return -1;
    if (ra->word_hash > rb->word_hash) return 1;
    if (ra->page_id < rb->page_id) return -1;
    if (ra->page_id > rb->page_id) return 1;
    if (ra->position < rb->position) return -1;
    if (ra->position > rb->position) return 1;
    return 0;
}

typedef struct {
    FILE *fp;
    char *path;
    jh_occurrence_record current;
    int has_current;
} jh_run_file;

static void jh_remove_runs(jh_run_file *runs, size_t run_count) {
    size_t i;
    if (!runs) return;
    for (i = 0; i < run_count; ++i) {
        if (runs[i].fp) {
            fclose(runs[i].fp);
        }
        if (runs[i].path) {
            remove(runs[i].path);
            free(runs[i].path);
        }
    }
    free(runs);
}

static void jh_sort_occurrences(const char *in_path, const char *out_path) {
    FILE *in_fp;
    jh_occurrence_record *buf;
    size_t buf_cap;
    size_t run_count = 0;
    size_t run_cap = 0;
    jh_run_file *runs = NULL;
    size_t i;
    FILE *out_fp;

    in_fp = fopen(in_path, "rb");
    if (!in_fp) {
        jh_die_sort("open input occurrences file failed");
    }

    {
        size_t max_bytes = 64 * 1024 * 1024;
        size_t rec_size = sizeof(jh_occurrence_record);
        buf_cap = max_bytes / rec_size;
        if (buf_cap == 0) {
            buf_cap = 1;
        }
    }

    buf = (jh_occurrence_record *)malloc(sizeof(jh_occurrence_record) * buf_cap);
    if (!buf) {
        fclose(in_fp);
        jh_die_sort("alloc buffer failed");
    }

    for (;;) {
        size_t nread = fread(buf, sizeof(jh_occurrence_record), buf_cap, in_fp);
        if (nread == 0) {
            break;
        }
        qsort(buf, nread, sizeof(jh_occurrence_record), jh_occurrence_cmp);

        if (run_count == run_cap) {
            size_t nc = run_cap ? run_cap * 2 : 16;
            jh_run_file *nruns = (jh_run_file *)realloc(runs, sizeof(jh_run_file) * nc);
            if (!nruns) {
                free(buf);
                fclose(in_fp);
                jh_remove_runs(runs, run_count);
                jh_die_sort("alloc runs array failed");
            }
            runs = nruns;
            run_cap = nc;
        }

        {
            char tmp_name[64];
            FILE *rf;
            size_t len;
            snprintf(tmp_name, sizeof(tmp_name), "occ_run_%06zu.tmp", run_count);
            rf = fopen(tmp_name, "wb");
            if (!rf) {
                free(buf);
                fclose(in_fp);
                jh_remove_runs(runs, run_count);
                jh_die_sort("open run file failed");
            }
            len = fwrite(buf, sizeof(jh_occurrence_record), nread, rf);
            if (len != nread) {
                fclose(rf);
                free(buf);
                fclose(in_fp);
                jh_remove_runs(runs, run_count);
                jh_die_sort("write run file failed");
            }
            fclose(rf);

            runs[run_count].fp = fopen(tmp_name, "rb");
            if (!runs[run_count].fp) {
                free(buf);
                fclose(in_fp);
                jh_remove_runs(runs, run_count);
                jh_die_sort("reopen run file failed");
            }
            runs[run_count].path = (char *)malloc(strlen(tmp_name) + 1);
            if (!runs[run_count].path) {
                free(buf);
                fclose(in_fp);
                jh_remove_runs(runs, run_count + 1);
                jh_die_sort("alloc run path failed");
            }
            strcpy(runs[run_count].path, tmp_name);
            runs[run_count].has_current = 0;
            run_count += 1;
        }
    }

    free(buf);
    fclose(in_fp);

    out_fp = fopen(out_path, "wb");
    if (!out_fp) {
        jh_remove_runs(runs, run_count);
        jh_die_sort("open output file failed");
    }

    for (i = 0; i < run_count; ++i) {
        size_t n = fread(&runs[i].current, sizeof(jh_occurrence_record), 1, runs[i].fp);
        runs[i].has_current = (n == 1);
    }

    for (;;) {
        ssize_t best = -1;
        for (i = 0; i < run_count; ++i) {
            if (!runs[i].has_current) {
                continue;
            }
            if (best < 0) {
                best = (ssize_t)i;
            } else {
                if (jh_occurrence_cmp(&runs[i].current, &runs[best].current) < 0) {
                    best = (ssize_t)i;
                }
            }
        }
        if (best < 0) {
            break;
        }

        if (fwrite(&runs[best].current, sizeof(jh_occurrence_record), 1, out_fp) != 1) {
            fclose(out_fp);
            jh_remove_runs(runs, run_count);
            jh_die_sort("write output record failed");
        }

        {
            size_t n = fread(&runs[best].current, sizeof(jh_occurrence_record), 1, runs[best].fp);
            if (n == 1) {
                runs[best].has_current = 1;
            } else {
                runs[best].has_current = 0;
            }
        }
    }

    fclose(out_fp);
    jh_remove_runs(runs, run_count);
}

int main(int argc, char **argv) {
    const char *in_path = "occurrences.tmp";
    const char *out_path = "occurrences.sorted.tmp";
    if (argc > 1) {
        in_path = argv[1];
    }
    if (argc > 2) {
        out_path = argv[2];
    }
    jh_sort_occurrences(in_path, out_path);
    return 0;
}

