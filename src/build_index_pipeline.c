#include <stdio.h>
#include <stdlib.h>

static void jh_die_pipeline(const char *msg, int rc) {
    fprintf(stderr, "[build_index_pipeline] %s (rc=%d)\n", msg, rc);
    fflush(stderr);
    exit(1);
}

static void jh_run_cmd(const char *cmd, const char *label) {
    int rc = system(cmd);
    if (rc != 0) {
        jh_die_pipeline(label, rc);
    }
    printf("[build_index_pipeline] %s completed\n", label);
}

int main(int argc, char **argv) {
    const char *books_dir = "../../books";
    char cmd[512];
    if (argc > 1) {
        books_dir = argv[1];
    }
    snprintf(cmd, sizeof(cmd), "./build_from_sqlite %s", books_dir);
    jh_run_cmd(cmd, "build_from_sqlite");
    jh_run_cmd("./build_occurrences", "build_occurrences");
    jh_run_cmd("./sort_occurrences", "sort_occurrences");
    jh_run_cmd("./build_postings", "build_postings");
    jh_run_cmd("./build_words_index", "build_words_index");
    return 0;
}

