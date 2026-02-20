#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static void jh_die_pipeline(const char *msg, int rc) {
    fprintf(stderr, "[indexer] %s (rc=%d)\n", msg, rc);
    fflush(stderr);
    exit(1);
}

static double jh_wall_seconds(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0.0;
    }
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1000000000.0;
}

static double jh_run_cmd(const char *cmd, const char *label) {
    double start = jh_wall_seconds();
    int rc = system(cmd);
    double end = jh_wall_seconds();
    if (rc != 0) {
        jh_die_pipeline(label, rc);
    }
    printf("[indexer] %s completed in %.3f s\n", label, end - start);
    return end - start;
}

int main(int argc, char **argv) {
    const char *books_dir = "../../books";
    char cmd[512];
    double t_build_from_sqlite;
    double t_build_occurrences;
    double t_sort_occurrences;
    double t_build_postings;
    double t_build_words_index;
    double total;

    if (argc > 1) {
        books_dir = argv[1];
    }
    snprintf(cmd, sizeof(cmd), "./build_from_sqlite %s", books_dir);
    t_build_from_sqlite = jh_run_cmd(cmd, "build_from_sqlite");
    t_build_occurrences = jh_run_cmd("./build_occurrences", "build_occurrences");
    t_sort_occurrences = jh_run_cmd("./sort_occurrences", "sort_occurrences");
    t_build_postings = jh_run_cmd("./build_postings", "build_postings");
    t_build_words_index = jh_run_cmd("./build_words_index", "build_words_index");
    total = t_build_from_sqlite + t_build_occurrences + t_sort_occurrences + t_build_postings + t_build_words_index;
    printf("[indexer] total time %.3f s\n", total);
    return 0;
}
