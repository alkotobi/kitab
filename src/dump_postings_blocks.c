#include "jamharah/index_format.h"
#include <stdio.h>
#include <stdlib.h>

#ifdef JH_HAVE_ZSTD
#include <zstd.h>
#endif

static void jh_die_dump(const char *msg) {
    fprintf(stderr, "[dump_postings_blocks] %s\n", msg);
    fflush(stderr);
    exit(1);
}

static jh_u32 jh_read_u32_le_local(const jh_u8 *p) {
    return (jh_u32)p[0]
        | ((jh_u32)p[1] << 8)
        | ((jh_u32)p[2] << 16)
        | ((jh_u32)p[3] << 24);
}

int main(int argc, char **argv) {
    const char *path = "postings.bin";
    jh_postings_file_header hdr;
    FILE *f;
    jh_u64 offset;
    jh_u64 block_index = 0;
    jh_u64 total_comp = 0;
    jh_u64 total_uncomp = 0;
    int compressed_flag = 0;

    if (argc > 1) {
        path = argv[1];
    }

    if (jh_read_postings_file_header(path, &hdr) != 0) {
        jh_die_dump("jh_read_postings_file_header failed");
    }
    if (path == NULL) {
        return 0;
    }

    compressed_flag = (hdr.flags & 1u) ? 1 : 0;

    f = fopen(path, "rb");
    if (!f) {
        jh_die_dump("open postings.bin failed");
    }
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        jh_die_dump("fseek end failed");
    }
    {
        jh_u64 file_size = (jh_u64)ftell(f);
        if (file_size < hdr.blocks_data_offset) {
            fclose(f);
            jh_die_dump("file too small for blocks_data_offset");
        }
        offset = hdr.blocks_data_offset;
        if (fseek(f, (long)offset, SEEK_SET) != 0) {
            fclose(f);
            jh_die_dump("seek to blocks_data_offset failed");
        }
        printf("postings file: %s\n", path);
        printf("version: %u flags: %u total_postings: %llu\n",
               (unsigned)hdr.version,
               (unsigned)hdr.flags,
               (unsigned long long)hdr.total_postings);
        printf("blocks_data_offset: %llu file_size: %llu\n",
               (unsigned long long)hdr.blocks_data_offset,
               (unsigned long long)file_size);
        if (compressed_flag) {
            printf("compression: ZSTD (flag bit 0 set)\n");
        } else {
            printf("compression: none (flag bit 0 clear)\n");
        }
        printf("index  offset  comp_bytes  uncomp_bytes  ratio\n");
        while (offset + 4 <= file_size) {
            jh_u8 len_buf[4];
            jh_u32 block_size;
            size_t n = fread(len_buf, 1, 4, f);
            if (n != 4) {
                break;
            }
            block_size = jh_read_u32_le_local(len_buf);
            if (block_size == 0 || offset + 4 + block_size > file_size) {
                break;
            }
            total_comp += block_size;
            {
                double ratio_value = 1.0;
                jh_u64 uncomp_bytes = 0;
#ifdef JH_HAVE_ZSTD
                if (compressed_flag) {
                    jh_u8 *buf = (jh_u8 *)malloc(block_size);
                    if (!buf) {
                        fclose(f);
                        jh_die_dump("malloc failed");
                    }
                    if (fread(buf, 1, block_size, f) != block_size) {
                        free(buf);
                        fclose(f);
                        jh_die_dump("read block failed");
                    }
                    {
                        unsigned long long content_size = ZSTD_getFrameContentSize(buf, block_size);
                        if (content_size == ZSTD_CONTENTSIZE_ERROR || content_size == ZSTD_CONTENTSIZE_UNKNOWN) {
                            uncomp_bytes = 0;
                        } else {
                            uncomp_bytes = (jh_u64)content_size;
                        }
                    }
                    free(buf);
                    if (uncomp_bytes > 0) {
                        total_uncomp += uncomp_bytes;
                        ratio_value = uncomp_bytes ? (double)block_size / (double)uncomp_bytes : 0.0;
                    }
                } else
#endif
                {
                    if (fseek(f, (long)block_size, SEEK_CUR) != 0) {
                        fclose(f);
                        jh_die_dump("seek over block failed");
                    }
                }
                printf("%6llu  %10llu  %10u  %12llu  %.4f\n",
                       (unsigned long long)block_index,
                       (unsigned long long)offset,
                       (unsigned)block_size,
                       (unsigned long long)uncomp_bytes,
                       ratio_value);
            }
            offset += 4 + (jh_u64)block_size;
            if (fseek(f, (long)offset, SEEK_SET) != 0) {
                break;
            }
            block_index += 1;
        }
        printf("total compressed bytes: %llu\n", (unsigned long long)total_comp);
        if (total_uncomp > 0) {
            double ratio_total = (double)total_comp / (double)total_uncomp;
            printf("total uncompressed bytes: %llu\n", (unsigned long long)total_uncomp);
            printf("overall ratio: %.4f\n", ratio_total);
        }
        fclose(f);
    }

    return 0;
}

