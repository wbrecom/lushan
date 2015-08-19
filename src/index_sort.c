#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdint.h>
#include <stdarg.h>

typedef struct {
    uint64_t key;
    uint64_t pos;
} idx_t;

static int cmp_idx_key(const void* p1, const void* p2)
{
    uint64_t key1 = ((idx_t*)p1)->key;
    uint64_t key2 = ((idx_t*)p2)->key;

    return key1 >= key2 ? 1 : -1;
}

static void tcpl_error(char* fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    fprintf(stdout, "error: ");
    vfprintf(stdout, fmt, args);
    fprintf(stdout, "\n");
    va_end(args);

    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[])
{
    if (argc != 2) {
	tcpl_error("usage: %s idx_file", argv[0]);
    }

    const char* idx_file = argv[1];
    
    struct stat st;
    if (stat(idx_file, &st) == -1 ||
	(st.st_size % sizeof(idx_t) != 0)) {
	tcpl_error("bad idx file: %s", idx_file);
    }
    FILE* idx_fp = fopen(idx_file, "r+b");
    if (idx_fp == NULL) {
	tcpl_error("open %s fail", idx_file);
    }
    idx_t* idx = (idx_t*)malloc(st.st_size);
    if (idx == NULL) {
	tcpl_error("malloc fail");
    }
    uint32_t idx_num = st.st_size / sizeof(idx_t);

    // 读索引文件
    if (fread(idx, sizeof(idx_t), idx_num, idx_fp) != idx_num) {
	tcpl_error("fread fail");
    }

    // 根据索引的key值升序排序
    qsort(idx, idx_num, sizeof(idx_t), cmp_idx_key);

    // 将排序好的结果覆盖原索引文件
    rewind(idx_fp);
    if (fwrite(idx, sizeof(idx_t), idx_num, idx_fp) != idx_num) {
	tcpl_error("fwrite fail");
    }
    
    free(idx);
    idx = NULL;

    fclose(idx_fp);
    idx_fp = NULL;

    exit(EXIT_SUCCESS);
}
