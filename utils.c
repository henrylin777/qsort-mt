#include <stdlib.h>
#define DTYPE int

void print_array(void *base, int nums) {
    printf("Array: [");
    for (char *i = (char *) base; i < (char *) base + nums * sizeof(DTYPE); i += sizeof(DTYPE)) {
        printf(" %d", *(DTYPE *)i);
    };
    printf(" ]\n");
}

int check(void *darray, int num_elem, size_t elem_size) {
    int idx = 0;
    DTYPE cur, next;
    while(idx < num_elem - 1) {
        cur = *((DTYPE *) darray + idx);
        next = *((DTYPE *) darray + idx + 1);
        if(cur > next)
            return idx;
        idx++;
    }
    return -1;
}

int load_testcase(void **darray) {
    int buf[1024];
    int data;
    int cnt = 0;
    FILE *ptr = fopen("testcase1.txt", "r");
    while(fscanf(ptr, "%d", &data) == 1) {
        buf[cnt] = data;
        cnt++;
    }
    DTYPE *arr = malloc(sizeof(DTYPE) * cnt);
    for (int i = 0; i < cnt; i++) {
        arr[i] = buf[i];
    };
    *darray = (char *)arr;
    return cnt;
}
