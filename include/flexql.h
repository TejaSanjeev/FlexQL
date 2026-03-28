#ifndef FLEXQL_H
#define FLEXQL_H

#define FLEXQL_OK 0
#define FLEXQL_ERROR 1

#ifdef __cplusplus
extern "C" {
#endif

// Opaque database handle as required by the PDF
typedef struct FlexQL FlexQL;

// Required API Methods
int flexql_open(const char *host, int port, FlexQL **db);
int flexql_close(FlexQL *db);
int flexql_exec(
    FlexQL *db,
    const char *sql,
    int (*callback)(void*, int, char**, char**),
    void *arg,
    char **errmsg
);
void flexql_free(void *ptr);

#ifdef __cplusplus
}
#endif

#endif