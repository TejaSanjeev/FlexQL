#ifndef FLEXQL_H
#define FLEXQL_H

/* Error Codes as defined by the specification */
#define FLEXQL_OK 0
#define FLEXQL_ERROR 1

/* * Opaque Database Handle 
 * The internal structure must not be exposed to the user.
 */
typedef struct FlexQL FlexQL;

/* * Callback function signature for SELECT queries.
 * Invoked once for every row returned by the query.
 * Returns 0 to continue processing, 1 to abort.
 */
typedef int (*flexql_callback)(void *data, int columnCount, char **values, char **columnNames);

/* Establishes a connection to the FlexQL database server. */
int flexql_open(const char *host, int port, FlexQL **db);

/* Closes the connection to the FlexQL server and releases resources. */
int flexql_close(FlexQL *db);

/* Executes an SQL statement on the FlexQL database server. */
int flexql_exec(
    FlexQL *db,
    const char *sql,
    flexql_callback callback,
    void *arg,
    char **errmsg
);

/* Frees memory allocated by the FlexQL API (e.g., error messages). */
void flexql_free(void *ptr);

#endif // FLEXQL_H