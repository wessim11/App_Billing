//
// Created by wessim on 10/12/17.
//

//Both first for our locks and logger
#include <asterisk.h>
#include <asterisk/lock.h>
#include "app_billing_databases.h"


/**
 * My Macros [Lazy person]
 */
#define db_connect_standard(dbInfo) \
         db_connect( \
            &(dbInfo)->connection , \
            (dbInfo)->hostname    , \
            (dbInfo)->username    , \
            (dbInfo)->password    , \
            (dbInfo)->dbName      , \
            (dbInfo)->port        , \
            (dbInfo)->socket \
        )\


/**
 * Attempt Connection to database
 * @param dbInfo
 * @return MYSQL_CONNECTION_SUCCESS in case of sucess Otherwise MYSQL_CONNECTION_FAILURE
 */
int
db_connect(MYSQL *conn, char *hostname, char *username, char *password, char *dbName, unsigned int port, char *socket) {
    mysql_init(conn);
    //Reconnect Options in case of connection lost (Commented because we need to alter my.cnf  on the database side!)
    /**mysql_options(conn , MYSQL_OPT_RECONNECT , "app_billing");**/
    //Connect To Database
    if (!mysql_real_connect(conn, hostname, username, password, dbName, port, socket, 0)) {
        ast_log(LOG_NOTICE, "Failed To connect to database , Error %s\n", mysql_error(conn));
        return MYSQL_CONNECTION_FAILURE;
    };

    return MYSQL_CONNECTION_SUCCESS;
}

/**
 * Query the database and return response
 * @param mysqlRes
 * @param numRows
 * @param queryString
 * @return
 */
MYSQL_RES *mysql_stmt(MYSQL_RES *mysqlRes, unsigned long *numRows, char *queryString) {
    ast_mutex_lock(&_mysql_mutex_select);
    //ast_log(LOG_DEBUG, "Query: %s\n", queryString);
    mysql_free_result(mysqlRes);
    mysqlRes = NULL;

    /** Query data on sql Server **/
    mysql_real_query(&dbinfo_select->connection, queryString, strlen(queryString));

    /** Check for errors and reconnect if connection was lost **/
    if (mysql_errno(&dbinfo_select->connection)) {
        ast_log(LOG_ERROR,
                "Mysql return an Error (%i) : %s \nOn Mysql Query %s\n",
                mysql_errno(&dbinfo_select->connection),
                mysql_error(&dbinfo_select->connection),
                queryString
        );
        /**
         * Trying To Reconnect to sql Server
         */
        if (mysql_errno(&dbinfo_select->connection) == 2006 || mysql_errno(&dbinfo_select->connection) == 2013) {
            ast_log(LOG_NOTICE, "Trying to reconnect to Mysql Server\n");
            if (db_connect_standard(dbinfo_select) == MYSQL_CONNECTION_SUCCESS) {
                /** Make another try now that we have reconnected to the server **/
                mysql_real_query(&dbinfo_select->connection, queryString, strlen(queryString));
                if (mysql_errno(&dbinfo_select->connection)) {
                    ast_log(LOG_ERROR,
                            "Mysql return an Error (%i) : %s \nOn Mysql Query %s\n",
                            mysql_errno(&dbinfo_select->connection),
                            mysql_error(&dbinfo_select->connection),
                            queryString
                    );
                    ast_mutex_unlock(&_mysql_mutex_select);
                    *numRows = -1;
                    return NULL;
                }
            } else {
                ast_log(LOG_ERROR, "Reconnection To Mysql Has failed\n");
                ast_mutex_unlock(&_mysql_mutex_select);
                *numRows = -1;
                return NULL;
            }

        }
    }

    /**
     * No Errors on Sql Request , Let's get Results
     */
    mysqlRes = mysql_store_result(&dbinfo_select->connection);

    if (mysql_affected_rows(&dbinfo_select->connection) != (my_ulonglong) -1) {
        /** We Got A Result **/
        *numRows = (unsigned long) mysql_affected_rows(&dbinfo_select->connection);
        ast_mutex_unlock(&_mysql_mutex_select);
        return mysqlRes;
    } else {
        /** ZERO RESULT **/
        ast_mutex_unlock(&_mysql_mutex_select);
        *numRows = 0;
        return NULL;
    }

    /** We should never come here ! **/
    ast_mutex_unlock(&_mysql_mutex_select);
    return NULL;


}




/**
 * Query the Master database and return response
 * @param mysqlRes
 * @param numRows
 * @param queryString
 * @return
 */
MYSQL_RES *mysql_stmt_master(MYSQL_RES *mysqlRes, unsigned long *numRows, char *queryString) {
    ast_mutex_lock(&_mysql_mutex_update);
    //ast_log(LOG_DEBUG, "Query: %s\n", queryString);
    mysql_free_result(mysqlRes);
    mysqlRes = NULL;

    /** Query data on sql Server **/
    mysql_real_query(&dbinfo_update->connection, queryString, strlen(queryString));

    /** Check for errors and reconnect if connection was lost **/
    if (mysql_errno(&dbinfo_update->connection)) {
        ast_log(LOG_ERROR,
                "Mysql return an Error (%i) : %s \nOn Mysql Query %s\n",
                mysql_errno(&dbinfo_update->connection),
                mysql_error(&dbinfo_update->connection),
                queryString
        );
        /**
         * Trying To Reconnect to sql Server
         */
        if (mysql_errno(&dbinfo_update->connection) == 2006 || mysql_errno(&dbinfo_update->connection) == 2013) {
            ast_log(LOG_NOTICE, "Trying to reconnect to Mysql Server\n");
            if (db_connect_standard(dbinfo_update) == MYSQL_CONNECTION_SUCCESS) {
                /** Make another try now that we have reconnected to the server **/
                mysql_real_query(&dbinfo_update->connection, queryString, strlen(queryString));
                if (mysql_errno(&dbinfo_update->connection)) {
                    ast_log(LOG_ERROR,
                            "Mysql return an Error (%i) : %s \nOn Mysql Query %s\n",
                            mysql_errno(&dbinfo_update->connection),
                            mysql_error(&dbinfo_update->connection),
                            queryString
                    );
                    ast_mutex_unlock(&_mysql_mutex_update);
                    *numRows = -1;
                    return NULL;
                }
            } else {
                ast_log(LOG_ERROR, "Reconnection To Mysql Has failed\n");
                ast_mutex_unlock(&_mysql_mutex_update);
                *numRows = -1;
                return NULL;
            }

        }
    }

    /**
     * No Errors on Sql Request , Let's get Results
     */
    mysqlRes = mysql_store_result(&dbinfo_update->connection);

    if (mysql_affected_rows(&dbinfo_update->connection) != (my_ulonglong) -1) {
        /** We Got A Result **/
        *numRows = (unsigned long) mysql_affected_rows(&dbinfo_update->connection);
        ast_mutex_unlock(&_mysql_mutex_update);
        return mysqlRes;
    } else {
        /** ZERO RESULT **/
        ast_mutex_unlock(&_mysql_mutex_update);
        *numRows = 0;
        return NULL;
    }

    /** We should never come here ! **/
    ast_mutex_unlock(&_mysql_mutex_update);
    return NULL;


}
