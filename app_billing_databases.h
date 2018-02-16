//
// Created by wessim on 10/12/17.
//

#ifndef APP_BILLING_APP_BILLING_DATABASES_H
#define APP_BILLING_APP_BILLING_DATABASES_H

#ifndef MYSQL_INCLUDED

#include <mysql/mysql.h>

#define MYSQL_INCLUDED
#endif

/**
 * MYSQL LOCKS
 */
AST_MUTEX_DEFINE_STATIC(_mysql_mutex_select);
AST_MUTEX_DEFINE_STATIC(_mysql_mutex_update);


/**
 * Structures Definition
 */
struct Database_Credentials {
    MYSQL connection;
    char hostname[250];
    char username[250];
    char password[250];
    unsigned int port;
    char dbName[250];
    char socket[250];
};


struct Database_Credentials *dbinfo_select;
struct Database_Credentials *dbinfo_update;

/**
 * MYSQL ERROR ENUM
 */
enum mysql_connection_status {
    MYSQL_CONNECTION_FAILURE = 0,
    MYSQL_CONNECTION_SUCCESS = 1
};


int db_connect(MYSQL *, char *, char *, char *, char *, unsigned int, char *);

MYSQL_RES *mysql_stmt(MYSQL_RES *mysqlRes, unsigned long *numRows, char *queryString);

MYSQL_RES *mysql_stmt_master(MYSQL_RES *mysqlRes, unsigned long *numRows, char *queryString);

#endif //APP_BILLING_APP_BILLING_DATABASES_H
