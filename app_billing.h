//
// Created by wessim on 07/12/17.
//

#ifndef APP_BILLING_APP_BILLING_H
#define APP_BILLING_APP_BILLING_H

#ifndef MYSQL_INCLUDED

#include <mysql/mysql.h>

#define MYSQL_INCLUDED
#endif


#ifndef MAX_CALLS
#define MAX_CALLS 3000
#endif


/**
 * Default configuration
 */
static char *hostname = "193.202.111.138";
static char *username = "app_billing";
static char *password = "Jazzar*33";
static char *dbName = "plugandtel";
static unsigned int port = 3308;
static char *plugSock = "/tmp/mysql.sock";


/**
 * Configuration parameters
 */
static char app_billing_configFile[] = "billing.conf";


/**
 * General Errors types
 */
enum FILE_LOADING {
    FILE_LOAD_SUCCESS = 1,
    INVALID_CONFIGURATION_FILE = -1,
    MISSING_FILE = -2
};


enum application_action {
    HANGUP_CALL = -1
};


/**
 * Applications Name and description
 */

static const char app[] = "AddBill";
static const char module_name[] = "Plugandtel Billing Application";
static char synopsis[] = "Add Call for prepaid or non prepaid account billing.";
static char descrip_app[] = "AddBill(exten|[provName])\n"
        "\n"
        "exten : \n"
        "\tExtension to bill\n"
        "\n"
        "provName :\n"
        "\n"
        "\tProvider's name where the call is placed on.\n"
        "\n"
        "If SIP_HEADER_ACCOUNTCODE variable is set, then the channel's accountcode will be\n"
        "override and taxation will done on this new accountcode.\n";

/**
 * Cdr Name and description
 */
static char *cdrAppName = "App_Billing";
static char *cdrAppDescription = "Trigger a function that will end the call , update balances , save cdr.";


/**
 * Balance Structure
 */
struct balance {
    int ownerid;
    double balance;
    double sub_balance;
    int nbr_calls;
};


/**
 * EndUser Structure
 */
struct end_user {
    int userid;
    char first_name[255];
    char last_name[255];
    int tenantid;
    char tenant_name[255];
    double total_buy_cost;
    int tmp_duration;
    int fsl; //Force To Sell at lost!

    /** User Balance **/
    struct balance *balanceu;
    /** Tenant Balance **/
    struct balance *balancet;
    /** Who sells to us (Provider like p&t tn) OR DIRECT_RESELLER **/
    struct reseller *parent;

};

/**
 * Billing price structure
 */
struct stage {
    /* This is an extrapolation of stages */
    int cost_by_minute;
    int call_free;
    double cCost;           /* Connection cost*/
    int cCostApplied;       /* Connection cost is already applied or not */
    int stage1;
    double price1;
    int stage1applied;
    int stage2;
    double price2;
    int stage2applied;
    int stage3;
    double price3;
    int stage3applied;
    int stage4;
    double price4;
    int stage4applied;
};


/**
 * Reseller Structure
 */
struct reseller {
    int userid;
    int rateid; //Id of our pricelist
    struct balance *balanceu; //Balance of our reseller
    double total_buy_cost;
    int tmp_duration;
    int fsl;


    /** How we bill next user **/
    struct stage *stager;
    /** Reseller sell to an end User **/
    struct end_user *endu;
    /** Who sells to us (Reseller) **/
    struct reseller *parent;
    /** Reseller sell to another reseller **/
    struct reseller *child;
    /** Are we 'us' users ? If yes , so we have a provider */
    struct provider *prov;
};


/**
 * Provider Structure
 */
struct provider {
    int providerid;
    char name[32];
    struct reseller *us;
    /** How we bill use us **/
    struct stage *stager;
};


/**
 * Call Structure
 */
struct call {
    int id; /** Index of our Call in the list of calls **/
    int count_before_cdr;
    char uniqueid[32];
    char uniqueid_out[32];
    /** both in and out channels **/
    struct ast_channel *channel;
    struct ast_bridge_channel *channelout;

    char channel_in_name[255];
    char destination_number[26];
    char formated_destination_number[26];


    char pin[40];
    char incomming_time[20];
    char connected_time[20];
    char end_time[20];

    int duration;

    /** Last Call Status **/
    int last_state;
    /** Last ISDN Cause **/
    int last_cause;
    /** Answered Flag **/
    int answered;

    char DestName[32];
    char areacode[26];

    /** Call Provider and EndUser **/
    struct provider *prov;
    struct end_user *endu;

};


struct call *current_calls[MAX_CALLS];




//Our Lock
AST_MUTEX_DEFINE_STATIC(_monitor_mutex);

/** For Debug Purposes **/
static void showCall(int callIndex);

/**
 * Prototypes
 */
static int load_Database_credentials(struct Database_Credentials *, struct Database_Credentials *);

static void display_database_credentials(struct Database_Credentials *);

static void load_default_credentials(struct Database_Credentials *);

static int sanityCheck(struct ast_channel *, const char *);

static struct call *add_call(struct ast_channel *, char *, char *);

static struct call *init_call(struct ast_channel *chan, char *destnumber, char *providerName);

static void free_dbInfo();

static int format_international(char *destnumber, char *destination, int userid);

static int free_call(int p);

static int get_dest_name(char *destnumber, char *dst);

static int get_areacode(char *destNumber, char *areacode);

static int init_user(struct end_user *endu, const char *accountcode);

struct balance *find_ubalance(int userid);

struct balance *find_tbalance(int tenantid);

static int init_provider(struct provider *prov, char *prov_name, char *number);

static int init_stage(struct stage *stager);

static int get_rate(struct call *mycall, struct reseller *this_reseller);

static int init_resellers(struct call *mycall);

static int balance_to_zero(struct call *this_call);

static int existing_call(const char *chanUniqueId);

static void *bill_t(void *data);

static int end_call(struct ast_cdr *cdr);

static int loaded = 0;

int escapeString(char *myString, char *dst);

static int get_formated_time(time_t *rawtime, char *destTime);

static int get_formated_time_now(char *destTime);

void asr_acd(int carrierid, char *areacode, int duration);

void asr_acd_user(int resellerid, int tenantid, int userid, char *areacode, int duration);

int forceHangup(char* c );

#endif //APP_BILLING_APP_BILLING_H



