//
// Created by wessim on 07/12/17.
//

#include <asterisk.h>

ASTERISK_FILE_VERSION(__FILE__, "$Revision 01$");
//Needed For every module
#include <asterisk/module.h>
//Config Loaded
#include <asterisk/config.h>
//Logger
#include <asterisk/logger.h>
//Channels
#include <asterisk/channel.h>
#include <asterisk/bridge.h>
#include <asterisk/bridge_channel.h>
//Access pbx vars
#include <asterisk/pbx.h>
#include <asterisk/strings.h>
#include <math.h>
#include "app_billing_databases.h"
#include "app_billing.h"

/** String format to format date to string time **/
#define DATE_FORMAT "%Y-%m-%d %H:%M:%S"
#define DATE_FORMAT_DAY "%Y-%m-%d"


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
 * Save DateTime in destTIme as a string time suiting th format of DATE_FORMAT
 * @param destTime
 * @return
 */
static int get_formated_time_now(char *destTime) {

    time_t rawtime;
    struct tm *info;
    /** Get Time **/
    time(&rawtime);
    info = localtime(&rawtime);


    strftime(destTime, 128, DATE_FORMAT, info);

    return 0;
}


/**
 * Return today as a string format suiting DATE_FORMAT_DAY definition
 */
static int get_formated_time_of_the_day_today(char *destTime) {
    time_t rawtime;
    struct tm *info;
    /** Get Time **/
    time(&rawtime);
    info = localtime(&rawtime);


    strftime(destTime, 128, DATE_FORMAT_DAY, info);

    return 0;
}


/**
 * Return String format of date rawtime
 * @param rawtime
 * @param destTime
 * @return
 */
static int get_formated_time(time_t *rawtime, char *destTime) {
    

    struct tm *info;
    info = localtime(rawtime);

    strftime(destTime, 128, DATE_FORMAT, info);


    return 0;
}


/**
* Force Call to hangup by absoluteTimeout
* @param channel_name
**/
int forceHangup(char*  channel_name )
{
	struct ast_channel* c;
	int causecode = 10 ;

	if(ast_strlen_zero(channel_name)){
		ast_log(LOG_WARNING , "Invalid Channel Name passed , Impossible to  hangup channel!\n");
		return 0 ;
	}


	if(!(c=ast_channel_get_by_name(channel_name))){
		ast_log(LOG_WARNING , "No Such Channel [%s] found  to be hangup up\n" , channel_name);
		return 0 ;
	}

	ast_channel_softhangup_withcause_locked(c , causecode);
	c = ast_channel_unref(c);

	return 0 ;
}




/**
 * Escape String in order to inject it in Database
 * @param myString
 * @param dst
 * @return
 */
int escapeString(char *myString, char *dst) {
    int i, strLength = (int) strlen(myString);
    dst[0] = '\0';
    for (i = 0; i < strLength; i++) {
        switch (myString[i]) {
            case '\0':
                sprintf(dst, "%s%c", dst, '\0');
                break;
            case '\n':
                sprintf(dst, "%s%s", dst, "\\n");
                break;
            case '\r':
                sprintf(dst, "%s%s", dst, "\\r");
                break;
            case '\'':
                sprintf(dst, "%s%s", dst, "\\'");
                break;
            case '"':
                sprintf(dst, "%s%s", dst, "\\\"");
                break;
            case '\\':
                sprintf(dst, "%s%s", dst, "\\\\");
                break;
            default :
                sprintf(dst, "%s%c", dst, myString[i]);
                break;
        }
    }
    return 0;
}

/**
 * Gather list of credentials from config file
 * @return (Database_Credentials |  NULL )
 */
static int load_Database_credentials(struct Database_Credentials *dbInfo, struct Database_Credentials *dbInfo_update) {

    struct ast_config *cfg;
    struct ast_flags flags = {};
    char *category = NULL;
    struct ast_variable *param;


    //Load Configuration from our file
    cfg = ast_config_load(app_billing_configFile, flags);

    if (cfg == CONFIG_STATUS_FILEINVALID) {
        ast_log(LOG_ERROR, "Invalid file configuration , [%s]\n", app_billing_configFile);
        return INVALID_CONFIGURATION_FILE;
    } else if (cfg == CONFIG_STATUS_FILEMISSING) {
        ast_log(LOG_ERROR, "File is missing , [%s]\n", app_billing_configFile);
        return MISSING_FILE;
    }
    //Loop over categories
    while ((category = ast_category_browse(cfg, category)) && !ast_strlen_zero(category)) {
        if (!strcasecmp(category, "Database_credentials_select")) {
            //Loop Over Parameters in Database_credentials_select
            param = ast_variable_browse(cfg, category);
            while (param) {
                if (!strcasecmp(param->name, "hostname"))
                    sscanf(param->value, "%s", dbInfo->hostname);
                else if (!strcasecmp(param->name, "username"))
                    sscanf(param->value, "%s", dbInfo->username);
                else if (!strcasecmp(param->name, "password"))
                    sscanf(param->value, "%s", dbInfo->password);
                else if (!strcasecmp(param->name, "dbName"))
                    sscanf(param->value, "%s", dbInfo->dbName);
                else if (!strcasecmp(param->name, "port"))
                    dbInfo->port = (unsigned int) atoi(param->value);
                else if (!strcasecmp(param->name, "socket"))
                    sscanf(param->value, "%s", dbInfo->socket);
                else
                    ast_log(LOG_NOTICE, "Context %s , Unknow keyword %s at line %d of %s\n", category, param->name,
                            param->lineno, app_billing_configFile);

                param = param->next;
            }

        } else if (!strcasecmp(category, "Database_credentials_update")) {
            //Loop Over Parameters in Database_credentials_select
            param = ast_variable_browse(cfg, category);
            while (param) {
                if (!strcasecmp(param->name, "hostname"))
                    sscanf(param->value, "%s", dbInfo_update->hostname);
                else if (!strcasecmp(param->name, "username"))
                    sscanf(param->value, "%s", dbInfo_update->username);
                else if (!strcasecmp(param->name, "password"))
                    sscanf(param->value, "%s", dbInfo_update->password);
                else if (!strcasecmp(param->name, "dbName"))
                    sscanf(param->value, "%s", dbInfo_update->dbName);
                else if (!strcasecmp(param->name, "port"))
                    dbInfo_update->port = (unsigned int) atoi(param->value);
                else if (!strcasecmp(param->name, "socket"))
                    sscanf(param->value, "%s", dbInfo_update->socket);
                else
                    ast_log(LOG_NOTICE, "Context %s , Unknow keyword %s at line %d of %s\n", category, param->name,
                            param->lineno, app_billing_configFile);

                param = param->next;
            }
        } else {
            ast_log(LOG_NOTICE, "Unknown Category %s\n", category);
        }
    }


    //display loaded Data
    display_database_credentials(dbInfo);
    ast_log(LOG_NOTICE, "Update Credentials\n");
    display_database_credentials(dbInfo_update);

    //clear vars and return
    ast_config_destroy(cfg);
    return FILE_LOAD_SUCCESS;
}


/**
 * this is our main function , it will be called everytime we call AddBill application
 * @param chan : Channel of our call
 * @param data : data passed in parametres : Should contains providerName and extension
 **/
static int bill_exec(struct ast_channel *chan, const char *data) {
    char *destNumber;
    char *providerName;
    char *params;
    //pbx vars
    char tmp[1024];
    char *userAccountId;
    int pcall;
    struct call *this_call = NULL;
    struct reseller *us;


    /** Out Thread that will not be joined **/
    pthread_t t;
    pthread_attr_t attr;

    /**
     * Sanity checks
     */
    if (!sanityCheck(chan, data))
        return HANGUP_CALL;

    //Parse Data
    params = strdup(data);
    if (strchr(params, ',')) {
        providerName = params;
        destNumber = strsep(&providerName, ",");
    } else {
        destNumber = params;
        providerName = NULL;
    }
    //Check if destnumber is special extension
    if ((!strcasecmp("s", destNumber)) || (!strcasecmp("h", destNumber)) || (!strcasecmp("t", destNumber)) ||
        (!strcasecmp("i", destNumber)) || (!strcasecmp("failed", destNumber))) {
	free(params);
        return HANGUP_CALL;
    }


    //Data are parsed , let's get our user AccountCode
    pbx_retrieve_variable(chan, "SIP_HEADER_ACCOUNTCODE", &userAccountId, tmp, sizeof(tmp), NULL);
    //Write our accountCode header in channel accountcode | Error
    if (userAccountId) {
        if (strlen(userAccountId))
            ast_channel_accountcode_set(chan, userAccountId);
        else {
            ast_log(LOG_WARNING,
                    "SIP_HEADER_ACCOUNTCODE is set but empty , Cannot bil on channel %s with uniqueid %s\n",
                    ast_channel_name(chan),
                    ast_channel_uniqueid(chan)
            );
	    free(params);
            return HANGUP_CALL;
        }
    } else {
        //User Accound Code not Set
        ast_log(LOG_WARNING, "SIP_HEADER_ACCOUNTCODE was not set , Cannot bill on channel %s with uniqueid %s\n",
                ast_channel_name(chan),
                ast_channel_uniqueid(chan)
        );
	free(params);
        return HANGUP_CALL;
    }


    //If Call Exist Bill it otherwise it's a new call so let's Add it
    ast_mutex_lock(&_monitor_mutex);
    if ((pcall = existing_call(ast_channel_uniqueid(chan))) > -1) {

        // Dump ASR and ACD statistics.
        if (current_calls[pcall]->prov ? current_calls[pcall]->prov->providerid > 0 : 0) {
            asr_acd(current_calls[pcall]->prov->providerid, current_calls[pcall]->areacode, 0);
        }

        us = current_calls[pcall]->prov->us;
        free(current_calls[pcall]->prov);

        /* Initialize the provider structure */
        current_calls[pcall]->prov = (struct provider *) malloc(sizeof(struct provider));
        if (!current_calls[pcall]->prov) {
            ast_log(LOG_ERROR, "We cannot allocate memory for provider description! Out of memory!\n");
            free_call(pcall);
	    free(params);
            ast_mutex_unlock(&_monitor_mutex);
            return -1;
        }

        if (init_provider(current_calls[pcall]->prov, providerName,
                          current_calls[pcall]->formated_destination_number)) {
            free_call(pcall);
	    free(params);
            ast_mutex_unlock(&_monitor_mutex);
            return -1;
        }

        current_calls[pcall]->prov->us = us;

        /* Check we are not selling at a loss */
        if (current_calls[pcall]->prov) {
            if (current_calls[pcall]->prov->us) {
                if (current_calls[pcall]->prov->us->stager->cost_by_minute > -1) {
                    if (current_calls[pcall]->prov->stager ? current_calls[pcall]->prov->stager->cost_by_minute >
                                                             current_calls[pcall]->prov->us->stager->cost_by_minute
                                                           : 1) {
                        if (current_calls[pcall]->prov->stager && current_calls[pcall]->prov->us->stager) {
                            if (current_calls[pcall]->prov->us->child) {
                                if (current_calls[pcall]->prov->us->child->fsl) {
                                    ast_log(LOG_WARNING,
                                            "We are allowed to FSL to reseller (Id %i) (Number : %s via carrier %s). We are selling at %f but buying %f!\n",
                                            current_calls[pcall]->prov->us->child->userid,
                                            current_calls[pcall]->destination_number, current_calls[pcall]->prov->name,
                                            (float) current_calls[pcall]->prov->us->stager->cost_by_minute / 10000,
                                            (float) current_calls[pcall]->prov->stager->cost_by_minute / 10000);
                                } else {
                                    ast_log(LOG_WARNING,
                                            "We cannot sell this route selling it at loss (Number : %s via carrier %s). We are selling at %f but buying %f!\n",
                                            current_calls[pcall]->destination_number, current_calls[pcall]->prov->name,
                                            (float) current_calls[pcall]->prov->us->stager->cost_by_minute / 10000,
                                            (float) current_calls[pcall]->prov->stager->cost_by_minute / 10000);
                                    free_call(pcall);
	    			    free(params);
                                    ast_mutex_unlock(&_monitor_mutex);
                                    return -1;
                                }
                            } else {
                                if (current_calls[pcall]->endu->fsl) {
                                    ast_log(LOG_WARNING,
                                            "We are allowed to FSL to user (Id %i) (Number : %s via carrier %s). We are selling at %f but buying %f!\n",
                                            current_calls[pcall]->endu->userid,
                                            current_calls[pcall]->destination_number, current_calls[pcall]->prov->name,
                                            (float) current_calls[pcall]->prov->us->stager->cost_by_minute / 10000,
                                            (float) current_calls[pcall]->prov->stager->cost_by_minute / 10000);
                                } else {
                                    ast_log(LOG_WARNING,
                                            "We cannot sell this route selling it at loss (Number : %s via carrier %s). We are selling at %f but buying %f!\n",
                                            current_calls[pcall]->destination_number, current_calls[pcall]->prov->name,
                                            (float) current_calls[pcall]->prov->us->stager->cost_by_minute / 10000,
                                            (float) current_calls[pcall]->prov->stager->cost_by_minute / 10000);
                                    free_call(pcall);
                                    free(params);
				    ast_mutex_unlock(&_monitor_mutex);
                                    return -1;
                                }
                            }
                        } else {
                            ast_log(LOG_WARNING, "No stager sctructure to test sell cost on carrier %s for number %s\n",
                                    current_calls[pcall]->prov->name, current_calls[pcall]->destination_number);
                        }
                    }
                }
            }
        }


        current_calls[pcall]->prov->us = us;
        ast_mutex_unlock(&_monitor_mutex);
        return 0;

    }
    ast_mutex_unlock(&_monitor_mutex);

    /** Add this Call to our list of calls **/
    ast_mutex_lock(&_monitor_mutex);
    this_call = add_call(chan, destNumber, providerName);
    ast_mutex_unlock(&_monitor_mutex);

    if (this_call) {
        //ast_log(LOG_DEBUG, "Everything IS Okay!\n");
        //Everything IS Perfect , Let's start our thread to count call duration and detach it !
        sprintf(tmp, "%i", (int) round(this_call->prov->us->stager->price1 * 60 * 100));
        pbx_builtin_setvar_helper(chan, "SELLRATE", tmp);
        pbx_builtin_setvar_helper(chan, "INT_EXTEN", this_call->formated_destination_number);
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        if (chan && ast_pthread_create(&t, &attr, bill_t, (void *) this_call)) {
            ast_log(LOG_ERROR, "Failed To create the bill thread that will make billing calculation\n");
        }
        pthread_attr_destroy(&attr);
	free(params);
    } else {
	free(params);
        return -1;
    }


    //ast_log(LOG_DEBUG, "Success For accountcode %s\n", userAccountId);


    return 0;
}

/**
 * Main Billing function , it will be called on each thread
 * @param data
 * @return
 */
static void *bill_t(void *data) {
    //ast_log(LOG_DEBUG, "Thread Started:)\n");
    struct call *this_call = (struct call *) data;
    struct reseller *next;
    int pointer = -1, res = 0;
    char uniqueid[32];

    if (!this_call) {
        ast_log(LOG_WARNING, "Caution : thread started but no call found.\n");
        return NULL;
    }
    ast_mutex_lock(&_monitor_mutex);
    pointer = this_call->id;
    strcpy(uniqueid, this_call->uniqueid);
    ast_mutex_unlock(&_monitor_mutex);

    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

    /**
     * While Our Module is still up and we didnt end up our billing
     */
    while (loaded) {
        ast_mutex_lock(&_monitor_mutex);

        if ((pointer < 0) || (pointer >= MAX_CALLS)) { /* Dirty, but works if pointer is corrupted */
            ast_mutex_unlock(&_monitor_mutex);
            return NULL;
        }

        if (!current_calls[pointer]) { /* Hum, pthread_cancel append when we waited for mutex ? */
            ast_mutex_unlock(&_monitor_mutex);
            return NULL;
        }

        if (current_calls[pointer] != this_call) {       /* Sur, call was changed in the array. So I am a zombie. */
            ast_mutex_unlock(&_monitor_mutex);
            return NULL;
        }


        /* Last Test to be OK */
        if (strcasecmp(uniqueid, this_call->uniqueid)) {
            ast_log(LOG_NOTICE, "Thread running for different call. -> Stop thread\n");
            ast_log(LOG_NOTICE, "uniqueid : %s ---- this_call->uniqueid : %s\n", uniqueid, this_call->uniqueid);
            ast_mutex_unlock(&_monitor_mutex);
            return NULL;
        }

	if (!this_call->channel) {
            if (this_call->count_before_cdr > 10) {
                ast_log(LOG_WARNING, "Thread already running but cannot find inbound channel [%s].\n",
                        this_call->channel_in_name);
                free_call(pointer);
		/** Unlock our Channel **/
		//ast_channel_unlock(this_call->channel);
                ast_mutex_unlock(&_monitor_mutex);
                return NULL;
            }
            this_call->count_before_cdr++;

        } else {
            this_call->count_before_cdr = 0;
            if(ast_channel_state(this_call->channel) == AST_STATE_UP )
		this_call->answered = 1 ;
        }
	

        /*** Start billing ***/
        if ((!balance_to_zero(this_call)) && this_call->answered) {
            this_call->duration++;
            next = this_call->prov->us->child ? this_call->prov->us->child : NULL;

            while (next) {
                next->tmp_duration++;

                /* Apply connection cost */
                if (!next->parent->stager->cCostApplied) {
                    //ast_log(LOG_NOTICE, "Apply connection cost : %f\n", next->parent->stager->cCost);
                    next->total_buy_cost += next->parent->stager->cCost;
                    if (next->balanceu) {
                        next->balanceu->balance -= next->parent->stager->cCost;
                        next->balanceu->sub_balance += next->parent->stager->cCost;
                    }
                    next->parent->stager->cCostApplied = 1;
                }

                /* Apply parent stage on this current reseller */
                if (!next->parent->stager->stage1applied) {      /* Everythings what can appened, we bill stage1, because the call is bridged */
                    if (next->tmp_duration ==
                        1) {                                   /* Applied rate on first second consummed */
                        next->total_buy_cost += next->parent->stager->price1;
                        if (next->balanceu) {
                            next->balanceu->balance -= next->parent->stager->price1;
                            next->balanceu->sub_balance += next->parent->stager->price1;
                        }
                    }

                    if (next->tmp_duration >= next->parent->stager->stage1) {
                        next->parent->stager->stage1applied = 1;
                        next->tmp_duration = 0;
                    }

                }

                if (next->parent->stager->stage1applied &&
                    !next->parent->stager->stage2applied) {
                    if (next->tmp_duration ==
                        1) {                                   /* Applied rate on first second consummed */
                        next->total_buy_cost += next->parent->stager->price2;
                        if (next->balanceu) {
                            next->balanceu->balance -= next->parent->stager->price2;
                            next->balanceu->sub_balance += next->parent->stager->price2;
                        }
                    }

                    if (next->tmp_duration >= next->parent->stager->stage2) {
                        next->parent->stager->stage2applied = 1;
                        next->tmp_duration = 0;
                    }
                }

                if (next->parent->stager->stage1applied &&
                    next->parent->stager->stage2applied &&
                    !next->parent->stager->stage3applied) {
                    if (next->tmp_duration ==
                        1) {                                   /* Applied rate on first second consummed */
                        next->total_buy_cost += next->parent->stager->price3;
                        if (next->balanceu) {
                            next->balanceu->balance -= next->parent->stager->price3;
                            next->balanceu->sub_balance += next->parent->stager->price3;
                        }
                    }

                    if (next->tmp_duration >= next->parent->stager->stage3) {
                        next->parent->stager->stage3applied = 1;
                        next->tmp_duration = 0;
                    }
                }

                if (next->parent->stager->stage1applied &&
                    next->parent->stager->stage2applied &&
                    next->parent->stager->stage3applied) {

                    if (next->tmp_duration ==
                        1) {                                   /* Applied rate on first second consummed */
                        next->total_buy_cost += next->parent->stager->price4;
                        if (next->balanceu) {
                            next->balanceu->balance -= next->parent->stager->price4;
                            next->balanceu->sub_balance += next->parent->stager->price4;
                        }
                    }

                    if (next->tmp_duration >= next->parent->stager->stage4) {
                        next->parent->stager->stage4applied = 1;
                        next->tmp_duration = 0;
                    }
                }

                next = next->child ? next->child : NULL;
            }

            /* Apply parent stage on user */

            /* Apply connection cost */
            if (!this_call->endu->parent->stager->cCostApplied) {
                //ast_log(LOG_NOTICE, "Apply connection cost : %f\n", this_call->endu->parent->stager->cCost);
                this_call->endu->total_buy_cost += this_call->endu->parent->stager->cCost;
                if (this_call->endu->balanceu) {
                    this_call->endu->balanceu->balance -= this_call->endu->parent->stager->cCost;
                    this_call->endu->balanceu->sub_balance += this_call->endu->parent->stager->cCost;
                }
                if (this_call->endu->balancet) {
                    this_call->endu->balancet->balance -= this_call->endu->parent->stager->cCost;
                    this_call->endu->balancet->sub_balance += this_call->endu->parent->stager->cCost;
                }
                this_call->endu->parent->stager->cCostApplied = 1;
            }


            this_call->endu->tmp_duration++;
            /* Apply parent stage on this current reseller */
            if (!this_call->endu->parent->stager->stage1applied) {   /* Everythings what can appened, we bill stage1, because the call is bridged */
                if (this_call->endu->tmp_duration ==
                    1) {                                        /* Applied rate on first second consummed */
                    this_call->endu->total_buy_cost += this_call->endu->parent->stager->price1;
                    if (this_call->endu->balanceu) {
                        this_call->endu->balanceu->balance -= this_call->endu->parent->stager->price1;
                        this_call->endu->balanceu->sub_balance += this_call->endu->parent->stager->price1;
                    }
                    if (this_call->endu->balancet) {
                        this_call->endu->balancet->balance -= this_call->endu->parent->stager->price1;
                        this_call->endu->balancet->sub_balance += this_call->endu->parent->stager->price1;
                    }
                }

                if (this_call->endu->tmp_duration >= this_call->endu->parent->stager->stage1) {
                    this_call->endu->parent->stager->stage1applied = 1;
                    this_call->endu->tmp_duration = 0;
                }
            }

            if (this_call->endu->parent->stager->stage1applied &&
                !this_call->endu->parent->stager->stage2applied) {
                if (this_call->endu->tmp_duration ==
                    1) {                                        /* Applied rate on first second consummed */
                    this_call->endu->total_buy_cost += this_call->endu->parent->stager->price2;
                    if (this_call->endu->balanceu) {
                        this_call->endu->balanceu->balance -= this_call->endu->parent->stager->price2;
                        this_call->endu->balanceu->sub_balance += this_call->endu->parent->stager->price2;
                    }
                    if (this_call->endu->balancet) {
                        this_call->endu->balancet->balance -= this_call->endu->parent->stager->price2;
                        this_call->endu->balancet->sub_balance += this_call->endu->parent->stager->price2;
                    }
                }

                if (this_call->endu->tmp_duration >= this_call->endu->parent->stager->stage2) {
                    this_call->endu->parent->stager->stage2applied = 1;
                    this_call->endu->tmp_duration = 0;
                }
            }

            if (this_call->endu->parent->stager->stage1applied &&
                this_call->endu->parent->stager->stage2applied &&
                !this_call->endu->parent->stager->stage3applied) {
                if (this_call->endu->tmp_duration ==
                    1) {                                        /* Applied rate on first second consummed */
                    this_call->endu->total_buy_cost += this_call->endu->parent->stager->price3;
                    if (this_call->endu->balanceu) {
                        this_call->endu->balanceu->balance -= this_call->endu->parent->stager->price3;
                        this_call->endu->balanceu->sub_balance += this_call->endu->parent->stager->price3;
                    }
                    if (this_call->endu->balancet) {
                        this_call->endu->balancet->balance -= this_call->endu->parent->stager->price3;
                        this_call->endu->balancet->sub_balance += this_call->endu->parent->stager->price3;
                    }
                }

                if (this_call->endu->tmp_duration >= this_call->endu->parent->stager->stage3) {
                    this_call->endu->parent->stager->stage3applied = 1;
                    this_call->endu->tmp_duration = 0;
                }
            }

            if (this_call->endu->parent->stager->stage2applied &&
                this_call->endu->parent->stager->stage3applied) {
                if (this_call->endu->tmp_duration ==
                    1) {                                        /* Applied rate on first second consummed */
                    this_call->endu->total_buy_cost += this_call->endu->parent->stager->price4;
                    if (this_call->endu->balanceu) {
                        this_call->endu->balanceu->balance -= this_call->endu->parent->stager->price4;
                        this_call->endu->balanceu->sub_balance += this_call->endu->parent->stager->price4;
                    }
                    if (this_call->endu->balancet) {
                        this_call->endu->balancet->balance -= this_call->endu->parent->stager->price4;
                        this_call->endu->balancet->sub_balance += this_call->endu->parent->stager->price4;
                    }
                }

                if (this_call->endu->tmp_duration >= this_call->endu->parent->stager->stage4) {
                    this_call->endu->parent->stager->stage4applied = 1;
                    this_call->endu->tmp_duration = 0;
                }
            }

            if (balance_to_zero(this_call)) {
                ast_log(LOG_NOTICE, "-- %s : No more money in the loop. Hangup the call for User %s %s ( Id : %i )\n",
                        this_call->channel_in_name, this_call->endu->last_name, this_call->endu->first_name,
                        this_call->endu->userid);
                ast_mutex_unlock(&_monitor_mutex);
		forceHangup(this_call->channel_in_name);
                break;
            }
        } else if (this_call->answered) {
            ast_log(LOG_NOTICE, "-- %s : No more money in the loop. Hangup the call for User %s %s ( Id : %i )\n",
                    this_call->channel_in_name, this_call->endu->last_name, this_call->endu->first_name,
                    this_call->endu->userid);
            ast_mutex_unlock(&_monitor_mutex);
            forceHangup(this_call->channel_in_name);
	    break;
        }
        /******* End billing *******/
        res = ast_mutex_unlock(&_monitor_mutex);
        if (res == EINVAL) {
            ast_log(LOG_WARNING, "-- Mutex not properly initialized.\n");
        } else if (res == EPERM) {
            ast_log(LOG_WARNING, "-- I am not owner of mutex.\n");
        }

        if (this_call->answered) {
            //ast_log(LOG_DEBUG, "Duration Increased %i\n", this_call->duration);
            sleep(1);       /* Sleep 1s if call is answered */
        } else {
            usleep(500000); /* Sleep 500ms to get the right channel's state quickly */
        }
    }

    return NULL;


}


/**
 * Return the index of the call if it was found in our arrays , otherwise -1
 * @param chanUniqueId
 * @return
 */
static int existing_call(const char *chanUniqueId) {
    int i;


    if (!chanUniqueId)
        return -2;

    for (i = 0; i < MAX_CALLS; i++) {
        if (current_calls[i] ? !strcasecmp(current_calls[i]->uniqueid, chanUniqueId) : 0) {
            return i;
        }
    }

    return -1;
}


/**
 * Sanity Checks
 * @Return 1 : success , 0: failure
 */
static int sanityCheck(struct ast_channel *chan, const char *data) {
    if (!data) {
        ast_log(LOG_WARNING, "data are required in %s application", module_name);
        return 0;
    }
    //Check if there is data
    if (ast_strlen_zero(data)) {
        ast_log(LOG_WARNING,
                "No data has been passed to our application , Call with channelName %s and uniqueid %s Will be hanged up \n",
                ast_channel_name(chan), ast_channel_uniqueid(chan)
        );
        return 0;
    }


    //Check if channel exist and uniqueid exist
    if (chan ? !strlen(ast_channel_uniqueid(chan)) : 1) {
        return 0;
    }

    if (!strcasecmp("OutgoingSpoolFailed", ast_channel_name(chan))) {
        return 0;
    }


    return 1;
}

/**
 * Create Our Call structure and return a pointer to it
 * Init all data structure of our call !!!
 * @param chan
 * @param destnumber
 * @param providerName
 * @return
 */
static struct call *add_call(struct ast_channel *chan, char *destnumber, char *providerName) {
    struct call *this_call = NULL;

    /** We handle Only calls that the right number of digit **/
    if (strlen(destnumber) > 25) {
        ast_log(LOG_WARNING, "Destination number is too long (%s).\n", destnumber);
        return NULL;
    }

    /** Check if it's a valid channel **/
    if (!strlen(ast_channel_accountcode(chan)))
        return NULL;

    /** Init data in call **/
    if (!(this_call = init_call(chan, destnumber, providerName))) {
        return NULL;
    }

    return this_call;

}


/**
 * Init the Call Structure
 * @param chan
 * @param destnumber
 * @param providerName
 * @return
 */
static struct call *init_call(struct ast_channel *chan, char *destnumber, char *providerName) {
    int callIndex = 0;

    /** Add our call to the array **/
    for (callIndex = 0; callIndex < MAX_CALLS; callIndex++) {
        if (!current_calls[callIndex]) {
            // We Found our index //
            break;
        }
    }

    /** Check whether list of call is full **/
    if (callIndex == MAX_CALLS) {
        ast_log(LOG_WARNING, "-- %s : Buffer is full , we can't handle this call\n", ast_channel_uniqueid(chan));
        return NULL;
    }

    /**
     * Allocate Call structure
     */
    current_calls[callIndex] = (struct call *) malloc(sizeof(struct call));
    if (!current_calls[callIndex]) {
        ast_log(LOG_ERROR, "We cannot allocate memory for call structure ! OUT OF MEMORY:\n");
        return NULL;
    }
    /** Everything is fine , let's init our call struct data **/
    current_calls[callIndex]->id = callIndex;
    current_calls[callIndex]->count_before_cdr = 0;
    sprintf(current_calls[callIndex]->uniqueid, "%s", ast_channel_uniqueid(chan));
    //ast_log(LOG_DEBUG, "Callid is set to %s", current_calls[callIndex]->uniqueid);
    current_calls[callIndex]->answered = 0;
    current_calls[callIndex]->channel = NULL;
    current_calls[callIndex]->channelout = NULL;
    sprintf(current_calls[callIndex]->channel_in_name, "%s", ast_channel_name(chan));
    sprintf(current_calls[callIndex]->destination_number, "%s", destnumber);
    current_calls[callIndex]->pin[0] = '\0';
    /** For the moment We dont give a fuck about time , we will fix that after **/
    current_calls[callIndex]->incomming_time[0] = '\0';
    get_formated_time_now(current_calls[callIndex]->incomming_time);
    current_calls[callIndex]->connected_time[0] = '\0';
    current_calls[callIndex]->end_time[0] = '\0';
    current_calls[callIndex]->duration = 0;
    /** Putting last_state to 5 while having no clue about description of it , Should recheck this after **/
    current_calls[callIndex]->last_state = 5;
    current_calls[callIndex]->last_cause = -1;
    current_calls[callIndex]->endu = NULL;
    current_calls[callIndex]->prov = NULL;
    //Added By Wessim
    current_calls[callIndex]->channel = chan;
    /** Get Formatted destNumber **/
    if (format_international(destnumber, current_calls[callIndex]->formated_destination_number,
                             atoi(ast_channel_accountcode(chan)))) {
        free_call(callIndex);
        return NULL;
    }

    /** Get DestName **/
    get_dest_name(current_calls[callIndex]->formated_destination_number, current_calls[callIndex]->DestName);

    /** Get and check AreaCode **/
    get_areacode(current_calls[callIndex]->formated_destination_number, current_calls[callIndex]->areacode);
    if (current_calls[callIndex]->areacode ? !strlen(current_calls[callIndex]->areacode) : 1) {
        ast_log(LOG_ERROR, "We Don't route this destination!\n");
        free_call(callIndex);
        return NULL;
    }

    /** Let's now init our EndUser entity **/
    current_calls[callIndex]->endu = (struct end_user *) malloc(sizeof(struct end_user));
    if (!current_calls[callIndex]->endu) {
        ast_log(LOG_ERROR, "We cannot allocate memory to init enduser struct , OUT OF MEMORY!\n");
        free_call(callIndex);
        return NULL;
    }

    if (init_user(current_calls[callIndex]->endu, ast_channel_accountcode(chan))) {
        /** Error : 0 is Success here**/
        free(current_calls[callIndex]->endu);
        current_calls[callIndex]->endu = NULL;
        free_call(callIndex);
        return NULL;
    }

    /** Let's now init the provider structure **/
    current_calls[callIndex]->prov = (struct provider *) malloc(sizeof(struct provider));
    if (!current_calls[callIndex]->prov) {
        ast_log(LOG_ERROR, "We Cannot allocate memory for provider description! Out Of Memory!\n");
        free_call(callIndex);
        return NULL;
    }

    if (init_provider(current_calls[callIndex]->prov, providerName,
                      current_calls[callIndex]->formated_destination_number)) {
        free(current_calls[callIndex]->prov);
        current_calls[callIndex]->prov = NULL;
        free_call(callIndex);
        return NULL;
    }

    /** Let's init multi level rates on user and tenant and reseller if our tenant has a reseller **/
    if (init_resellers(current_calls[callIndex])) {
        free_call(callIndex);
        return NULL;
    }


    //ast_log(LOG_DEBUG, "Balance tO Zero is set to %d\n", balance_to_zero(current_calls[callIndex]));
    /** Balance To Zero **/
    if (balance_to_zero(current_calls[callIndex])) {
        free_call(callIndex);
        return NULL;
    }

    /* Check we are not selling at a loss */
    if (current_calls[callIndex]->prov) {
        if (current_calls[callIndex]->prov->us) {
            if (current_calls[callIndex]->prov->us->stager->cost_by_minute > -1) {
                if (current_calls[callIndex]->prov->stager ? current_calls[callIndex]->prov->stager->cost_by_minute >
                                                          current_calls[callIndex]->prov->us->stager->cost_by_minute : 1) {
                    if (current_calls[callIndex]->prov->stager && current_calls[callIndex]->prov->us->stager) {
                        if (current_calls[callIndex]->prov->us->child) {
                            if (current_calls[callIndex]->prov->us->child->fsl) {
                                ast_log(LOG_WARNING,
                                        "We are allowed to FSL to reseller (Id %i) (Number : %s via carrier %s). We are selling at %f but buying %f!\n",
                                        current_calls[callIndex]->prov->us->child->userid,
                                        current_calls[callIndex]->destination_number, current_calls[callIndex]->prov->name,
                                        (float) current_calls[callIndex]->prov->us->stager->cost_by_minute / 10000,
                                        (float) current_calls[callIndex]->prov->stager->cost_by_minute / 10000);
                            } else {
                                ast_log(LOG_WARNING,
                                        "We cannot sell this route selling it at loss (Number : %s via carrier %s). We are selling at %f but buying %f!\n",
                                        current_calls[callIndex]->destination_number, current_calls[callIndex]->prov->name,
                                        (float) current_calls[callIndex]->prov->us->stager->cost_by_minute / 10000,
                                        (float) current_calls[callIndex]->prov->stager->cost_by_minute / 10000);
                                free_call(callIndex);
                                return NULL;
                            }
                        } else {
                            if (current_calls[callIndex]->endu->fsl) {
                                ast_log(LOG_WARNING,
                                        "We are allowed to FSL to user (Id %i) (Number : %s via carrier %s). We are selling at %f but buying %f!\n",
                                        current_calls[callIndex]->endu->userid, current_calls[callIndex]->destination_number,
                                        current_calls[callIndex]->prov->name,
                                        (float) current_calls[callIndex]->prov->us->stager->cost_by_minute / 10000,
                                        (float) current_calls[callIndex]->prov->stager->cost_by_minute / 10000);
                            } else {
                                ast_log(LOG_WARNING,
                                        "We cannot sell this route selling it at loss (Number : %s via carrier %s). We are selling at %f but buying %f!\n",
                                        current_calls[callIndex]->destination_number, current_calls[callIndex]->prov->name,
                                        (float) current_calls[callIndex]->prov->us->stager->cost_by_minute / 10000,
                                        (float) current_calls[callIndex]->prov->stager->cost_by_minute / 10000);
                                free_call(callIndex);
                                return NULL;
                            }
                        }
                    } else {
                        ast_log(LOG_WARNING, "No stager structure to test sell cost on carrier %s for number %s\n",
                                current_calls[callIndex]->prov->name, current_calls[callIndex]->destination_number);
                    }
                }
            }
        }
    }


    return current_calls[callIndex];
}

/** Is Balance Is Zero **/
static int balance_to_zero(struct call *this_call) {
    struct reseller *next;

    if (!this_call) {
        return 1;
    }

    if ((this_call->prov ? this_call->prov->us : 0) && (this_call->endu)) {
        next = this_call->prov->us->child ? this_call->prov->us->child : NULL;

        while (next) {
            if (next->balanceu ? (next->balanceu->balance <= 0.0) : 0) {
                return 1;
            } else if (next->balanceu ? (next->balanceu->balance - next->parent->stager->cCost) <= 0.0 : 0) {
                return 1;
            }
            next = next->child ? next->child : NULL;
        }

        if ((this_call->endu->balanceu ? this_call->endu->balanceu->balance <= 0.0 : 0) ||
            (this_call->endu->balancet ? this_call->endu->balancet->balance <= 0.0 : 0)) {
            if (this_call->endu->parent ? this_call->endu->parent->stager : 0) {
                if (this_call->endu->parent->stager->call_free)
                    return 0;
                else
                    return 1;
            }
            return 1;
        } else if ((this_call->endu->balanceu ?
                    (this_call->endu->balanceu->balance - this_call->endu->parent->stager->cCost) <= 0.0 : 0) ||
                   (this_call->endu->balancet ?
                    (this_call->endu->balancet->balance - this_call->endu->parent->stager->cCost) <= 0.0 : 0)) {
            if (this_call->endu->parent ? this_call->endu->parent->stager : 0) {
                if (this_call->endu->parent->stager->call_free)
                    return 0;
                else
                    return 1;
            }
        }
    }

    return 0;

}


/**
 * Init Pricelists for user tenant and reseller if it does exist
 * @param mycall
 * @return
 */
static int init_resellers(struct call *mycall) {
    MYSQL_RES *myres = NULL;
    MYSQL_ROW myrow;
    int on_reseller = 0;
    int user_rateid = -1, user_resellrateid = -1, tenant_rateid = -1, tenant_resellrateid = -1;
    char querystring[512];
    char *pEnd;
    unsigned long numRows = 0;

    if (!mycall->endu) {
        return 1;
    }

    sprintf(querystring, "SELECT tenantowner.UserID FROM tenantowner WHERE TenantID=%i", mycall->endu->tenantid);
    myres = mysql_stmt(myres, &numRows, querystring);
    if (numRows < 0) {
        mysql_free_result(myres);
        return 1;
    }

    if (numRows) { /* Billing is done via a reseller */
        mysql_data_seek(myres, 0);
        myrow = mysql_fetch_row(myres);
        on_reseller = 1;
        //ownerid = atoi(myrow[0]);
        //ast_log(LOG_DEBUG, "OwnerId from Database is %i", ownerid);
    }

    sprintf(querystring,
            "SELECT users.RateID, users.ResellRateID, users.Balance, tenant.RateID, tenant.ResellRateID, tenant.Balance FROM users INNER  JOIN tenant USING(TenantID) WHERE users.UserID=%i",
            mycall->endu->userid);

    myres = mysql_stmt(myres, &numRows, querystring);
    if (numRows < 1) {
        mysql_free_result(myres);
        return 1;
    }

    mysql_data_seek(myres, 0);
    myrow = mysql_fetch_row(myres);

    user_rateid = atoi(myrow[0]);
    user_resellrateid = atoi(myrow[1]);
    tenant_rateid = atoi(myrow[3]);
    tenant_resellrateid = atoi(myrow[4]);

    if ((user_rateid < 0) && (user_resellrateid < 0)) { /* Billing is done on tenant */
        if ((tenant_resellrateid > 0) && on_reseller) { /* Billing is done on tenant via a reseller */
            mycall->endu->parent = (struct reseller *) malloc(sizeof(struct reseller));
            if (!mycall->endu->parent) {
                ast_log(LOG_ERROR, "We cannot allocate memory for reseller description! Out of memory!\n");
                mysql_free_result(myres);
                return 1;
            }
            sprintf(querystring,
                    "SELECT users.RateID, users.PrePaid, users.Balance, users.UserID, FSL FROM users INNER JOIN reseller_rate USING(UserID) WHERE reseller_rate.RateID=%i",
                    tenant_resellrateid);

            myres = mysql_stmt(myres, &numRows, querystring);
            if (numRows < 1) {
                ast_log(LOG_ERROR, "Cannot get reseller informations.\n");
                mysql_free_result(myres);
                free(mycall->endu->parent);
                mycall->endu->parent = NULL;
                return 1;
            }

            mysql_data_seek(myres, 0);
            myrow = mysql_fetch_row(myres);

            mycall->endu->parent->fsl = atoi(myrow[4]);
            mycall->endu->parent->userid = atoi(myrow[3]);
            mycall->endu->parent->rateid = tenant_resellrateid;
            mycall->endu->parent->total_buy_cost = 0.0;
            mycall->endu->parent->tmp_duration = 0;

            if (atoi(myrow[1])) {
                if (!(mycall->endu->parent->balanceu = find_ubalance(mycall->endu->parent->userid))) {
                    /* Reseller's ballance not found */
                    mycall->endu->parent->balanceu = (struct balance *) malloc(sizeof(struct balance));
                    if (!mycall->endu->parent->balanceu) {
                        ast_log(LOG_ERROR, "We cannot allocate memory for reseller's balance! Out of memory!\n");
                        mysql_free_result(myres);
                        return 1;
                    }
                    mycall->endu->parent->balanceu->ownerid = mycall->endu->userid;
                    mycall->endu->parent->balanceu->balance = strtod(myrow[2], &pEnd);
                    mycall->endu->parent->balanceu->nbr_calls = 1;
                    mycall->endu->parent->balanceu->sub_balance = 0.0;
                } else {
                    mycall->endu->parent->balanceu->balance = strtod(myrow[2], &pEnd) -
                                                              mycall->endu->parent->balanceu->sub_balance;        /* update from database */
                    mycall->endu->parent->balanceu->nbr_calls++;
                }
            } else {
                mycall->endu->parent->balanceu = NULL;
            }

            mycall->endu->parent->stager = (struct stage *) malloc(sizeof(struct stage));
            if (!mycall->endu->parent->stager) {
                ast_log(LOG_ERROR, "We cannot allocate memory for stage reseller description! Out of memory!\n");
                mysql_free_result(myres);
                return 1;
            }
            init_stage(mycall->endu->parent->stager);
            mycall->endu->parent->endu = mycall->endu;
            mycall->endu->parent->child = NULL;
            mycall->endu->parent->parent = NULL;
            mycall->endu->parent->prov = NULL;

            if (get_rate(mycall, mycall->endu->parent)) {
                mysql_free_result(myres);
                return 1;
            }

            mycall->prov->us = (struct reseller *) malloc(sizeof(struct reseller));
            if (!mycall->prov->us) {
                ast_log(LOG_ERROR, "We cannot allocate memory for reseller 'us' description! Out of memory!\n");
                mysql_free_result(myres);
                return 1;
            }
            mycall->prov->us->fsl = 0;
            mycall->prov->us->userid = 0;
            mycall->prov->us->rateid = atoi(myrow[0]);
            mycall->prov->us->balanceu = NULL;
            mycall->prov->us->total_buy_cost = 0.0;
            mycall->prov->us->tmp_duration = 0;

            mycall->prov->us->stager = (struct stage *) malloc(sizeof(struct stage));
            if (!mycall->prov->us->stager) {
                ast_log(LOG_ERROR, "We cannot allocate memory for stage reseller description! Out of memory!\n");
                mysql_free_result(myres);
                return 1;
            }
            init_stage(mycall->prov->us->stager);
            mycall->prov->us->endu = NULL;
            mycall->prov->us->child = mycall->endu->parent;
            mycall->endu->parent->parent = mycall->prov->us;
            mycall->prov->us->parent = NULL;
            mycall->prov->us->prov = mycall->prov;

            if (get_rate(mycall, mycall->prov->us)) {
                mysql_free_result(myres);
                return 1;
            }

            if (mycall->endu->parent->balanceu ? mycall->endu->parent->balanceu->balance < 0.0 : 0) {
                ast_log(LOG_NOTICE, "No more money for reseller (Id : %i)\n", mycall->endu->parent->userid);
                mysql_free_result(myres);
                free(mycall->endu->parent);
                mycall->endu->parent = NULL;
                return 1;
            }
        } else if ((tenant_rateid > -1) && !on_reseller) { /* Billing is done on tenant without reseller */
            mycall->prov->us = (struct reseller *) malloc(sizeof(struct reseller));
            if (!mycall->prov->us) {
                ast_log(LOG_ERROR, "We cannot allocate memory for reseller 'us' description! Out of memory!\n");
                mysql_free_result(myres);
                return 1;
            }
            mycall->prov->us->fsl = 0;
            mycall->prov->us->userid = 0;
            mycall->prov->us->rateid = tenant_rateid;
            mycall->prov->us->balanceu = NULL;
            mycall->prov->us->total_buy_cost = 0.0;
            mycall->prov->us->tmp_duration = 0;

            mycall->prov->us->stager = (struct stage *) malloc(sizeof(struct stage));
            if (!mycall->prov->us->stager) {
                ast_log(LOG_ERROR, "We cannot allocate memory for stage reseller description! Out of memory!\n");
                mysql_free_result(myres);
                return 1;
            }
            init_stage(mycall->prov->us->stager);
            mycall->prov->us->endu = mycall->endu;
            mycall->prov->us->child = NULL;
            mycall->prov->us->parent = NULL;
            mycall->prov->us->prov = mycall->prov;

            if (get_rate(mycall, mycall->prov->us)) {
                mysql_free_result(myres);
                return 1;
            }

            mycall->endu->parent = mycall->prov->us;
        } else { /* Ouch :S Any price list */
            mysql_free_result(myres);
            return 1;
        }
    } else if ((user_resellrateid > 0) && on_reseller) {    /* Billing is done on user via a reseller */
        mycall->endu->parent = (struct reseller *) malloc(sizeof(struct reseller));
        if (!mycall->endu->parent) {
            ast_log(LOG_ERROR, "We cannot allocate memory for reseller description! Out of memory!\n");
            mysql_free_result(myres);
            return 1;
        }
        sprintf(querystring,
                "SELECT users.RateID, users.PrePaid, users.Balance, users.UserID, FSL FROM users INNER JOIN reseller_rate USING(UserID) WHERE reseller_rate.RateID=%i",
                user_resellrateid);

        myres = mysql_stmt(myres, &numRows, querystring);
        if (numRows < 1) {
            ast_log(LOG_ERROR, "Cannot get reseller informations.\n");
            free(mycall->endu->parent);
            mycall->endu->parent = NULL;
            mysql_free_result(myres);
            return 1;
        }

        mysql_data_seek(myres, 0);
        myrow = mysql_fetch_row(myres);

        mycall->endu->parent->fsl = atoi(myrow[4]);
        mycall->endu->parent->userid = atoi(myrow[3]);
        mycall->endu->parent->rateid = user_resellrateid;
        mycall->endu->parent->total_buy_cost = 0.0;
        mycall->endu->parent->tmp_duration = 0;

        if (atoi(myrow[1])) {
            if (!(mycall->endu->parent->balanceu = find_ubalance(mycall->endu->parent->userid))) {
                /* Reseller's ballance not found */
                mycall->endu->parent->balanceu = (struct balance *) malloc(sizeof(struct balance));
                if (!mycall->endu->parent->balanceu) {
                    ast_log(LOG_ERROR, "We cannot allocate memory for reseller's balance! Out of memory!\n");
                    mysql_free_result(myres);
                    return 1;
                }
                mycall->endu->parent->balanceu->ownerid = mycall->endu->parent->userid;
                mycall->endu->parent->balanceu->balance = strtod(myrow[2], &pEnd);
                mycall->endu->parent->balanceu->nbr_calls = 1;
                mycall->endu->parent->balanceu->sub_balance = 0.0;
            } else {
                mycall->endu->parent->balanceu->balance = strtod(myrow[2], &pEnd) -
                                                          mycall->endu->parent->balanceu->sub_balance;        /* update from database */
                mycall->endu->parent->balanceu->nbr_calls++;
            }
        } else {
            mycall->endu->parent->balanceu = NULL;
        }

        mycall->prov->us = (struct reseller *) malloc(sizeof(struct reseller));
        if (!mycall->prov->us) {
            ast_log(LOG_ERROR, "We cannot allocate memory for reseller 'us' description! Out of memory!\n");
            mysql_free_result(myres);
            return 1;
        }
        mycall->prov->us->fsl = 0;
        mycall->prov->us->userid = 0;
        mycall->prov->us->rateid = atoi(myrow[0]);
        mycall->prov->us->balanceu = NULL;
        mycall->prov->us->total_buy_cost = 0.0;
        mycall->prov->us->tmp_duration = 0;

        mycall->endu->parent->stager = (struct stage *) malloc(sizeof(struct stage));
        if (!mycall->endu->parent->stager) {
            ast_log(LOG_ERROR, "We cannot allocate memory for stage reseller description! Out of memory!\n");
            mysql_free_result(myres);
            return 1;
        }
        init_stage(mycall->endu->parent->stager);
        mycall->endu->parent->endu = mycall->endu;
        mycall->endu->parent->child = NULL;
        mycall->endu->parent->parent = NULL;
        mycall->endu->parent->prov = NULL;

        if (get_rate(mycall, mycall->endu->parent)) {
            mysql_free_result(myres);
            return 1;
        }

        mycall->prov->us->stager = (struct stage *) malloc(sizeof(struct stage));
        if (!mycall->prov->us->stager) {
            ast_log(LOG_ERROR, "We cannot allocate memory for stage reseller description! Out of memory!\n");
            mysql_free_result(myres);
            return 1;
        }
        init_stage(mycall->prov->us->stager);
        mycall->prov->us->endu = NULL;
        mycall->prov->us->child = mycall->endu->parent;
        mycall->endu->parent->parent = mycall->prov->us;
        mycall->prov->us->parent = NULL;
        mycall->prov->us->prov = mycall->prov;

        if (get_rate(mycall, mycall->prov->us)) {
            mysql_free_result(myres);
            return 1;
        }

        if (mycall->endu->parent->balanceu ? (mycall->endu->parent->balanceu->balance < 0.0) : 0) {
            ast_log(LOG_NOTICE, "No more money for reseller (Id : %i)\n", mycall->endu->parent->userid);
            free(mycall->endu->parent);
            mycall->endu->parent = NULL;
            mysql_free_result(myres);
            return 1;
        }
    } else if ((user_rateid > -1) && !on_reseller) { /* Billing is done on user without reseller */
        mycall->prov->us = (struct reseller *) malloc(sizeof(struct reseller));
        if (!mycall->prov->us) {
            ast_log(LOG_ERROR, "We cannot allocate memory for reseller 'us' description! Out of memory!\n");
            mysql_free_result(myres);
            return 1;
        }
        mycall->prov->us->fsl = 0;
        mycall->prov->us->userid = 0;
        mycall->prov->us->rateid = user_rateid;
        mycall->prov->us->balanceu = NULL;
        mycall->prov->us->total_buy_cost = 0.0;
        mycall->prov->us->tmp_duration = 0;

        mycall->prov->us->stager = (struct stage *) malloc(sizeof(struct stage));
        if (!mycall->prov->us->stager) {
            ast_log(LOG_ERROR, "We cannot allocate memory for stage reseller description! Out of memory!\n");
            mysql_free_result(myres);
            return 1;
        }
        init_stage(mycall->prov->us->stager);
        mycall->prov->us->endu = mycall->endu;
        mycall->prov->us->child = NULL;
        mycall->prov->us->parent = NULL;
        mycall->prov->us->prov = mycall->prov;

        if (get_rate(mycall, mycall->prov->us)) {
            mysql_free_result(myres);
            return 1;
        }

        mycall->endu->parent = mycall->prov->us;
    } else {
        return 1;
    }

    mysql_free_result(myres);
    return 0;

}


/*
 * Function to initialize stage structure
 */

static int init_stage(struct stage *stager) {

    if (!stager)
        return 1;

    stager->cost_by_minute = 1;
    stager->stage1 = 1;
    stager->price1 = 1;
    stager->stage1applied = 0;
    stager->stage2 = 1;
    stager->price2 = 1;
    stager->stage2applied = 0;
    stager->stage3 = 1;
    stager->price3 = 1;
    stager->stage3applied = 0;
    stager->stage4 = 1;
    stager->price4 = 1;
    stager->stage4applied = 0;
    stager->call_free = 0;
    stager->cCost = 0;
    stager->cCostApplied = 0;

    return 0;
}


static int get_special_rate(struct reseller *this_reseller, char *number, char *formated_number) {
    MYSQL_RES *myres = NULL;
    MYSQL_ROW myrow;
    char querystring[512];
    unsigned long numRows = 0;

    /* Fisrt, We check if it's a special short number. */
    sprintf(querystring,
            "SELECT stage1, price1, stage2, price2, stage3, price3, stage4, price4 FROM stages WHERE (stages.areacode LIKE '%s') AND (stages.RateID=%i) AND (stages.short=1)",
            number, this_reseller->rateid);

    myres = mysql_stmt(myres, &numRows, querystring);
    if ((numRows < 1) &&
        (this_reseller->rateid != 0)) {             /* If there is no result, we check on default price list */

        sprintf(querystring,
                "SELECT stage1, price1, stage2, price2, stage3, price3, stage4, price4 FROM stages WHERE (stages.areacode LIKE '%s') AND (stages.RateID=0) AND (stages.short=1)",
                number);
        myres = mysql_stmt(myres, &numRows, querystring);
    }

    /* There is no result, we check other special numbers */
    if (numRows < 1) {
        sprintf(querystring,
                "SELECT stage1, price1, stage2, price2, stage3, price3, stage4, price4 FROM stages WHERE ((SELECT '%s' LIKE BINARY CONCAT(stages.areacode,'%%') ) AND (stages.RateID=%i)) AND (stages.short=0) ORDER BY CHAR_LENGTH(stages.areacode) DESC LIMIT 1",
                formated_number, this_reseller->rateid);
        myres = mysql_stmt(myres, &numRows, querystring);

        if ((numRows < 1) &&
            (this_reseller->rateid != 0)) {             /* If there is no result, we check on default price list */

            sprintf(querystring,
                    "SELECT stage1, price1, stage2, price2, stage3, price3, stage4, price4 FROM stages WHERE ((SELECT '%s' LIKE BINARY CONCAT(stages.areacode,'%%') ) AND (stages.RateID=0)) AND (stages.short=0) ORDER BY CHAR_LENGTH(stages.areacode) DESC LIMIT 1",
                    formated_number);

            myres = mysql_stmt(myres, &numRows, querystring);
            if (numRows < 1) {
                mysql_free_result(myres);
                return 1;
            }
        } else if (numRows < 1) {
            mysql_free_result(myres);
            return 1;
        }
    }
    //ast_log(LOG_DEBUG, "Found Special stage rate\n");

    if (numRows > 0) {

        mysql_data_seek(myres, 0);
        myrow = mysql_fetch_row(myres);
        this_reseller->stager->cost_by_minute = -1;
        this_reseller->stager->stage1 = atoi(myrow[0]);
        this_reseller->stager->price1 = atoi(myrow[1]) / 100.0; /* Unit to bill is the cent */
        this_reseller->stager->stage2 = atoi(myrow[2]);
        this_reseller->stager->price2 = atoi(myrow[3]) / 100.0;
        this_reseller->stager->stage3 = atoi(myrow[4]);
        this_reseller->stager->price3 = atoi(myrow[5]) / 100.0;
        this_reseller->stager->stage4 = atoi(myrow[6]);
        this_reseller->stager->price4 = atoi(myrow[7]) / 100.0;
        this_reseller->stager->cCost = 0;
        if (this_reseller->stager->price4 == 0 && this_reseller->stager->price3 == 0 &&
            this_reseller->stager->price2 == 0 && this_reseller->stager->price1 == 0) {
            this_reseller->stager->call_free = 0;
        }
        mysql_free_result(myres);
        return 0;
    }
    mysql_free_result(myres);
    return 1;
}


/*
 * Return the price for spÃ<88>cifique destination.
 *
 * mycall :
 *              struct call pointer reference.
 * this_reseller :
 *              reseller structure to get rate for.
 */
static int get_rate(struct call *mycall, struct reseller *this_reseller) {
    MYSQL_RES *myres = NULL;
    MYSQL_ROW myrow;
    double marge = 0.0;
    double reseller_marge = 0.0;
    int assigned_RateID = 0, bigger_len = 0;
    unsigned long numRows = 0;
    char querystring[512];
    char *pEnd;
    struct reseller *next;

    /* Check for Special number*/
    //ast_log(LOG_DEBUG, "-- %s -- Check for special number.\n", mycall->uniqueid);
    if (this_reseller->userid > -1) {        /* Should be us if userid = 0 */
        if (!get_special_rate(this_reseller, mycall->destination_number, mycall->formated_destination_number)) {
            if (this_reseller->prov ? this_reseller->prov->stager : 0) {
                free(this_reseller->prov->stager);
                this_reseller->prov->stager = NULL;
            }

            /* Special rate can be assigned only by us. Reseller are not allowed to assign special rate. */
            next = this_reseller->child ? this_reseller->child : NULL;
            while (next) {
                if (next->stager) {
                    free(next->stager);
                }
                next->stager = (struct stage *) malloc(sizeof(struct stage));
                if (!next->stager) {
                    ast_log(LOG_ERROR, "Out of memory!\n");
                } else {
                    next->stager->cost_by_minute = this_reseller->stager->cost_by_minute;
                    next->stager->stage1 = this_reseller->stager->stage1;
                    next->stager->price1 = this_reseller->stager->price1;
                    next->stager->stage1applied = 0;
                    next->stager->stage2 = this_reseller->stager->stage2;
                    next->stager->price2 = this_reseller->stager->price2;
                    next->stager->stage2applied = 0;
                    next->stager->stage3 = this_reseller->stager->stage3;
                    next->stager->price3 = this_reseller->stager->price3;
                    next->stager->stage3applied = 0;
                    next->stager->stage4 = this_reseller->stager->stage4;
                    next->stager->price4 = this_reseller->stager->price4;
                    next->stager->stage4applied = 0;
                    next->stager->call_free = this_reseller->stager->call_free;
                    next->stager->cCost = 0;
                }

                if (next->child) {
                    next = next->child;
                } else if (!next->endu) {
                    ast_log(LOG_WARNING, "Assign special rate for all reseller, but cannot detect the end user.\n");
                    next = NULL;
                } else {
                    next = NULL;
                }
            }

            return 0;
        }
    }

    if (mycall->areacode) {
        bigger_len = strlen(mycall->areacode); /* This the bigger lenthg to get */
    }

    /* Get the rate */
    if (this_reseller->userid == 0) {

       // ast_log(LOG_DEBUG, "-- %s -- Search for rate (direct sell)...\n", mycall->uniqueid);

        sprintf(querystring, "SELECT marge FROM rate WHERE RateID=%i", this_reseller->rateid);
        myres = mysql_stmt(myres, &numRows, querystring);
        if (numRows < 0) {
            mysql_free_result(myres);
            return 1;
        }

        if (numRows) {
            mysql_data_seek(myres, 0);
            myrow = mysql_fetch_row(myres);
            marge = strtod(myrow[0], &pEnd);
        } else {
            marge = 0;
        }

        sprintf(querystring,
                "SELECT rate, cCost, areacode FROM sell_rate WHERE (RateID=%i) AND (SELECT '%s' LIKE BINARY CONCAT(areacode,'%s')) ORDER BY CHAR_LENGTH(areacode) DESC LIMIT 1",
                this_reseller->rateid, mycall->formated_destination_number, "%");
        myres = mysql_stmt(myres, &numRows, querystring);
        if (numRows < 1) {
            sprintf(querystring,
                    "SELECT rate*(1+%f), cCost*(1+%f) FROM sell_rate WHERE RateID=0 AND (SELECT '%s' LIKE BINARY CONCAT(areacode,'%s')) ORDER BY CHAR_LENGTH(areacode) DESC LIMIT 1",
                    marge / 100, marge / 100, mycall->formated_destination_number, "%");

            myres = mysql_stmt(myres, &numRows, querystring);
            if (numRows < 1) {
                mysql_free_result(myres);
                return 1;
            }
        } else {
            mysql_data_seek(myres, 0);
            myrow = mysql_fetch_row(myres);

            if (bigger_len >
                strlen(myrow[2])) {             /* Ouch, we find a bigger length for areacode, so we have to check on buy price*marge */
                //ast_log(LOG_DEBUG, "Bigger is right, check on buy price * marge\n");
                sprintf(querystring,
                        "SELECT rate*(1+%f), cCost*(1+%f) FROM sell_rate WHERE RateID=0 AND (SELECT '%s' LIKE BINARY CONCAT(areacode,'%s')) ORDER BY CHAR_LENGTH(areacode) DESC LIMIT 1",
                        marge / 100, marge / 100, mycall->formated_destination_number, "%");

                myres = mysql_stmt(myres, &numRows, querystring);
                if (numRows < 1) {
                    mysql_free_result(myres);
                    return 1;
                }
            }
        }
        mysql_data_seek(myres, 0);
        myrow = mysql_fetch_row(myres);
        this_reseller->stager->cost_by_minute = atoi(myrow[0]);
        this_reseller->stager->stage1 = 1;              /* It's not a special number, Bill is done by second */
        this_reseller->stager->price1 = (this_reseller->stager->cost_by_minute / 100.0) / 60.0;
        this_reseller->stager->stage2 = 1;
        this_reseller->stager->price2 = this_reseller->stager->price1;
        this_reseller->stager->stage3 = 1;
        this_reseller->stager->price3 = this_reseller->stager->price1;
        this_reseller->stager->stage4 = 1;
        this_reseller->stager->price4 = this_reseller->stager->price1;
        this_reseller->stager->cCost = atoi(myrow[1]) / 100.0;
        if (this_reseller->stager->price1 == 0)
            this_reseller->stager->call_free = 1;
        mysql_free_result(myres);
        return 0;

    } else if (this_reseller->userid > 0) {
        //ast_log(LOG_DEBUG, "-- Search for rate (for reseller)...\n");
        sprintf(querystring,
                "SELECT reseller_rate.marge, users.RateID FROM reseller_rate INNER JOIN users USING(UserID) WHERE reseller_rate.RateID=%i",
                this_reseller->rateid);

        myres = mysql_stmt(myres, &numRows, querystring);
        if (numRows < 0) {
            mysql_free_result(myres);
            return 1;
        }

        if (numRows) {
            mysql_data_seek(myres, 0);
            myrow = mysql_fetch_row(myres);
            reseller_marge = strtod(myrow[0], &pEnd);
        } else {
            mysql_free_result(myres);
            return 1;
        }

        if (atoi(myrow[1])) {
            sprintf(querystring,
                    "SELECT rate.marge, users.RateID FROM rate INNER JOIN users ON rate.RateID = users.RateID INNER JOIN reseller_rate ON users.UserID = reseller_rate.UserID WHERE reseller_rate.RateID=%i",
                    this_reseller->rateid);

            myres = mysql_stmt(myres, &numRows, querystring);
            if (numRows < 0) {
                mysql_free_result(myres);
                return 1;
            }

            if (numRows) {
                mysql_data_seek(myres, 0);
                myrow = mysql_fetch_row(myres);
                marge = strtod(myrow[0], &pEnd);
                assigned_RateID = atoi(myrow[1]);
            } else {
                marge = 0;
                assigned_RateID = 0;
            }
        } else {
            marge = 0;
            assigned_RateID = 0;
        }

        sprintf(querystring,
                "SELECT rate, cCost, areacode FROM resell_rate WHERE ResellRateID=%i AND (SELECT '%s' LIKE BINARY CONCAT(areacode,'%s')) ORDER BY CHAR_LENGTH(areacode) DESC LIMIT 1",
                this_reseller->rateid, mycall->formated_destination_number, "%");

        myres = mysql_stmt(myres, &numRows, querystring);
        if (numRows < 1) {
            //ast_log(LOG_DEBUG, "Bigger is right, check on buy price * marge\n");
            sprintf(querystring,
                    "SELECT rate*(1+%f), cCost*(1+%f), areacode FROM sell_rate WHERE RateID=%i AND (SELECT '%s' LIKE BINARY CONCAT(areacode,'%s')) ORDER BY CHAR_LENGTH(areacode) DESC LIMIT 1",
                    reseller_marge / 100, reseller_marge / 100, assigned_RateID,
                    mycall->formated_destination_number,
                    "%");

            myres = mysql_stmt(myres, &numRows, querystring);
            if (numRows < 1) {
                sprintf(querystring,
                        "SELECT (rate*(1+%f))*(1+%f), (cCost*(1+%f))*(1+%f) FROM sell_rate WHERE RateID=0 AND (SELECT '%s' LIKE BINARY CONCAT(areacode,'%s')) ORDER BY CHAR_LENGTH(areacode) DESC LIMIT 1",
                        marge / 100, reseller_marge / 100, marge / 100, reseller_marge / 100,
                        mycall->formated_destination_number, "%");

                myres = mysql_stmt(myres, &numRows, querystring);
                if (numRows < 1) {
                    mysql_free_result(myres);
                    return 1;
                }
            } else {
                mysql_data_seek(myres, 0);
                myrow = mysql_fetch_row(myres);

                if (bigger_len >
                    strlen(myrow[2])) {             /* Ouch, we find a bigger length for areacode, so we have to check on buy price*marge */
                    //ast_log(LOG_DEBUG, "Bigger is right, check on buy price * marge\n");
                    sprintf(querystring,
                            "SELECT (rate*(1+%f))*(1+%f), (cCost*(1+%f))*(1+%f) FROM sell_rate WHERE RateID=0 AND (SELECT '%s' LIKE BINARY CONCAT(areacode,'%s')) ORDER BY CHAR_LENGTH(areacode) DESC LIMIT 1",
                            marge / 100, reseller_marge / 100, marge / 100, reseller_marge / 100,
                            mycall->formated_destination_number, "%");

                    myres = mysql_stmt(myres, &numRows, querystring);
                    if (numRows < 1) {
                        mysql_free_result(myres);
                        return 1;
                    }
                }
            }
        } else {
            mysql_data_seek(myres, 0);
            myrow = mysql_fetch_row(myres);

            if (bigger_len >
                strlen(myrow[2])) {             /* Ouch, we find a bigger length for areacode, so we have to check on buy price*marge */
                //ast_log(LOG_DEBUG, "Bigger is right, check on buy price * marge\n");
                sprintf(querystring,
                        "SELECT rate*(1+%f), cCost*(1+%f), areacode FROM sell_rate WHERE RateID=%i AND (SELECT '%s' LIKE BINARY CONCAT(areacode,'%s')) ORDER BY CHAR_LENGTH(areacode) DESC LIMIT 1",
                        reseller_marge / 100, reseller_marge / 100, assigned_RateID,
                        mycall->formated_destination_number, "%");

                myres = mysql_stmt(myres, &numRows, querystring);
                if (numRows < 1) {
                    sprintf(querystring,
                            "SELECT (rate*(1+%f))*(1+%f), (cCost*(1+%f))*(1+%f) FROM sell_rate WHERE RateID=0 AND (SELECT '%s' LIKE BINARY CONCAT(areacode,'%s')) ORDER BY CHAR_LENGTH(areacode) DESC LIMIT 1",
                            marge / 100, reseller_marge / 100, marge / 100, reseller_marge / 100,
                            mycall->formated_destination_number, "%");

                    myres = mysql_stmt(myres, &numRows, querystring);
                    if (numRows < 1) {
                        mysql_free_result(myres);
                        return 1;
                    }
                } else {
                    mysql_data_seek(myres, 0);
                    myrow = mysql_fetch_row(myres);

                    if (bigger_len >
                        strlen(myrow[2])) {             /* Ouch, we find a bigger length for areacode, so we have to check on buy price*marge */
                        //ast_log(LOG_DEBUG, "Bigger is right, check on buy price * marge\n");
                        sprintf(querystring,
                                "SELECT (rate*(1+%f))*(1+%f), (cCost*(1+%f))*(1+%f) FROM sell_rate WHERE RateID=0 AND (SELECT '%s' LIKE BINARY CONCAT(areacode,'%s')) ORDER BY CHAR_LENGTH(areacode) DESC LIMIT 1",
                                marge / 100, reseller_marge / 100, marge / 100, reseller_marge / 100,
                                mycall->formated_destination_number, "%");

                        myres = mysql_stmt(myres, &numRows, querystring);
                        if (numRows < 1) {
                            mysql_free_result(myres);
                            return 1;
                        }
                    }
                }
            }
        }

        /* Assigne the reseller rate */
        mysql_data_seek(myres, 0);
        myrow = mysql_fetch_row(myres);
        this_reseller->stager->cost_by_minute = atoi(myrow[0]);
        this_reseller->stager->stage1 = 1;              /* It's not a special number, Bill is done by second */
        this_reseller->stager->price1 = (this_reseller->stager->cost_by_minute / 100.0) / 60.0;
        this_reseller->stager->stage2 = 1;
        this_reseller->stager->price2 = this_reseller->stager->price1;
        this_reseller->stager->stage3 = 1;
        this_reseller->stager->price3 = this_reseller->stager->price1;
        this_reseller->stager->stage4 = 1;
        this_reseller->stager->price4 = this_reseller->stager->price1;
        this_reseller->stager->cCost = atoi(myrow[1]) / 100.0;
        if (this_reseller->stager->price1 == 0)
            this_reseller->stager->call_free = 1;
    }
    mysql_free_result(myres);
    return 0;


}








/**
 *
 */

/**
 * Init Provider Structure
 * @param prov
 * @param prov_name
 * @param number
 * @return
 */
static int init_provider(struct provider *prov, char *prov_name, char *number) {
    MYSQL_RES *myres = NULL;
    MYSQL_ROW myrow;
    unsigned long numRows = 0;
    char querystring[512];

    if (prov_name ? strlen(prov_name) : 0) {
        sprintf(prov->name, "%s", prov_name);
        sprintf(querystring, "SELECT RouteID, ProviderID FROM provider NATURAL JOIN route WHERE provider.name='%s'",
                prov->name);
        myres = mysql_stmt(myres, &numRows, querystring);
        if ((numRows < 1) || (!myres)) {
            prov->stager = NULL;
            prov->providerid = 0;
            prov->us = NULL;
            mysql_free_result(myres);
            return 0;
        }

        mysql_data_seek(myres, 0);
        myrow = mysql_fetch_row(myres);

        prov->providerid = atoi(myrow[1]);

        sprintf(querystring,
                "SELECT buy_rate.rate, areacode FROM buy_rate WHERE routeid=%s AND areacode LIKE '%c%%' AND (SELECT '%s' LIKE BINARY CONCAT(areacode,'%%')) ORDER BY CHAR_LENGTH(areacode) DESC LIMIT 1",
                myrow[0], number[0], number);

        myres = mysql_stmt(myres, &numRows, querystring);
        if (numRows < 1) {
            prov->stager = NULL;
            prov->providerid = 0;
            prov->us = NULL;
            mysql_free_result(myres);
            return 0;
        }

        mysql_data_seek(myres, 0);
        myrow = mysql_fetch_row(myres);

        prov->stager = NULL;
        prov->stager = (struct stage *) malloc(sizeof(struct stage));
        if (!prov->stager) {
            mysql_free_result(myres);
            return 1;
        }
        prov->stager->cost_by_minute = atoi(myrow[0]);
        prov->stager->stage1 = 1;
        prov->stager->price1 = (prov->stager->cost_by_minute / 60.0) / 100.0;       /* Price in cents /s */
        prov->stager->stage2 = 1;
        prov->stager->price2 = prov->stager->price1;    /* Price in cents /s */
        prov->stager->stage3 = 1;
        prov->stager->price3 = prov->stager->price1;    /* Price in cents /s */
        prov->stager->stage4 = 1;
        prov->stager->price4 = prov->stager->price1;    /* Price in cents /s */
        prov->us = NULL;
        mysql_free_result(myres);
    } else {
        prov->name[0] = '\0';
        prov->stager = NULL;
        prov->us = NULL;
        prov->providerid = 0;
    }

    return 0;


}


/**
 * Init endUser structure present in Call structure by using accountcode param
 * @param endu
 * @param accountcode
 * @return
 */
static int init_user(struct end_user *endu, const char *accountcode) {
    MYSQL_RES *myres = NULL;
    MYSQL_ROW myrow;
    unsigned long numRows = 0;
    char querystring[512];
    char *pEnd;

    if (!(accountcode ? strlen(accountcode) : 0)) {
        return 1;
    }

    sprintf(querystring,
            "SELECT users.UserID, users.FirstName, users.LastName, users.PrePaid, users.TenantID, users.Balance, tenant.name, tenant.PrePaid, tenant.Balance, FSL FROM users INNER JOIN tenant USING(TenantID) WHERE users.UserID=%s",
            accountcode);

    myres = mysql_stmt(myres, &numRows, querystring);
    if (numRows < 1) {
        mysql_free_result(myres);
        ast_log(LOG_NOTICE, "-- No user found for accountcode %s\n", accountcode);
        return 1;
    }

    mysql_data_seek(myres, 0);
    myrow = mysql_fetch_row(myres);

    endu->userid = atoi(myrow[0]);
    endu->fsl = atoi(myrow[9]);
    sprintf(endu->first_name, "%s", myrow[1]);
    sprintf(endu->last_name, "%s", myrow[2]);
    endu->tenantid = atoi(myrow[4]);
    sprintf(endu->tenant_name, "%s", myrow[6]);
    endu->total_buy_cost = 0;
    endu->tmp_duration = 0;
    endu->balanceu = NULL;
    endu->balancet = NULL;
    endu->parent = NULL;
    if (atoi(myrow[3])) { /* User has a prepaid account */
        if (!(endu->balanceu = find_ubalance(endu->userid))) {
            /* User's balance not found */
            endu->balanceu = (struct balance *) malloc(sizeof(struct balance));
            if (!endu->balanceu) {
                ast_log(LOG_ERROR, "We cannot allocate memory for user's balance! Out of memory!\n");
                return 1;
            }
            endu->balanceu->ownerid = endu->userid;
            endu->balanceu->balance = strtod(myrow[5], &pEnd);
            endu->balanceu->nbr_calls = 1;
            endu->balanceu->sub_balance = 0.0;
        } else {
            endu->balanceu->balance =
                    strtod(myrow[5], &pEnd) - endu->balanceu->sub_balance;        /* update from database */
            endu->balanceu->nbr_calls++;
        }
    } else {
        endu->balanceu = NULL;
    }

    if (atoi(myrow[7])) { /* tenant has a prepaid account */
        if (!(endu->balancet = find_tbalance(endu->tenantid))) {
            /* Tenant's balance not found */
            endu->balancet = (struct balance *) malloc(sizeof(struct balance));
            if (!endu->balancet) {
                ast_log(LOG_ERROR, "We cannot allocate memory for tenant's balance! Out of memory!\n");
                return 1;
            }
            endu->balancet->ownerid = endu->tenantid;
            endu->balancet->balance = strtod(myrow[8], &pEnd);
            endu->balancet->nbr_calls = 1;
            endu->balancet->sub_balance = 0.0;
        } else {
            endu->balancet->balance =
                    strtod(myrow[8], &pEnd) - endu->balancet->sub_balance;        /* update from database */
            endu->balancet->nbr_calls++;
        }
    } else {
        endu->balancet = NULL;
    }

    mysql_free_result(myres);
    endu->parent = NULL;

    /* No more money */
    /*if((endu->balanceu ? (endu->balanceu->balance <= 0.0) :  0) || (endu->balancet ? (endu->balancet->balance <= 0.0) : 0)) {
            if(endu->balanceu ? (endu->balanceu->balance <= 0.0) :  0) {
                    ast_log(LOG_NOTICE, "No more money for user %s %s from tenant %s\n", endu->last_name, endu->first_name, endu->tenant_name);
            } else if(endu->balancet ? (endu->balancet->balance <= 0.0) : 0) {
                    ast_log(LOG_NOTICE, "No more money for tenant %s\n", endu->tenant_name);
            }
            return 1;
    }*/

    return 0;

}

/**
 * Search for tenant balance in our calls list !
 * @param tenantid
 * @return
 */
struct balance *find_tbalance(int tenantid) {
    int i;

    for (i = 0; i < MAX_CALLS; i++) {
        if (current_calls[i]) {
            if (current_calls[i]->endu) {
                if (current_calls[i]->endu->balancet ? current_calls[i]->endu->balancet->ownerid == tenantid : 0) {
                    return current_calls[i]->endu->balancet;
                }
            }
        }
    }

    return NULL;
}


/**
 * Found and return the balance of the user having id = userid
 * We search for that balance in our list of calls
 * @param userid
 * @return
 */
struct balance *find_ubalance(int userid) {
    int i;
    struct reseller *this_reseller;

    for (i = 0; i < MAX_CALLS; i++) {
        if (current_calls[i]) {
            this_reseller = current_calls[i]->prov ? (current_calls[i]->prov->us ? current_calls[i]->prov->us : NULL)
                                                   : NULL;
            while (this_reseller) { /* Walk in resellers */
                if (this_reseller->balanceu ? this_reseller->balanceu->ownerid == userid : 0) {
                    return this_reseller->balanceu;
                }
                if (this_reseller->child)
                    this_reseller = this_reseller->child;
                else
                    break;
            }

            /* At this point, it's a user */
            if (this_reseller ? this_reseller->endu : 0) {
                if (this_reseller->endu->balanceu ? this_reseller->endu->balanceu->ownerid == userid : 0) {
                    return this_reseller->endu->balanceu;
                }
            }
        }
    }

    return NULL;

}


/**
 * We Need To format international number in order to compare them after to pricelist rates
 * Pretty simple , We Query to get all international number in a way that we have the digit_to_delete and the new prefix and we exchange them !
 * If it's not found in our DB , We Consider that is a valid Number and we put the response in destination
 * @param destnumber
 * @param destination
 * @param userid
 * @return 0 : Success , 1 : Failure
 */
static int format_international(char *number, char *dst, int userid) {
    MYSQL_RES *myres = NULL;
    MYSQL_ROW myrow;
    unsigned long numRows = 0;
    char querystring[512];
    char destnum_tmp[26];

    //ast_log(LOG_DEBUG, "-- Getting internationnal number...\n");
    sprintf(querystring, "SELECT prefix_in.digit_delete, prefix_in.new_prefix FROM prefix_in INNER JOIN tenant USING(TenantID) INNER JOIN users ON tenant.TenantID=users.TenantID WHERE ((SELECT '%s' LIKE BINARY CONCAT(prefix_in.prefix,'%%') ) AND (users.UserID=%i)) ORDER BY CHAR_LENGTH(prefix_in.prefix) DESC LIMIT 1", number, userid);

    myres = mysql_stmt(myres, &numRows, querystring);
    if(numRows < 1) {
        sprintf(querystring, "SELECT prefix_in.digit_delete, prefix_in.new_prefix FROM prefix_in WHERE ((SELECT '%s' LIKE BINARY CONCAT(prefix_in.prefix,'%%') ) AND (prefix_in.TenantID=1)) ORDER BY CHAR_LENGTH(prefix_in.prefix) DESC LIMIT 1", number);
        //ast_log(LOG_DEBUG, "-- No result found. Try on default tenant...\n");
        myres = mysql_stmt(myres, &numRows, querystring);
        if(numRows < 0) {
            mysql_free_result(myres);
            sprintf(dst, "%s", number);
            return 1;
        }
    }

    if(numRows) {
        mysql_data_seek(myres, 0);
        myrow = mysql_fetch_row(myres);

        /* Format destination number to compare with price liste */
        strncpy(destnum_tmp, number+atoi(myrow[0]), 26-atoi(myrow[0]));
        sprintf(dst, "%s%s", myrow[1], destnum_tmp);
    } else {
        sprintf(dst, "%s", number);
    }

    //ast_log(LOG_DEBUG, "-- International number is %s.\n", dst);
    mysql_free_result(myres);
    return 0;

}

/**
 * Get DestName mapped to this particular destNumber
 * @param destnumber
 * @param destName
 * @return
 */
static int get_dest_name(char *destnumber, char *destName) {
    MYSQL_RES *myres = NULL;
    MYSQL_ROW myrow;
    unsigned long numRows = 0;
    char querystring[512];

    /** First Let's get stageid to check whether it is a special Number or not **/
    sprintf(querystring, "SELECT stageid from stages WHERE areacode='%s' AND short=1", destnumber);
    myres = mysql_stmt(myres, &numRows, querystring);
    if (numRows > 0) {
        sprintf(destName, "%s", "Special Number");
    } else {
        sprintf(querystring,
                "SELECT DestName FROM destination WHERE (SELECT '%s' LIKE BINARY CONCAT(areacode , '%%')) ORDER BY CHAR_LENGTH(areacode) DESC LIMIT 1 ",
                destnumber);
        //ast_log(LOG_DEBUG, "-- Getting DestName using Query\n%s\n", querystring);
        myres = mysql_stmt(myres, &numRows, querystring);
        /** Check Results **/
        if (numRows > 0) {
            sprintf(destName, "%s", "Special Number");
            mysql_data_seek(myres, 0);
            myrow = mysql_fetch_row(myres);
            sprintf(destName, "%s", myrow[0]);
        } else {
            /** No data , set to empty **/
            destName[0] = '\0';
        }
    }

    /** Free Mysql results **/
    mysql_free_result(myres);
    return 0;

}


/**
 * Get areaCode mapped to this particular destNumber
 * @param destNumber
 * @param areacode
 * @return
 */
static int get_areacode(char *destNumber, char *areacode) {
    MYSQL_RES *myres = NULL;
    MYSQL_ROW myrow;
    unsigned long numRows = 0;
    char queryString[512];
    /** Query for AreaCode **/
    sprintf(queryString,
            "SELECT areacode FROM destination WHERE (SELECT '%s' LIKE BINARY CONCAT(areacode,'%%')) ORDER BY CHAR_LENGTH(areacode) DESC LIMIT 1",
            destNumber);
    myres = mysql_stmt(myres, &numRows, queryString);
    /** Check Results **/
    if (numRows > 0) {
        mysql_data_seek(myres, 0);
        myrow = mysql_fetch_row(myres);
        sprintf(areacode, "%s", myrow[0]);
    } else {
        areacode[0] = '\0';
    }

    mysql_free_result(myres);
    return 0;
}

/**
 * Dump stats in our database about calls of the day
 * @param carrierid
 * @param areacode
 * @param duration
 */
void asr_acd(int carrierid, char *areacode, int duration) {
    MYSQL_RES *myres = NULL;
    char *querystring;
    char date[24];
    unsigned long numRows = 0;

    querystring = (char *) malloc(sizeof(char) * 768);

    get_formated_time_of_the_day_today(date);

    if (duration > 0) {
        sprintf(querystring,
                "UPDATE asr_acd SET Successfull_Calls = Successfull_Calls+1, Total_Calls = Total_Calls+1, Total_Duration = Total_Duration+%i WHERE Date_Of_Day='%s' AND carrierid=%i AND areacode='%s'",
                duration, date, carrierid, areacode);
    } else
        sprintf(querystring,
                "UPDATE asr_acd SET Total_Calls = Total_Calls+1 WHERE Date_Of_Day = '%s' AND carrierid=%i AND areacode='%s'",
                date, carrierid, areacode);

    myres = mysql_stmt_master(myres, &numRows, querystring);
    if (!numRows) {
        if (duration > 0) {
            sprintf(querystring,
                    "INSERT INTO  asr_acd(Successfull_Calls, Total_Calls, Total_Duration, Date_Of_Day, carrierid, areacode) VALUES(1,1,%i,'%s',%i,'%s')",
                    duration, date, carrierid, areacode);
        } else {
            sprintf(querystring,
                    "INSERT INTO  asr_acd(Successfull_Calls, Total_Calls, Total_Duration, Date_Of_Day, carrierid, areacode) VALUES(0,1,0,'%s',%i,'%s')",
                    date, carrierid, areacode);
        }
        myres = mysql_stmt_master(myres, &numRows, querystring);
    }
    mysql_free_result(myres);
    free(querystring);
    return;

}



void asr_acd_user(int resellerid, int tenantid, int userid, char *areacode, int duration) {

    MYSQL_RES *myres = NULL;
    char *querystring;
    char date[24];
    unsigned long numRows = 0;

    querystring = (char *)malloc(sizeof(char)*768);

    get_formated_time_of_the_day_today(date);

    if(duration > 0) {
        sprintf(querystring, "UPDATE asr_acd_user SET Successfull_Calls = Successfull_Calls+1, Total_Calls = Total_Calls+1, Total_Duration = Total_Duration+%i WHERE Date_Of_Day='%s' AND userid=%i AND areacode='%s'", duration, date, userid, areacode);
    } else
        sprintf(querystring, "UPDATE asr_acd_user SET Total_Calls = Total_Calls+1 WHERE Date_Of_Day = '%s' AND userid=%i AND areacode='%s'", date, userid, areacode);

    myres = mysql_stmt_master(myres, &numRows, querystring);
    if(!numRows) {
        if(duration > 0) {
            sprintf(querystring, "INSERT INTO  asr_acd_user(Successfull_Calls, Total_Calls, Total_Duration, Date_Of_Day, resellerid, tenantid, userid, areacode) VALUES(1,1,%i,'%s',%i,%i,%i,'%s')", duration, date, resellerid, tenantid, userid, areacode);
        } else {
            sprintf(querystring, "INSERT INTO  asr_acd_user(Successfull_Calls, Total_Calls, Total_Duration, Date_Of_Day, resellerid, tenantid, userid, areacode) VALUES(0,1,0,'%s',%i,%i,%i,'%s')", date, resellerid, tenantid, userid, areacode);
        }
        myres = mysql_stmt_master(myres, &numRows, querystring);
    }
    mysql_free_result(myres);
    free(querystring);
    return;
}


/**
 * Once Call has been ended , All Cdr data and update on balances will be made , this function is reponsible for it
 * @param cdr
 * @return
 */
static int end_call(struct ast_cdr *cdr) {

    MYSQL_RES *myres = NULL;
    MYSQL_ROW myrow;
    int pcall = -1, res = 0;
    char *querystring, *userFirstName, *userLastName, *tenantName, *callerId;
    char timestart[128], timeconnected[128], timeend[128];
    struct reseller *next;
    unsigned long numRows = 0;
    char *myhost = "LEA18";

    querystring = (char *) malloc(sizeof(char) * 768);
    ast_mutex_lock(&_monitor_mutex);

    //ast_log(LOG_DEBUG, "disposition is : %s\n" , ast_cdr_disp2str(cdr->disposition) );

    /** Check Call is still up **/
    if ((pcall = existing_call(cdr->uniqueid)) > -1) {
        get_formated_time_now(current_calls[pcall]->end_time);

        userLastName = (char *) malloc(sizeof(char *) * (strlen(current_calls[pcall]->endu->last_name) * 2 + 1));
        userFirstName = (char *) malloc(sizeof(char *) * (strlen(current_calls[pcall]->endu->first_name) * 2 + 1));
        tenantName = (char *) malloc(sizeof(char *) * (strlen(current_calls[pcall]->endu->tenant_name) * 2 + 1));
        callerId = (char *) malloc(sizeof(char *) * (strlen(cdr->clid) * 2 + 1));

        escapeString(current_calls[pcall]->endu->first_name, userFirstName);
        escapeString(current_calls[pcall]->endu->last_name, userLastName);
        escapeString(current_calls[pcall]->endu->tenant_name, tenantName);
        escapeString(cdr->clid, callerId);

	//Get Connected Time
	get_formated_time(&cdr->answer.tv_sec, timeconnected);

        if (current_calls[pcall]->endu->parent)
            sprintf(querystring,
                    "INSERT DELAYED INTO "
                            "cdr(UniqueID,UserID,UserFirstName,UserLastName,TenantID,TenantName,CallerID,ChannelIn,ChannelOut,DestNumber,Pin,"
                            "IncommingTime,ConnectedTime,EndTime,Duration,BuyCost,SellCost,ResellCost,TotalResellCost,DestName,areacode,resellerid,carrierid,cause) "
                            "VALUES('%s-%s','%i','%s','%s','%i','%s','%s','%s','%s','%s','%s','%s','%s','%s','%i','%i','%i','%i','%f','%s','%s','%i','%i','%d')",
                    myhost, cdr->uniqueid, current_calls[pcall]->endu->userid, userFirstName, userLastName,
                    current_calls[pcall]->endu->tenantid, tenantName, callerId,
                    current_calls[pcall]->channel_in_name, cdr->dstchannel, current_calls[pcall]->destination_number,
                    cdr->userfield, current_calls[pcall]->incomming_time,
                    (!strstr(timeconnected, "1970")) ? timeconnected : current_calls[pcall]->incomming_time,
		    current_calls[pcall]->end_time, current_calls[pcall]->duration,
                    current_calls[pcall]->prov->stager ? current_calls[pcall]->prov->stager->cost_by_minute : -1,
                    current_calls[pcall]->prov->us->stager->cost_by_minute,
                    current_calls[pcall]->endu->parent == current_calls[pcall]->prov->us ? -1
                                                                                         : current_calls[pcall]->endu->parent->stager->cost_by_minute,
                    current_calls[pcall]->endu->total_buy_cost,
                    current_calls[pcall]->DestName, current_calls[pcall]->areacode,
                    current_calls[pcall]->endu->parent->userid,
                    current_calls[pcall]->prov ? current_calls[pcall]->prov->providerid : 0, ast_channel_hangupcause(current_calls[pcall]->channel));
        else
            sprintf(querystring,
                    "INSERT DELAYED INTO "
                            "cdr(UniqueID,UserID,UserFirstName,UserLastName,TenantID,TenantName,CallerID,ChannelIn,ChannelOut,DestNumber,Pin,"
                            "IncommingTime,ConnectedTime,EndTime,Duration,BuyCost,SellCost,ResellCost,TotalResellCost,DestName,areacode,carrierid,cause) "
                            "VALUES('%s-%s','%i','%s','%s','%i','%s','%s','%s','%s','%s','%s','%s','%s','%s','%i','%i','%i','%i','%f','%s','%s','%i',%d)",
                    myhost, cdr->uniqueid, current_calls[pcall]->endu->userid, userFirstName, userLastName,
                    current_calls[pcall]->endu->tenantid, tenantName, callerId,
                    current_calls[pcall]->channel_in_name, cdr->dstchannel, current_calls[pcall]->destination_number,
                    cdr->userfield, current_calls[pcall]->incomming_time,
		    (!strstr(timeconnected , "1970")) ? timeconnected : current_calls[pcall]->incomming_time,
		    current_calls[pcall]->end_time,
                    current_calls[pcall]->duration,
                    current_calls[pcall]->prov->stager ? current_calls[pcall]->prov->stager->cost_by_minute : -1,
                    current_calls[pcall]->prov->us->stager->cost_by_minute,
                    current_calls[pcall]->endu->parent == current_calls[pcall]->prov->us ? -1
                                                                                         : current_calls[pcall]->endu->parent->stager->cost_by_minute,
                    current_calls[pcall]->endu->total_buy_cost,
                    current_calls[pcall]->DestName, current_calls[pcall]->areacode,
                    current_calls[pcall]->prov ? current_calls[pcall]->prov->providerid : 0, ast_channel_hangupcause(current_calls[pcall]->channel)
            );

        // Dump ASR and ACD statistics.
        if(current_calls[pcall]->prov ? current_calls[pcall]->prov->providerid > 0 : 0) {
            asr_acd(current_calls[pcall]->prov->providerid, current_calls[pcall]->areacode, current_calls[pcall]->duration);
            asr_acd(0, current_calls[pcall]->areacode, current_calls[pcall]->duration);
        } else {
            asr_acd(0, current_calls[pcall]->areacode, current_calls[pcall]->duration);
        }

        if(current_calls[pcall]->endu->parent)
            asr_acd_user(current_calls[pcall]->endu->parent->userid, current_calls[pcall]->endu->tenantid, current_calls[pcall]->endu->userid, current_calls[pcall]->areacode, current_calls[pcall]->duration);
        else
            asr_acd_user(0, current_calls[pcall]->endu->tenantid, current_calls[pcall]->endu->userid, current_calls[pcall]->areacode, current_calls[pcall]->duration);

        free(userFirstName);
        free(userLastName);
        free(tenantName);
        free(callerId);


        myres = mysql_stmt_master(myres, &numRows, querystring);

        if (current_calls[pcall]->endu->balanceu) {
            sprintf(querystring, "UPDATE users SET users.Balance = users.Balance - %f WHERE users.UserID=%i",
                    current_calls[pcall]->endu->total_buy_cost, current_calls[pcall]->endu->userid);
            myres = mysql_stmt_master(myres, &numRows, querystring);
        }

        if (current_calls[pcall]->endu->balancet) {
            sprintf(querystring, "UPDATE tenant SET tenant.Balance = tenant.Balance - %f WHERE tenant.TenantID=%i",
                    current_calls[pcall]->endu->total_buy_cost, current_calls[pcall]->endu->tenantid);
            myres = mysql_stmt_master(myres, &numRows, querystring);
        }

        next = current_calls[pcall]->prov->us->child ? current_calls[pcall]->prov->us->child : NULL;
        while (next) {
            if (next->balanceu) {
                sprintf(querystring, "UPDATE users SET users.Balance = users.Balance - %f WHERE users.UserID=%i",
                        next->total_buy_cost, next->userid);
                myres = mysql_stmt_master(myres, &numRows, querystring);
            }
            next = next->child ? next->child : NULL;
        }

        free_call(pcall);

        res = ast_mutex_unlock(&_monitor_mutex);
        if (res == EINVAL) {
            ast_log(LOG_WARNING, "-- Mutex not properly initialized.\n");
        } else if (res == EPERM) {
            ast_log(LOG_WARNING, "-- I am not owner of mutex.\n");
        }
        mysql_free_result(myres);
        free(querystring);
        //ast_log(LOG_DEBUG, "Line %i : Stop billing process\n", __LINE__);
        return 0;
    } else if (strlen(cdr->accountcode)) {
        if ((cdr->dst ? !strlen(cdr->dst) : 1) || (!strcasecmp("s", cdr->dst ? cdr->dst : ""))) {
            ast_mutex_unlock(&_monitor_mutex);
            mysql_free_result(myres);
            free(querystring);
            //ast_log(LOG_DEBUG, "Line %i : Stop billing process\n", __LINE__);
            return 0;
        } else if ((cdr->dst ? !strlen(cdr->dst) : 1) || (!strcasecmp("h", cdr->dst ? cdr->dst : ""))) {
            ast_mutex_unlock(&_monitor_mutex);
            mysql_free_result(myres);
            free(querystring);
            //ast_log(LOG_DEBUG, "Line %i : Stop billing process\n", __LINE__);
            return 0;
        } else if ((cdr->dst ? !strlen(cdr->dst) : 1) || (!strcasecmp("t", cdr->dst ? cdr->dst : ""))) {
            ast_mutex_unlock(&_monitor_mutex);
            mysql_free_result(myres);
            free(querystring);
            //ast_log(LOG_DEBUG, "Line %i : Stop billing process\n", __LINE__);
            return 0;
        } else if ((cdr->dst ? !strlen(cdr->dst) : 1) || (!strcasecmp("i", cdr->dst ? cdr->dst : ""))) {
            ast_mutex_unlock(&_monitor_mutex);
            mysql_free_result(myres);
            free(querystring);
            //ast_log(LOG_DEBUG, "Line %i : Stop billing process\n", __LINE__);
            return 0;
        } else if ((cdr->dst ? !strlen(cdr->dst) : 1) || (!strcasecmp("failed", cdr->dst ? cdr->dst : ""))) {
            ast_mutex_unlock(&_monitor_mutex);
            mysql_free_result(myres);
            free(querystring);
            //ast_log(LOG_DEBUG, "Line %i : Stop billing process\n", __LINE__);
            return 0;
        }

        sprintf(querystring,
                "SELECT users.FirstName, users.LastName, tenant.TenantID, tenant.name, tenantowner.UserID FROM users INNER JOIN tenant USING(TenantID) "
                        "LEFT JOIN tenantowner USING(TenantID) WHERE (users.UserID=%s)", cdr->accountcode);


        myres = mysql_stmt(myres, &numRows, querystring);

        if (numRows < 0) {
            ast_mutex_unlock(&_monitor_mutex);
            mysql_free_result(myres);
            free(querystring);
            //ast_log(LOG_DEBUG, "Line %i : Stop billing process\n", __LINE__);
            return 0;
        }


        get_formated_time(&cdr->start.tv_sec, timestart);
        get_formated_time(&cdr->answer.tv_sec, timeconnected);
        get_formated_time(&cdr->end.tv_sec, timeend);


        if (numRows) {
            mysql_data_seek(myres, 0);
            myrow = mysql_fetch_row(myres);
            userLastName = (char *) malloc(sizeof(char *) * (strlen(myrow[1]) * 2 + 1));
            userFirstName = (char *) malloc(sizeof(char *) * (strlen(myrow[0]) * 2 + 1));
            tenantName = (char *) malloc(sizeof(char *) * (strlen(myrow[3]) * 2 + 1));
            callerId = (char *) malloc(sizeof(char *) * (strlen(cdr->clid) * 2 + 1));

            escapeString(myrow[0], userFirstName);
            escapeString(myrow[1], userLastName);
            escapeString(myrow[3], tenantName);
            escapeString(cdr->clid, callerId);

            if (myrow[4]) {
                sprintf(querystring, "INSERT DELAYED INTO "
                                "cdr(UniqueID,UserID,UserFirstName,UserLastName,TenantID,TenantName,CallerID,ChannelIn,ChannelOut,DestNumber,IncommingTime,"
                                "ConnectedTime,EndTime,Duration,BuyCost,SellCost,ResellCost,TotalResellCost,resellerid,cause,Pin)"
                                " VALUES('%s-%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%li',0,0,0,0,%s,%lu,'%s')",
                        myhost, cdr->uniqueid, cdr->accountcode, userFirstName, userLastName, myrow[2], tenantName,
                        callerId, cdr->channel, cdr->dstchannel, cdr->dst, timestart,
                        timeconnected, timeend, cdr->billsec, myrow[4], cdr->disposition, cdr->userfield);
            } else {
                sprintf(querystring, "INSERT DELAYED INTO"
                                " cdr(UniqueID,UserID,UserFirstName,UserLastName,TenantID,TenantName,CallerID,ChannelIn,ChannelOut,DestNumber,IncommingTime,ConnectedTime,EndTime,Duration,"
                                "BuyCost,SellCost,ResellCost,TotalResellCost,cause,Pin) "
                                "VALUES('%s-%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%li',0,0,0,0,%lu,'%s')",
                        myhost, cdr->uniqueid, cdr->accountcode, userFirstName, userLastName, myrow[2], tenantName,
                        callerId, cdr->channel, cdr->dstchannel, cdr->dst, timestart,
                        timeconnected, timeend, cdr->billsec, cdr->disposition, cdr->userfield);
            }

            myres = mysql_stmt_master(myres, &numRows, querystring);
            ast_mutex_unlock(&_monitor_mutex);
            mysql_free_result(myres);
            free(userFirstName);
            free(userLastName);
            free(tenantName);
            free(callerId);
            free(querystring);
            //ast_log(LOG_DEBUG, "Line %i : Stop billing process\n", __LINE__);
            return 0;

        } else {
            callerId = (char *) malloc(sizeof(char *) * (strlen(cdr->clid) * 2 + 1));
            escapeString(cdr->clid, callerId);
            sprintf(querystring, "INSERT DELAYED INTO "
                            "cdr(UniqueID,UserID,UserFirstName,UserLastName,TenantID,TenantName,CallerID,ChannelIn,ChannelOut,DestNumber,IncommingTime"
                            ",ConnectedTime,EndTime,Duration,BuyCost,SellCost,ResellCost,TotalResellCost,Pin,cause) "
                            "VALUES('%s-%s',0,'<Unknown>','<Unknown>',0,'<Unknown>','%s','%s','%s','%s','%s','%s','%s','%li',0,0,0,0,'%s',%ld)",
                    myhost, cdr->uniqueid, callerId, cdr->channel, cdr->dstchannel, cdr->dst, timestart, timeconnected,
                    timeend, cdr->billsec, cdr->userfield, cdr->disposition);
            myres = mysql_stmt_master(myres, &numRows, querystring);
            ast_mutex_unlock(&_monitor_mutex);
            mysql_free_result(myres);
            free(querystring);
            free(callerId);
            //ast_log(LOG_DEBUG, "Line %i : Stop billing process\n", __LINE__);
            return 0;

        }


    } else {
        if ((!strcasecmp("s", cdr->dst)) || (!strcasecmp("h", cdr->dst)) || (!strcasecmp("t", cdr->dst)) ||
            (!strcasecmp("i", cdr->dst)) || (!strcasecmp("failed", cdr->dst))) {
            ast_mutex_unlock(&_monitor_mutex);
            free(querystring);
            return 0;
        }

        get_formated_time(&cdr->start.tv_sec , timestart);
        get_formated_time(&cdr->answer.tv_sec , timeconnected);
        get_formated_time(&cdr->end.tv_sec , timeend);

        callerId = (char *) malloc(sizeof(char *) * (strlen(cdr->clid) * 2 + 1));
        escapeString(cdr->clid, callerId);
        sprintf(querystring, "INSERT DELAYED INTO "
                        "cdr(UniqueID,UserID,UserFirstName,UserLastName,TenantID,TenantName,CallerID,ChannelIn,ChannelOut,DestNumber,IncommingTime,"
                        "ConnectedTime,EndTime,Duration,BuyCost,SellCost,ResellCost,TotalResellCost,Pin,cause) "
                        "VALUES('%s-%s',0,'<Unknown>','<Unknown>',0,'<Unknown>','%s','%s','%s','%s','%s','%s','%s','%li',0,0,0,0,'%s',%ld)",
                myhost, cdr->uniqueid, callerId, cdr->channel, cdr->dstchannel, cdr->dst, timestart, timeconnected,
                timeend, cdr->billsec, cdr->userfield, cdr->disposition);
        myres = mysql_stmt_master(myres, &numRows, querystring);
        ast_mutex_unlock(&_monitor_mutex);
        mysql_free_result(myres);
        free(querystring);
        free(callerId);
        //ast_log(LOG_DEBUG, "Line %i : Stop billing process\n", __LINE__);
        return 0;

    }

    ast_mutex_unlock(&_monitor_mutex);
    mysql_free_result(myres);
    free(querystring);
    //ast_log(LOG_DEBUG, "Line %i : Stop billing process\n", __LINE__);
    return 0;

}


/**
 * Free The call from our list of calls
 * @param p : Call index we want to free
 */
static int free_call(int p) {

    struct reseller *parent = NULL, *prev = NULL;
    if ((p > -1) && (p < MAX_CALLS)) {
        //ast_log(LOG_DEBUG, "-- Freeing the call now.\n");
        if (current_calls[p]) {
            if (current_calls[p]->endu) {
                parent = current_calls[p]->endu->parent ? current_calls[p]->endu->parent : NULL;
                prev = parent ? (parent->parent ? parent->parent : NULL) : NULL;

                /* Routine to free all resellers */
                while (parent) {

                    /* Free stage if there is one */
                    if (parent->stager) {
                        free(parent->stager);
                    }

                    /* Free reseller's balance */
                    if (parent->balanceu ? parent->balanceu->nbr_calls < 2 : 0) {
                        //ast_log(LOG_DEBUG, "-- Free prepaid reseller's balance.\n");
                        free(parent->balanceu);
                    } else if (parent->balanceu ? parent->balanceu->nbr_calls > 1 : 0) {
                        //ast_log(LOG_DEBUG, "-- Dicrease number of calls on prepaid reseller's balance.\n");
                        parent->balanceu->nbr_calls--;
                        parent->balanceu->sub_balance -= parent->total_buy_cost;
                    }
                    free(parent);

                    parent = prev;
                    prev = parent ? (parent->parent ? parent->parent : NULL) : NULL;
                }

                /* Free tenant's balance */
                if (current_calls[p]->endu->balancet ? current_calls[p]->endu->balancet->nbr_calls < 2 : 0) {
                    //ast_log(LOG_DEBUG, "-- Free prepaid tenant's balance in list.\n");
                    free(current_calls[p]->endu->balancet);
                } else if (current_calls[p]->endu->balancet ? current_calls[p]->endu->balancet->nbr_calls > 1 : 0) {
                    //ast_log(LOG_DEBUG, "-- Dicrease number of calls on prepaid tenant's balance.\n");
                    current_calls[p]->endu->balancet->nbr_calls--;
                    current_calls[p]->endu->balancet->sub_balance -= current_calls[p]->endu->total_buy_cost;
                }

                /* Free user's balance */
                if (current_calls[p]->endu->balanceu ? current_calls[p]->endu->balanceu->nbr_calls < 2 : 0) {
                    //ast_log(LOG_DEBUG, "-- Free prepaid user's balance in list.\n");
                    free(current_calls[p]->endu->balanceu);
                } else if (current_calls[p]->endu->balanceu ? current_calls[p]->endu->balanceu->nbr_calls > 1 : 0) {
                    //ast_log(LOG_DEBUG, "-- Dicrease number of calls on prepaid user's balance.\n");
                    current_calls[p]->endu->balanceu->nbr_calls--;
                    current_calls[p]->endu->balanceu->sub_balance -= current_calls[p]->endu->total_buy_cost;
                }

                /* Free user */
                free(current_calls[p]->endu);
            }

            if (current_calls[p]->prov) {
                if (current_calls[p]->prov->stager)
                    free(current_calls[p]->prov->stager);
                free(current_calls[p]->prov);
            }
        }
        memset(current_calls[p], 0, sizeof(current_calls[p]));
        free(current_calls[p]);
        current_calls[p] = NULL;
    } else {

        ast_log(LOG_WARNING, "Pointer to free call member is deferenced\n");
        //ast_log(LOG_DEBUG, "Pointer to free call member is deferenced\n");
        return 1;
    }
    //ast_log(LOG_DEBUG, "-- Call is freed.\n");
    return 0;

}


/**
 * Called Everytime module is loaded
 * @return 1 For Success , -1 For  Failure
 */
static int load_module(void) {
    int i = 0;
    //Load database credentials from config file
    dbinfo_select = malloc(sizeof(struct Database_Credentials));
    dbinfo_update = malloc(sizeof(struct Database_Credentials));
    if (dbinfo_select == NULL || dbinfo_update == NULL) {
        ast_log(LOG_ERROR, "Memory Error When requesting for Database_Credentials structure\n");
        return AST_MODULE_LOAD_FAILURE;
    }

    /**
     * Load Default configuration In case of error loading files for Mysql Credentials select && update
     */
    if (load_Database_credentials(dbinfo_select, dbinfo_update) != FILE_LOAD_SUCCESS) {
        ast_log(LOG_NOTICE, "Using default configuration\n");
        load_default_credentials(dbinfo_select);
        //Since it's the same structure , there is no problem if i cast it
        load_default_credentials(dbinfo_update);
        display_database_credentials(dbinfo_select);
    };


    //Connect To database using those credentials
    ast_mutex_lock(&_mysql_mutex_select);
    ast_mutex_lock(&_mysql_mutex_update);
    if (db_connect_standard(dbinfo_select) != MYSQL_CONNECTION_SUCCESS ||
        db_connect_standard(dbinfo_update) != MYSQL_CONNECTION_SUCCESS) {
        //Reconnect using default credentials
        load_default_credentials(dbinfo_select);
        load_default_credentials(dbinfo_update);
        ast_log(LOG_NOTICE, "Reconnecting Using default credentials\n");
        if (db_connect_standard(dbinfo_select) != MYSQL_CONNECTION_SUCCESS ||
            db_connect_standard(dbinfo_update) != MYSQL_CONNECTION_SUCCESS) {
            ast_log(LOG_ERROR, "Reconnection Failure , Abort\n");
            ast_mutex_unlock(&_mysql_mutex_select);
            ast_mutex_unlock(&_mysql_mutex_update);
            free(dbinfo_select);
            free(dbinfo_update);
            return AST_MODULE_LOAD_FAILURE;
        }

    }
    ast_mutex_unlock(&_mysql_mutex_select);
    ast_mutex_unlock(&_mysql_mutex_update);

    /**
     * Null Values means that we can use this spot , So we initialize table with NULL data
     */
    for (i = 0; i < MAX_CALLS; i++) {
        current_calls[i] = NULL;
    }

    if (ast_cdr_register(cdrAppName, cdrAppDescription, end_call) < 0) {
        ast_log(LOG_ERROR, "Cant register cdr Application for AddBill\n");
        return AST_MODULE_LOAD_FAILURE;
    };
    //Register application
    loaded = 1;
    return ast_register_application(app, bill_exec, synopsis, descrip_app);
}


/**
 * Called Everytime module is unloaded
 * @return Success
 */
static int unload_module(void) {

    /**
     * Free Variables
     */
    free_dbInfo();
    ast_cdr_unregister(cdrAppName);
    loaded = 0;
    return ast_unregister_application(app);
}

/**
 * Free Database information
 */
static void free_dbInfo() {
    mysql_close(&dbinfo_select->connection);
    mysql_close(&dbinfo_update->connection);

    memset(&dbinfo_select->dbName[0] , 0 , sizeof(char) * 250 );
    memset(&dbinfo_select->hostname[0] , 0 , sizeof(char) * 250 );
    memset(&dbinfo_select->password[0] , 0 , sizeof(char) * 250 );
    memset(&dbinfo_select->username[0] , 0 , sizeof(char) * 250 );
    memset(&dbinfo_select->socket[0] , 0  , sizeof(char) * 250 );
    dbinfo_select->port = 0 ;

    memset(&dbinfo_update->dbName[0] , 0 , sizeof(char) * 250 );
    memset(&dbinfo_update->hostname[0] , 0 , sizeof(char) * 250 );
    memset(&dbinfo_update->password[0] , 0 , sizeof(char) * 250 );
    memset(&dbinfo_update->username[0] , 0 , sizeof(char) * 250 );
    memset(&dbinfo_update->socket[0] , 0  , sizeof(char) * 250 );
    dbinfo_update->port = 0 ;


    free(dbinfo_select);
    free(dbinfo_update);

}


/**
 * Display The database credentials
 * @param dbInfo
 */
static void display_database_credentials(struct Database_Credentials *dbInfo) {
    ast_log(LOG_NOTICE,
            "Database Credentials :\n\t[Hostname : %s ]\n\t[Username : %s]\n\t[Password : %s]\n\t[DbName : %s]\n\t[port : %d]\n\t[socket : %s]\n",
            dbInfo->hostname,
            dbInfo->username,
            dbInfo->password,
            dbInfo->dbName,
            dbInfo->port,
            dbInfo->socket
    );
}


static void load_default_credentials(struct Database_Credentials *dbInfo) {
    sscanf(hostname, "%s", dbInfo->hostname);
    sscanf(username, "%s", dbInfo->username);
    sscanf(password, "%s", dbInfo->password);
    sscanf(dbName, "%s", dbInfo->dbName);
    sscanf(plugSock, "%s", dbInfo->socket);
    dbInfo->port = port;
}


/** For Debug Purposes **/
static void showCall(int callIndex) {
    struct call *call;

    call = current_calls[callIndex];

    ast_log(LOG_DEBUG,
            "Call Structure\nFormatNumber:%s,AreaCode:%s,DestName:%s,lastCause:%d,lastState:%d,duration:%d,end_time:%s,conn_time:%s,destnumber:%s,chan_in:%s,UniqueId:%s\nid:%d\n",
            call->formated_destination_number,
            call->areacode,
            call->DestName,
            call->last_cause,
            call->last_state,
            call->duration,
            call->end_time,
            call->connected_time,
            call->destination_number,
            call->channel_in_name,
            call->uniqueid,
            call->id
    );
}


AST_MODULE_INFO_STANDARD(ASTERISK_GPL_KEY, module_name);


