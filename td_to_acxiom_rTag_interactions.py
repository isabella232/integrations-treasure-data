# Description
# Reads the last X days worth of logs from the rAPID data repository;
# REALID_AGE_OF_LOGS_IN_DAYS set in this script determines number of days 
# Writes the Owned and Paid Behavior data back to Treasure Data
#################################################################################################################
# This code to add the classes to the path is not needed in the workflow as the directory structure will be flat
import sys, os
file_path = os.path.abspath(os.path.dirname(__file__))
file_path = file_path + '\classes'
print(file_path)
sys.path.append(file_path)
##################################################################################################################

import treasureData
import acxiom

def rtag_to_table(queryResults):
    results={}
    results['hashed_ip']=queryResults[0]
    results['event_timestamp']=queryResults[1]
    results['event_date']=queryResults[2]
    results['event_hour']=queryResults[3]
    results['host']=queryResults[4]
    results['path']=queryResults[5]
    results['referer']=queryResults[6]
    results['status_code']=queryResults[7]
    results['dnt']=queryResults[8]
    results['user_agent']=queryResults[9]
    results['browser']=queryResults[10]
    results['browser_version']=queryResults[11]
    results['os']=queryResults[12]
    results['os_version']=queryResults[13]
    results['device']=queryResults[14]
    results['accept_header']=queryResults[15]
    results['accept_language_header']=queryResults[16]
    results['accept_encoding_header']=queryResults[17]
    results['country_code']=queryResults[18]
    results['region_code']=queryResults[19]
    results['latitude']=queryResults[20]
    results['longitude']=queryResults[21]
    results['dma']=queryResults[22]
    results['msa']=queryResults[23]
    results['timezone']=queryResults[24]
    results['area_code']=queryResults[25]
    results['fips']=queryResults[26]
    results['city']=queryResults[27]
    results['zip']=queryResults[28]
    results['network']=queryResults[29]
    results['network_type']=queryResults[30]
    results['throughput']=queryResults[31]
    results['tag_type']=queryResults[32]
    results['cls']=queryResults[33]
    results['event_id']=queryResults[34]
    results['uu']=queryResults[35]
    results['suu']=queryResults[36]
    results['puu']=queryResults[37]
    results['domain']=queryResults[38]
    results['redirect_domain']=queryResults[39]
    results['partner_urls']=queryResults[40]
    return(results)

def run_flow():
    # Instantiate Acxiom and Treasure Data objects.
    TD_DB_NAME="scwalk_db"
    REALID_AGE_OF_LOGS_IN_DAYS="100"
    REALID_NAMED_QUERY="c040_log_export"
    DEST_TABLE="import_rtag_logs"
    DEST_TABLE_DEF="hashed_ip varchar," + \
                    "event_timestamp int," + \
                    "event_date varchar, " + \
                    "event_hour varchar, " + \
                    "host varchar, " + \
                    "path varchar, " + \
                    "referer varchar, " + \
                    "status_code varchar, " + \
                    "dnt varchar, " + \
                    "user_agent varchar, " + \
                    "browser varchar, " + \
                    "browser_version varchar, " + \
                    "os varchar, " + \
                    "os_version varchar, " + \
                    "device varchar, " + \
                    "accept_header varchar, "+ \
                    "accept_language_header varchar, " + \
                    "accept_encoding_header varchar, " + \
                    "country_code varchar, " + \
                    "region_code varchar, " + \
                    "latitude varchar, " + \
                    "longitude varchar, " + \
                    "dma varchar, " + \
                    "msa varchar, " + \
                    "timezone varchar, " \
                    "area_code varchar, " + \
                    "fips varchar, " + \
                    "city varchar, " + \
                    "zip varchar, " + \
                    "network varchar, " + \
                    "network_type varchar, " + \
                    "throughput varchar, " + \
                    "tag_type varchar, " + \
                    "cls varchar, " + \
                    "event_id varchar, " +\
                    "uu varchar, " + \
                    "suu varchar, " + \
                    "puu varchar, " + \
                    "domain varchar, " + \
                    "redirect_domain varchar, " + \
                    "partner_urls varchar"
    DEST_TABLE_COLUMN_LIST=[
                    "hashed_ip",
                    "event_timestamp",
                    "event_date",
                    "event_hour",
                    "host",
                    "path",
                    "referer",
                    "status_code",
                    "dnt",
                    "user_agent",
                    "browser",
                    "browser_version",
                    "os",
                    "os_version",
                    "device",
                    "accept_header",
                    "accept_language_header",
                    "accept_encoding_header",
                    "country_code",
                    "region_code",
                    "latitude",
                    "longitude",
                    "dma",
                    "msa",
                    "timezone",
                    "area_code",
                    "fips",
                    "city",
                    "zip",
                    "network",
                    "network_type",
                    "throughput",
                    "tag_type",
                    "cls",
                    "event_id",
                    "uu",
                    "suu",
                    "puu",
                    "domain",
                    "redirect_domain",
                    "partner_urls"
    ]

    TreasureData = treasureData.TreasureData()
    TreasureData.connect(TD_DB_NAME)
    realIdentity = acxiom.Acxiom()

    # Pull back x (CONST : REALID_AGE_OF_LOGS_IN_DAYS) days of logs from a realIdentity query
    # Behind the scenes in realIdentity this is running a named SQL query that expects parameters
    # A list of available named queries can be obtained using the get_named_queries method
    # The parameters for these queries can be checked using the get_parameter_list method

    results=realIdentity.named_query(REALID_NAMED_QUERY,{"days": REALID_AGE_OF_LOGS_IN_DAYS})
    write_to_td=[]
    for row in results:
        write_to_td.append(rtag_to_table(row))
    # Load this data to Treasure Data appending to or creating a table to store the results
    TreasureData.create_table(DEST_TABLE,DEST_TABLE_DEF)
    TreasureData.clear_table(DEST_TABLE)
    TreasureData.write_to_table(DEST_TABLE,DEST_TABLE_COLUMN_LIST,write_to_td)

run_flow()