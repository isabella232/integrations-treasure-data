# Description

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

def rgraph_to_table(rgraph_json):
    # Simple conversion for a basic rGraph entry, can be adapted for full implemented schema supplied by Acxiom.
    output_dict={}
    output_dict['realid'] = rgraph_json['key']
    output_dict['tdid'] = rgraph_json['attributes']['tdid']
    output_dict['userid'] = rgraph_json['attributes']['tdid']
    output_dict['thirdptyid'] =rgraph_json['attributes']['thirdptyid']
    return(output_dict)

def run_flow():
    # Instantiate Acxiom and Treasure Data objects.
    TD_DB_NAME="scwalk_db"
    TD_SOURCE_SQL="select distinct acx_realid from pageviews where acx_realid is not NULL"
    REALID_DOMAIN="acxiomdemo.com"
    DEST_TABLE="import_rgraph"
    DEST_TABLE_DEF="realid varchar,tdid varchar,userid varchar,thirdptyid varchar"
    DEST_TABLE_COLUMN_LIST=['realid','tdid','userid','thirdptyid']

    TreasureData = treasureData.TreasureData()
    TreasureData.connect(TD_DB_NAME)
    realIdentity = acxiom.Acxiom()

    # Obtain a list of first pty cookie id's to go and pull from acxiom - this could alternately be a hashed IP address or hashed email if these are stored on the graph.
    df = TreasureData.execute_sql(TD_SOURCE_SQL)

    # Pull back the objects from the graph and format to a flat table structure
    write_to_td=[]
    for index,rec in df.iterrows():
        rec_dict=rec.to_dict()
        cookieId=rec_dict['acx_realid']
        graph_json = realIdentity.lookup_rgraph('cookie',REALID_DOMAIN,cookieId)
        write_to_td.append(rgraph_to_table(graph_json))

    #print(write_to_td)

    # Write back to Treasure Data table
    # Creating if does not exists
    # Clearing down data in the table first

    TreasureData.create_table(DEST_TABLE,DEST_TABLE_DEF)
    TreasureData.clear_table(DEST_TABLE)
    TreasureData.write_to_table(DEST_TABLE,DEST_TABLE_COLUMN_LIST,write_to_td)

run_flow()