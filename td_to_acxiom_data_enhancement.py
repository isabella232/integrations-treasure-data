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

# These two methods are used to format the payloads and decide which data is being brought into the database from the source and dsapi data
def source_to_dsapi(source_row):
  # Pre DSAPI data processing / standardisation goes here.
  output={}
  output['name'] = source_row['firstname'] + ' ' + source_row['lastname']
  output['primaryStreetAddress'] = source_row['address']
  output['postalCode'] = source_row['postcode']
  output['email'] = source_row['email']
  output['phone'] = source_row['phone']
  return output

def run_flow():
    # Constants used in this example
    TD_DB_NAME="scwalk_db"
    SOURCE_SQL= 'select sourcekey, address,postcode, firstname, lastname, email, phone from scwalk_db.import_nameadd_20210821 order by time desc, sourcekey desc'
    DEST_TABLE='import_enhancement_dsapi'
    BUNDLE_NAME='ukHouseholdDemographicsWithInferred'

    TreasureData = treasureData.TreasureData()
    TreasureData.connect(TD_DB_NAME)

    # Run a query and grab a data frame of results
    df = TreasureData.execute_sql(SOURCE_SQL)

    # Convert the records to DSAPI format as the results are stored in memory this may need to be broken into chunks for processing.
    # This example uses a SQL Query that only returns a few hundred rows in a data frame.

    source_rows=[]
    send_to_dsapi=[]

    for index,rec in df.iterrows():
        source_rows.append(rec.to_dict())
        rec_dict=source_to_dsapi(rec.to_dict())
        send_to_dsapi.append(rec_dict)
    
    # Send to DSAPI
    dsapi = acxiom.Acxiom()
    updated_rows=dsapi.dsapi_enhance(send_to_dsapi)

    write_to_td=[]
    for i in range(len(source_rows)):
        print(updated_rows[i]['code'])
        # Check that the bundle has been returned for this record

        if (updated_rows[i]['code'] == 200 and updated_rows[i] and updated_rows[i]["document"] and updated_rows[i]["document"]["place"] and BUNDLE_NAME in updated_rows[i]["document"]["place"]):
            updated_rows[i]["document"]["place"][BUNDLE_NAME]["sourcekey"]=source_rows[i]["sourcekey"]
            write_to_td.append(dsapi.flatten_json(updated_rows[i]["document"]["place"][BUNDLE_NAME]))

    
    #updated_rows[0]["document"]["sourcekey"]=source_rows[0]["sourcekey"]
    #print(updated_rows[0]["document"]["place"]["ukHouseholdDemographicsWithInferred"])
    #print(dsapi.flatten_json(updated_rows[0]["document"]["place"]["ukHouseholdDemographicsWithInferred"]))
    print(write_to_td[0].keys())

    TreasureData.write_to_table(DEST_TABLE,write_to_td[0].keys(),write_to_td)
run_flow()