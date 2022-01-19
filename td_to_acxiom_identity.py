# Blueprint code to take PII data from a table in Treasure Data, run it against Acxiom DSAPI and retrieve standardised and identified data back.

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
import hashlib

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

def dsapi_to_target(source_row, updated_row):
  # Bespoke data processing for formatting the DSAPI output
  # Sample Household is standardizedLastName + placeId then hashed.

  myRow={}
  myRow["sourceid"] = source_row["sourcekey"]
  myRow["personId"] = updated_row["document"]["entity"]["clientIdentityGraph"].get("personId","")
  myRow["placeId"] = updated_row["document"]["entity"]["clientIdentityGraph"].get("placeId","")
  myRow["address1"] = updated_row["document"]["entity"]["inputGlobalAddress"].get("address1","")
  myRow["address2"] = updated_row["document"]["entity"]["inputGlobalAddress"].get("address2","")
  myRow["address3"] = updated_row["document"]["entity"]["inputGlobalAddress"].get("address3","")
  myRow["address4"] = updated_row["document"]["entity"]["inputGlobalAddress"].get("address4", "")
  myRow["dependentLocality"] = updated_row["document"]["entity"]["inputGlobalAddress"].get("dependentLocality", "")
  myRow["locality"] = updated_row["document"]["entity"]["inputGlobalAddress"].get("locality", "")
  myRow["postalCode"] = updated_row["document"]["entity"]["inputGlobalAddress"].get("postalCode", "")
  myRow["country"] = updated_row["document"]["entity"]["inputGlobalAddress"].get("country", "")
  myRow["countryCode"] = updated_row["document"]["entity"]["inputGlobalAddress"].get("countryCode", "")
  myRow["standardizedFirstName"]=""
  myRow["standardizedLastName"]=""
  myRow["householdId"]=""
  #print(response["document"]["entity"].keys())
  # inputName is an array of objects, these keys could be in any object
  if("inputName" in updated_row["document"]["entity"].keys()):
    #myRow["standardizedFirstName"]="BobTest";
    if("nameStandardization" in updated_row["document"]["entity"]["inputName"].keys()):
      for nameDict in updated_row["document"]["entity"]["inputName"]["nameStandardization"]:        
        if(nameDict.get("standardizedFirstName")):
          myRow["standardizedFirstName"] = nameDict.get("standardizedFirstName","")
          myRow["standardizedLastName"] = nameDict.get("standardizedLastName","")
          if (myRow["standardizedLastName"] != "" and myRow["placeId"] != ""):
            hh_id_raw = myRow["standardizedLastName"] + '@:' + myRow["placeId"]
            encoded=hh_id_raw.encode()
            result = hashlib.sha256(encoded)
            myRow["householdId"] = result.hexdigest()
  #print(myRow)
  return(myRow)

def run_flow():
  # Constants used in this example
  TD_DB_NAME="scwalk_db"
  SOURCE_SQL= 'select sourcekey, address,postcode, firstname, lastname, email, phone from scwalk_db.import_nameadd_20210821 order by time desc, sourcekey desc limit 1000'
  DEST_TABLE='import_nameadd_dsapi'
  DEST_TABLE_DEF='sourceid varchar,personId varchar,placeId varchar,address1 varchar,address2 varchar, address3 varchar,address4 varchar,dependentLocality varchar, locality varchar, postalCode varchar, country varchar, countryCode varchar, standardizedFirstName varchar, standardizedLastName varchar,householdId varchar'
  DEST_TABLE_COLUMN_LIST= ['sourceid',
                          'personId',
                          'placeId',
                          'address1',
                          'address2',
                          'address3',
                          'address4',
                          'dependentLocality',
                          'locality',
                          'postalCode',
                          'country',
                          'countryCode',
                          'standardizedFirstName',
                          'standardizedLastName',
                          'householdId']

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
  updated_rows=dsapi.dsapi_match(send_to_dsapi)

  # Join to source rows and create target records
  print(source_rows[0])
  outrow=dsapi_to_target(source_rows[0], updated_rows[0])
  print(outrow)

  write_to_td=[]

  for i in range(len(source_rows)):
    outrow=dsapi_to_target(source_rows[i], updated_rows[i])
    write_to_td.append(outrow)

  # Write back to Treasure Data
  # Create or truncate the table

  TreasureData.create_table(DEST_TABLE,DEST_TABLE_DEF)
  TreasureData.clear_table(DEST_TABLE)
  TreasureData.write_to_table(DEST_TABLE,DEST_TABLE_COLUMN_LIST,write_to_td)

run_flow()