import requests
import logging
import urllib3
import urllib.parse
import time
from dotenv import load_dotenv
from pathlib import Path
import os
from requests.structures import CaseInsensitiveDict

class Acxiom:
    # Methods to integrate with RealIdentity.
    def __init__(self) -> None:
        # Read environment variables
        load_dotenv()
        env_path = Path('..')/'.env'
        load_dotenv(dotenv_path=env_path)

        #Set up Logging
        self.logger = logging.getLogger('Acxiom')        
        logging.basicConfig(filename="acxLogfile.txt")
        self.stderrLogger=logging.StreamHandler()
        self.stderrLogger.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
        #logging.getLogger().addHandler(self.stderrLogger)       
        self.logger.setLevel(os.getenv('logLevel'))
        self.logger.debug("Class Initialised")

        # Environment Variables
        # rAPID environment variables
        self.oauth_url=os.environ.get('realId_oauthEndpoint',"") + os.environ.get('realId_oauthMethod',"")
        self.refresh_token=os.environ.get('realId_oathRefreshToken',"")
        self.base_url=os.environ.get('realId_baseUrl',"")
        self.client_id=os.environ.get('realId_clientId',"")
        self.access_token=""
        self.access_token_expiry=0

        # DSAPI environment variables
        self.dsapi_oauth_endpoint=os.environ.get('dsapi_oauth_endpoint',"")
        self.dsapi_oauth_method=os.environ.get('dsapi_oauth_method',"")
        self.dsapi_oauth_grant_type=os.environ.get('dsapi_oauth_grant_type',"")
        self.dsapi_oauth_scope=os.environ.get('dsapi_oauth_scope',"")
        self.dsapi_oauth_username=os.environ.get('dsapi_oauth_username',"")
        self.dsapi_oauth_password=os.environ.get('dsapi_oauth_password',"")
        self.dsapi_client_id=os.environ.get('dsapi_client_id',"")
        self.dsapi_client_secret=os.environ.get('dsapi_client_secret',"")
        self.dsapi_tenant=os.environ.get('dsapi_tenant',"")
        self.dsapi_role=os.environ.get('dsapi_role',"")
        self.dsapi_match_method=os.environ.get('dsapi_match_method',"")
        self.dsapi_endpoint=os.environ.get('dsapi_endpoint',"")
        self.dsapi_match_options=os.environ.get('dsapi_match_options',"")
        self.dsapi_match_bundles=os.environ.get('dsapi_match_bundles',"")
        self.dsapi_enhance_method=os.environ.get('dsapi_enhance_method',"")       
        self.dsapi_enhance_bundles=os.environ.get('dsapi_enhance_bundles',"")
        self.dsapi_enhance_options=os.environ.get('dsapi_enhance_options',"")
        self.dsapi_access_token=""
        self.dsapi_token_expiry=0
        self.dsapi_batchsize=os.environ.get('dsapi_batchsize',"")

    # Generic Methods
    def connect(self,service="rAPID"):
        time_now = int(time.time())
        if (service == "rAPID"):
            if(time_now > self.access_token_expiry):
                self.logger.debug(service + ":Getting new access token")
                self.access_token=self._get_access_token()
                self.logger.debug(service + ":Access token obtained")
                self.logger.debug(service + ":Access token now expires in " + str(self.access_token_expiry - time_now) + "seconds")
            else:
                self.logger.debug(service + ":Access token not expired")
                self.logger.debug(service + ":Access token expires in " + str(self.access_token_expiry - time_now) + " seconds")
        elif (service == "DSAPI"):
            if(time_now > self.dsapi_token_expiry):
                self.logger.debug(service + ":Getting new access token")
                self.dsapi_access_token=self._get_dsapi_token()
                self.logger.debug(service + ":Access token obtained")
                self.logger.debug(service + ":Access token now expires in " + str(self.dsapi_token_expiry - time_now) + " seconds")
            else:
                self.logger.debug(service + ":Access token not expired")
                self.logger.debug(service + ":Access token expires in " + str(self.dsapi_token_expiry - time_now) + " seconds")                      

    #rAPID methods
    def lookup_rgraph(self, type, domain, key):
        # Performs a lookup to a single record in the digital identity graph
        request_url = "http://graphds." + self.base_url + '/v2/cids/' + self.client_id + '/entities/' + type + '~' + domain + '~' + key
        self.logger.debug("Calling: " + request_url)        
        # Call connect in case token has expired
        self.connect("rAPID")

        # Set Headers
        headers = CaseInsensitiveDict()
        headers["Accept"] = "application/json"
        headers["Authorization"] = "Bearer " + self.access_token
        response = requests.get(
            request_url,
            headers=headers
        )

        return response.json()

    def named_query(self, queryname, queryParameters=[], timeout=100):
        # Executes the preset named query with the correct parameters and retrieves results, giving up after a timeout number of attempts
        execution_id = self._start_named_query(queryname, queryParameters)
        for x in range(100):
            query_status=self._check_named_query_status(execution_id)  
            if(query_status=='SUCCEEDED'):
                break
            time.sleep(1)
        
        if(query_status=='SUCCEEDED'):
            query_results=self._get_named_query_results(execution_id)
        
        return(query_results)

    def get_parameter_list(self):
        # Obtains a list of parameters and their definitions
        request_url = "http://pail." + self.base_url + '/v1/cids/' + self.client_id + '/param_defs'
        # Call connect in case token has expired
        self.connect("rAPID")

        headers = CaseInsensitiveDict()
        headers["Accept"] = "application/json"
        headers["Authorization"] = "Bearer " + self.access_token

        response = requests.get(
            request_url,
            headers=headers
        )
        self.logger.debug("Query Status: " + str(len(response.json())))      
        return response.json()
        
    def get_named_queries(self):
        # Obtains a list of available named queries
        request_url = "http://pail." + self.base_url + '/v1/cids/' + self.client_id + '/named_queries'
        # Call connect in case token has expired
        self.connect("rAPID")

        headers = CaseInsensitiveDict()
        headers["Accept"] = "application/json"
        headers["Authorization"] = "Bearer " + self.access_token

        response = requests.get(
            request_url,
            headers=headers
        )
        self.logger.debug("Query Status: " + str(len(response.json())))      
        return response.json()
    
    # DSAPI Methods
    def dsapi_match(self,input=[]):
        # Loops through the records provided in the input, batches them into DSAPI calls according to the environment variable dsapi_batchsize
        # Assembling the results and returning them.
        body_list = self._dataset_to_dsapi_body(input)
        results=[]

        for i in range(0, len(body_list), 100):
            results.extend(self._execute_dsapi_microbatch(body_list[i:i+100]))

        self.logger.debug("DSAPI Complete Processed:" + str(len(results)) + " records")
        return(results)

    def dsapi_enhance(self,input=[]):
        # Loops through the records provided in the input, batches them into DSAPI calls according to the environment variable dsapi_batchsize
        # Assembling the results and returning them.
        body_list = self._dataset_to_dsapi_body(input,'enhance')
        results=[]

        for i in range(0, len(body_list), 100):
            results.extend(self._execute_dsapi_microbatch(body_list[i:i+100]))

        self.logger.debug("DSAPI Complete Processed:" + str(len(results)) + " records")
        return(results)

    # rAPID internal Methods
    def _start_named_query(self, queryName, queryParameters=[]):
        #TODO add parameters
        request_url = "http://pail." + self.base_url + '/v1/cids/' + self.client_id + '/named_queries/' + queryName + '/execute'

        # Call connect in case token has expired
        self.connect()
        headers = CaseInsensitiveDict()
        headers["Accept"] = "application/json"
        headers["Authorization"] = "Bearer " + self.access_token

        response = requests.post(
            request_url,
            headers=headers,
            params=queryParameters
        )
        self.logger.debug("Query Started: " + response.json()["query_execution_id"]) 
        return response.json()["query_execution_id"]

    def _check_named_query_status(self,execution_id):
        # Checks to see the current state of a named query
        request_url = "http://pail." + self.base_url + '/v1/cids/' + self.client_id + '/query_status/' + execution_id
        # Call connect in case token has expired
        self.connect("rAPID")

        headers = CaseInsensitiveDict()
        headers["Accept"] = "application/json"
        headers["Authorization"] = "Bearer " + self.access_token

        response = requests.get(
            request_url,
            headers=headers
        )
        self.logger.debug("Query Status: " + response.json()["State"])      
        return response.json()["State"]

    def _get_named_query_results(self,execution_id):
        
        request_url = "http://pail." + self.base_url + '/v1/cids/' + self.client_id + '/query_executions/' + execution_id
        # Call connect in case token has expired
        self.connect("rAPID")

        headers = CaseInsensitiveDict()
        headers["Accept"] = "application/json"
        headers["Authorization"] = "Bearer " + self.access_token

        response = requests.get(
            request_url,
            headers=headers
        )
        #self.logger.debug("Query Results: " + str(response.json()["data"]))
        self.logger.debug("realIdentity Query Complete Processed:" + str(len(response.json()["data"])) + " rows")
        return response.json()["data"]        

    def _get_access_token(self):
        expiry_time=int(time.time())
        response = requests.post(
            self.oauth_url,
            data={"grant_type": "refresh_token", "refresh_token": self.refresh_token }
        )
        self.access_token_expiry = expiry_time + response.json()["expires_in"]
        return response.json()["jwt_token"]

# DSAPI Specific internal methods

    def _get_dsapi_token(self):
        # Get DS-API oAuth2 token.
        # Fetch token from token URL

        expiry_time=int(time.time())
        token_url = self.dsapi_oauth_endpoint + self.dsapi_oauth_method

        if self.dsapi_oauth_grant_type =="" or self.dsapi_oauth_grant_type=='client_credentials':
            user_creds =  [('client_id', self.dsapi_client_id),
                            ('client_secret', self.dsapi_client_secret), 
                            ('grant_type', 'client_credentials')]      
        else:
             user_creds =  [('client_id', self.dsapi_client_id),
                            ('client_secret', self.dsapi_client_secret), 
                            ('grant_type', self.dsapi_oauth_grant_type),
                            ('scope',self.dsapi_oauth_scope),
                            ('username',self.dsapi_oauth_username),
                            ('password',self.dsapi_oauth_password)]

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        response_token = requests.post(url=token_url, data=user_creds, verify=False)
        self.logger.debug(response_token)
        self.logger.debug(response_token.json())

        if response_token.status_code!=200: 
           raise Exception("HTTPError: "+str(response_token.status_code)+" getting token")

        token_dict = response_token.json()        
        self.dsapi_token = token_dict["access_token"]

        if not self.dsapi_token:
            self.logger.error("oAuth Failed to get token")
            self.logger.debug("Credentials:" + str(user_creds))

        self.dsapi_token_expiry = expiry_time + token_dict["expires_in"]
        return(token_dict["access_token"])

    def _execute_dsapi_microbatch(self,body=[]):
        # runs a microbatch of records (1 - 100) against DSAPI and retrieves the results as a list
        batch_endpoint = self.dsapi_endpoint +'/batch/'+ self.dsapi_match_method.split('/')[2]
        if self.dsapi_tenant and self.dsapi_role:
            batch_endpoint=batch_endpoint+("?role="+ self.dsapi_role +"&tenant="+self.dsapi_tenant).replace('=None&','=&').replace('=nan&','=&')
        
        headers = {"Accept": "application/json"}
        self.connect("DSAPI")
        headers.update({"Authorization": "Bearer {token}".format(token=self.dsapi_access_token)})

        self.logger.info("--- Executing DS_API on " + str(len(body)) +" records")
        start_time = time.time()        
        self.logger.debug("Calling DSAPI:"+ batch_endpoint)
        responses_obj=requests.post(url=batch_endpoint, json=body, headers=headers, verify=False)

        if responses_obj.status_code == 200:
            responses=responses_obj.json()
        else:   
            raise Exception("DS-API exception. Return Code=",str(responses_obj.status_code))
        
        self.logger.debug("Records Retrieved : " +str(len(responses)))
        return(responses)

    def _dataset_to_dsapi_body(self,data=[], type="match"):
        # Converts an inbound json object into the parameter call format for DSAPI
        # Input [{"name": "Mr Test McTest", "primaryStreetAddress": "1 The street, the town", "postalCode": "WF2 2AA", "email": "fake@email.com", "phone": "225252525"}]
        # Output
        body=[]        
        if (type=="match"):
            for rec in data:
                params=(urllib.parse.urlencode(rec, doseq=True)+ self.dsapi_match_options +"&bundle="+ self.dsapi_match_bundles).replace('=None&','=&').replace('=nan&','=&')
                body.append(self.dsapi_match_method +'?'+ params)
        elif (type=="enhance"):
            for rec in data:
                params=(urllib.parse.urlencode(rec, doseq=True)+ self.dsapi_enhance_options +"&bundle="+ self.dsapi_enhance_bundles).replace('=None&','=&').replace('=nan&','=&')
                body.append(self.dsapi_enhance_method +'?'+ params)            
        return(body)

    # General Utilities

    def flatten_json(self,y):
        out = {}
  
        def flatten(x, name =''):          
            # If the Nested key-value 
            # pair is of dict type
            if type(x) is dict:              
                for a in x:
                    flatten(x[a], name + a + '_')
                  
            # If the Nested key-value
            # pair is of list type
            elif type(x) is list:              
                i = 0              
                for a in x:                
                    flatten(a, name + str(i) + '_')
                    i += 1
            else:
                out[name[:-1]] = x
  
        flatten(y)
        return out