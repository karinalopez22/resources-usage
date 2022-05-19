import requests
import json
import os
from dotenv import load_dotenv
import pandas as pd

def getToken():
    url = f"https://iam.cloud.ibm.com/identity/token"
    headers = {
      "Content-Type": "application/x-www-form-urlencoded"
    }
    query = {
        "grant_type":"urn:ibm:params:oauth:grant-type:apikey",
        "apikey": os.environ["CE_API_KEY"]
    }
    response = requests.post(url,headers=headers,params=query)
    token = json.loads(response.text)
    token = token["access_token"]
    return token

def uploadCOS(data):
    url = f"https://s3.ams03.cloud-object-storage.appdomain.cloud/bucket-resources-manager/{os.getenv('CE_ACCOUNT_ID')}-results.csv"
    headers = {
      "Authorization": f"Bearer {os.getenv('TOKEN')}",
      "Content-Type": "text/csv"
    }
    results= data.to_csv(sep=';')
    response = requests.put(url,headers=headers,data=results)

def main(): 
    os.environ['TOKEN'] = getToken()
    resources = getResources()
    users=getUsers()    
    resources_dict = createResourcesDict(resources["resources"], users)
    consumption = getServicesConsumption()
    consumption = consumption.merge(resources_dict, left_on='Resource Instance ID', right_on='id', how="left")
    consumption = consumption.drop(['id'], axis=1)
    uploadCOS(consumption)

def createResourcesDict(resources, users):
    df = pd.DataFrame(columns = ['id', 'Creator_mail'])
    for resource in resources:
        df.loc[len(df.index)] = [resource["id"],users.get(resource["created_by"], "")]
    return df
    
def getServicesConsumption():
    csv = getRequest(f"https://s3.ams03.cloud-object-storage.appdomain.cloud/bucket-resources-manager/{os.getenv('CE_ACCOUNT_ID')}.csv")
    csvString = csv.text.replace("\r", "")
    df = pd.DataFrame([x.split(';') for x in csvString.split('\n')[1:]], columns=[x for x in csvString.split('\n')[0].split(';')] )
    return df

def addServicesID(users):
    response = getRequest(f"https://iam.cloud.ibm.com/v1/serviceids?account_id={os.environ['CE_ACCOUNT_ID']}")
    services = json.loads(response.text)["serviceids"]
    for service_id in services:
        users[service_id["iam_id"]] = users.get(service_id["created_by"], "") 

def getUsers():
    response = getRequest(f"https://user-management.cloud.ibm.com/v2/accounts/{os.environ['CE_ACCOUNT_ID']}/users")
    users = json.loads(response.text)["resources"]
    users_dict = {}
    for user in users:
        users_dict[user["iam_id"]] = user["user_id"]
    addServicesID(users_dict)
    return users_dict

def getResources():
    response = getRequest("https://resource-controller.cloud.ibm.com/v2/resource_instances")
    return json.loads(response.text) 

def getRequest(url):
    headers = {
      "Accept": "application/json",
      "Authorization": f"Bearer {os.getenv('TOKEN')}"
    }
    response = requests.get(url,headers=headers)
    return response

if __name__ == '__main__':
    load_dotenv(  )
    main()