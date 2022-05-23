import requests
import json
import os
from dotenv import load_dotenv
import pandas as pd

def getToken(apikey):
    url = f"https://iam.cloud.ibm.com/identity/token"
    headers = {
      "Content-Type": "application/x-www-form-urlencoded"
    }
    query = {
        "grant_type":"urn:ibm:params:oauth:grant-type:apikey",
        "apikey": apikey
    }
    response = requests.post(url,headers=headers,params=query)
    token = json.loads(response.text)
    token = token["access_token"]
    return token

def uploadCOS(data):
    token = getToken(os.environ.get('CE_COS_API_KEY'))
    url = f"https://s3.ams03.cloud-object-storage.appdomain.cloud/bucket-resources-manager/{os.getenv('CE_ACCOUNT_ID')}-results.csv"
    headers = {
      "Authorization": f"Bearer {token}",
      "Content-Type": "text/csv"
    }
    results= data.to_csv(sep=';')
    response = requests.put(url,headers=headers,data=results)

def main(): 
    os.environ['TOKEN'] = getToken(os.environ.get('CE_API_KEY'))
    resources = getResources()
    creators=getCreators()    
    resources_dict = createResourcesDict(resources["resources"], creators)
    consumption = getServicesConsumption()
    consumption = consumption.merge(resources_dict, left_on='Resource Instance ID', right_on='id', how="left")
    consumption = consumption.drop(['id', 'Account State', 'Account Type','Space Name','Space ID','Organization Name','Organization ID', 'Pricing Country','Billing Country'], axis=1)
    uploadCOS(consumption)

def createResourcesDict(resources, creators):
    df = pd.DataFrame(columns = ['id', 'Creator_mail'])
    for resource in resources:
        df.loc[len(df.index)] = [resource["id"],creators.get(resource["created_by"], "")]
    return df
    
def getServicesConsumption():
    csv = getRequest(f"https://s3.ams03.cloud-object-storage.appdomain.cloud/bucket-resources-manager/{os.environ.get('CE_ACCOUNT_ID')}.csv")
    csvString = csv.text.replace("\r", "")
    df = pd.DataFrame([x.split(';') for x in csvString.split('\n')[1:]], columns=[x for x in csvString.split('\n')[0].split(';')] )
    return df

def addServicesID(users):
    response = getRequest(f"https://iam.cloud.ibm.com/v1/serviceids?account_id={os.environ.get('CE_ACCOUNT_ID')}")
    services = json.loads(response.text)["serviceids"]
    with open(f'./serviceid.json', 'w', encoding='utf-8') as f:
      json.dump(services, f, ensure_ascii=False, indent=4)
    for service_id in services:
        users[service_id["iam_id"]] = users.get(service_id["created_by"], service_id["name"]) 

def getCreators():
    response = getRequest(f"https://user-management.cloud.ibm.com/v2/accounts/{os.environ.get('CE_ACCOUNT_ID')}/users")
    users = json.loads(response.text)["resources"]
    creators_dict = {}
    for user in users:
        creators_dict[user["iam_id"]] = user["user_id"]
    addServicesID(creators_dict)
    return creators_dict

def getResources():
    response = getRequest("https://resource-controller.cloud.ibm.com/v2/resource_instances")
    return json.loads(response.text) 

def getRequest(url):
    headers = {
      "Accept": "application/json",
      "Authorization": f"Bearer {os.environ.get('TOKEN')}"
    }
    response = requests.get(url,headers=headers)
    return response

if __name__ == '__main__':
    load_dotenv(  )
    main()