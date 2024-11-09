from BaseValueObjects import *

class BaseApiHelper:
    """Keeper of all base calls for making calls to PBI."""

    def __init__(self):
        pass

    class TokenHelper:
        def __init__(self, tenant, accountKey, accountSecret):
            self.resource = 'https://analysis.windows.net/powerbi/api'
            self.tenant = tenant
            self.accountKey = accountKey
            self.accountSecret = accountSecret
            self.TokenRequestObject = None
            self.TokenResponseObject = None

        def getAADToken(self, isManagementScope=False):
            tokenUrl = f"https://login.microsoftonline.com/{self.tenant}/oauth2/token"

            if (isManagementScope == False):
                tokenParam = BaseValueObjects.TokenRequestObject(self.accountKey, self.accountSecret)
                payload = {'resource': tokenParam.resource, 'grant_type': tokenParam.grant_type,
                           'client_id': tokenParam.client_id, 'client_secret': tokenParam.client_secret}
            else:
                tokenParam = BaseValueObjects.XmlaTokenRequestObject()
                payload = {'resource': tokenParam.resource, 'grant_type': tokenParam.grant_type,
                           'client_id': tokenParam.client_id, 'client_secret': tokenParam.client_secret,
                           'scope': tokenParam.scope}

            result = requests.post(tokenUrl, data=payload, timeout=30)
            jsonstring = json.loads(result.text)
            self.TokenResponseObject = BaseValueObjects.TokenResponseObject.from_dict(jsonstring)
            print("New token generated as requested.")
            return self.TokenResponseObject

        def getValidatedAADToken(self, token: BaseValueObjects.TokenRequestObject, isManagementScope=False):
            if (token is None):
                print("Token not generated. Will create a new one.")
                return self.getAADToken(isManagementScope)
            else:
                token_not_before = token.expires_on  # token.not_before
                expires_utc_time = datetime.fromtimestamp(int(token_not_before))
                print(f"{expires_utc_time} expires token time")
                current_time = datetime.now()
                print(f"{current_time} current datetime now")
                difference = expires_utc_time - current_time
                print(f"Token Valid for {difference.total_seconds() / 60} more minutes.")
                if (difference.total_seconds() / 60 > 10.0):
                    print("Existing token is still valid.")
                    return token
                else:
                    return self.getAADToken(isManagementScope)

    class PbiApiHandler:
        def __init__(self,tenant, accountKey, accountSecret):              
            self.tenant = tenant
            self.accountKey = accountKey
            self.accountSecret = accountSecret

        def retryWithBackOff(fn, args=None, kwargs=None, retries=3, backoffInSeconds=2):
            x = 0
            if args is None:
                args = []
            if kwargs is None:
                kwargs = {}

            while True:
                try:
                    value = fn(*args, **kwargs)
                    if (value == None):
                        raise Exception("Refresh status object is None")
                    return value
                except Exception as ex:
                    print(f"Retry because of error. {traceback.print_exc()}")
                    if (x == retries):
                        raise Exception("Refresh status object is None.")
                sleep = (backoffInSeconds * 2 ** x + random.uniform(0, 1))
                print(f"Retry after {sleep} seconds.")
                time.sleep(sleep)
                x += 1

        def getGroup(self, tokenObject: BaseValueObjects.TokenResponseObject, groupName = None):
            tokenHelper = BaseApiHelper.TokenHelper(self.tenant,self.accountKey,self.accountSecret)
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, False)
            endpointUrl = "https://api.powerbi.com/v1.0/myorg/groups"
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            result = requests.get(endpointUrl, headers=headers, timeout=30)
            jsonstring = json.loads(result.text)
            groupObject = BaseValueObjects.RootGroup.from_dict(jsonstring)    
            if(groupName is None):
                groupValueObject = list(groupObject.value)
                return groupValueObject
            else:
                groupValueObject = list(
                    filter(lambda x: (x.name == groupName), groupObject.value))
                if (len(groupValueObject)) != 1:
                    print("Expected to find {groupName} group, but the group does not exists.")
                    return None
                return groupValueObject[0]

        def getDataset(self, tokenObject: BaseValueObjects.TokenResponseObject, group: BaseValueObjects.ValueGroup, datasetName = None):
            tokenHelper = BaseApiHelper.TokenHelper(self.tenant,self.accountKey,self.accountSecret)
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, False)
            endpointUrl = f"https://api.powerbi.com/v1.0/myorg/groups/{group.id}/datasets"
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            result = requests.get(endpointUrl, headers=headers, timeout=30)
            jsonstring = json.loads(result.text)
            datasetObject = BaseValueObjects.RootDataset.from_dict(jsonstring)
            if(datasetName is None):
                 datasetValueObject = list(datasetObject.value)
                 return datasetValueObject
            else:
                datasetValueObject = list(
                    filter(lambda y: (y.name == datasetName), datasetObject.value))
                if (len(datasetValueObject)) != 1:
                    print(f"Expected to find {datasetName} dataset, but the dataset does not exists.")
                    return None
                return datasetValueObject[0]

        def refreshDataset(self, tokenObject: BaseValueObjects.TokenResponseObject, group: BaseValueObjects.ValueGroup, dataset: BaseValueObjects.ValueDataset,
                           payloadData):
            tokenHelper = BaseApiHelper.TokenHelper(self.tenant,self.accountKey,self.accountSecret)
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, False)
            endpointUrl = f"https://api.powerbi.com/v1.0/myorg/groups/{group.id}/datasets/{dataset.id}/refreshes"
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            payload = payloadData
            payload = {'type': 'Full'}
            print(f"Posting API Refresh with {payload}")
            response = requests.post(endpointUrl, data=payload, headers=headers, timeout=30)
            responseObject = BaseValueObjects.ApiResponse.from_dict(response.headers)
            if response.status_code != 202:
                print(
                    f"Refresh failed with response code of {response.status_code}. Existing Refresh may be in progress.")
                return None
            return responseObject

        def refreshInProgress(self, tokenObject: BaseValueObjects.TokenResponseObject, group: BaseValueObjects.ValueGroup, dataset: BaseValueObjects.ValueDataset):
            tokenHelper = BaseApiHelper.TokenHelper(self.tenant,self.accountKey,self.accountSecret)
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, False)
            endpointUrl = f"https://api.powerbi.com/v1.0/myorg/groups/{group.id}/datasets/{dataset.id}/refreshes?$top=10"
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            result = requests.get(endpointUrl, headers=headers, timeout=30)
            jsonstring = json.loads(result.text)
            print(f"Checking If existing request is in progress.")
            datasetObject = BaseValueObjects.RootRefreshStatus.from_dict(jsonstring)
            datasetValueObject = list(filter(lambda y: (y.status == "Unknown"), datasetObject.value))
            if (len(datasetValueObject)) > 0:
                return True
            return False

        def refreshStatus(self, tokenObject: BaseValueObjects.TokenResponseObject, group: BaseValueObjects.ValueGroup, dataset: BaseValueObjects.ValueDataset,
                          apiresponse: BaseValueObjects.ApiResponse):
            tokenHelper = BaseApiHelper.TokenHelper(self.tenant,self.accountKey,self.accountSecret)
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, False)
            pushedRequestId = apiresponse.RequestId
            endpointUrl = f"https://api.powerbi.com/v1.0/myorg/groups/{group.id}/datasets/{dataset.id}/refreshes?$top=10"
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            result = requests.get(endpointUrl, headers=headers, timeout=30)
            jsonstring = json.loads(result.text)
            print(f"Checking Status for Request {pushedRequestId}")
            datasetObject = BaseValueObjects.RootRefreshStatus.from_dict(jsonstring)
            datasetValueObject = list(filter(lambda y: (y.requestId == pushedRequestId), datasetObject.value))
            if (len(datasetValueObject)) != 1:
                print("Expected to find refresh status, but cannot find it.")
                return None
            return datasetValueObject[0]

        def getUsersInDataset(self, tokenObject: BaseValueObjects.TokenResponseObject, group: BaseValueObjects.ValueGroup, dataset: BaseValueObjects.ValueDataset):
            tokenHelper = BaseApiHelper.TokenHelper(self.tenant,self.accountKey,self.accountSecret)
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, False)
            endpointUrl = f"https://api.powerbi.com/v1.0/myorg/groups/{group.id}/datasets/{dataset.id}/users"
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            print(f"Calling API to Get Users in Dataset")
            result = requests.get(endpointUrl, headers=headers, timeout=30)
            jsonstring = json.loads(result.text)
            if result.status_code != 200:
                print(
                    f"Failed getting users in dataset with status code of {result.status_code}.")
                return None
            userValueObject = list(BaseValueObjects.ValueDatasetUsers.from_dict(jsonstring))
            return userValueObject
