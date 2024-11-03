
class BaseApiHelper:
    """Keeper of all base calls for making calls to PBI."""

    def __init__(self):
        pass

    class TokenHelper:
        def __init__(self, tenant):
            self.resource = 'https://analysis.windows.net/powerbi/api'
            self.tenant = tenant
            self.TokenRequestObject = None
            self.TokenResponseObject = None

        def getAADToken(self, isManagementScope=False):
            tokenUrl = f"https://login.microsoftonline.com/{self.tenant}/oauth2/token"

            if (isManagementScope == False):
                tokenParam = BaseValueObjects.TokenRequestObject()
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

        def getValidatedAADToken(self, token: TokenRequestObject, isManagementScope=False):
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
        def __init__(self):
            environment_name = 'DestinysChild'
            listKeys = spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")
            jsonObject = json.loads(listKeys)
            for tag in jsonObject:
                if tag['key'] == 'Environment':
                    environment_name = tag['value'].upper()
            self.environmentName = environment_name
            print(f"Say my name say my name, my name is {self.environmentName}")

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

        def getWorkspaceName(self):
            if (self.environmentName.lower() == "prd"):
                return "ARC Corporate Analytics"
            else:
                return f"ARC Corporate Analytics [{self.environmentName.upper()}]"

        def getGroup(self, tokenObject: TokenResponseObject):
            tokenHelper = BaseApiHelper.TokenHelper()
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, False)
            endpointUrl = "https://api.powerbi.com/v1.0/myorg/groups"
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            result = requests.get(endpointUrl, headers=headers, timeout=30)
            jsonstring = json.loads(result.text)
            groupObject = BaseValueObjects.RootGroup.from_dict(jsonstring)
            groupValueObject = list(
                filter(lambda x: (x.name.lower() == self.getWorkspaceName().lower()), groupObject.value))
            if (len(groupValueObject)) != 1:
                print("Expected to find group, but the group does not exists.")
                return None
            return groupValueObject[0]

        def getDataset(self, tokenObject: TokenResponseObject, group: ValueGroup):
            tokenHelper = BaseApiHelper.TokenHelper()
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, False)
            endpointUrl = f"https://api.powerbi.com/v1.0/myorg/groups/{group.id}/datasets"
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            result = requests.get(endpointUrl, headers=headers, timeout=30)
            jsonstring = json.loads(result.text)
            datasetObject = BaseValueObjects.RootDataset.from_dict(jsonstring)
            datasetValueObject = list(
                filter(lambda y: (y.name.lower() == forecastDatabase.lower()), datasetObject.value))
            if (len(datasetValueObject)) != 1:
                print("Expected to find Forecast dataset, but the dataset does not exists.")
                return None
            return datasetValueObject[0]

        def refreshDataset(self, tokenObject: TokenResponseObject, group: ValueGroup, dataset: ValueDataset,
                           payloadData):
            tokenHelper = BaseApiHelper.TokenHelper()
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, False)
            endpointUrl = f"https://api.powerbi.com/v1.0/myorg/groups/{group.id}/datasets/{dataset.id}/refreshes"
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            payload = payloadData
            response = requests.post(endpointUrl, json=payload, headers=headers, timeout=30)
            responseObject = BaseValueObjects.ApiResponse.from_dict(response.headers)
            if response.status_code != 202:
                print(
                    f"Refresh failed with response code of {response.status_code}. Existing Refresh may be in progress.")
                return None
            return responseObject

        def refreshInProgress(self, tokenObject: TokenResponseObject, group: ValueGroup, dataset: ValueDataset):
            tokenHelper = BaseApiHelper.TokenHelper()
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

        def refreshStatus(self, tokenObject: TokenResponseObject, group: ValueGroup, dataset: ValueDataset,
                          apiresponse: ApiResponse):
            tokenHelper = BaseApiHelper.TokenHelper()
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

    class PbiXmlApiHandler:
        def __init__(self):
            environment_name = 'DestinysChild'
            listKeys = spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")
            jsonObject = json.loads(listKeys)
            for tag in jsonObject:
                if tag['key'] == 'Environment':
                    environment_name = tag['value'].upper()
            self.environmentName = environment_name
            print(f"Say my name say my name, my name is {self.environmentName}")
            self.aaRunbookUrl = dbutils.secrets.get(scope=secretScope, key=aaRunbookUrl)
            self.aaMdxRunbookUrl = dbutils.secrets.get(scope=secretScope, key=aaMdxRunbookUrl)
            self.tenant = dbutils.secrets.get(scope=secretScope, key=tenantIdKey)
            self.subscriptionId = dbutils.secrets.get(scope=secretScope, key=subscriptionId)
            self.resourceGroupName = dbutils.secrets.get(scope=secretScope, key=resourceGroupName)
            self.automationAccount = dbutils.secrets.get(scope=secretScope, key=automationAccountName)

        def getWorkspaceName(self):
            if (self.environmentName.lower() == "prd"):
                return "ARC Corporate Analytics"
            else:
                return f"ARC Corporate Analytics [{self.environmentName.upper()}]"

        def submitPayloadJob(self, tokenObject: TokenResponseObject, payloadData, description):
            tokenHelper = BaseApiHelper.TokenHelper()
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, True)
            endpointUrl = self.aaRunbookUrl
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            payload = {'workspace': self.getWorkspaceName(), 'TenantId': self.tenant, 'Xmla': payloadData,
                       'EntityTypeDescription': description}
            retries = 1
            while retries < 3:
                try:
                    response = requests.post(endpointUrl, json=payload, headers=headers, timeout=(3.05, 30))
                    if response.status_code == 202:
                        print(f"Request successfully posted on {retries} retry")
                        retries = 3
                    else:
                        retries += 1
                except Exception as ex:
                    print(f"Retry because of error. {ex}")
                    retries += 1
                    time.sleep(5)
            if response.status_code != 202:
                print(
                    f"Refresh failed with response code of {response.status_code}. Existing Refresh may be in progress.")
                return None
            jsonstring = json.loads(response.text)
            print(response)
            aaJobRootObject = BaseValueObjects.AAJobRoot.from_dict(jsonstring)

            print(f"JobId {aaJobRootObject} Created.")
            if (aaJobRootObject == None):
                print("Expected to find jobid from job trigger, but found none.")
                return None
            return aaJobRootObject

        def submitMdxQuery(self, tokenObject: TokenResponseObject, payloadData, database):
            tokenHelper = BaseApiHelper.TokenHelper()
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, True)
            endpointUrl = self.aaMdxRunbookUrl
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            payload = {'workspace': self.getWorkspaceName(), 'TenantId': self.tenant, 'Xmla': payloadData,
                       'Database': database}
            response = requests.post(endpointUrl, json=payload, headers=headers, timeout=30)
            if response.status_code != 202:
                print(
                    f"Refresh failed with response code of {response.status_code}. Existing Refresh may be in progress.")
                return None
            jsonstring = json.loads(response.text)
            print(response)
            aaJobRootObject = BaseValueObjects.AAJobRoot.from_dict(jsonstring)

            print(f"JobId {aaJobRootObject} Created.")
            if (aaJobRootObject == None):
                print("Expected to find jobid from job trigger, but found none.")
                return None
            return aaJobRootObject

        def payloadJobStatus(self, tokenObject: TokenResponseObject, jobId):
            tokenHelper = BaseApiHelper.TokenHelper()
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, True)
            pushedRequestId = jobId
            endpointUrl = f"https://management.azure.com/subscriptions/{self.subscriptionId}/resourceGroups/{self.resourceGroupName}/providers/Microsoft.Automation/automationAccounts/{self.automationAccount}/Jobs/{jobId}/output?api-version=2019-06-01"
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            retries = 1
            result = None
            while retries < 3:
                try:
                    result = requests.get(endpointUrl, headers=headers, timeout=30)
                    if result.status_code == 202 or result.status_code == 200:
                        print(f"Request successfully posted on {retries} retry")
                        retries = 3
                    else:
                        retries += 1
                except Exception as ex:
                    print(f"Retry because of error. {ex}")
                    retries += 1
                    time.sleep(30)

            resultText = result.text
            print(f"Checking Status for Request {jobId}")
            return resultText
