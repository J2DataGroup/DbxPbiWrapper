# Databricks notebook source
#Builder
import json
import requests
from __future__ import annotations
from abc import ABC, abstractmethod, ABCMeta
from typing import Any
from typing import List
from uuid import UUID
from dataclasses import dataclass
from datetime import datetime, timezone
import pytz
import random, time
import asyncio
import traceback
import ast

# COMMAND ----------

forecastDatabase = "Reserves"
refreshType = "full"
resource = "https://analysis.windows.net/powerbi/api"
grant_type = "client_credentials"
secretScope = "defaultSecretScope"
accountKey = "accessAccountAppId"
accountSecretKey = "accessAccountSecret"
tenantIdKey = "tenantId"
aaRunbookUrl = "aaRunbookUrl"
aaMdxRunbookUrl = "aaMdxRunbookUrl"
subscriptionId = "subscriptionId"
resourceGroupName = "resourceGroupName"
automationAccountName = "automationAccountName"

# COMMAND ----------

class BaseValueObjects:
    """Keeper of all objects that can be hydrated."""
    def __init__(self):
        pass
    
    @dataclass
    class TokenRequestObject:
        def __init__(self):
            self.resource = "https://analysis.windows.net/powerbi/api"
            self.grant_type = "client_credentials"
            self.client_id = dbutils.secrets.get(scope=secretScope, key=accountKey)
            self.client_secret = dbutils.secrets.get(scope=secretScope, key=accountSecretKey)            
    
    @dataclass
    class XmlaTokenRequestObject:
        def __init__(self):
            self.resource = "https://management.azure.com/"
            self.grant_type = "client_credentials"
            self.client_id = dbutils.secrets.get(scope=secretScope, key=accountKey)
            self.client_secret = dbutils.secrets.get(scope=secretScope, key=accountSecretKey)
            self.scope = "https://management.azure.com/.default"

    @dataclass
    class TokenResponseObject:
        token_type: str
        expires_in: str
        ext_expires_in: str
        expires_on: str
        not_before: str
        resource: str
        access_token: str

        @staticmethod
        def from_dict(obj: Any) -> 'Root':
            _token_type = str(obj.get("token_type"))
            _expires_in = str(obj.get("expires_in"))
            _ext_expires_in = str(obj.get("ext_expires_in"))
            _expires_on = str(obj.get("expires_on"))
            _not_before = str(obj.get("not_before"))
            _resource = str(obj.get("resource"))
            _access_token = str(obj.get("access_token"))
            return BaseValueObjects.TokenResponseObject(_token_type, _expires_in, _ext_expires_in, _expires_on, _not_before, _resource, _access_token)

    class AAJobRoot:
        JobId: str

        @staticmethod
        def from_dict(obj: Any) -> 'Root':
            _JobIds = ast.literal_eval(str(obj.get("JobIds")))
            return _JobIds[0]

    @dataclass
    class ApiResponse:
        Cache_Control: str
        Pragma: str
        Content_Length: str
        Content_Type: str
        Content_Encoding: str
        Strict_Transport_Security: str
        X_Frame_Options: str
        X_Content_Type_Options: str
        RequestId: str
        Access_Control_Expose_Headers: str
        request_redirected: str
        home_cluster_uri: str
        Date: str

        @staticmethod
        def from_dict(obj: Any) -> 'Root':
            _Cache_Control = str(obj.get("Cache-Control"))
            _Pragma = str(obj.get("Pragma"))
            _Content_Length = str(obj.get("Content-Length"))
            _Content_Type = str(obj.get("Content-Type"))
            _Content_Encoding = str(obj.get("Content-Encoding"))
            _Strict_Transport_Security = str(obj.get("Strict-Transport-Security"))
            _X_Frame_Options = str(obj.get("X-Frame-Options"))
            _X_Content_Type_Options = str(obj.get("X-Content-Type-Options"))
            _RequestId = str(obj.get("RequestId"))
            _Access_Control_Expose_Headers = str(obj.get("Access-Control-Expose-Headers"))
            _request_redirected = str(obj.get("request-redirected"))
            _home_cluster_uri = str(obj.get("home-cluster-uri"))
            _Date = str(obj.get("Date"))
            return BaseValueObjects.ApiResponse(_Cache_Control, _Pragma, _Content_Length, _Content_Type, _Content_Encoding, _Strict_Transport_Security, _X_Frame_Options, _X_Content_Type_Options, _RequestId,_Access_Control_Expose_Headers, _request_redirected, _home_cluster_uri, _Date)

    @dataclass
    class RootRefreshStatus:
        _odata_context: str
        value: List[BaseValueObjects.ValueRefreshStatus]

        @staticmethod
        def from_dict(obj: Any) -> 'Root':
            _odata_context = str(obj.get("@odata.context"))
            _value = [BaseValueObjects.ValueRefreshStatus.from_dict(y) for y in obj.get("value")]
            return BaseValueObjects.RootRefreshStatus(_odata_context, _value)

    @dataclass
    class ValueRefreshStatus:
        requestId: str
        id: int
        refreshType: str
        startTime: str    
        status: str
        endTime: Optional[str] = None

        @staticmethod
        def from_dict(obj: Any) -> 'Value':
            _requestId = str(obj.get("requestId"))
            _id = int(obj.get("id"))
            _refreshType = str(obj.get("refreshType"))
            _startTime = str(obj.get("startTime"))
            _endTime = str(obj.get("endTime"))
            _status = str(obj.get("status"))
            return BaseValueObjects.ValueRefreshStatus(_requestId, _id, _refreshType, _startTime, _status, _endTime)

    @dataclass
    class RootGroup:
        odata_context: str
        odata_count: int
        value: List[BaseValueObjects.ValueGroup]

        @staticmethod
        def from_dict(obj: Any) -> 'Root':
            _odata_context = str(obj.get("@odata.context"))
            _odata_count = int(obj.get("@odata.count"))
            _value = [BaseValueObjects.ValueGroup.from_dict(y) for y in obj.get("value")]
            return BaseValueObjects.RootGroup(_odata_context, _odata_count, _value)

    @dataclass
    class ValueGroup:
        id: str
        isReadOnly: bool
        isOnDedicatedCapacity: bool
        capacityId: str
        type: str
        name: str

        @staticmethod
        def from_dict(obj: Any) -> 'Value':
            _id = str(obj.get("id"))
            _isReadOnly = bool(obj.get("isReadOnly"))
            _isOnDedicatedCapacity = bool(obj.get("isOnDedicatedCapcity"))
            _capacityId = str(obj.get("capacityId"))
            _type = str(obj.get("type"))
            _name = str(obj.get("name"))
            return BaseValueObjects.ValueGroup(_id, _isReadOnly, _isOnDedicatedCapacity, _capacityId, _type, _name)
    
    @dataclass
    class RootDataset:
        odata_context: str
        value: List[BaseValueObjects.ValueDataset]

        @staticmethod
        def from_dict(obj: Any) -> 'Root':
            _odata_context = str(obj.get("@odata.context"))
            _value = [BaseValueObjects.ValueDataset.from_dict(y) for y in obj.get("value")]
            return BaseValueObjects.RootDataset(_odata_context, _value)

    @dataclass
    class ValueDataset:
        id: str
        name: str
        expressions: List[object]
        roles: List[object]
        webUrl: str
        addRowsAPIEnabled: bool
        configuredBy: str
        isRefreshable: bool
        isEffectiveIdentityRequired: bool
        isEffectiveIdentityRolesRequired: bool
        isOnPremGatewayRequired: bool
        targetStorageMode: str
        createdDate: str
        createReportEmbedURL: str
        qnaEmbedURL: str
        upstreamDatasets: List[object]
        users: List[object]

        @staticmethod
        def from_dict(obj: Any) -> 'Value':
            _id = str(obj.get("id"))
            _name = str(obj.get("name"))
            _expressions = None
            _roles = None
            _webUrl = str(obj.get("webUrl"))
            _addRowsAPIEnabled = bool(obj.get("addRowsAPIEnabled"))
            _configuredBy = str(obj.get("configuredBy"))
            _isRefreshable = bool(obj.get("isRefreshable"))
            _isEffectiveIdentityRequired = bool(obj.get("isEffectiveIdentityRequired"))
            _isEffectiveIdentityRolesRequired = bool(obj.get("isEffectiveIdentityRolesRequired"))
            _isOnPremGatewayRequired = bool(obj.get("isOnPremGatewayRequired"))
            _targetStorageMode = str(obj.get("targetStorageMode"))
            _createdDate = str(obj.get("createdDate"))
            _createReportEmbedURL = str(obj.get("createReportEmbedURL"))
            _qnaEmbedURL = str(obj.get("qnaEmbedURL"))
            _upstreamDatasets = None
            _users = None
            return BaseValueObjects.ValueDataset(_id, _name, _expressions, _roles, _webUrl, _addRowsAPIEnabled, _configuredBy, _isRefreshable, _isEffectiveIdentityRequired, _isEffectiveIdentityRolesRequired, _isOnPremGatewayRequired, _targetStorageMode, _createdDate, _createReportEmbedURL, _qnaEmbedURL, _upstreamDatasets, _users)

# COMMAND ----------

class BaseApiHelper:
    """Keeper of all base calls for making calls to PBI."""
    def __init__(self):
        pass

    class TokenHelper:    
        def __init__(self):
            self.resource = resource
            self.tenant = dbutils.secrets.get(scope=secretScope, key=tenantIdKey)
            self.TokenRequestObject = None
            self.TokenResponseObject = None
            
        def getAADToken(self, isManagementScope = False):
            tokenUrl = f"https://login.microsoftonline.com/{self.tenant}/oauth2/token"

            if(isManagementScope == False):
                tokenParam = BaseValueObjects.TokenRequestObject()
                payload = {'resource': tokenParam.resource, 'grant_type': tokenParam.grant_type, 'client_id': tokenParam.client_id, 'client_secret': tokenParam.client_secret}
            else:
                tokenParam = BaseValueObjects.XmlaTokenRequestObject()
                payload = {'resource': tokenParam.resource, 'grant_type': tokenParam.grant_type, 'client_id': tokenParam.client_id, 'client_secret': tokenParam.client_secret, 'scope': tokenParam.scope}

            result = requests.post(tokenUrl, data=payload, timeout=30)
            jsonstring = json.loads(result.text)
            self.TokenResponseObject = BaseValueObjects.TokenResponseObject.from_dict(jsonstring)    
            print("New token generated as requested.")
            return self.TokenResponseObject
        
        def getValidatedAADToken(self, token:TokenRequestObject, isManagementScope = False):
            if(token is None):
                print("Token not generated. Will create a new one.")
                return self.getAADToken(isManagementScope)
            else:
                token_not_before = token.expires_on # token.not_before
                expires_utc_time = datetime.fromtimestamp(int(token_not_before))
                print(f"{expires_utc_time} expires token time")
                current_time = datetime.now()
                print(f"{current_time} current datetime now")
                difference =  expires_utc_time - current_time
                print(f"Token Valid for {difference.total_seconds()/60 } more minutes.")
                if(difference.total_seconds()/60 > 10.0):
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
                    if(value == None):
                        raise Exception("Refresh status object is None")
                    return value
                except Exception as ex:
                    print(f"Retry because of error. {traceback.print_exc()}")
                    if( x== retries):    
                        raise Exception("Refresh status object is None.")
                sleep = (backoffInSeconds * 2 ** x + random.uniform(0,1))
                print(f"Retry after {sleep} seconds.")
                time.sleep(sleep)
                x += 1

        def getWorkspaceName(self):
            if(self.environmentName.lower() == "prd"):
                return "ARC Corporate Analytics"
            else:
                return f"ARC Corporate Analytics [{self.environmentName.upper()}]"
        
        def getGroup(self,tokenObject: TokenResponseObject):
            tokenHelper = BaseApiHelper.TokenHelper()
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, False)
            endpointUrl = "https://api.powerbi.com/v1.0/myorg/groups"
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            result = requests.get(endpointUrl, headers=headers, timeout=30)
            jsonstring = json.loads(result.text)
            groupObject = BaseValueObjects.RootGroup.from_dict(jsonstring)        
            groupValueObject = list(filter(lambda x: (x.name.lower() == self.getWorkspaceName().lower()),groupObject.value))
            if(len(groupValueObject)) != 1:
                print("Expected to find group, but the group does not exists.")
                return None
            return groupValueObject[0]
        
        def getDataset(self, tokenObject:TokenResponseObject, group:ValueGroup):
            tokenHelper = BaseApiHelper.TokenHelper()
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, False)        
            endpointUrl = f"https://api.powerbi.com/v1.0/myorg/groups/{group.id}/datasets"
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            result = requests.get(endpointUrl, headers=headers, timeout=30)
            jsonstring = json.loads(result.text)
            datasetObject = BaseValueObjects.RootDataset.from_dict(jsonstring)
            datasetValueObject = list(filter(lambda y: (y.name.lower() == forecastDatabase.lower()), datasetObject.value))
            if(len(datasetValueObject)) != 1:
                print("Expected to find Forecast dataset, but the dataset does not exists.")
                return None
            return datasetValueObject[0]
        
        def refreshDataset(self, tokenObject:TokenResponseObject, group:ValueGroup, dataset: ValueDataset, payloadData):
            tokenHelper = BaseApiHelper.TokenHelper()
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, False)        
            endpointUrl = f"https://api.powerbi.com/v1.0/myorg/groups/{group.id}/datasets/{dataset.id}/refreshes"
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            payload = payloadData
            response = requests.post(endpointUrl, json=payload, headers=headers, timeout=30)        
            responseObject = BaseValueObjects.ApiResponse.from_dict(response.headers)
            if response.status_code != 202:
                print(f"Refresh failed with response code of {response.status_code}. Existing Refresh may be in progress.")
                return None
            return responseObject
        
        def refreshInProgress(self,tokenObject:TokenResponseObject, group:ValueGroup, dataset: ValueDataset):
            tokenHelper = BaseApiHelper.TokenHelper()
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, False)
            endpointUrl = f"https://api.powerbi.com/v1.0/myorg/groups/{group.id}/datasets/{dataset.id}/refreshes?$top=10"
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            result = requests.get(endpointUrl, headers=headers, timeout=30)
            jsonstring = json.loads(result.text) 
            print(f"Checking If existing request is in progress.")
            datasetObject = BaseValueObjects.RootRefreshStatus.from_dict(jsonstring)  
            datasetValueObject = list(filter(lambda y: (y.status == "Unknown"), datasetObject.value))
            if(len(datasetValueObject)) > 0:
                return True
            return False

        def refreshStatus(self, tokenObject:TokenResponseObject, group:ValueGroup, dataset: ValueDataset, apiresponse: ApiResponse):
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
            if(len(datasetValueObject)) != 1:
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
            if(self.environmentName.lower() == "prd"):
                return "ARC Corporate Analytics"
            else:
                return f"ARC Corporate Analytics [{self.environmentName.upper()}]"        
        
        
        def submitPayloadJob(self,tokenObject:TokenResponseObject, payloadData, description):
            tokenHelper = BaseApiHelper.TokenHelper()
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, True)        
            endpointUrl = self.aaRunbookUrl
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            payload = {'workspace': self.getWorkspaceName(), 'TenantId': self.tenant, 'Xmla': payloadData, 'EntityTypeDescription': description}
            retries = 1
            while retries < 3:
                try:
                    response = requests.post(endpointUrl, json=payload, headers=headers, timeout=(3.05,30))       
                    if response.status_code == 202:                        
                        print(f"Request successfully posted on {retries} retry")
                        retries = 3
                    else:
                        retries+=1
                except Exception as ex:
                    print(f"Retry because of error. {ex}")
                    retries+=1
                    time.sleep(5)
            if response.status_code != 202:
                print(f"Refresh failed with response code of {response.status_code}. Existing Refresh may be in progress.")
                return None                
            jsonstring = json.loads(response.text) 
            print(response)            
            aaJobRootObject = BaseValueObjects.AAJobRoot.from_dict(jsonstring)

            print(f"JobId {aaJobRootObject} Created.")
            if(aaJobRootObject == None):
                print("Expected to find jobid from job trigger, but found none.")
                return None
            return aaJobRootObject
        
        def submitMdxQuery(self,tokenObject:TokenResponseObject, payloadData, database):
            tokenHelper = BaseApiHelper.TokenHelper()
            tokenObject = tokenHelper.getValidatedAADToken(tokenObject, True)        
            endpointUrl = self.aaMdxRunbookUrl
            headers = {"Authorization": f"Bearer {tokenObject.access_token}"}
            payload = {'workspace': self.getWorkspaceName(), 'TenantId': self.tenant, 'Xmla': payloadData, 'Database': database}
            response = requests.post(endpointUrl, json=payload, headers=headers, timeout=30)        
            if response.status_code != 202:
                print(f"Refresh failed with response code of {response.status_code}. Existing Refresh may be in progress.")
                return None                
            jsonstring = json.loads(response.text) 
            print(response)            
            aaJobRootObject = BaseValueObjects.AAJobRoot.from_dict(jsonstring)

            print(f"JobId {aaJobRootObject} Created.")
            if(aaJobRootObject == None):
                print("Expected to find jobid from job trigger, but found none.")
                return None
            return aaJobRootObject

        def payloadJobStatus(self, tokenObject:TokenResponseObject, jobId):
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
                    if result.status_code == 202 or result.status_code==200:                        
                        print(f"Request successfully posted on {retries} retry")
                        retries = 3
                    else:
                        retries+=1
                except Exception as ex:
                    print(f"Retry because of error. {ex}")
                    retries+=1
                    time.sleep(30)
            
            resultText = result.text
            print(f"Checking Status for Request {jobId}")
            return resultText

