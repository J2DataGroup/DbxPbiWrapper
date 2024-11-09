# Databricks notebook source
#Builder
from __future__ import annotations
import json
import requests
from abc import ABC, abstractmethod, ABCMeta
from typing import Any
from typing import List
from uuid import UUID
from dataclasses import dataclass
from datetime import datetime, timezone
import random, time
import asyncio
import traceback
import ast

# COMMAND ----------

refreshType = "full"
resource = "https://analysis.windows.net/powerbi/api"
grant_type = "client_credentials"
secretScope = "defaultSecretScope"
accountKey = "accessAccountAppId"
accountSecretKey = "accessAccountSecret"
tenantIdKey = "tenantId"
subscriptionId = "subscriptionId"

# COMMAND ----------

class BaseValueObjects:
    """Keeper of all objects that can be hydrated."""
    def __init__(self):
        pass
    
    @dataclass
    class TokenRequestObject:
        def __init__(self, accountKey, accountSecret):
            self.resource = "https://analysis.windows.net/powerbi/api"
            self.grant_type = "client_credentials"
            self.client_id = accountKey
            self.client_secret = accountSecret
    
    @dataclass
    class XmlaTokenRequestObject:
        def __init__(self, accountKey, accountSecret):
            self.resource = "https://management.azure.com/"
            self.grant_type = "client_credentials"
            self.client_id = accountKey
            self.client_secret = accountSecret
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
        def from_dict(obj: Any) -> 'Root': # type: ignore
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
        def from_dict(obj: Any) -> 'Root': # type: ignore
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
        def from_dict(obj: Any) -> 'Root': # type: ignore
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
        def from_dict(obj: Any) -> 'Root': # type: ignore
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
        endTime: Optional[str] = None # type: ignore

        @staticmethod
        def from_dict(obj: Any) -> 'Value': # type: ignore
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
        def from_dict(obj: Any) -> 'Root': # type: ignore
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
        def from_dict(obj: Any) -> 'Value': # type: ignore
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
        def from_dict(obj: Any) -> 'Root': # type: ignore
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
        def from_dict(obj: Any) -> 'Value': # type: ignore
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

    @dataclass
    class ValueDatasetUser:
        identifier: str
        principalType: str
        datasetUserAccessRight: str
        
        @staticmethod
        def from_dict(obj: Any) -> 'Value': # type: ignore
            _identifier = str(obj.get("identifier"))
            _principalType = str(obj.get("principalType"))
            _datasetUserAccessRight = str(obj.get("datsetUserAccessRight"))
            return BaseValueObjects.ValueDatasetUser(_identifier, _principalType, _datasetUserAccessRight)

    @dataclass
    class ValueDatasetUsers:
        odata_context: str
        value: List[BaseValueObjects.ValueDatasetUser]

        @staticmethod
        def from_dict(obj: Any) -> 'Root': # type: ignore
            _odata_context = str(obj.get("@odata.context"))
            _value = [BaseValueObjects.ValueDatasetUser.from_dict(y) for y in obj.get("value")]
            print(type(_value))
            return list(_value)
