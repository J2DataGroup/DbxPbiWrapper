from BaseValueObjects import *
from BaseApiHelper import *

class PbiDatasetValueObject:
    def __init__(self):
        self.Token = None
        self.ValueGroup = None
        self.ValueGroups = None
        self.ValueDataset = None
        self.ValueDatasets = None
        self.ApiResponseObject = None
        self.RefreshStatus = None        
        self.RefreshHistory = None        
        self.RefreshJson = None   
        self.ValueDatasetUsers = None     
        
class IPbiDatasetValueBuilder(metaclass= ABCMeta):
    
    @property
    @abstractmethod
    def getSafeAadToken(self) -> None:
        pass  
    
    @property
    @abstractmethod
    def getGroup(self, groupName) -> None:
        pass
    
    @property
    @abstractmethod
    def getDataset(self, datasetName) -> None:
        pass
    
    @property
    @abstractmethod
    def xmlaPostRequest(self) -> None:
        pass 

    @property
    @abstractmethod
    def existingRefresh(self) -> None:
        pass    

    @property
    @abstractmethod
    def getRefreshJson(self) -> None:
        pass  

    @property
    @abstractmethod
    def getGroups(self) -> None:
        pass  

    @property
    @abstractmethod
    def getRefreshHistory(self,groupName,datasetName) -> None:
        pass   

    @property
    @abstractmethod
    def getDatasets(self) -> None:
        pass     
    
    @property
    def getUsersInDataset(self, groupName, datasetName) -> None:
        pass

class PbiDatasetValueBuilder(IPbiDatasetValueBuilder):
    def __init__(self, tenant, accountKey, accountSecret):
        self.PbiDatasetValueObject = PbiDatasetValueObject()
        self.accountKey = accountKey
        self.accountSecret = accountSecret
        self.tenant = tenant
        self.pbiApiHelper = BaseApiHelper.PbiApiHandler(self.tenant,self.accountKey,self.accountSecret)        
        return None
    
    def getRefreshJson(self):
        jsonCreator = f"""{{ 'type': 'Full' }}        
                        """      
        self.PbiDatasetValueObject.RefreshJson = jsonCreator
        return self
    
    def getSafeAadToken(self):
        tokenHelper = BaseApiHelper.TokenHelper(self.tenant, self.accountKey, self.accountSecret)
        self.PbiDatasetValueObject.Token = tokenHelper.getValidatedAADToken(self.PbiDatasetValueObject.Token)
        print(f"GetSafeToken = {self.PbiDatasetValueObject.Token}")
        return self
    
    def getGroups(self):
        self.PbiDatasetValueObject.ValueGroups = self.pbiApiHelper.getGroup(self.PbiDatasetValueObject.Token, groupName = None)        
        return self

    def getGroup(self, groupName):
        self.PbiDatasetValueObject.ValueGroup = self.pbiApiHelper.getGroup(self.PbiDatasetValueObject.Token, groupName = groupName)
        print(f"GetGroup = {self.PbiDatasetValueObject.ValueGroup}")
        return self
    
    def getDatasets(self):
        self.PbiDatasetValueObject.ValueDatasets = self.pbiApiHelper.getDataset(self.PbiDatasetValueObject.Token, self.PbiDatasetValueObject.ValueGroup, datasetName=None)
        return self

    def getDataset(self, datasetName):
        self.PbiDatasetValueObject.ValueDataset = self.pbiApiHelper.getDataset(self.PbiDatasetValueObject.Token, self.PbiDatasetValueObject.ValueGroup, datasetName=datasetName)
        print(f"GetDataset = {self.PbiDatasetValueObject.ValueDataset}")
        return self
    
    def existingRefresh(self):        
        refreshRunning = self.pbiApiHelper.refreshInProgress(self.PbiDatasetValueObject.Token, self.PbiDatasetValueObject.ValueGroup, self.PbiDatasetValueObject.ValueDataset)
        print(f"Existing refresh running status = {refreshRunning}")
        return refreshRunning

    def xmlaPostRequest(self):
        if(self.PbiDatasetValueObject.RefreshJson == None):
            print("No partitions to refresh. Skipping request to refresh.")
            return self
        print(f"Refreshing GroupId: {self.PbiDatasetValueObject.ValueGroup.id} and DatasetId: {self.PbiDatasetValueObject.ValueDataset.id}")
        self.PbiDatasetValueObject.ApiResponseObject = self.pbiApiHelper.refreshDataset(self.PbiDatasetValueObject.Token, self.PbiDatasetValueObject.ValueGroup, self.PbiDatasetValueObject.ValueDataset, self.PbiDatasetValueObject.RefreshJson)                
        return self
    
    def getUsersInDataset(self):
        self.PbiDatasetValueObject.ValueDatasetUsers = self.pbiApiHelper.getUsersInDataset(self.PbiDatasetValueObject.Token, self.PbiDatasetValueObject.ValueGroup, self.PbiDatasetValueObject.ValueDataset)
        return self
    
    def getRefreshHistory(self):
        self.PbiDatasetValueObject.RefreshHistory = self.pbiApiHelper.getRefreshHistory(self.PbiDatasetValueObject.Token, self.PbiDatasetValueObject.ValueGroup, self.PbiDatasetValueObject.ValueDataset)
        return self

class DbxPbiDatasetWrapper:
    def __init__(self, tenant, accountKey, accountSecret) -> None:
        self.tenant = tenant
        self.accountKey = accountKey
        self.accountSecret = accountSecret
        

    def refreshPbiDataset(self,workspaceName, datasetName):
        builder = PbiDatasetValueBuilder(self.tenant, self.accountKey, self.accountSecret)
        builder = builder.getSafeAadToken()
        builder = builder.getGroupId(workspaceName)
        builder = builder.getDatasetId(datasetName)
        builder = builder.getRefreshJson()
        if(builder.PbiRefresh.RefreshJson == None):
            print("No partitions found for refresh")
            return
        existingRunning = builder.existingRefresh()
        if(existingRunning):
            print("Existing refresh in progress, cannot call a new refresh on dataset.")
            raise Exception("Existing refresh in progress!!. Aborting Task.")

        builder = builder.xmlaPostRequest()
        print(f"API Call completed with following result {builder.PbiRefresh.ApiResponseObject}")
    
    def getDatasetsInWorkspace(self, workspaceName):
        builder = PbiDatasetValueBuilder(self.tenant, self.accountKey, self.accountSecret)
        builder = builder.getSafeAadToken()
        builder = builder.getGroupId(workspaceName)
        builder =  builder.getDataset(workspaceName)
        return builder.PbiDatasetValueObject.ValueDataset
    
    def getAllDatasetsInWorkspace(self, workspaceName):
        builder = PbiDatasetValueBuilder(self.tenant, self.accountKey, self.accountSecret)
        builder = builder.getSafeAadToken()
        builder = builder.getGroupId(workspaceName)
        builder =  builder.getDatasets()
        return builder.PbiDatasetValueObject.ValueDatasets
    
    def getAllWorkspaces(self):
        builder = PbiDatasetValueBuilder(self.tenant, self.accountKey, self.accountSecret)
        builder = builder.getSafeAadToken()
        builder =  builder.getGroups()        
        return builder.PbiDatasetValueObject.ValueGroups
    
    def getUsersInDataset(self, workspaceName, datasetName):
        builder = PbiDatasetValueBuilder(self.tenant, self.accountKey, self.accountSecret)
        builder = builder.getSafeAadToken()
        builder = builder.getGroupId(workspaceName)
        builder = builder.getDatasetId(datasetName)
        builder = builder.getUsersInDataset()
        return builder.PbiDatasetValueObject.ValueDatasetUsers
    
    def getDatasetRefreshHistory(self, workspaceName, datasetName):
        builder = PbiDatasetValueBuilder(self.tenant, self.accountKey, self.accountSecret)
        builder = builder.getSafeAadToken()
        builder = builder.getGroupId(workspaceName)
        builder = builder.getDatasetId(datasetName)
        builder = builder.getRefreshHistory()
        return builder.PbiDatasetValueObject.RefreshHistory