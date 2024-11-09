from BaseValueObjects import *
from BaseApiHelper import *

class PbiValueObject:
    def __init__(self):
        self.Token = None
        self.ValueGroup = None
        self.ValueGroups = None
        self.ValueDataset = None
        self.ValueDatasets = None
        self.ApiResponseObject = None
        self.RefreshStatus = None        
        self.RefreshJson = None   
        self.ValueDatasetUsers = None     
        
class IPbiValueBuilder(metaclass= ABCMeta):
    
    @property
    @abstractmethod
    def getSafeAadToken(self) -> None:
        pass  
    
    @property
    @abstractmethod
    def getGroupId(self, groupName) -> None:
        pass
    
    @property
    @abstractmethod
    def getDatasetId(self, datasetName) -> None:
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
    def getDatasets(self) -> None:
        pass     
    
    @property
    def getUsersInDataset(self, groupName, datasetName) -> None:
        pass

class PbiValueBuilder(IPbiValueBuilder):
    def __init__(self, tenant, accountKey, accountSecret):
        self.PbiValueObject = PbiValueObject()
        self.accountKey = accountKey
        self.accountSecret = accountSecret
        self.tenant = tenant
        self.pbiApiHelper = BaseApiHelper.PbiApiHandler(self.tenant,self.accountKey,self.accountSecret)        
        return None
    
    def getRefreshJson(self):
        jsonCreator = f"""{{ 'type': 'Full' }}        
                        """      
        self.PbiValueObject.RefreshJson = jsonCreator
        return self
    
    def getSafeAadToken(self):
        tokenHelper = BaseApiHelper.TokenHelper(self.tenant, self.accountKey, self.accountSecret)
        self.PbiValueObject.Token = tokenHelper.getValidatedAADToken(self.PbiValueObject.Token)
        print(f"GetSafeToken = {self.PbiValueObject.Token}")
        return self
    
    def getGroups(self):
        self.PbiValueObject.ValueGroups = self.pbiApiHelper.getGroup(self.PbiValueObject.Token, groupName = None)        
        return self

    def getGroupId(self, groupName):
        self.PbiValueObject.ValueGroup = self.pbiApiHelper.getGroup(self.PbiValueObject.Token, groupName = groupName)
        print(f"GetGroup = {self.PbiValueObject.ValueGroup}")
        return self
    
    def getDatasets(self):
        self.PbiValueObject.ValueDatasets = self.pbiApiHelper.getDataset(self.PbiValueObject.Token, self.PbiValueObject.ValueGroup, datasetName=None)
        return self

    def getDatasetId(self, datasetName):
        self.PbiValueObject.ValueDataset = self.pbiApiHelper.getDataset(self.PbiValueObject.Token, self.PbiValueObject.ValueGroup, datasetName=datasetName)
        print(f"GetDataset = {self.PbiValueObject.ValueDataset}")
        return self
    
    def existingRefresh(self):        
        refreshRunning = self.pbiApiHelper.refreshInProgress(self.PbiValueObject.Token, self.PbiValueObject.ValueGroup, self.PbiValueObject.ValueDataset)
        print(f"Existing refresh running status = {refreshRunning}")
        return refreshRunning

    def xmlaPostRequest(self):
        if(self.PbiValueObject.RefreshJson == None):
            print("No partitions to refresh. Skipping request to refresh.")
            return self
        print(f"Refreshing GroupId: {self.PbiValueObject.ValueGroup.id} and DatasetId: {self.PbiValueObject.ValueDataset.id}")
        self.PbiValueObject.ApiResponseObject = self.pbiApiHelper.refreshDataset(self.PbiValueObject.Token, self.PbiValueObject.ValueGroup, self.PbiValueObject.ValueDataset, self.PbiValueObject.RefreshJson)                
        return self
    
    def getUsersInDataset(self):
        self.PbiValueObject.ValueDatasetUsers = self.pbiApiHelper.getUsersInDataset(self.PbiValueObject.Token, self.PbiValueObject.ValueGroup, self.PbiValueObject.ValueDataset)
        return self

class DbxPbiWrapper:
    def __init__(self, tenant, accountKey, accountSecret) -> None:
        self.tenant = tenant
        self.accountKey = accountKey
        self.accountSecret = accountSecret
        

    def refreshPbiDataset(self,workspaceName, datasetName):
        builder = PbiValueBuilder(self.tenant, self.accountKey, self.accountSecret)
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
    
    def getAllDatasetsInWorkspace(self, workspaceName):
        builder = PbiValueBuilder(self.tenant, self.accountKey, self.accountSecret)
        builder = builder.getSafeAadToken()
        builder = builder.getGroupId(workspaceName)
        builder =  builder.getDatasets()
        return builder.PbiValueObject.ValueDatasets
    
    def getAllWorkspaces(self):
        builder = PbiValueBuilder(self.tenant, self.accountKey, self.accountSecret)
        builder = builder.getSafeAadToken()
        builder =  builder.getGroups()        
        return builder.PbiValueObject.ValueGroups
    
    def getUsersInDataset(self, workspaceName, datasetName):
        builder = PbiValueBuilder(self.tenant, self.accountKey, self.accountSecret)
        builder = builder.getSafeAadToken()
        builder = builder.getGroupId(workspaceName)
        builder = builder.getDatasetId(datasetName)
        builder = builder.getUsersInDataset()
        return builder.PbiValueObject.ValueDatasetUsers