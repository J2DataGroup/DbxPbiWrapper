from DbxPbiApiWrapper.BaseValueObjects import *
from DbxPbiApiWrapper.BaseApiHelper import *

class PbiRefresh:
    def __init__(self):
        self.Token = None
        self.ValueGroup = None
        self.ValueDataset = None
        self.ApiResponseObject = None
        self.RefreshStatus = None        
        self.RefreshJson = None        
        
class IPbiRefreshBuilder(metaclass= ABCMeta):
    
    @property
    @abstractmethod
    def getSafeAadToken(self) -> None:
        pass  
    
    @property
    @abstractmethod
    def getGroupId(self) -> None:
        pass
    
    @property
    @abstractmethod
    def getDatasetId(self) -> None:
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
    

class PbiRefreshBuilder(IPbiRefreshBuilder):
    def __init__(self, tenant, accountKey, accountSecret, workspaceName, datasetName):
        self.PbiRefresh = PbiRefresh()
        self.accountKey = accountKey
        self.accountSecret = accountSecret
        self.tenant = tenant
        self.workspaceName = workspaceName
        self.datasetName = datasetName
        self.pbiApiHelper = BaseApiHelper.PbiApiHandler(self.tenant,self.accountKey,self.accountSecret,self.workspaceName, self.datasetName)        
        return None
    
    def getRefreshJson(self):
        jsonCreator = f"""{{ 'type': 'Full' }}        
                        """      
        self.PbiRefresh.RefreshJson = jsonCreator
        return self
    
    def getSafeAadToken(self):
        tokenHelper = BaseApiHelper.TokenHelper(self.tenant, self.accountKey, self.accountSecret)
        self.PbiRefresh.Token = tokenHelper.getValidatedAADToken(self.PbiRefresh.Token)
        print(f"GetSafeToken = {self.PbiRefresh.Token}")
        return self

    def getGroupId(self):
        self.PbiRefresh.ValueGroup = self.pbiApiHelper.getGroup(self.PbiRefresh.Token)
        print(f"GetGroup = {self.PbiRefresh.ValueGroup}")
        return self
    
    def getDatasetId(self):
        self.PbiRefresh.ValueDataset = self.pbiApiHelper.getDataset(self.PbiRefresh.Token, self.PbiRefresh.ValueGroup)
        print(f"GetDataset = {self.PbiRefresh.ValueDataset}")
        return self
    
    def existingRefresh(self):        
        refreshRunning = self.pbiApiHelper.refreshInProgress(self.PbiRefresh.Token, self.PbiRefresh.ValueGroup, self.PbiRefresh.ValueDataset)
        print(f"Existing refresh running status = {refreshRunning}")
        return refreshRunning

    def xmlaPostRequest(self):
        if(self.PbiRefresh.RefreshJson == None):
            print("No partitions to refresh. Skipping request to refresh.")
            return self
        print(f"Refreshing GroupId: {self.PbiRefresh.ValueGroup.id} and DatasetId: {self.PbiRefresh.ValueDataset.id}")
        self.PbiRefresh.ApiResponseObject = self.pbiApiHelper.refreshDataset(self.PbiRefresh.Token, self.PbiRefresh.ValueGroup, self.PbiRefresh.ValueDataset, self.PbiRefresh.RefreshJson)                
        return self

class DbxPbiWrapper:
    def __init__(self, tenant, accountKey, accountSecret, workspaceName, datasetName) -> None:
        self.tenant = tenant
        self.accountKey = accountKey
        self.accountSecret = accountSecret
        self.workspaceName = workspaceName
        self.datasetName = datasetName

    def refreshPbiDataset(self):
        builder = PbiRefreshBuilder(self.tenant, self.accountKey, self.accountSecret, self.workspaceName, self.datasetName)
        builder = builder.getSafeAadToken()
        builder = builder.getGroupId()
        builder = builder.getDatasetId()
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
    
