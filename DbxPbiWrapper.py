from BaseValueObjects import *
from BaseApiHelper import *

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
    def getXmlaJson(self) -> None:
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
    def getRequestStatus(self) -> None:
        pass

    @property
    @abstractmethod
    def setRefreshComplete(self) -> None:
        pass    

    @property
    @abstractmethod
    def existingRefresh(self) -> None:
        pass    
    

class PbiRefreshBuilder(IPbiRefreshBuilder):
    def __init__(self, tenant, accountKey, accountSecret):
        self.PbiRefresh = PbiRefresh()
        self.accountKey = accountKey
        self.accountSecret = accountSecret
        self.tenant = tenant
        self.pbiApiHelper = BaseApiHelper()        
        self.jsonCreator = f"""{{                            
                            "type": "Full",
                                }}        
                        """  
        self.PbiRefresh.RefreshJson = self.jsonCreator
        print(f"RefreshJson = {self.PbiRefresh.RefreshJson}")
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
        self.PbiRefresh.ApiResponseObject = self.pbiApiHelper.refreshDataset(self.pbiApiHelper.Token, self.pbiApiHelper.ValueGroup, self.pbiApiHelper.ValueDataset, self.pbiApiHelper.FactRefreshJson)        
        print(f"PostRequest = {self.PbiRefresh.ApiResponseObject}")
        return self
    
    def Results(self):
        return self.PbiRefresh.ApiResponseObject
        

class DbxPbiWrapper:
    def __init__(self, tenant, accountKey, accountSecret) -> None:
        self.tenant = tenant
        self.accountKey = accountKey
        self.accountSecret = accountSecret

    def refreshPbiDataset(self):
        builder = PbiRefreshBuilder(self.tenant, self.accountkey, self.accountsecret)
        builder = builder.getSafeAadToken()
        builder = builder.getGroupId()
        builder = builder.getDatasetId()
        builder = builder.getXmlaJson()
        if(builder.FactRefreshXmla.FactRefreshJson == None):
            print("No partitions found for refresh")
            return
        existingRunning = builder.existingRefresh()
        if(existingRunning):
            print("Existing refresh in progress, cannot call a new refresh on dataset.")
            raise Exception("Existing refresh in progress!!. Aborting Task.")

        return    builder\
                  .xmlaPostRequest()\
                  .Results()     