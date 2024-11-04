# Databricks notebook source
# MAGIC %run ./BaseXmlaRefresh

# COMMAND ----------

class ReservesFactXmlaRefresh(BaseApiHelper):
    """Fact refresh implementation of keeper of all base calls for making calls to PBI."""
    def __init__(self):
        super().__init__()        

    class TokenHelper(BaseApiHelper.TokenHelper):    
        def __init__(self):
            super().__init__()
            
        def getAADToken(self, isManagementScope):
            return super().getAADToken(isManagementScope)
        
        def getValidatedAADToken(self, token:TokenRequestObject, isManagementScope):
            return super().getValidatedAADToken(token, isManagementScope)

    class PbiApiHandler(BaseApiHelper.PbiApiHandler):
        def __init__(self):
            super().__init__()
            
        def getWorkspaceName(self):
            return super().getWorkspaceName()
        
        def getGroup(self,tokenObject: TokenResponseObject):
            return super().getGroup(tokenObject)
        
        def getDataset(self, tokenObject:TokenResponseObject, group:ValueGroup):
            return super().getDataset(tokenObject, group)
        
        def refreshDataset(self, tokenObject:TokenResponseObject, group:ValueGroup, dataset: ValueDataset, payloadData):
            return super().refreshDataset(tokenObject, group, dataset, payloadData)
        
        def refreshStatus(self, tokenObject:TokenResponseObject, group:ValueGroup, dataset: ValueDataset, apiresponse: ApiResponse):
            return super().refreshStatus(tokenObject, group, dataset, apiresponse)
            
    class PbiXmlApiHandler(BaseApiHelper.PbiXmlApiHandler):
        def __init__(self):
            super().__init__()

        def getWorkspaceName(self):
            return super().getWorkspaceName()
        
        def submitPayloadJob(self, tokenObject:TokenResponseObject, payloadData, description):
            return super().submitPayloadJob(tokenObject, payloadData, description)  

        def payloadJobStatus(self, tokenObject:TokenResponseObject,jobId):
            return super().payloadJobStatus(tokenObject, jobId)  

# COMMAND ----------

class ReservesFactXmlaRefreshGenerator:
    def __init__(self):
        self.relational_table_name = 'reserves.fact_reservesvolume'
        self.host_name       = spark.conf.get("spark.databricks.workspaceUrl")
        self.cluster_id      = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        self.host_name_parts = self.host_name.split('.')
        self.url_id          = self.host_name_parts[0].split('-')[1]
        self.http_path       = f"sql/protocolv1/o/{self.url_id}/{self.cluster_id}"
        self.schema_name     = self.relational_table_name.split('.')[0]
        self.table_name      = self.relational_table_name.split('.')[1]
        self.partitionColumn = "ForecastID"

    def setPartitionRefreshCompletion(self, jobId):
        spark.sql(f"update reserves.reservesincremental set CubeProcessDate = current_timestamp() where JobId = {jobId} and CubeProcessDate is null")
        print("Creation of new partition marked as completed.")

    def GetReservesXmlaRefreshQuery(self, jobId):
        reservesIncremental_df = spark.sql(f"select * from reserves.reservesincremental where JobId = {jobId} and CubeProcessDate is null")
        partition_count = reservesIncremental_df.count()
        if(partition_count is None or partition_count < 1):
            print("No partitions to refresh.")
            return None
        else:
            print("Found partitions to refresh.")
            tmslJson = f"""{{
                            "refresh": {{
                            "type": "Full",
                            "objects": [
                                {{
                                "database": "Reserves",
                                "table": "FACT Reserves Present Value"
                                }},                                  
                                {{
                                "database": "Reserves",
                                "table": "FACT Reserves Capital"
                                }},        
                        """
            
            partition_list = reservesIncremental_df.select(self.partitionColumn).distinct().collect()
            for row in partition_list:
                vintageIdToRefresh = row[self.partitionColumn]                
                tmslJson += f"""
                            {{
                                'database': 'Reserves',
                                'table': 'FACT Reserves Reconciliation',
                                'partition': 'FACT Reserves Reconciliation_{vintageIdToRefresh}'
                            }},
                """ 
            for row in partition_list:
                vintageIdToRefresh = row[self.partitionColumn]                
                tmslJson += f"""
                            {{
                                'database': 'Reserves',
                                'table': 'FACT Reserves',
                                'partition': 'FACT Reserves Volume_{vintageIdToRefresh}'
                            }},
                """ 
            tmslJson = tmslJson[:-1] +  f"""
            ]
            }}}}"""
        return tmslJson

# COMMAND ----------

class FactRefreshXmla:
    def __init__(self):
        self.Token = None
        self.ValueGroup = None
        self.ValueDataset = None
        self.ApiResponseObject = None
        self.FactRefreshJson = None      
        self.RefreshStatus = None
        self.XmlaJobId = None
        self.JobStatus = None
        
class IReservesXmlaRefreshBuilder(metaclass= ABCMeta):
    
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
    def setXmlaRefreshComplete(self) -> None:
        pass

    @property
    @abstractmethod
    def existingRefresh(self) -> None:
        pass

    @property
    @abstractmethod
    def submitPayloadJob(self) -> None:
        pass

    @property
    @abstractmethod
    def getJobstatus(self) -> None:
        pass

class ReservesXmlaRefreshBuilder(IReservesXmlaRefreshBuilder):
    def __init__(self,  jobId, jobDescription):
        self.FactRefreshXmla = FactRefreshXmla()
                
        self.pbiApiHelper = ReservesFactXmlaRefresh.PbiApiHandler()        
        self.jsonCreator = ReservesFactXmlaRefreshGenerator()
        self.jobId = jobId
        self.pbiXmlApiHelper = ReservesFactXmlaRefresh.PbiXmlApiHandler()    
        self.jobDescription = jobDescription

    def getXmlaJson(self):        
        self.FactRefreshXmla.FactRefreshJson = self.jsonCreator.GetReservesXmlaRefreshQuery(self.jobId)
        print(f"GetXmla = {self.FactRefreshXmla.FactRefreshJson}")
        return self
    
    def setRefreshComplete(self):
        if(self.FactRefreshXmla.FactRefreshJson == None):
            print("No partitions to refresh. Skip updating tables.")
            return self
        fetched_status = self.FactRefreshXmla.RefreshStatus.status
        #availableStatus = ["Unknown", "Completed","Failed","Disabled"]
        if(fetched_status == "Completed"):
            self.jsonCreator.setPartitionRefreshCompletion(self.jobId)
            print(f"Got Status as Completed. Updating CubeProcessDate for {self.jobId}")
        else:
            print("Paritions refresh failed. Will not mark partitions as refreshed.")
    
    def getSafeAadToken(self):
        tokenHelper = ReservesFactXmlaRefresh.TokenHelper()
        self.FactRefreshXmla.Token = tokenHelper.getValidatedAADToken(self.FactRefreshXmla.Token)
        print(f"GetSafeToken = {self.FactRefreshXmla}")
        return self
    
    def getSafeManagementScopeAadToken(self):
        tokenHelper = ReservesFactXmlaRefresh.TokenHelper()
        self.FactRefreshXmla.Token = tokenHelper.getValidatedAADToken(self.FactRefreshXmla.Token, True)
        print(f"GetSafeToken = {self.FactRefreshXmla}")
        return self

    def getGroupId(self):
        self.FactRefreshXmla.ValueGroup = self.pbiApiHelper.getGroup(self.FactRefreshXmla.Token)
        print(f"GetGroup = {self.FactRefreshXmla}")
        return self
    
    def getDatasetId(self):
        self.FactRefreshXmla.ValueDataset = self.pbiApiHelper.getDataset(self.FactRefreshXmla.Token, self.FactRefreshXmla.ValueGroup)
        print(f"GetDataset = {self.FactRefreshXmla}")
        return self
    
    def existingRefresh(self):
        if(self.FactRefreshXmla.FactRefreshJson == None):
            print("No partitions to refresh. Skipping request to refresh.")
            return self
        refreshRunning = self.pbiApiHelper.refreshInProgress(self.FactRefreshXmla.Token, self.FactRefreshXmla.ValueGroup, self.FactRefreshXmla.ValueDataset)
        print(f"Existing refresh running status = {refreshRunning}")
        return refreshRunning

    def xmlaPostRequest(self):
        if(self.FactRefreshXmla.FactRefreshJson == None):
            print("No partitions to refresh. Skipping request to refresh.")
            return self
        self.FactRefreshXmla.ApiResponseObject = self.pbiApiHelper.refreshDataset(self.FactRefreshXmla.Token, self.FactRefreshXmla.ValueGroup, self.FactRefreshXmla.ValueDataset, self.FactRefreshXmla.FactRefreshJson)        
        print(f"PostRequest = {self.FactRefreshXmla}")
        return self
    
    def submitPayloadJob(self):
        self.FactRefreshXmla.XmlaJobId = self.pbiXmlApiHelper.submitPayloadJob(self.FactRefreshXmla.Token,self.FactRefreshXmla.FactRefreshJson, self.jobDescription)        
        print(f"PostRequest = {self.FactRefreshXmla}")
        return self

    def getJobstatus(self):
        time.sleep(10)
        xmlaJobId = self.FactRefreshXmla.XmlaJobId 
        currentStatus = "Initialized"
        maxTime = time.time() + 60 * 30
        while time.time() < maxTime:   
            print(f"Refreshing...")
            currentStatus =  self.pbiXmlApiHelper.payloadJobStatus(self.FactRefreshXmla.Token,xmlaJobId)
            print(currentStatus)
            if("200:Success" in currentStatus or "417:Failed" in currentStatus or "error" in currentStatus):
                break
            time.sleep(30)
        self.FactRefreshXmla.JobStatus = currentStatus
        if("417:Failed" in currentStatus or "error" in currentStatus):
            print(currentStatus)
            raise Exception(f"Refresh Failed. Check AA Runbook for details.")
        if("200:Success" in currentStatus):
            print(currentStatus)
            print("Refresh was success.")
        return self

    def setXmlaRefreshComplete(self):
        currentStatus = self.FactRefreshXmla.JobStatus
        if("417:Failed" in currentStatus or "error" in currentStatus):
            print("Refresh failed. Status not set to complete.")
        if("200:Success" in currentStatus):
            generator = ReservesFactXmlaRefreshGenerator()
            generator.setPartitionRefreshCompletion(self.jobId)
            print("Refresh status set to complete.") 
            
        return self

    def getRequestStatus(self):
        x = 0
        if(self.FactRefreshXmla.FactRefreshJson == None):
            print("No Partitions to refresh. Skipping refresh status call")
            return self
        if(self.FactRefreshXmla.ApiResponseObject == None):
            print("Refresh request has failed. Will skip refresh status call.")
            return self
        status = "Unknown"       
        # Try with backoff retries
        maxTime = time.time() + 60 * 30
        while time.time() < maxTime:   
            time.sleep(7)
            print(f"status is {status}")            
            self.FactRefreshXmla.RefreshStatus =  self.pbiApiHelper.refreshStatus(self.FactRefreshXmla.Token,\
                                                  self.FactRefreshXmla.ValueGroup, self.FactRefreshXmla.ValueDataset, self.FactRefreshXmla.ApiResponseObject)
            fetched_status = self.FactRefreshXmla.RefreshStatus.status
            print(f"Refresh status is : {fetched_status}")
            if(fetched_status != status):
                break
            time.sleep(30)
            print(f"GetStatus = {self.FactRefreshXmla}")
            #availableStatus = ["Unknown", "Completed","Failed","Disabled"]
            if(fetched_status == "Failed" or fetched_status == "Disabled"):
                raise Exception(f"Refresh Status is {fetched_status}")

        return self
    
    def Results(self):
        return self.FactRefreshXmla
        

class ReservesRefreshDirector:
    
    @staticmethod
    def refreshReservesPartitions( jobId):
        builder = ReservesXmlaRefreshBuilder( jobId)
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
                  .getRequestStatus()\
                  .setRefreshComplete()\
                  .Results()     

    def performXmlaRefresh( jobId, description):
        return    ReservesXmlaRefreshBuilder(jobId, description)\
                  .getSafeManagementScopeAadToken()\
                  .getXmlaJson()\
                  .submitPayloadJob()\
                  .getJobstatus()\
                  .setXmlaRefreshComplete()\
                  .Results()

# COMMAND ----------

## TESTING Xmla GENERATION
#helper =  ReservesFactXmlaRefreshGenerator()
#xmla = helper.GetReservesXmlaRefreshQuery(481521341042750)
#print(xmla)

# COMMAND ----------

######TESTING FULL CLASS

#import traceback
#environment_name = "DEV"
#jobId = 481521341042750
#try:
#   factXmlaResultObject = ReservesRefreshDirector.performXmlaRefresh(jobId,"Refresh Reserves FACT partitions.")    
#   print(factXmlaResultObject)
#except Exception as ex:
#   print(f"Refresh Failed. Check PBI / Log for more details. {traceback.print_exc()}")    

# COMMAND ----------

###TESTING - SELECT ONLY ONE PARTITION
#spark.sql("update reserves.reservesincremental set CubeProcessDate=current_timestamp() where ForecastID not in (0)")
