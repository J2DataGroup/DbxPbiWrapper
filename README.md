# DbxPbiApiWrapper

## Overview
`DbxPbiApiWrapper` is a Python-based wrapper designed to simplify interactions with the Power BI API. It provides a set of classes and methods for handling common tasks such as dataset management, refresh operations, and user access management. This library is optimized for integration with Databricks, making it ideal for seamless data analytics workflows.

## Features
- **Azure AD Token Management**: Automated handling of Azure Active Directory (AAD) token validation.
- **Dataset Operations**: Retrieve, list, and manage datasets within specified Power BI workspaces.
- **Refresh Functionality**: Easily trigger and monitor dataset refreshes, with built-in checks for ongoing operations.
- **User Access Management**: Fetch detailed user access information for specific datasets.
- **Refresh History**: Retrieve the history of dataset refresh operations.

## Prerequisites
- Python 3.8 or higher
- Microsoft Power BI Service account with the following API permissions:
  - `Dataset.Read.All`
  - `Dataset.Write.All`
  - `Group.Read.All`
- Azure AD App registration configured with the necessary permissions.

## Installation
To install the package, clone the repository and use:

```bash
pip install DbxPbiApiWrapper
```
## Usage
### Initialization
Initialize the `DbxPbiDatasetWrapper` class with your Azure AD tenant details and credentials.

```bash
from DbxPbiWrapper import DbxPbiDatasetWrapper

# Define your credentials
tenant = "your-tenant-id"
account_key = "your-account-key"
account_secret = "your-account-secret"

# Initialize the wrapper
pbi_wrapper = DbxPbiDatasetWrapper(tenant, account_key, account_secret)
```
### Example 1: Refresh a Power BI Dataset
Trigger a refresh for a specific dataset in a workspace:
```python
workspace_name = "SalesWorkspace"
dataset_name = "SalesDataset"
try:
    *Trigger dataset refresh*    pbi_wrapper.refreshPbiDataset(workspace_name, dataset_name)
    print("Dataset refresh initiated successfully.")
except Exception as e:
    print(f"Failed to refresh dataset: {str(e)}")
```
### Example 2: List All Datasets in a Workspace
Retrieve all datasets in a specified workspace:
```python
workspace_name = "MarketingWorkspace"
# Fetch and display datasets
datasets = pbi_wrapper.getAllDatasetsInWorkspace(workspace_name)
if datasets:
    for dataset in datasets:
        print(f"Dataset Name: {dataset['name']}, Dataset ID: {dataset['id']}")
else:
    print("No datasets found in the workspace.")

```
### Example 3: Get Users with Access to a Dataset
Fetch a list of users who have access to a specific dataset:
```python
workspace_name = "FinanceWorkspace"
dataset_name = "RevenueDataset"

# Retrieve user access details
users = pbi_wrapper.getUsersInDataset(workspace_name, dataset_name)
if users:
    for user in users:
        print(f"User: {user['displayName']}, Role: {user['datasetUserAccessRight']}")
else:
    print("No users found with access to this dataset.")

```
## Class Descriptions
### DbxPbiDatasetWrapper
Main interface for interacting with Power BI services:

* `refreshPbiDataset(workspaceName, datasetName)`: Initiates a dataset refresh.
* `getAllDatasetsInWorkspace(workspaceName)`: Lists all datasets in a workspace.
* `getAllWorkspaces()`: Retrieves a list of all available workspaces.
* `getUsersInDataset(workspaceName, datasetName)`: Fetches users with access to a dataset.
* `getDatasetRefreshHistory(workspaceName, datasetName)`: Returns the refresh history of a dataset.

## Error Handling
The library includes basic error handling for common issues like:
* Invalid Azure AD credentials
* Insufficient API permissions
* Ongoing dataset refresh preventing new refresh requests
* It is recommended to implement proper exception handling when integrating this library into your projects.

## Contribution
We welcome contributions! Please follow these steps:

1. Fork the repository.
2. Create a new branch (feature/your-feature-name).
3. Submit a pull request for review.

## Contact
For support or questions, please contact the author at info@j2datagroup.com



