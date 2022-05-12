#!/bin/bash

# Variable requirements from user.
export DATA_SOURCE_SUBCRIPTION="AZURE_SUB_WITH_DATASET_STORAGE"
export DATA_SOURCE_RESOURCE_GROUP="RG_OF_DATASET_STORAGE"
export DATA_SOURCE_ACCOUNT="tnodataset6830"
export DATA_SOURCE_ACCOUNT_SHARE="tnodataset"

# Location of data in source, format:
# path:extension||path:extension.....
export DATA_SOURCE_MAP="datasets/documents:pdf||datasets/documents:doc"

export EXPERIENCE_LAB_SUBSCRIPTION="AZURE_SUB_OF_OSDU_DEPLOYMENT"
export EXPERIENCE_LAB_RESOURCE_GROUP="AZURE_RG_OF_OSDU_DEPLOYMENT" 

