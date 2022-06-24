#!/bin/bash

# Variable requirements from user.
export DATA_SOURCE_SUBCRIPTION="YOUR_SUB_ID_WITH_STORAGE"
export DATA_SOURCE_RESOURCE_GROUP="YOUR_RG_NAME_STORAGE"
export DATA_SOURCE_ACCOUNT="tnodataset6830"
export DATA_SOURCE_ACCOUNT_SHARE="tnodataset"

# Location of data in source, format:
# path:extension||path:extension.....

# Pattern, it will scan all files in the filter. However, you can run once to filter for a larger
# number of records to get it into the tables, then reduce it to a small number as it will still
# scan the table for un-processed records and pick them up. 

# 8
export DATA_SOURCE_MAP="datasets/documents:pdf"

# 16
#export DATA_SOURCE_MAP="datasets/documents:pdf||datasets/test:log"

# 939
# export DATA_SOURCE_MAP="datasets/documents:pdf||datasets/documents:doc||datasets/well-logs:las"

# 5904
#export DATA_SOURCE_MAP="datasets/markers:csv"

export EXPERIENCE_LAB_SUBSCRIPTION="YOUR_LAB_SUB_ID"
export EXPERIENCE_LAB_RESOURCE_GROUP="YOUR_LAB_RG_NAME"