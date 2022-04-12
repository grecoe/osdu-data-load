#!/bin/bash

##########################################################
# Test script to launch the actual container script.
##########################################################

# These would be environment variables set by docker launch itself.
export AZURE_TENANT="YOUR_TENANT"
export SHARE_MOUNT="FILE_MOUNT_LOCATION"
export EXPERIENCE_CLIENT="YOUR_LAB_APP_ID"
export EXPERIENCE_CRED="YOUR_APP_SECRET"
export ENERGY_PLATFORM="YOUR_PLATFORM_NAME"

python ./load.py