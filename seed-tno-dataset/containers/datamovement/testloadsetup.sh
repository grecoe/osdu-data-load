#!/bin/bash

##########################################################
# Test script to launch the actual container script.
##########################################################

# These would be environment variables set by docker launch itself.
export SUBSCRIPTION="YOUR_SUB_ID"
export RESOURCE_GROUP="YOUR_RG_NAME_EXISTS"
export VALIDATION_IMAGE="IMAGE_THAT_VALIDATES_MOUNT"

./loaddataset.sh