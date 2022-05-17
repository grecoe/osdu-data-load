##########################################################
# Copyright (c) Microsoft Corporation.
##########################################################
import os

class MetadataGenerator:
    """
    Generic OSDU metadata generator. 
    """

    @staticmethod
    def generate_metadata(aclViewer:str, aclOwner:str, legalTag:str, fileName:str) -> dict:
        return {
            "kind": "osdu:wks:dataset--File.Generic:1.0.0",
            "acl": {
                "viewers": [
                    aclViewer
                ],
                "owners": [
                    aclOwner
                ]
            },
            "legal": {
                "legaltags": [
                    legalTag
                ],
                "otherRelevantDataCountries": [
                    "US"
                ],
                "status": "compliant"
            },
            "data": {
                "DatasetProperties": {
                    "FileSourceInfo": {
                        "FileSource": "||UPLOAD_URL||",
                        "Name" : os.path.split(fileName)[-1]
                    }
                }
            }
        }
