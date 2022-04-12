import os
from utils.configuration.config import Config
from utils.requests.file import UploadUrl

class MetadataGenerator:

    @staticmethod
    def generate_metadata(config:Config, url:UploadUrl, fileName:str) -> dict:
        return {
            "kind": "osdu:wks:dataset--File.Generic:1.0.0",
            "acl": {
                "viewers": [
                    config.aclViewer
                ],
                "owners": [
                    config.aclOwner
                ]
            },
            "legal": {
                "legaltags": [
                    config.legalTag
                ],
                "otherRelevantDataCountries": [
                    "US"
                ],
                "status": "compliant"
            },
            "data": {
                "DatasetProperties": {
                    "FileSourceInfo": {
                        "FileSource": url.FileSource,
                        "Name" : os.path.split(fileName)[-1]
                    }
                }
            }
        }
