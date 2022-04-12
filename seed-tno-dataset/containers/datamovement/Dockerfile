# Python seems to be included in this image, but having two FROM lines
# side by side seemed to be an issue. 

FROM mcr.microsoft.com/azure-cli:latest

RUN mkdir /app
WORKDIR /app
ADD . /app/

# Environment variables to set when launching either using docker -e or with az container
# https://docs.microsoft.com/en-us/azure/container-instances/container-instances-environment-variables
ENV SUBSCRIPTION="YOUR_SUB_ID"
ENV RESOURCE_GROUP="EXPERIENCE_LAB_RG"
ENV VALIDATION_IMAGE="VALIDATION_IMAGE"

CMD ["bash", "loaddataset.sh"]