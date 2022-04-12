# Python seems to be included in this image, but having two FROM lines
# side by side seemed to be an issue. 

FROM python:3.9

RUN mkdir /app
WORKDIR /app
ADD requirements.txt /app/
RUN pip install -r requirements.txt
ADD . /app/

ENV AZURE_TENANT="YOUR_TENANT"
ENV SHARE_MOUNT="FILE_MOUNT_LOCATION"
ENV EXPERIENCE_CLIENT="YOUR_LAB_APPID"
ENV EXPERIENCE_CRED="YOUR_APPID_SECRET"
ENV ENERGY_PLATFORM="YOUR_DEPLOY_NAME"

ENTRYPOINT [ "python" ]
CMD [ "load.py" ]