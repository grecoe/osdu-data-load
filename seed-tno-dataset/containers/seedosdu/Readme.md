# Seed OSDU 

This container is connected with a file share and reads all of the files within that share and loads them into the OSDU instance. 

Files are selected using the FileClass class

Each of them get the same metadata associated, but now we have them by folder, we can then determine if they need to be modified/udpated before being pushed and even modify the contents of the metadata based on what is being sent. 

Right now metadata is:

utils/requests/metagenerator.py