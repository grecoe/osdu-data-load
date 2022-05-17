##########################################################
# Copyright (c) Microsoft Corporation.
##########################################################
import time
import requests

class RetryRequestResponse:
    """
    Holds the information to return to the caller when attempting to make
    a requests.XXX call
    """
    def __init__(self, url, kwargs):
        # Action of call - get/put/etc
        self.action = None
        # URL Provided
        self.url = url
        # Additional arguments, i.e. data, json, headers, etc
        self.kwargs = kwargs
        # Status code if found
        self.status_code = None
        # All status codes
        self.status_codes = []
        # Map of correlation key to code
        self.status_error_map = {}
        # Attempts to get there
        self.attempts = 0
        # Result if any
        self.result = None
        # Error if found
        self.error = None
        # Connection errors
        self.connection_errors = []

    def __str__(self):
        return "ACTION: {}\nURL: {}\nKWARGS: {}\nCODE: {}\nATTEMPTS: {}\nRESULT: {}\nERROR: {}\n".format(
            self.action,
            self.url,
            "Present" if self.kwargs else "Missing",
            self.status_code,
            self.attempts,
            type(self.result),
            str(self.error) if self.error else "None"
        )

class RequestsRetryCommand:
    # Retry count, alter with RequestsRetryCommand.RETRY_MAX = XX 
    RETRY_MAX = 8
    # The range we expect a succesful call and will pull results
    ACCEPT_RANGE = list(range(200,300))
    # The range of server errors we'll retry on as it may be temporary with the OSDU
    # system overwhelmed. During testing at load, some smaller files threw a 404 on 
    # getting a version which would indicate the indexer wasn't fast enough, so allow it
    RETRY_RANGE = [404]
    RETRY_RANGE.extend(list(range(500,600)))
    # Flag to allow connection error retries, default is TRUE because in OSDU
    # When the system has a bunch of requests and existing files, these pop up
    # fairly frequently. But, appears that follow on attempts work. 
    ALLOW_CONNECTION_ERROR_RETRY = True
    # Upon a failure for retry, time we wait to go again.
    ROLL_BACK_SECS = 1.0
    ROLL_BACK_INCREASE = 1.0

    @staticmethod
    def is_success(retry_response:RetryRequestResponse) -> bool:
        """
        Determine if the recieved status code is in the acceptable range. 
        """
        return retry_response.status_code in RequestsRetryCommand.ACCEPT_RANGE

    @staticmethod
    def make_request(fn, url:str, **kwargs) -> RetryRequestResponse:
        """
        Makes a request to the requests library with retry logic. 

        If the call succeeds, as defined with a status code in ACCEPT_RANGE the call result
        is collected. Will try JSON first, but if that fails, gets the text 

        If the call fails and the response code is NOT within RETRY_RANGE, it will not attempt the 
        call again. 

        If the call throws an exception, it will retry for
        RETRY_MAX attempts with a wait of ROLL_BACK_SECONDS in between attempts.   

        If the call fails with a ConnectionError and ALLOW_CONNECTION_ERROR_RETRY is False, then
        only one attempt is made. If ALLOW_CONNECTION_ERROR_RETRY is True then it will retry for
        RETRY_MAX attempts with a wait of ROLL_BACK_SECONDS in between attempts. 
        
        Parameters
        
            fn: A function from requests, i.e. requests.get
            url: URL to hit with the call
            kwargs: Additional requests data, i.e. {headers={}, json={}}

        Returns:
        RetryRequestResponse in all cases except when:
            fn is None or is not a function at all
            fn is not from the requests library
            url is None

            In these cases throws a generic Exception
        """
        if not fn or not callable(fn):
            raise Exception("fn parameter is expected to be a function call")
        elif "requests" not in fn.__module__:
            raise Exception("fn parameter must be in requests library")
        elif not url:
            raise Exception("URL is a required parameter")

        retry_response = RetryRequestResponse(url, kwargs)
        retry_response.action = fn.__name__

        roll_back = RequestsRetryCommand.ROLL_BACK_SECS

        # OSDU can have containers fall asleep/go cold. While it's not a good 
        # idea to allow a retry on a 400, we allow it ONCE and wait to see if 
        # the container gets into a safe state
        HAVE_BAD_REQUEST = False

        while retry_response.attempts < RequestsRetryCommand.RETRY_MAX:
            retry_response.attempts += 1
            retry_response.error = None

            try:
                response = fn(url, **kwargs)
                retry_response.status_code = response.status_code
                retry_response.status_codes.append(response.status_code)

                # If response in acceptable range, use it and get out, if 
                # not in the retry range report it and get out. 

                if response.status_code in RequestsRetryCommand.ACCEPT_RANGE:
                    try:
                        retry_response.result = response.json()
                    except Exception as ex:
                        retry_response.result = response.text
                    # All good, get out
                    break
                elif response.status_code not in RequestsRetryCommand.RETRY_RANGE:
                    if HAVE_BAD_REQUEST:
                        retry_response.error = Exception("Command returned unexpected status code : {}".format(
                            response.status_code
                        ))
                        break
                    # Attempt to overcome the container being cold an non-responsive. Happens
                    # once only so let it sit for a few seconds to come up. Happens specifically
                    # if the system has been idle for some time (overnight) - April 12, 2022
                    time.sleep(5.0)
                    HAVE_BAD_REQUEST = True

                retry_response.error = response.status_code

                if "headers" in kwargs:
                    if "correlation-id" in kwargs["headers"]:
                        retry_response.status_error_map[kwargs["headers"]["correlation-id"]] = response.status_code

            except requests.exceptions.ConnectionError as ex:
                # There is no recovery from this I don't think
                #ConnectionResetError
                #NewConnectionError
                retry_response.error = str(ex)
                retry_response.connection_errors.append("CONN EX: {} {}".format(fn.__name__, url))
                if not RequestsRetryCommand.ALLOW_CONNECTION_ERROR_RETRY:
                    break
            except Exception as ex:
                retry_response.error = str(ex)
                retry_response.connection_errors.append("EX : {} {}".format(fn.__name__, url))
                retry_response.error = str(ex)

            # We didn't get a fatal nor a success, let system recover for retry
            time.sleep(roll_back)
            # For each time we come here increase to see if it helps
            roll_back += RequestsRetryCommand.ROLL_BACK_INCREASE

            if retry_response.attempts >= RequestsRetryCommand.RETRY_MAX:
                retry_response.error = "Retry maximum hit at {}".format(retry_response.attempts)

        return retry_response
