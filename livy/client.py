import http
import http.client
import json
import logging
import socket
import ssl
import sys
import typing
import urllib.parse
from typing import Union, Optional, List, Dict

import livy
from livy.exception import OperationError, RequestError, TypeError as _TypeError

if typing.TYPE_CHECKING:
    import http.client

__all__ = ["LivyClient"]


if sys.version_info < (3, 8):  # pragma: no cover
    Batch = typing.TypeVar("Batch", bound=dict)
else:  # pragma: no cover

    class Batch(typing.TypedDict):
        id: int
        appId: str
        appInfo: Dict[str, str]
        log: List[str]
        state: str


logger = logging.getLogger(__name__)


class LivyClient:
    """Client that wraps requests to Livy server

    This implementation follows livy API v0.7.0 spec.
    """

    def __init__(
        self,
        url: str,
        verify: Union[bool, ssl.SSLContext] = True,
        timeout: float = 30.0,
    ) -> None:
        """
        Parameters
        ----------
            url : str
                URL to the livy server
            verify : Union[bool, ssl.SSLContext]
                Verifies SSL certificates or not; or use customized SSL context
            timeout : float
                Timeout seconds for the connection.

        Raises
        ------
        TypeError
            On a invalid data type is used for inputted argument
        OperationError
            On URL scheme is not supportted

        Note
        ----
        This package is designed to use lesser third-party libraries, thus it
        does not use high-level interface to handling the requests. And it
        results with limitations that we could not use such rich features as
        `requests`.
        """
        # URL
        if not isinstance(url, str):
            raise _TypeError("url", str, url)

        purl = urllib.parse.urlsplit(url)
        self._prefix = purl.path.rstrip("/")
        self.host = purl.hostname.lower()

        # SSL verify
        if not isinstance(verify, (bool, ssl.SSLContext)):
            raise _TypeError("verify", (bool, ssl.SSLContext), verify)
        if isinstance(verify, ssl.SSLContext):
            ssl_context = verify
        elif verify == True:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        else:
            ssl_context = None

        # client
        scheme = purl.scheme.upper()
        if scheme == "HTTP":
            self._client = http.client.HTTPConnection(
                host=purl.hostname, port=purl.port, timeout=timeout
            )
        elif scheme == "HTTPS":
            self._client = http.client.HTTPSConnection(
                host=purl.hostname, port=purl.port, timeout=timeout, context=ssl_context
            )
        else:
            raise OperationError(f"Unsupported scheme: {scheme}")

    def __repr__(self) -> str:
        return f"<LivyClient for '{self.host}'>"

    def _request(self, method: str, path: str, data: dict = None) -> dict:
        """Firing request and decode response

        Parameters
        ----------
            method : str
                HTTP request method
            path : str
                Resource path
            data : dict
                Data to be sent to server. Would be JSON stringify and encoded.

        Return
        ------
            data : dict
                Decoded response data

        Raises
        ------
        RequestError
            On any error during this funcition, including connection error, http
            status is not expected or response data decode error.
        """
        assert method in ("GET", "POST", "DELETE", "HEAD")
        assert isinstance(path, str)
        assert data is None or isinstance(data, dict)

        logger.debug("%s %s", method, path)

        # start request
        try:
            self._client.connect()
        except socket.timeout as e:
            raise RequestError(0, "Connection timeout", e)
        except ConnectionRefusedError as e:
            raise RequestError(0, "Connection refused", e)

        self._client.putrequest(
            method=method,
            url=self._prefix + path,
        )

        self._client.putheader("Connection", "Keep-Alive")
        self._client.putheader("Accept", "application/json")
        self._client.putheader("User-Agent", f"python-livyclient/{livy.__version__}")
        self._client.putheader("Keep-Alive", "timeout=3600, max=10000")

        if data:
            data = json.dumps(data, ensure_ascii=True).encode()

            self._client.putheader("Content-Type", "application/json")
            self._client.putheader("Content-Length", str(len(data)))

            self._client.endheaders()
            self._client.send(data)

        else:
            self._client.endheaders()

        try:
            response = self._client.getresponse()
        except KeyboardInterrupt:
            # keyboard interruption causes connection failed on next request
            self._client.close()
            raise
        except socket.timeout as e:
            raise RequestError(0, "Connection timeout", e)
        except ConnectionError as e:
            self._client.close()
            raise RequestError(0, "Connection error", e)

        with response as buf:
            response_bytes = buf.read()

        if response.status < 200 or response.status >= 400:
            raise RequestError(response.status, response.reason)

        if not response_bytes:
            return {}

        try:
            response_data = json.loads(response_bytes)
        except json.JSONDecodeError as e:
            raise RequestError(response.status, "JSON decode error", e)

        return response_data

    def check(self, capture: bool = True) -> bool:
        """Check if server is live.

        Parameters
        ----------
            capture : bool
                Capture exceptions or not.
        """
        try:
            self._request("HEAD", "/batches")
            return True
        except RequestError:
            if capture:
                return False
            else:
                raise

    def create_batch(
        self,
        file: str,
        proxy_user: Optional[str] = None,
        class_name: Optional[str] = None,
        args: Optional[List[str]] = None,
        jars: Optional[List[str]] = None,
        py_files: Optional[List[str]] = None,
        files: Optional[List[str]] = None,
        driver_memory: Optional[str] = None,
        driver_cores: Optional[int] = None,
        executor_memory: Optional[str] = None,
        executor_cores: Optional[int] = None,
        num_executors: Optional[int] = None,
        archives: Optional[List[str]] = None,
        queue: Optional[str] = None,
        name: Optional[str] = None,
        conf: Optional[Dict[str, str]] = None,
    ) -> Batch:
        """Request to create a batch (task)

        Parameters
        ----------
            file : str
                File containing the application to execute
            proxy_user : str
                User to impersonate when running the job
            class_name : str
                Application Java/Spark main class
            args : List[str]
                Command line arguments for the application
            jars : List[str]
                Java dependencies to be used in this batch
            py_files : List[str]
                Python dependencies to be used in this batch
            files : List[str]
                files to be used in this batch
            driver_memory : str
                Amount of memory to use for the driver process
            driver_cores : int
                Number of cores to use for the driver process
            executor_memory : str
                Amount of memory to use per executor process
            executor_cores : int
                Number of cores to use for each executor
            num_executors : int
                Number of executors to launch for this batch
            archives : List[str]
                Archives to be used in this batch
            queue : str
                The name of the YARN queue to which submitted
            name : str
                The session name to execute this batch
            conf : Dict[str, str]
                Spark configuration properties

        Return
        ------
            batch : dict
                Created batch object from livy server

        Raises
        ------
        TypeError
            On input parameters not matches expected data type
        RequestError
            On connection error
        """
        if not isinstance(file, str):
            raise _TypeError("file", str, file)

        logger.info("Create batch. Main script= %s", file)

        data = {
            "file": file,
        }

        # hacky way to check type
        var = locals()

        def is_type(name, type_):
            item = var[name]
            if item is None:
                return False
            elif not isinstance(item, type_):
                raise _TypeError(name, type_, item)
            else:
                return True

        if is_type("proxy_user", str):
            data["proxyUser"] = proxy_user
        if is_type("class_name", str):
            data["className"] = class_name
        if is_type("args", (list, tuple)):
            data["args"] = args
        if is_type("jars", (list, tuple)):
            data["jars"] = jars
        if is_type("py_files", (list, tuple)):
            data["pyFiles"] = py_files
        if is_type("files", (list, tuple)):
            data["files"] = files
        if is_type("driver_memory", str):
            data["driverMemory"] = driver_memory
        if is_type("driver_cores", int):
            data["driverCores"] = driver_cores
        if is_type("executor_memory", str):
            data["executorMemory"] = executor_memory
        if is_type("executor_cores", int):
            data["executorCores"] = executor_cores
        if is_type("num_executors", int):
            data["numExecutors"] = num_executors
        if is_type("archives", (list, tuple)):
            data["archives"] = archives
        if is_type("queue", str):
            data["queue"] = queue
        if is_type("name", str):
            data["name"] = name
        if is_type("conf", dict):
            data["conf"] = conf

        return self._request("POST", "/batches", data)

    def delete_batch(self, batch_id: int) -> None:
        """Kill the batch job.

        Parameters
        ----------
            batch_id : int
                Batch ID

        Raises
        ------
        TypeError
            On input parameters not matches expected data type
        RequestError
            On connection error
        """
        if not isinstance(batch_id, int):
            raise _TypeError("batch_id", int, batch_id)
        self._request("DELETE", f"/batches/{batch_id}")

    def get_batch_information(self, batch_id: int) -> Batch:
        """Get summary information for specific batch

        Parameters
        ----------
            batch_id : int
                Batch ID

        Return
        ------
            batch : dict
                Batch information form livy server

        Raises
        ------
        TypeError
            On input parameters not matches expected data type
        RequestError
            On connection error
        """
        if not isinstance(batch_id, int):
            raise _TypeError("batch_id", int, batch_id)
        return self._request("GET", f"/batches/{batch_id}")

    def get_batch_state(self, batch_id: int) -> str:
        """Get state of the batch

        Parameters
        ----------
            batch_id : int
                Batch ID

        Return
        ------
            state : str
                Current state

        Raises
        ------
        TypeError
            On input parameters not matches expected data type
        RequestError
            On connection error
        """
        if not isinstance(batch_id, int):
            raise _TypeError("batch_id", int, batch_id)
        resp = self._request("GET", f"/batches/{batch_id}/state")
        return resp.get("state", "(unknown)")

    def is_batch_finished(self, batch_id: int) -> bool:
        """Check batch state and return True if it is finished.

        Parameters
        ----------
            batch_id : int
                Batch ID

        Return
        ------
            finished : bool
                Task is over

        Raises
        ------
        TypeError
            On input parameters not matches expected data type
        RequestError
            On connection error
        """
        return self.get_batch_state(batch_id).lower() not in ("starting", "running")

    def get_batch_log(
        self, batch_id: int, from_: Optional[int] = None, size: Optional[int] = None
    ):
        """Get logs from the batch

        Parameters
        ----------
            batch_id : int
                Batch ID
            from_ : int
                Offset
            size : int
                Max line numbers to return

        Return
        ------
            logs : List[str]
                Log lines

        Raises
        ------
        TypeError
            On input parameters not matches expected data type
        RequestError
            On connection error
        """
        if not isinstance(batch_id, int):
            raise _TypeError("batch_id", int, batch_id)
        if from_ is not None and not isinstance(from_, int):
            raise _TypeError("from_", int, from_)
        if size is not None and not isinstance(size, int):
            raise _TypeError("size", int, size)

        query = {}
        if from_ is not None:
            query["from"] = from_
        if size is not None:
            query["size"] = size
        query_string = urllib.parse.urlencode(query)

        path = f"/batches/{batch_id}/log"
        if query_string:
            path += "?" + query_string

        resp = self._request("GET", path)
        return resp.get("log", [])
