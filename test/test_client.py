import http.client
import io
import ssl
import unittest
import unittest.mock

import livy.client as client


class LivyClientInitTester(unittest.TestCase):
    def setUp(self) -> None:
        self.client = client.LivyClient("http://example.com")
        self.client._request = self.request = unittest.mock.Mock()

    def test___init___url(self):
        with self.assertRaises(TypeError):
            client.LivyClient(1234)
        with self.assertRaises(NotImplementedError):
            client.LivyClient("hxxp://example.com")

    def test___init___verify(self):
        # false
        client.LivyClient("http://example.com", False)

        # customized
        client.LivyClient("http://example.com", ssl.create_default_context())

        # failed: path is currently not supportted
        with self.assertRaises(TypeError):
            client.LivyClient("http://example.com", "/path/to/certificates")

    def test_create_batch(self):
        # required argument (file)
        self.client.create_batch("foo.py")

        with self.assertRaises(TypeError):
            self.client.create_batch(1234)
        with self.assertRaises(TypeError):
            self.client.create_batch(None)

        # optional arguments
        def parameterize(name, normal, *malformed):
            # normal
            self.request.reset_mock()
            self.client.create_batch(**{"file": "foo.py", name: normal})
            self.request.assert_called()

            # none
            self.request.reset_mock()
            self.client.create_batch(**{"file": "foo.py", name: None})
            self.request.assert_called()

            # malformed
            for m in malformed:
                self.request.reset_mock()
                with self.assertRaises(TypeError):
                    self.client.create_batch(**{"file": "foo.py", name: m})
                self.request.assert_not_called()

        parameterize("proxy_user", "user", 1234)
        parameterize("class_name", "class", 1234)
        parameterize("args", ("-d", "foo"), "-t error", {"f"})
        parameterize("jars", ["mock.jar"], "foo.jar")
        parameterize("py_files", ["mock.py"], "foo.py")
        parameterize("files", ["ers.txt"], "ers.txt")
        parameterize("driver_memory", "10G", 1234)
        parameterize("driver_cores", 4, "5678")
        parameterize("executor_memory", "10G", 1234)
        parameterize("executor_cores", 4, "5678")
        parameterize("num_executors", 4, "5678")
        parameterize("archives", ["foo", "bar"], "baz")
        parameterize("queue", "queue", 1234)
        parameterize("name", "name", 1234)
        parameterize("conf", {"foo": "bar"}, ["bar"], "baz")

    def test_delete_batch(self):
        # success
        self.client.delete_batch(1234)
        self.request.assert_called_with("DELETE", "/batches/1234")

        # fail
        self.request.reset_mock()
        with self.assertRaises(TypeError):
            self.client.delete_batch("app")
        self.request.assert_not_called()

    def test_get_batch_infomation(self):
        # success
        self.client.get_batch_infomation(1234)
        self.request.assert_called_with("GET", "/batches/1234")

        # fail
        self.request.reset_mock()
        with self.assertRaises(TypeError):
            self.client.get_batch_infomation("app")
        self.request.assert_not_called()

    def test_get_batch_state(self):
        # success
        self.request.return_value = {"id": 1234, "state": "running"}
        self.assertEqual(self.client.get_batch_state(1234), "running")
        self.request.assert_called_with("GET", "/batches/1234/state")

        # unexpected
        self.request.reset_mock()
        self.request.return_value = {}
        self.assertEqual(self.client.get_batch_state(1234), "(unknown)")
        self.request.assert_called_with("GET", "/batches/1234/state")

        # type error
        self.request.reset_mock()
        with self.assertRaises(TypeError):
            self.client.get_batch_state("app")
        self.request.assert_not_called()

    def test_get_batch_log(self):
        # success: no optional arg
        self.request.return_value = {
            "id": 1234,
            "from": 0,
            "size": 3,
            "log": ["foo", "bar", "baz"],
        }
        self.assertSequenceEqual(self.client.get_batch_log(1234), ["foo", "bar", "baz"])
        self.request.assert_called_with("GET", "/batches/1234/log")

        # success: optional args
        self.request.reset_mock()
        self.client.get_batch_log(123, from_=456)
        self.request.assert_called_with("GET", "/batches/123/log?from=456")

        self.request.reset_mock()
        self.client.get_batch_log(123, size=789)
        self.request.assert_called_with("GET", "/batches/123/log?size=789")

        self.request.reset_mock()
        self.client.get_batch_log(123, from_=456, size=789)
        self.request.assert_called_with("GET", "/batches/123/log?from=456&size=789")

        # type error
        self.request.reset_mock()

        with self.assertRaises(TypeError):
            self.client.get_batch_log("app")
        self.request.assert_not_called()

        with self.assertRaises(TypeError):
            self.client.get_batch_log(1234, "foo")
        self.request.assert_not_called()

        with self.assertRaises(TypeError):
            self.client.get_batch_log(1234, 100, "baz")
        self.request.assert_not_called()


class LivyClientRequestTester(unittest.TestCase):
    def setUp(self) -> None:
        self.client = client.LivyClient("http://example.com", True)
        self.client._client = unittest.mock.MagicMock(spec=http.client.HTTPConnection)
        self.getresponse = self.client._client.getresponse

    def patch(self, *args, **kwargs):
        patcher = unittest.mock.patch("urllib.request.urlopen", *args, **kwargs)
        mocker = patcher.start()
        self.addCleanup(patcher.stop)
        return mocker

    @staticmethod
    def mock_response(code: int, data: bytes = None):
        resp = unittest.mock.MagicMock(spec=http.client.HTTPResponse)
        resp.status = code
        resp.reason = "test"
        resp.__enter__.return_value = io.BytesIO(data or b"")
        return resp

    def test_success(self):
        self.getresponse.return_value = self.mock_response(200)
        self.assertIsInstance(self.client._request("GET", "/test"), dict)

    def test_post(self):
        self.getresponse.return_value = self.mock_response(200, b'{"bar": 456}')

        resp = self.client._request("POST", "/test", {"foo": 123})
        self.assertIsInstance(resp, dict)
        self.assertIn("bar", resp)

    def test_http_status_wanted(self):
        self.getresponse.return_value = self.mock_response(500)

        with self.assertRaises(IOError):
            self.client._request("GET", "/test")

    def test_connection_error(self):
        self.getresponse.side_effect = ConnectionError()

        with self.assertRaises(IOError):
            self.client._request("GET", "/test")

    def test_json_error(self):
        self.getresponse.return_value = self.mock_response(200, b"{")

        with self.assertRaises(IOError):
            self.client._request("GET", "/test")

    def test_real_get(self):
        c = client.LivyClient("http://httpbin.org")

        # run twice for testing keep-alive
        self.assertIsInstance(c._request("GET", "/get"), dict)
        self.assertIsInstance(c._request("GET", "/get"), dict)

    def test_real_post(self):
        c = client.LivyClient("https://httpbin.org")
        resp = c._request("POST", "/post", {"foo": 123})
        self.assertIsInstance(resp, dict)
        self.assertIn("foo", resp["json"])

    def test_real_connection_error(self):
        c = client.LivyClient("http://127.0.0.1", True)
        with self.assertRaises(IOError):
            c._request("GET", "/foo")