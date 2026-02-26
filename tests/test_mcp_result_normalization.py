import unittest

from sandbox_compute_server.mcp import _normalize_mcp_result


class _ModelDumpObj:
    def __init__(self, payload):
        self.payload = payload

    def model_dump(self):
        return {"payload": self.payload}


class _PlainObj:
    def __init__(self, text):
        self.type = "text"
        self.text = text


class MCPResultNormalizationTests(unittest.TestCase):
    def test_normalize_nested_non_serializable_content(self):
        nested = {
            "content": [
                _PlainObj("hello"),
                {"extra": _ModelDumpObj(_PlainObj("world"))},
            ],
            "bytes": b"ok",
        }

        normalized = _normalize_mcp_result(nested)

        self.assertEqual(normalized["bytes"], "ok")
        self.assertEqual(normalized["content"][0]["type"], "text")
        self.assertEqual(normalized["content"][0]["text"], "hello")
        self.assertEqual(normalized["content"][1]["extra"]["payload"]["text"], "world")


if __name__ == "__main__":
    unittest.main()
