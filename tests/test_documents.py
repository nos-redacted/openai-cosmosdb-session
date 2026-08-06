from __future__ import annotations

import copy
import traceback
import unittest
from typing import Any

from openai_cosmosdb_session import _documents
from openai_cosmosdb_session.exceptions import CosmosSessionCorruptDocumentError


class DocumentValidationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.document = _documents.build_message_document(
            seq=7,
            message_data='{"role":"user","content":"hello"}',
            session_id="session-1",
            incarnation="a" * 32,
            ttl_seconds=None,
        )

    async def test_schema_validation_reports_only_invalid_field_names(self) -> None:
        cases: tuple[tuple[str, Any], ...] = (
            ("id", 7),
            ("sessionId", ""),
            ("type", "ledger"),
            ("schema_version", 1),
            ("incarnation", ""),
            ("seq", True),
            ("message_data", {"secret": "SENTINEL"}),
        )

        for field, value in cases:
            with self.subTest(field=field):
                document = copy.deepcopy(self.document)
                document[field] = value

                with self.assertRaises(CosmosSessionCorruptDocumentError) as raised:
                    await _documents.deserialize_document(
                        document,
                        _documents.deserialize_item,
                    )

                error = raised.exception
                self.assertEqual(error.code, "invalid_message_document")
                self.assertIn(field, error.invalid_fields)
                rendered = "\n".join(
                    (
                        str(error),
                        repr(error),
                        repr(vars(error)),
                        "".join(traceback.format_exception(error)),
                    )
                )
                self.assertNotIn("SENTINEL", rendered)

    async def test_id_must_match_incarnation_and_sequence(self) -> None:
        document = copy.deepcopy(self.document)
        document["id"] = f"{'a' * 32}:000000000008"

        with self.assertRaises(CosmosSessionCorruptDocumentError) as raised:
            await _documents.deserialize_document(document, _documents.deserialize_item)

        self.assertEqual(raised.exception.invalid_fields, ("id",))

    async def test_decoded_message_must_be_an_object(self) -> None:
        document = copy.deepcopy(self.document)
        document["message_data"] = '["SENTINEL"]'

        with self.assertRaises(CosmosSessionCorruptDocumentError) as raised:
            await _documents.deserialize_document(document, _documents.deserialize_item)

        error = raised.exception
        self.assertEqual(error.code, "invalid_message_shape")
        self.assertNotIn("SENTINEL", repr(vars(error)))


if __name__ == "__main__":
    unittest.main()
