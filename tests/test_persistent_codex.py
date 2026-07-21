from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace

from engines.persistent_codex import PersistentCodexWorker


class PersistentCodexJournalTest(unittest.TestCase):
    def test_delta_fragments_are_not_published_to_journal(self) -> None:
        async def run() -> None:
            published: list[str] = []

            async def collect(text: str) -> None:
                published.append(text)

            worker = PersistentCodexWorker(
                (1, 2),
                SimpleNamespace(returncode=None),
                "thread-1",
                "/tmp",
                None,
            )
            worker.on_intermediate = collect

            await worker._handle_notification({
                "method": "item/agentMessage/delta",
                "params": {"delta": "Од"},
            })
            await worker._handle_notification({
                "method": "item/reasoning/textDelta",
                "params": {"delta": "обр"},
            })

            self.assertEqual(published, [])

            await worker._handle_notification({
                "method": "item/completed",
                "params": {
                    "item": {
                        "type": "agentMessage",
                        "text": "Одобрение получил.",
                    },
                },
            })

            self.assertEqual(published, ["Одобрение получил."])

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
