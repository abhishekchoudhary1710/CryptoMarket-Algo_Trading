import os
import tempfile
import unittest
from datetime import datetime, timedelta

from core.dual_feed_pipeline import (
    DivergenceSignalEngine,
    MultiTimeframeCandlePipeline,
    NormalizedTick,
    SignalJournal,
)


class TestDualFeedIntegration(unittest.TestCase):
    def test_tick_to_signal_path(self):
        with tempfile.TemporaryDirectory() as td:
            journal = SignalJournal(os.path.join(td, "journal.csv"))
            engine = DivergenceSignalEngine(10, 3, 90, journal)
            pipeline = MultiTimeframeCandlePipeline([1, 3, 10], engine)

            # Build candles by ticks for both feeds.
            start = datetime(2026, 3, 8, 9, 0, 5)
            for i in range(0, 65):
                ts = start + timedelta(minutes=i)
                xau = 3000 + (i % 7) - (3 if i in (12, 45) else 0)
                mcx = 76000 + (i % 11) + (4 if i in (12, 45) else 0)
                pipeline.ingest_tick(NormalizedTick("xauusd", "XAU_USD", float(xau), ts))
                pipeline.ingest_tick(NormalizedTick("mcx_gold_fut", "GOLD", float(mcx), ts))

            # Ensure both streams produced candles and can be queried.
            self.assertIsNotNone(pipeline.latest_close("xauusd", 1))
            self.assertIsNotNone(pipeline.latest_close("mcx_gold_fut", 1))


if __name__ == "__main__":
    unittest.main()
