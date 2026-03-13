import os
import tempfile
import unittest
from datetime import datetime, timedelta

from core.dual_feed_pipeline import (
    DivergenceSignalEngine,
    MultiTimeframeCandlePipeline,
    NormalizedTick,
    SignalJournal,
    parse_timeframes,
)


class TestCandlePipeline(unittest.TestCase):
    def test_parse_timeframes(self):
        self.assertEqual(parse_timeframes("1,3,5,10,15"), [1, 3, 5, 10, 15])
        self.assertEqual(parse_timeframes("1, x, 3, 3"), [1, 3])

    def test_candle_aggregation(self):
        with tempfile.TemporaryDirectory() as td:
            journal = SignalJournal(os.path.join(td, "journal.csv"))
            engine = DivergenceSignalEngine(10, 3, 90, journal)
            pipeline = MultiTimeframeCandlePipeline([1, 3, 5, 10, 15], engine)

            t0 = datetime(2026, 3, 8, 10, 0, 5)
            pipeline.ingest_tick(NormalizedTick("xauusd", "XAU_USD", 3000.0, t0))
            pipeline.ingest_tick(NormalizedTick("xauusd", "XAU_USD", 3001.0, t0 + timedelta(seconds=30)))
            pipeline.ingest_tick(NormalizedTick("xauusd", "XAU_USD", 3002.0, t0 + timedelta(minutes=1)))

            one_min = pipeline.series["xauusd"][1]
            self.assertEqual(len(one_min.completed_candles), 1)
            self.assertEqual(one_min.completed_candles[0]["open"], 3000.0)
            self.assertEqual(one_min.completed_candles[0]["close"], 3001.0)


if __name__ == "__main__":
    unittest.main()
