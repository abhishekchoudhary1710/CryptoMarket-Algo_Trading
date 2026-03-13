import os
import tempfile
import unittest
from datetime import datetime, timedelta

from core.dual_feed_pipeline import DivergenceSignalEngine, SignalJournal
from data.ohlcv import LiveOHLCVData


def _series(tf, candles):
    s = LiveOHLCVData(timeframe_minutes=tf, name=f"test_{tf}")
    s.completed_candles = candles
    return s


class TestDivergenceSignalEngine(unittest.TestCase):
    def test_detects_divergence_and_entry(self):
        with tempfile.TemporaryDirectory() as td:
            journal = SignalJournal(os.path.join(td, "journal.csv"))
            engine = DivergenceSignalEngine(10, 3, 90, journal)

            base = datetime(2026, 3, 8, 10, 0, 0)
            xau_10 = _series(
                10,
                [
                    {"time": base, "high": 3004, "low": 2999, "open": 3000, "close": 3001, "volume": 1},
                    {"time": base + timedelta(minutes=10), "high": 3002, "low": 2990, "open": 2998, "close": 2992, "volume": 1},
                    {"time": base + timedelta(minutes=20), "high": 3006, "low": 2996, "open": 2997, "close": 3002, "volume": 1},
                    {"time": base + timedelta(minutes=30), "high": 3005, "low": 2998, "open": 3002, "close": 3004, "volume": 1},
                    {"time": base + timedelta(minutes=40), "high": 3001, "low": 2988, "open": 2999, "close": 2990, "volume": 1},
                    {"time": base + timedelta(minutes=50), "high": 3007, "low": 2997, "open": 2992, "close": 3005, "volume": 1},
                ],
            )
            mcx_10 = _series(
                10,
                [
                    {"time": base, "high": 76000, "low": 75500, "open": 75800, "close": 75700, "volume": 1},
                    {"time": base + timedelta(minutes=10), "high": 75900, "low": 75200, "open": 75600, "close": 75300, "volume": 1},
                    {"time": base + timedelta(minutes=20), "high": 76100, "low": 75600, "open": 75400, "close": 76000, "volume": 1},
                    {"time": base + timedelta(minutes=30), "high": 76200, "low": 75700, "open": 76000, "close": 76100, "volume": 1},
                    {"time": base + timedelta(minutes=40), "high": 76150, "low": 75400, "open": 76000, "close": 75500, "volume": 1},
                    {"time": base + timedelta(minutes=50), "high": 76400, "low": 75800, "open": 75600, "close": 76300, "volume": 1},
                ],
            )
            xau_3 = _series(
                3,
                [
                    {"time": base + timedelta(minutes=51), "high": 3004.0, "low": 2998.5, "open": 3000, "close": 3002, "volume": 1}
                ],
            )

            xau_step_1 = _series(10, xau_10.completed_candles[:3])
            mcx_step_1 = _series(10, mcx_10.completed_candles[:3])
            engine.evaluate_divergence(xau_step_1, mcx_step_1, xau_3)
            engine.evaluate_divergence(xau_10, mcx_10, xau_3)
            signal = engine.evaluate_entry_trigger(3004.5)

            self.assertIsNotNone(signal)
            assert signal is not None
            self.assertEqual(signal["direction"], "buy")


if __name__ == "__main__":
    unittest.main()
