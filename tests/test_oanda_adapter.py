import unittest

from core.oanda_client import OandaPricingClient


class TestOandaAdapter(unittest.TestCase):
    def test_normalize_price_response(self):
        payload = {
            "prices": [
                {
                    "instrument": "XAU_USD",
                    "time": "2026-03-08T12:00:00.000000000Z",
                    "bids": [{"price": "3000.10"}],
                    "asks": [{"price": "3000.30"}],
                }
            ]
        }

        tick = OandaPricingClient.normalize_price_response(payload, instrument="XAU_USD")
        self.assertIsNotNone(tick)
        assert tick is not None
        self.assertEqual(tick.instrument, "XAU_USD")
        self.assertAlmostEqual(tick.bid, 3000.10)
        self.assertAlmostEqual(tick.ask, 3000.30)
        self.assertAlmostEqual(tick.mid, 3000.20)


if __name__ == "__main__":
    unittest.main()
