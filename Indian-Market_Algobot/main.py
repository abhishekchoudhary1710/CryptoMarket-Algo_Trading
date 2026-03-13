# main.py
"""
Main entry point for Algo-Trading.
Keeps startup minimal and delegates to TradingEngine.
"""
from brokers.angelone import AngelOneBroker
from core.engine import TradingEngine
from utils.logger import logger

def main():
    logger.info("hiihello-Booting AlgoTrading...")
    broker = AngelOneBroker()
    engine = TradingEngine(broker)
    engine.start()

if __name__ == "__main__":
    main()
