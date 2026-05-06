import logging

from src.app.main import TradingEngine

logger = logging.getLogger(__name__)


def main() -> None:
    """Start the existing TradingEngine live runtime."""
    logger.info("Starting Bluegrey live runner")
    engine = TradingEngine()
    engine.start()


if __name__ == "__main__":
    main()
