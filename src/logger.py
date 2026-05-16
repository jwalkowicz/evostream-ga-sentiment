import os
import sys

from loguru import logger

logger.remove()

APP_ENV = os.getenv("APP_ENV", "test")

if APP_ENV == "prod":
    logger.add(sys.stdout, serialize=True, level="INFO", enqueue=True)
else:
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="DEBUG",
        enqueue=True,
    )
