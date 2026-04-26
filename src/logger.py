import sys
from loguru import logger

logger.remove()

logger.add(sys.stdout, format="{message}", serialize=True, level="INFO", enqueue=True)
