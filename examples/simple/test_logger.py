import logging

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

class Foo:
    def __init__(self):
        self.logger = logger

    async def run(self, x: str) -> None:
        self.logger.info(f"🌐 {x}")
