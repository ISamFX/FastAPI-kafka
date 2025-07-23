# Consumer_api/app/dependencies.py
# Consumer_api/app/dependencies.py
from threading import Lock
import logging
from .services.consumer_factory import ConsumerFactory
from .config import settings

logger = logging.getLogger(__name__)
_consumer_instance = None
_consumer_lock = Lock()

def get_consumer():
    global _consumer_instance
    with _consumer_lock:
        if _consumer_instance is None:
            try:
                factory = ConsumerFactory(consumer_type="kafka")
                _consumer_instance = factory.consumer
                if not hasattr(_consumer_instance, 'get_messages'):
                    raise AttributeError("Invalid consumer instance - missing get_messages method")
            except Exception as e:
                logger.error(f"Consumer initialization failed: {e}")
                raise
        return _consumer_instance