import threading
import logging
from ..models.db_config import get_weaviate_settings
from ..utils.scheduler import start_scheduler

logger = logging.getLogger(__name__)

_HEALER_STARTED = False

def initialize_vectorwave():
    """
    Initialize vectorwave system.
    Refer to config automatically run Auto Healer System.
    """
    global _HEALER_STARTED
    try:
        settings = get_weaviate_settings()
    except Exception as e:
        logger.warning(f"⚠️ Failed to load settings during initialization: {e}")
        return

    if _HEALER_STARTED:
        return

    if settings.ENABLE_AUTO_HEALER:
        logger.info("🔧 AutoHealer is enabled. Starting background scheduler...")

        healer_thread = threading.Thread(
            target=start_scheduler,
            kwargs={'interval_minutes': settings.HEALER_CHECK_INTERVAL_MINUTES},
            daemon=True,
            name="VectorWave-AutoHealer"
        )
        healer_thread.start()
        _HEALER_STARTED = True
    else:
        logger.debug("🔧 AutoHealer is disabled (ENABLE_AUTO_HEALER=False).")