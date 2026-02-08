import time
import logging
import schedule
from datetime import datetime, timezone, timedelta
from typing import Set

# VectorWave internal modules
from ..search.execution_search import find_executions
from ..models.db_config import get_weaviate_settings
from .healer import VectorWaveHealer

logger = logging.getLogger(__name__)


class AutoHealerBot:
    def __init__(self, check_interval_minutes: int = 5):
        self.healer = VectorWaveHealer()
        self.settings = get_weaviate_settings()
        self.check_interval_minutes = check_interval_minutes

        # Cooldown memory: {function_name: last_healed_time}
        # Prevents duplicate PRs by not touching a healed function for a certain period.
        self.healed_history = {}
        self.COOLDOWN_MINUTES = 60

    def scan_and_heal(self):
        logger.info("🕵️ [AutoHealer] Scanning for recent system errors...")

        # 1. Retrieve errors from the last N minutes
        # (Errors in .vtwignore are already marked as FAILURE by Tracer, so querying ERROR automatically filters them out)
        # Scan range is double the check interval to ensure no errors are missed.
        lookback_time = datetime.now(timezone.utc) - timedelta(minutes=60)
        time_limit = lookback_time.isoformat()

        try:
            recent_errors = find_executions(
                filters={
                    "status": "ERROR",
                    "timestamp_utc__gte": time_limit
                },
                limit=20,
                sort_by="timestamp_utc",
                sort_ascending=False
            )
        except Exception as e:
            logger.error(f"❌ Failed to fetch execution logs: {e}")
            return

        if not recent_errors:
            logger.info("✅ System is healthy. No active errors found.")
            return

        # 2. Identify problematic functions (Deduplication)
        target_functions = set()
        for log in recent_errors:
            func_name = log.get('function_name')
            if func_name:
                target_functions.add(func_name)

        logger.info(f"🚨 Found issues in {len(target_functions)} functions: {list(target_functions)}")

        # 3. Attempt healing for each function
        for func_name in target_functions:
            if self._is_in_cooldown(func_name):
                logger.info(f"⏳ Skipping '{func_name}': Already processed recently (Cooldown active).")
                continue

            logger.info(f"🚑 Initiating healing process for '{func_name}'...")

            try:
                # Call Healer in PR creation mode
                result = self.healer.diagnose_and_heal(
                    function_name=func_name,
                    lookback_minutes=60,
                    create_pr=True
                )

                # Check result
                if "PR Created Successfully" in result:
                    logger.info(f"🎉 AutoHealer Fixed '{func_name}'! PR Created.")
                    self._update_cooldown(func_name)
                elif "No errors found" in result:
                    logger.info(f"⚠️ Healer found no logs for '{func_name}' (Maybe intermittent).")
                else:
                    logger.warning(f"⚠️ Healing attempted for '{func_name}' but PR not created. Check logs.")

            except Exception as e:
                logger.error(f"❌ Critical error while healing '{func_name}': {e}")

    def _is_in_cooldown(self, func_name: str) -> bool:
        """Checks if the function is in the cooldown period (recently healed)."""
        if func_name not in self.healed_history:
            return False

        last_healed = self.healed_history[func_name]
        elapsed = datetime.now() - last_healed
        if elapsed < timedelta(minutes=self.COOLDOWN_MINUTES):
            return True

        # Remove from history if cooldown has passed
        del self.healed_history[func_name]
        return False

    def _update_cooldown(self, func_name: str):
        """Updates the cooldown timestamp upon successful healing."""
        self.healed_history[func_name] = datetime.now()


def start_scheduler(interval_minutes: int = 5):
    """Entry point to start the scheduler."""
    bot = AutoHealerBot(check_interval_minutes=interval_minutes)

    logger.info("=" * 60)
    logger.info(f"🚀 VectorWave AutoHealer Started!")
    logger.info(f"   - Check Interval: Every {interval_minutes} minutes")
    logger.info(f"   - Mode: Automatic Diagnosis & PR Creation")
    logger.info("=" * 60)

    # Register schedule
    schedule.every(interval_minutes).minutes.do(bot.scan_and_heal)

    # Run once immediately on start (for testing purposes)
    bot.scan_and_heal()

    while True:
        schedule.run_pending()
        time.sleep(1)