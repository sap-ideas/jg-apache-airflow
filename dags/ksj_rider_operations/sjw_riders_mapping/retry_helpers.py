"""
Retry with exponential backoff for IO-heavy external operations
Handles: timeouts, connection drops, rate limits
"""
import time
import logging


def retry_with_backoff(func, max_retries=3, base_delay=5, description=None):
    """
    Execute a function with exponential backoff retry.

    Args:
        func: Callable (no args) to execute. Use lambda to wrap functions with args.
        max_retries: Maximum number of attempts (default 3)
        base_delay: Initial delay in seconds (doubles each retry: 5s, 10s, 20s)
        description: Label for log messages (e.g. "KSJ Link fetch")

    Returns:
        Whatever func() returns

    Example:
        result = retry_with_backoff(
            lambda: get_missing_ksj_link_riders(config_ksj, config_dwh),
            description="KSJ Link fetch"
        )
    """
    label = description or "operation"

    for attempt in range(1, max_retries + 1):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries:
                logging.error(f"❌ {label} failed after {max_retries} attempts: {e}")
                raise

            wait = base_delay * (2 ** (attempt - 1))
            logging.warning(
                f"⚠️ {label} attempt {attempt}/{max_retries} failed: {e}. "
                f"Retrying in {wait}s..."
            )
            time.sleep(wait)
