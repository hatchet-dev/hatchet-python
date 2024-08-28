import grpc
from tenacity import RetryCallState

from hatchet_sdk.logger import logger


def tenacity_alert_retry(retry_state: RetryCallState) -> None:
    """Called between tenacity retries."""
    logger.debug(
        f"Retrying {retry_state.fn}: attempt "
        f"{retry_state.attempt_number} ended with: {retry_state.outcome}",
    )


def tenacity_should_retry(ex: Exception) -> bool:
    if isinstance(ex, grpc.aio.AioRpcError):
        if ex.code in [
            grpc.StatusCode.UNIMPLEMENTED,
            grpc.StatusCode.NOT_FOUND,
        ]:
            return False
        return True
    else:
        return False
