import traceback


def error_with_traceback(message: str, e: Exception) -> str:
    trace = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    return f"{message}\n{trace}"
