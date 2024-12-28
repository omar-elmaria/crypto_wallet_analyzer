from tenacity import retry_if_exception, RetryCallState
from zyte_api.aio.errors import RequestError
from zyte_api.aio.retry import RetryFactory

def is_http_521(exc: BaseException) -> bool:
    return isinstance(exc, RequestError) and (exc.status == 521 or exc.status == 500)

class CustomRetryFactory(RetryFactory):

    retry_condition = (
        RetryFactory.retry_condition
        | retry_if_exception(is_http_521)
    )

    def wait(self, retry_state: RetryCallState) -> float:
        if is_http_521(retry_state.outcome.exception()):
            return self.temporary_download_error_wait(retry_state=retry_state)
        return super().wait(retry_state)

    def stop(self, retry_state: RetryCallState) -> bool:
        if is_http_521(retry_state.outcome.exception()):
            return self.temporary_download_error_stop(retry_state)
        return super().stop(retry_state)

CUSTOM_RETRY_POLICY = CustomRetryFactory().build()