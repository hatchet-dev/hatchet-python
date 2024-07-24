import contextvars
import functools
from hatchet_sdk.logger import logger
from hatchet_sdk.clients.dispatcher import Action
from hatchet_sdk.context import Context
from hatchet_sdk.worker.runner.utils.capture_logs import copy_context_vars, sr, wr
from hatchet_sdk.worker.runner.utils.error_with_traceback import errorWithTraceback


class ThreadRunner:

    async def async_wrapped_action_func(
        self, context: Context, action_func, action: Action, run_id: str
    ):
        wr.set(context.workflow_run_id())
        sr.set(context.step_run_id)

        try:
            if action_func._is_coroutine:
                return await action_func(context)
            else:
                pfunc = functools.partial(
                    # we must copy the context vars to the new thread, as only asyncio natively supports
                    # contextvars
                    copy_context_vars,
                    contextvars.copy_context().items(),
                    self.thread_action_func,
                    context,
                    action_func,
                    action,
                )
                res = await self.loop.run_in_executor(self.thread_pool, pfunc)

                return res
        except Exception as e:
            logger.error(errorWithTraceback(f"Could not execute action: {e}", e))
            raise e
        finally:
            self.cleanup_run_id(run_id)

    def thread_action_func(self, context, action_func, action: Action):
        if action.step_run_id is not None and action.step_run_id != "":
            self.threads[action.step_run_id] = current_thread()
        elif (
            action.get_group_key_run_id is not None
            and action.get_group_key_run_id != ""
        ):
            self.threads[action.get_group_key_run_id] = current_thread()

        return action_func(context)
    
    def cleanup_run_id(self, run_id: str):
        if run_id in self.tasks:
            del self.tasks[run_id]

        if run_id in self.threads:
            del self.threads[run_id]

        if run_id in self.contexts:
            del self.contexts[run_id]

