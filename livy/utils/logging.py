import decimal
import importlib
import logging
import queue
import re
import threading
import time
import typing


class EnhancedConsoleHandler(logging.StreamHandler):
    """A stream handler that could shows progress bar on task set related logs
    found.

    It would create a progress bar using tqdm. However, frequently suppressing
    the progress bar (for printing logs) would make the screen flashing. To deal
    with this issue, this handler uses a producer-consumer architecture inside.
    Logs are not emitted in real time, they would be proceed by batch in flush().
    And a backend worker is created to regularly flushing the logs.

    This implementation comes with a pitfall: if the program exits too early,
    some of the logs would be dropped. PR is welcome if you could resolve this
    issue.
    """

    _PATTERN_ADD_TASKSET = re.compile(r"Adding task set ([\d.]+) with (\d+) tasks")
    _PATTERN_REMOVE_TASKSET = re.compile(r"Removed TaskSet ([\d.]+),")
    _PATTERN_FINISH_TASK = re.compile(
        r"Finished task [\d.]+ in stage ([\d.]+) \(.+?\) in \d+ ms on \S+ "
        r"\(executor \d+\) \((\d+)\/(\d+)\)"
    )

    def __new__(cls, stream: typing.TextIO) -> logging.StreamHandler:
        """Automatically fallback to normal handler if requirement not satisfied."""
        if not stream.isatty() or not importlib.util.find_spec("tqdm"):
            return super().__new__(logging.StreamHandler)
        return super().__new__(cls)

    def __init__(self, stream: typing.TextIO) -> None:
        """
        Parameters
        ----------
            stream : typing.TextIO
                Output stream. Might be stdout or stderr.
        """
        super().__init__(stream)
        import tqdm

        self._current_progressbar: tqdm.tqdm = None
        self._latest_taskset: decimal.Decimal = decimal.Decimal(-1)

        # tqdm is only avaliable in this scope
        self._tqdm_create = tqdm.tqdm
        self._tqdm_suppress = tqdm.tqdm.external_write_mode

        # background worker and queue for write log by batch
        self._log_queue = queue.Queue()
        self._stop_thread = threading.Event()

        thread = threading.Thread(target=self._worker, args=())
        thread.daemon = True
        thread.start()

    def _worker(self):
        """Worker loop to trigger flush() regularly"""
        while not self._stop_thread.is_set():
            if not self._log_queue.empty():
                self.flush()
            time.sleep(0.07)

    def handle(self, record: logging.LogRecord) -> None:
        """Override `handle` for reteriving the log before it is filtered."""
        # capture logs from YarnScheduler / TaskSetManager for updating progressbar
        if record.name == "YarnScheduler":
            msg = record.getMessage()
            m = self._PATTERN_ADD_TASKSET.match(msg)
            if m:
                self._set_progressbar(
                    task_set=m.group(1),
                    progress=0,
                    total=int(m.group(2)),
                )
            else:
                m = self._PATTERN_REMOVE_TASKSET.match(msg)
                if m:
                    self._close_progressbar(m.group(1))

        elif record.name == "TaskSetManager":
            m = self._PATTERN_FINISH_TASK.match(record.getMessage())
            if m:
                self._set_progressbar(
                    task_set=m.group(1),
                    progress=int(m.group(2)),
                    total=int(m.group(3)),
                )

        # filter should be proceed in logging.Handler
        if not self.filter(record):
            return

        # enqueue
        #
        # Normally, `handle()` might emits log here. But as it use tqdm, which
        # changes the cursor position.
        # Suppressing the progress bar every time it emits log might make the
        # progress bar flashing on too many logs proceed in short time.
        # As the alternative, it proceed the logs by batch (in flush) and use a
        # background thread to trigger flushing.
        self._log_queue.put(record)

    def _set_progressbar(self, task_set: str, progress: int, total: int) -> None:
        """Update progress bar status"""
        task_set = decimal.Decimal(task_set)

        # no update by status of older task set
        if task_set < self._latest_taskset:
            return

        # update progress
        elif self._current_progressbar and task_set == self._latest_taskset:
            update = progress - self._current_progressbar.n
            if update > 0:
                self._current_progressbar.update(update)
            return

        # overwrite progressbar for new task set
        elif task_set > self._latest_taskset:
            self._close_progressbar(self._latest_taskset)
            self._latest_taskset = task_set

        # create new progress bar
        self._current_progressbar = self._tqdm_create(
            desc=f"Stage {task_set}",
            total=total,
            leave=True,
        )

        self._current_progressbar.update(progress)

    def _close_progressbar(self, task_set: str) -> bool:
        """Close the progress bar on the screen"""
        if not self._current_progressbar:
            return False

        task_set = decimal.Decimal(task_set)
        if task_set != self._latest_taskset:
            return False

        self._current_progressbar.close()
        self._current_progressbar = None
        return True

    def flush(self) -> None:
        """Flush all logs to console"""
        # get logs
        logs: typing.List[logging.LogRecord] = []
        while True:
            try:
                logs.append(self._log_queue.get_nowait())
            except queue.Empty:
                break

        if not logs:
            return

        # emit logs
        with self.lock, self._tqdm_suppress():
            for record in logs:
                self.emit(record)

    def close(self):
        """Close this handler. Stop emitting logs to console."""
        self._stop_thread.set()
        self.flush()

        class SinkQueue:
            def put(self, _):
                ...  # ignored; drop all message

            def get_nowait(self):
                raise queue.Empty()

        self._log_queue = SinkQueue()
