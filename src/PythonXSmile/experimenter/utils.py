"""
experimenter.utils - Auxiliary functions

__author__ = "Anton Dries, Wannes Meert"
__copyright__ = "Copyright 2016 KU Leuven, DTAI Research Group"
__license__ = "APL"

..
    Part of the DTAI experimenter code.

    Copyright 2016 KU Leuven, DTAI Research Group

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
"""
import sys
import signal
import time
import logging
from queue import Empty
import multiprocessing as mp
from multiprocessing.queues import Queue as MPQueue
import threading


logger = logging.getLogger("be.kuleuven.cs.dtai.experimenter")


class Bunch(object):
    def __init__(self, *args, **kwargs):
        # You should use the attrs package instead of this
        for arg in args:
            if type(arg) == dict:
                self.__dict__.update(arg)
        self.__dict__.update(kwargs)


levels = Bunch({
    "STDERR": 70,
    "STDOUT": 60,
    "CRITICAL": logging.CRITICAL,  # 50
    "ERROR": logging.ERROR,        # 40
    "WARNING": logging.WARNING,    # 30
    "STATUS": 25,
    "INFO": logging.INFO,          # 20
    "SETTINGS": 15,
    "DEBUG": logging.DEBUG,        # 10
    "NOTSET": logging.NOTSET       # 0
})

level_prefix = {
    70: "[STDERR] ",
    60: "",
    levels.CRITICAL: "[CRITICAL] ",
    levels.ERROR: "[ERROR] ",
    levels.WARNING: "[WARNING] ",
    levels.STATUS: "[STATUS] ",
    levels.INFO: "[INFO] ",
    levels.SETTINGS: "[SETTINGS] ",
    levels.DEBUG: "[DEBUG] ",
    levels.NOTSET: ""
}


level_type = {
    70: "STDERR",
    60: "STDOUT",
    levels.CRITICAL: "CRITICAL",
    levels.ERROR: "ERROR",
    levels.WARNING: "WARNING",
    levels.STATUS: "STATUS",
    levels.INFO: "INFO",
    levels.SETTINGS: "SETTINGS",
    levels.DEBUG: "DEBUG",
    levels.NOTSET: "NOTSET"
}


class Timeout(Exception):
    def __init__(self, msg):
        super().__init__(msg)


class Timer:
    def __init__(self, description, logger, time_format='{:.3f} seconds', max_time=0):
        # Use max_time to set a timeout => If two timers with timeouts are nested the outer one will not time out.
        self._description = description
        self._logger = logger
        self._time_format = time_format
        self._exec_time = None
        self._max_time = max_time  # type: int
        if self._max_time > 0:
            signal.signal(signal.SIGALRM, self.on_time_out)
            signal.setitimer(signal.ITIMER_REAL, self._max_time)

    @property
    def total_time(self):
        return self._exec_time

    def __enter__(self):
        self._start_time = time.time()
        self._logger.info('Start: '+self._description)
        return self

    def __exit__(self, *args):
        self._exec_time = time.time() - self._start_time
        self._logger.info('Finished: '+self._description+" "+(self._time_format.format(self._exec_time)))
        if self._max_time > 0:
            signal.setitimer(signal.ITIMER_REAL, 0)

    def on_time_out(self, *args):
        self._logger.info('Execution timed out')
        raise Timeout("TimeOut during execution of '{} ({})'".format(self._description, args))


orig_stdout = sys.stdout
orig_stderr = sys.stderr


def print_orig(*args, **kwargs):
    if "file" not in kwargs:
        kwargs["file"] = orig_stdout
    return print(*args, **kwargs)


def wait_until_queue_empty(queue, timeout=1):
    stop = threading.Event()
    stop_timer = threading.Timer(timeout, stop.set)
    stop_timer.start()
    while not queue.empty() and not stop.is_set():
        try:
            time.sleep(0.1)
        except KeyboardInterrupt:
            stop.set()
    stop_timer.cancel()


# mp.set_start_method('spawn')  # should be protected by __main__

class MPStream(MPQueue):
    def __init__(self, *args, **kwargs):
        ctx = mp.get_context('spawn')
        super().__init__(*args, ctx=ctx, **kwargs)
        # Queue.__init__(self, *args, ctx=self.get_context(), **kwargs)
        self.line = ''
        self.closed = False

    def write(self, b):
        if self.closed:
            return 0
        self.put(b)
        return len(b)

    def flush(self):
        self.put("\n")
        sys.__stdout__.flush()

    def readline(self):
        found_newline = False
        result = None
        while not found_newline:
            try:
                line = str(self.get(True, 1))
                idx = line.find("\n")
                if idx == -1:
                    self.line += line
                else:
                    found_newline = True
                    result = self.line + line[:idx+1]
                    self.line = line[idx+1:]
            except Empty:
                found_newline = True
                result = ''
        return result

    def wait_until_empty(self, timeout=5):
        wait_until_queue_empty(self, timeout)

    def close(self):
        self.put("\n")
        self.closed = True
