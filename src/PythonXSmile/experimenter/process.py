"""
experimenter.process - Run external binary

__author__ = "Wannes Meert, Anton Dries"
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

import datetime
import logging
import multiprocessing as mp
import os
import platform
import pty
import subprocess as sp
import sys
import threading
import time
import traceback
from queue import Queue, Empty

import psutil

from . import monitor
from .utils import Timer, levels, MPStream, wait_until_queue_empty

logger = logging.getLogger("be.kuleuven.cs.dtai.experimenter")


class ProcessLogger(logging.Logger):
    def __init__(self, listeners):
        super().__init__(name="ProcessLogger")
        self.listeners = listeners

    def log(self, msg, identifier=levels.STDOUT, **kwargs):
        for listener in self.listeners:
            listener.get_log(msg, identifier)

    def info(self, msg, **kwargs):
        self.log(msg, levels.INFO)

    def warning(self, msg, **kwargs):
        self.log(msg, levels.WARNING)

    def error(self, msg, **kwargs):
        self.log(msg, levels.ERROR)


class ProcessBase:
    def __init__(self, monitors=None, params=None):
        """
        Create a process.

        :param monitors: list of ProcessMonitors objects to monitor the process. If empty a default print to
            stdout is assumed.
        :param params: Additional parameters that are printed to the logfile(s) (dictionary)
        """
        if monitors is None:
            self.monitors = []
        else:
            self.monitors = list(monitors)
        self.listeners = []
        if monitors is None or len(monitors) == 0:
            self.listeners.append(monitor.Print())
        self.stop = Queue()  # Add string with reason to stop
        self.output = Queue()  # Use a queue to avoid blocking
        self.result = Queue()  # Return values for called value (e.g. returncode)
        self.logger = ProcessLogger(self.listeners)
        self.params = params
        self.runtime = 0
        self.stdout = None
        self.stderr = None
        self.returncode = None
        self._set_up_monitors()

        self.process_time=None # Method that I added, to be sure I do not interfere with any other functionalities

    @staticmethod
    def _extract_arguments(args, prefix):
        result = {}
        for arg in args:
            if arg.startswith(prefix):
                result[arg[len(prefix):]] = args[arg]
        return result

    def _set_up_monitors(self):
        if self.monitors:
            for cur_monitor in self.monitors:
                cur_monitor.set_up(self, self.stop, self.output)
                if cur_monitor.listen_to_output:
                    self.listeners.append(cur_monitor)
                if cur_monitor.run_as_thread:
                    # monitor.start()
                    threading.Thread(target=cur_monitor.run).start()

    def _tear_down_monitors(self):
        for cur_monitor in self.monitors:
            cur_monitor.tear_down(self.returncode)

    def _watch_stream(self, identifier, stream):
        """Watch the output stream of the current process.

        We send the stream to a queue to avoid any blocking.
        """
        while self.stop.empty():
            # for line in stream:
            try:
                line = stream.readline()
                if line == b'' or line == '':
                    continue
                self.output.put((identifier, line))
                if not self.stop.empty():
                    break
            except IOError as exc:
                logger.warning("IOError({}): {}".format(identifier, exc))
            except ValueError as exc:
                logger.warning("ValueError({}): {}".format(identifier, exc))
            # except Exception as exc:
                # logger.warning("UnknownError({}): {}".format(identifier, exc))
        if not stream.closed:
            stream.close()

    def _process_stream(self):
        stop = threading.Event()
        stop_timer = threading.Timer(0.5, stop.set)
        while not stop.is_set() or not self.output.empty():
            try:
                identifier, line = self.output.get(True, 1)
            except Empty:
                pass
            else:
                if type(line) is bytes:
                    line = line.decode('utf-8')
                self.logger.log(line, identifier)
            if not self.stop.empty() and not stop_timer.is_alive():
                try:
                    stop_timer.start()  # Keep listening for another half second
                except RuntimeError:
                    pass

    def get_characteristics(self):
        params = dict()
        try:
            params["date"] = time.strftime("%Y/%m/%d %H:%M:%S")
            params["date_iso"] = datetime.datetime.now().isoformat()
            params["params"] = self.params

            # System
            sparams = dict()
            sparams["nodename"] = platform.node()
            sparams["machine"] = platform.machine()
            sparams["platform"] = platform.platform()
            sparams["processor"] = platform.processor()
            sparams["version"] = platform.version()
            sparams["system"] = platform.system()
            sparams["cpu_count"] = psutil.cpu_count()
            sparams["cpu_times"] = dict()
            cpu_times = psutil.cpu_times()
            for k in ['user', 'system', 'idle', 'nice']:
                try:
                    sparams["cpu_times"][k] = cpu_times.__getattribute__(k)
                except AttributeError:
                    logger.warning('Property {} cannot be retrieved'.format(k))
            sparams["cpu_count"] = psutil.cpu_count()
            sparams["cpu_percent"] = psutil.cpu_percent()
            virtual_memory = psutil.virtual_memory()
            sparams["virtual_memory"] = dict()
            for k in ['total', 'available', 'percent', 'used', 'free']:
                try:
                    sparams["virtual_memory"][k] = virtual_memory.__getattribute__(k)
                except AttributeError:
                    logger.warning('Property {} cannot be retrieved'.format(k))
            params["system"] = sparams

            # Process
            p = psutil.Process()
            pparams = dict()
            pparams["username"] = p.username()
            pparams["cwd"] = p.cwd()
            pparams["create_time"] = p.create_time()
            cpu_times = p.cpu_times()
            pparams["cpu_times"] = dict()
            for k in ['user', 'system']:
                try:
                    pparams["cpu_times"][k] = cpu_times.__getattribute__(k)
                except AttributeError:
                    logger.warning('Property {} cannot be retrieved'.format(k))
            params["process"] = pparams

            # Monitors
            for cur_monitor in self.monitors:
                params.update(cur_monitor.get_settings())

        except Exception as exc:
            logger.warning("Collecting properties of process failed, not all included.")
            logger.warning(str(exc))

        return params

    def print_characteristics(self):
        params = self.get_characteristics()
        # self.logger.log("\n"+json.dumps(params, indent=2), identifier=levels.SETTINGS)
        self.logger.log(params, identifier=levels.SETTINGS)

    def run(self):
        result = None
        self.print_characteristics()
        self.returncode = 999
        total_time = 0
        self.process_time=0
        try:
            self._run_setup()
            if self.stdout is None:
                self.logger.error("No stdout is set to track")
                sys.exit(1)
            if self.stderr is None:
                self.logger.error("No stderr is set to track")
                sys.exit(1)
            with Timer("Run process", self.logger) as timer:
                watch_stdout_thread = threading.Thread(target=self._watch_stream,
                                                       args=(levels.STDOUT, self.stdout))
                watch_stderr_thread = threading.Thread(target=self._watch_stream,
                                                       args=(levels.STDERR, self.stderr))
                process_stream_thread = threading.Thread(target=self._process_stream)
                watch_stdout_thread.start()
                watch_stderr_thread.start()
                process_stream_thread.start()

                if self.stop.empty():
                    result = self._run_execute()
                self.logger.info('Process ended (returncode {})'.format(self.returncode))

            total_time = timer.total_time
            self.process_time=timer.total_time

        except KeyboardInterrupt:
            self.kill(reason='keyboard')
        except Exception as exc:
            traceback.print_exc()
            self.logger.error('Exception: {}'.format(exc))
            self.kill(reason='other')

        self._run_teardown()

        # Report reasons for stopping
        reasons = []
        while not self.stop.empty():
            c, r = self.stop.get()
            reasons.append(r)
            if self.returncode is None:
                self.returncode = c
            else:
                self.returncode = max(c, self.returncode)
        if len(reasons) == 0:
            reasons.append('finished')
        self.logger.warning({
            'Exit': ', '.join(reasons),
            'Returncode': self.returncode,
            'Time': total_time
        })
        self.stop.put((0, 'completed'))
        wait_until_queue_empty(self.output, timeout=0.5)
        self._tear_down_monitors()
        return result

    def _run_setup(self):
        """Set up the process to run."""
        pass

    def _run_execute(self):
        """The run do-loop.

        :return: The value returned by the `run` function.
        """
        return None

    def _run_teardown(self):
        """Clean up everything."""
        pass

    def kill(self, reason=None, returncode=None):
        """Kill this process.

        This can be called in an atexit definition:

            @atexit.register
            def kill_process():
              process.kill()

        :param reason: Reason for killing the process
        :param returncode: Returncode of process to report (if not given, the
            saved returncode is used)
        """
        if returncode is not None:
            self.returncode = returncode
        if reason is not None:
            self.stop.put((self.returncode, reason))
        else:
            if self.stop.empty():
                self.stop.put((self.returncode, 'other'))


class Process(ProcessBase):
    def __init__(self,
                 cmd,
                 cwd=None,
                 env=None,
                 monitors=None,
                 params=None,
                 unbuffered=False,
                 **kwdargs):
        """
        Create a process to run an external binary.

        Extra arguments starting with 'popen_' are passed through to subprocess.Popen.
        Arguments that are not explained here can be found in `ProcessBase`.

        Note: If the output of the process is not shown live it might be
        because the called binary does not flush its buffer (or has
        limited output). As a consequence, the output will only be
        processed after a while or at the end of the process. Some binaries
        will delay emptying the buffer if they detect that they are part
        of a pipe (which is the case here).

        Some binaries have special modes to force unbuffered printing (e.g.
        `python -u`). Another option is to use the `unbuffer` binary but
        this will shield the returncode of the wrapped proces.
        In this module we make use of a pseudo-terminal if the `unbuffered`
        option is set to true.

        :param cmd: The command to run (as a list)
        :param cwd: Current working directory
        :param env: Environment variables to set
        :param unbuffered: Run the process in a pseudo-terminal that forces
                           unbuffered output (otherwise it is run as part of a pipe)
        """
        super().__init__(monitors=monitors, params=params)
        self.p = None
        self.cmd = cmd
        self.cwd = cwd
        self.env = env
        self.unbuffered = unbuffered

        if self.unbuffered:
            self.logger.info('Opening pseudo-terminal')
            self.stdout_master, self.stdout_slave = pty.openpty()
            self.stderr_master, self.stderr_slave = pty.openpty()
            self.stdout_pipe = self.stdout_slave
            self.stderr_pipe = self.stderr_slave
        else:
            self.stdout_master, self.stdout_slave = None, None
            self.stderr_master, self.stderr_slave = None, None
            self.stdout_pipe = sp.PIPE
            self.stderr_pipe = sp.PIPE

        self.popen_args = {
            'stderr': self.stderr_pipe,
            'stdout': self.stdout_pipe,
            'cwd': self.cwd,
            'env': self.env}
        self.popen_args.update(self._extract_arguments(kwdargs, 'popen_'))

    def __del__(self):
        self.kill()

    def _tear_down_monitors(self):
        if self.p is not None:
            self.returncode = self.p.returncode
        super()._tear_down_monitors()

    def _run_setup(self):
        self.logger.info({'cmd': ' '.join(self.cmd), 'cwd': self.cwd})
        try:
            self.p = sp.Popen(self.cmd, **self.popen_args)
        except FileNotFoundError as exc:
            self.logger.error(str(exc))
            self.kill()
        else:
            if self.unbuffered:
                os.close(self.stdout_slave)
                os.close(self.stderr_slave)
                self.stdout = os.fdopen(self.stdout_master)
                self.stderr = os.fdopen(self.stderr_master)
            else:
                self.stdout = self.p.stdout
                self.stderr = self.p.stderr

    def _run_execute(self):
        while self.p.poll() is None and self.stop.empty():
            time.sleep(0.2)
        if self.p.poll() is None:
            self.kill()
        self.returncode = self.p.returncode
        return self.returncode

    def _run_teardown(self):
        if self.unbuffered:
            self.logger.info('Closing pseudo-terminal')
            if self.stdout is not None and not self.stdout.closed:
                self.stdout.close()
            if self.stderr is not None and not self.stderr.closed:
                self.stderr.close()
            # os.close(stdout_master)
            # os.close(stdout_slave)
            # os.close(stderr_master)
            # os.close(stderr_slave)

    def get_characteristics(self):
        params = super().get_characteristics()
        params["command"] = ' '.join(self.cmd)
        return params

    def kill(self, reason=None, returncode=None):
        super().kill(reason=reason, returncode=returncode)
        if self.p is not None and self.p.poll() is None:
            if reason is not None and reason != '':
                reason = ' ({})'.format(reason)
            else:
                reason = ''
            self.logger.info('Kill process{}'.format(reason))
            procs = psutil.Process(self.p.pid).children(recursive=True)
            for p in procs:
                p.terminate()
            def on_terminate(proc):
                self.logger.info("Process {} terminated with exit code {}".format(proc, proc.returncode))
            gone, still_alive = psutil.wait_procs(procs, timeout=3, callback=on_terminate)
            for p in still_alive:
                p.kill()
            self.p.kill()


class Function(ProcessBase):
    def __init__(self,
                 target,
                 args=(),
                 monitors=None,
                 params=None,
                 **kwdargs):
        """
        Create a process to run a function. This is run through a
        multiprocessing.Process and has the same requirements for target
        and args.

        Extra arguments starting with 'process_' are passed through to multiprocessing.Process.
        Arguments that are not explained here can be found in `ProcessBase`.

        :param target: Function or callable object
        :param args: Arguments passed to target
        """
        super().__init__(monitors=monitors, params=params)
        self.target = target
        self.args = args
        self.p = None
        self.result = mp.Queue()
        self.process_args = {}
        self.process_args.update(self._extract_arguments(kwdargs, 'process_'))

    def _run_setup(self):
        self.stdout = MPStream()
        self.stderr = MPStream()

        def wrap(target, stdout, stderr, results):
            def wrapper(*args, **kwargs):
                sys.stdout = stdout
                sys.stderr = stderr
                result = target(*args, **kwargs)
                results.put(result)
            return wrapper
        self.p = mp.Process(target=wrap(self.target, self.stdout, self.stderr, self.result),
                            args=self.args, **self.process_args)

    def _run_execute(self):
        result = None
        self.returncode = 999
        self.p.start()
        self.logger.info("Start multiprocess ({})".format(self.p.pid))
        while self.p.is_alive() and self.stop.empty():
            time.sleep(0.2)
        if self.p.is_alive():
            self.kill()
        else:
            self.returncode = 0
        if not self.result.empty():
            result = self.result.get()
        self.stdout.wait_until_empty(timeout=0.5)
        self.stderr.wait_until_empty(timeout=0.5)
        return result

    def _run_teardown(self):
        pass

    def get_characteristics(self):
        params = super().get_characteristics()
        params["function"] = {
            "target": str(self.target),
            "args": [str(arg) for arg in self.args]
        }
        return params

    def kill(self, reason=None, returncode=None):
        super().kill(reason=reason, returncode=returncode)
        if self.p is not None and self.p.is_alive():
            if reason is not None and reason != '':
                reason = ' ({})'.format(reason)
            else:
                reason = ''
            self.logger.info('Kill process{}'.format(reason))
            procs = psutil.Process(self.p.pid).children(recursive=True)
            procs += [psutil.Process(self.p.pid)]
            for p in procs:
                p.terminate()
            def on_terminate(proc):
                self.logger.info("Process {} terminated with exit code {}".format(proc, proc.returncode))
            gone, still_alive = psutil.wait_procs(procs, timeout=3, callback=on_terminate)
            for p in still_alive:
                p.kill()
            self.p.terminate()
