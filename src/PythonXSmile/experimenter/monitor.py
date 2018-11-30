"""
experimenter.monitor - Monitor a Process

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

import os
import sys
import time
import logging
import resource
import psutil
import json
import datetime

from .utils import levels, level_prefix, level_type, print_orig

logger = logging.getLogger("be.kuleuven.cs.dtai.experimenter")


def data_to_str(data, depth=0):
    if type(data) in [list, set, tuple]:
        return "["+"; ".join([data_to_str(datum, depth+1) for datum in data])+"]"
    if type(data) == dict:
        if depth == 0 and any([type(datum) in [list, set, tuple, dict] for datum in data.values()]):
            return json.dumps(data, indent=2)
        return "; ".join(["{}:{}".format(key, data_to_str(datum, depth+1)) for key, datum in data.items()])
    if type(data) == float:
        return "{:.5f}".format(data)
    return str(data)


class ProcessMonitor:
    def __init__(self, interval=1, run_as_thread=False,
                 listen_to_output=False):
        """
        Class that monitors the process.

        Send a tuple (returncode, reason) to the self.stop queue to halt the process.
        The process itself is available as self.parent.
        :param interval: Time in between checks (seconds)
        :param run_as_thread: Register to list of verification monitors.
        :param listen_to_output: Register to list of listeners.
        """

        self.active = False
        self.stop = None  # Stop queue(returncode, reason)
        self.parent = None
        self.output = None  # Messages queue(identifier, msg)
        self.interval = float(interval)  # Seconds
        self.run_as_thread = run_as_thread
        self.listen_to_output = listen_to_output

    def __del__(self):
        self._tear_down(returncode=999)

    def set_up(self, parent,
               stop, output):
        self.active = True
        self.stop = stop
        self.parent = parent
        self.output = output
        self._set_up(parent)

    def _set_up(self, parent):
        pass

    def tear_down(self, returncode=0):
        self.active = False
        self._tear_down(returncode)

    def _tear_down(self, returncode):
        pass

    def get_settings(self):
        """Overview of relevant settings."""
        return dict()

    def verify(self):
        """
        :return: (returncode, description) or None verification succeeded
        """
        return None

    def run(self):
        if not self.run_as_thread:
            return
        while self.stop.empty() and self.active:
            result = self.verify()
            if result is not None:
                returncode, description = result
                self.stop.put((returncode, description))
            time.sleep(self.interval)

    def log(self, message, identifier=levels.STDOUT):
        self.output.put((identifier, message))

    def info(self, message):
        self.output.put((levels.INFO, message))

    def warn(self, message):
        self.output.put((levels.WARNING, message))

    def error(self, message):
        self.output.put((levels.ERROR, message))

    def status(self, message):
        self.output.put((levels.STATUS, message))

    def get_log(self, msg, identifier):
        pass


class Logfile(ProcessMonitor):
    def __init__(self, fn=None, force=False):
        """
        Write all output to a logfile. The logfile is also used as a cache and
        to avoid running two identical processes simultaneously.

        :param fn: Filename
        :param force: Proceed even if a successful log file exists
        """
        super().__init__(listen_to_output=True)
        self.force = force
        if fn is None:
            self.fn = None
            self.fnrunning = None
            self.fnsuccess = None
            self.fnfailure = None
        else:
            self.fn = fn
            self.fnrunning = self.fn + ".running.log"
            self.fnsuccess = self.fn + ".success.log"
            self.fnfailure = self.fn + ".failure.log"
        self.file = None

    def open(self):
        if self.fn is None:
            self.file = None
        else:
            self.info('Write log to {}'.format(self.fnrunning))
            self.file = open(self.fnrunning, 'w')

    def get_settings(self):
        return {
            "logfile": self.fn
        }

    def verify(self):
        if self.is_running():
            self.warn('Skipping, running logfile exists (remove manually): {}'.format(self.fnrunning))
            return 999, "already_running"
        if self.is_success():
            if self.force:
                self.warn('Rerunning experiment (successful logfile did already exists: {})'.format(self.fnsuccess))
            else:
                self.warn('Skipping, successful logfile exists: {}'.format(self.fnsuccess))
                return 999, "cached_version"
        return None

    def close(self, returncode=None):
        """
        Close the logfile.
        :param returncode: The process' return code.
        :return: None
        """
        if self.file is None:
            return
        self.info('Closing {}'.format(self.file.name))
        self.file.close()
        self.file = None
        if returncode is not None and returncode == 0:
            os.rename(self.fnrunning, self.fnsuccess)
            self.info('Renamed logfile to {}'.format(self.fnsuccess))
        else:
            os.rename(self.fnrunning, self.fnfailure)
            self.info('Renamed logfile to {}'.format(self.fnfailure))

    def _set_up(self, parent):
        result = self.verify()
        if result is not None:
            returncode, description = result
            self.stop.put((returncode, description))
        else:
            self.open()

    def _tear_down(self, returncode=0):
        self.close(returncode)

    def is_success(self):
        return self.fnsuccess is not None and os.path.exists(self.fnsuccess)

    def is_running(self):
        return self.fnrunning is not None and os.path.exists(self.fnrunning)

    def is_failed(self):
        return self.fnfailure is not None and os.path.exists(self.fnfailure)

    def get_log(self, msg, identifier):
        if self.file is None or self.file.closed:
            return
        msg = data_to_str(msg)
        if identifier >= levels.STDOUT:
            print(level_prefix[identifier]+msg, file=self.file, end='')
        else:
            print(level_prefix[identifier]+msg, file=self.file)


class Print(ProcessMonitor):
    def __init__(self, log_lvl=levels.INFO, include_stdout=True):
        """Print class

        :param log_lvl: Log level (from utils.levels: ERROR, WARNING, INFO, DEBUG)
        :param include_stdout: Print stdout and stderr to the console
        """
        super().__init__(listen_to_output=True)
        self.log_lvl = log_lvl
        self.include_stdout = include_stdout
        self.stdout = sys.stdout

    def get_log(self, msg, identifier):
        if identifier < self.log_lvl:
            return
        msg = data_to_str(msg)
        if identifier >= levels.STDOUT:
            if self.include_stdout:
                print_orig(level_prefix[identifier] + msg, file=self.stdout, end='')
        else:
            print_orig(level_prefix[identifier] + msg, file=self.stdout)


class JsonFile(ProcessMonitor):
    def __init__(self, fn, log_lvl=levels.SETTINGS, include_stdout=False):
        """Save to JSON file

        :param fn: Filename for json file
        :param log_lvl: Log level (from utils.levels: ERROR, WARNING, INFO, DEBUG)
        :param include_stdout: Include stdout output also in the json file
        """
        super().__init__(listen_to_output=True)
        self.fn = fn
        self.log_lvl = log_lvl
        self.include_stdout = include_stdout
        self.file = None
        self.first = True

    def _set_up(self, parent):
        self.file = open(self.fn, 'w')
        print("[", file=self.file)

    def _tear_down(self, returncode=0):
        if self.file is None or self.file.closed:
            return
        print("\n]", file=self.file)
        self.file.close()
        self.file = None

    def write(self, data, identifier):
        if self.file is None or self.file.closed:
            return
        row = {
            "ts": datetime.datetime.utcnow().isoformat(),
            "type": level_type[identifier],
            "values": data
        }
        if self.first:
            self.first = False
        else:
            print(",\n", file=self.file, end='')
        print(json.dumps(row, indent=2), file=self.file, end='')

    def get_log(self, data, identifier):
        if identifier < self.log_lvl:
            return
        if identifier >= levels.STDOUT:
            if self.include_stdout:
                self.write(data, identifier)
        else:
            self.write(data, identifier)


class MemoryLimit(ProcessMonitor):
    def __init__(self, maxmem=None, minavailable=None):
        """Add a process monitor for memory usage.
        :param maxmem: Kill process if memory usage exceeds this limit (in MB). If set to 0 it will print memory usage
            without killing the process.
        :param minavailable: Minimal amount of memory that should still be available on the machine (in MB).
        """
        super().__init__(run_as_thread=True)
        self.maxmem = float(maxmem) if maxmem else None
        self.minavailable = float(minavailable) if minavailable else None

    def _set_up(self, parent):
        if self.maxmem is None and self.minavailable is None:
            self.active = False

    def get_settings(self):
        return {
            "memory_limit": self.maxmem,
            "memory_minavailable": self.minavailable
        }

    def verify(self):
        if not self.active:
            return None
        rusage_denom = 1024 * 1024
        # Using resource
        usage = resource.getrusage(resource.RUSAGE_SELF)
        mem_1 = usage.ru_maxrss / rusage_denom
        # Using psutil
        mem_2 = 0  # p.memory_info().rss
        mem_3 = 0  # p.memory_info().vms
        mem_p = 0  # p.memory_percent()
        cpu_p = 0  # p.cpu_percent()
        cpu_u = 0  # p.cpu_times().user
        cpu_s = 0  # p.cpu_times().system
        mem_a = 0
        try:
            vm = psutil.virtual_memory()
            mem_a = vm.available/rusage_denom
            p = psutil.Process()  # psutil.Process(self.parent.p.pid)
            for child in p.children(recursive=True):
                mem_2 += child.memory_info().rss / rusage_denom
                mem_3 += child.memory_info().vms / rusage_denom
                mem_p += child.memory_percent()
                cpu_p += child.cpu_percent()
                cpu_u += child.cpu_times().user
                cpu_s += child.cpu_times().system
        except psutil.AccessDenied:
            pass
        self.status({
            "exp.usertime": usage.ru_utime,
            "exp.systime": usage.ru_stime,
            "exp.mem(MiB)": mem_1,
            "usertime": cpu_u,
            "systime": cpu_s,
            "mem(MiB)": mem_2,
            "vms(MiB)": mem_3,
            "%mem": mem_p,
            "%cpu": cpu_p,
            "sys.available(MiB)": mem_a
        })
        if self.maxmem is not None and self.maxmem != 0 and mem_2 > self.maxmem:
            self.warn("Memory limit reached, stopping.")
            return 999, 'memory_limit'
        if self.minavailable is not None and self.minavailable != 0 and mem_a < self.minavailable:
            self.warn("Not enough memory available, stopping.")
            return 999, 'memory_low'
        return None


class TimeLimit(ProcessMonitor):
    def __init__(self, walltime):
        """Monitor the process and kill it after a certain period of time.
        :param walltime: Maximum duration a process runs.
        """
        super().__init__(run_as_thread=True)
        self.maxtime = float(walltime) if walltime else None
        self.start_time = 0
        self.end_time = 0

    def _set_up(self, parent):
        self.start_time = time.time()
        if self.maxtime is None:
            self.active = False
        else:
            self.end_time = self.start_time + self.maxtime

    def get_settings(self):
        return {
            "max_time": self.maxtime
        }

    def verify(self):
        if self.maxtime is not None and time.time() > self.end_time:
            return 999, "time_limit"
        return None


class FileSizeLimit(ProcessMonitor):
    def __init__(self, filename, maxsize=None):
        """Monitor a given file and kill the process if it exceeds a certain size.
        :param filename: File to monitor
        :param maxsize: Maximal size the file can have (MiB)
        """
        super().__init__(run_as_thread=True)
        self.filename = filename
        self.maxsize = maxsize

    def get_settings(self):
        return {
            "max_filesize": self.maxsize
        }

    def verify(self):
        try:
            if os.path.exists(self.filename):
                size = os.path.getsize(self.filename) / (1024*1024)
                self.status({
                    "filename": self.filename,
                    "filesize(MiB)": size
                })
                if self.maxsize is not None and size > self.maxsize:
                    return 999, "filesize_limit"
            else:
                self.warn('filesize {}: File not found'.format(self.filename))
        except OSError:
            return 999, "file does not exist"
        return None


class DiskSpaceLimit(ProcessMonitor):
    def __init__(self, minavailable=None):
        """Monitor a the available disk space.
        :param minavailable: Minimal remaining free space on disk (MiB)
        """
        super().__init__(run_as_thread=True)
        self.minavailable = minavailable

    def get_settings(self):
        return {
            "diskspace_minavailable": self.minavailable
        }

    def verify(self):
        try:
            vfs = os.statvfs("/")
            size_available = (vfs.f_bavail * vfs.f_frsize) / (1024*1024)
            self.status({
                "diskspace.available(MiB)": size_available
            })
            if self.minavailable is not None and size_available < self.minavailable:
                return 999, "diskspace_low"
        except OSError:
            return 999, "file does not exist"
        return None
