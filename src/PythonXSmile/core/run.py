import numpy as np

from ..experimenter.monitor import Logfile, TimeLimit
from ..experimenter.process import Process

from .utils import debug_print
VERBOSITY = 0


# Main
def generate_command(script_fname,
                     config_fname,
                     script_prefix="python",
                     config_prefix="",
                     fold=None):
    """
    Generate the command to be executed.

    A command is a very specifically formatted string, to be entered in a
    terminal. When this happens, the process linked to this command will be
    executed by the computer.

    Parameters
    ----------
    script_fname: str
        Filename of script
    config_fname: str
        Filename of config file
    script_prefix: str, default="python"
        First entry of the command. Usually specifies the programming language.
        If this is the empty string, it is completely ignored and not added to
        the cmd array.
    config_prefix: str, default=""
        Flag that precedes the config_fname. If this is the empty string, it is
        completely ignored and not added to the cmd array.
            E.g.; $ -c config.json
    fold: int, default=None
        Fold that has to run.

    Returns
    -------
    cmd: array[str]
        Array of strings that represent the things that are entered in the
        terminal.

    """

    assert isinstance(script_fname, str)
    assert isinstance(config_fname, str)
    assert isinstance(script_prefix, str)
    assert isinstance(config_prefix, str)

    cmd = []

    if len(script_prefix) > 0:
        cmd.append(script_prefix)

    cmd.append(script_fname)

    if len(config_prefix) > 0:
        cmd.append(config_prefix)

    cmd.append(config_fname)

    if fold is not None:
        assert isinstance(fold, int)
        cmd.append(str(fold))

    return cmd


def generate_monitor(log_fname, timeout):
    """
    Create a monitor to go with a command.

    A monitor's job is to monitor a process. So, instead of just executing a
    command (i.e., running a process), we first build a monitor, and then pass
    monitor and command together to a dedicated Process class.

    This Process class executes the command, but also uses the monitor to
    capture logs and guard the timeout.

    Parameters
    ----------
    log_fname: str
        Filename of log life
    timeout: int
        Timeout in seconds. Script is automatically aborted after this period.

    Returns
    -------

    """
    assert isinstance(log_fname, str)
    assert isinstance(timeout, (int, np.int64))

    msg = """
    File is:        {}\n
    log_fname is:   {}\n
    timeout is:     {}\n
    """.format(__file__, log_fname, timeout)
    debug_print(msg, V=VERBOSITY)

    monitors = [Logfile(log_fname),
                TimeLimit(timeout)]

    return monitors


def run_process(command, monitors=None, cwd=None):
    """
    Execute the command, monitored by the specified monitors.

    Parameters
    ----------
    command: list, shape(nb_strings, )
        List of strings that constitute the command to be entered in the
        terminal
    monitors: list, shape (nb_monitors)
        List of monitors, which will be used to monitor the process that
        executes the command.

    Returns
    -------

    """
    if monitors is None:
        msg = """
        Running process without monitors
        """
        print(msg)
    p = Process(command, monitors=monitors, cwd=cwd)  # Init Process
    return p.run()
