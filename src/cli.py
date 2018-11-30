# region Imports
import os
import sys

from os.path import dirname, abspath

# Custom imports
this_dir = dirname(abspath(__file__))
root_dir = dirname(dirname(this_dir))
src_dir = os.path.join(root_dir, 'src')
sys.path.append(src_dir)

import exp
from exp.utils.run import run_script, generate_command, run_process,generate_monitor
from exp.utils.extra import debug_print
VERBOSITY = 1
# endregion


def main():
    # Init
    #fit_fn = "."+os.path.join(this_dir, "resc", "fit")
    #cfg_fn = os.path.join(this_dir, "resc", "config.json")
    #log_fn = os.path.join(this_dir, "prod", "log.txt")

    fit_fn = "./resc/fit"
    cfg_fn = "resc/config.json"
    log_fn = "prod/log.txt"

    # Setup
    cmd = generate_command(fit_fn, cfg_fn, script_prefix="", config_prefix="-c")
    mon = generate_monitor(log_fn, 360)

    #cmd = ["chmod","-R","777","/home/elia/Dropbox/Files/KUL/research/codebases/homework/libs/PxS2/resc/fit"]
    #cmd = ["./resc/predict", "-c", "resc/config.json"]

    msg = """
        cmd:    {}
        """.format(cmd)
    debug_print(msg, V=VERBOSITY)
    run_process(cmd, monitors=mon, cwd=this_dir)

    return


if __name__ == '__main__':
    main()