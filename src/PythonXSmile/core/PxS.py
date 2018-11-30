import json
import os
import pandas as pd

from os.path import abspath, dirname

from .run import (generate_command, generate_monitor, run_process)
from .utils import debug_print
VERBOSITY = 0

PXS_DIR = dirname(dirname(abspath(__file__)))


class PxS(object):
    """
    Python x SMILE main class.

    This object acts as some kind of front-end to the SMILE engine, and
    is designed to behave in a scikit-learn like way.
    """
    pxs_dir = PXS_DIR

    default_log_fn = "PxS_log"
    default_to = 600

    default_cfg_fn = "config.json"
    default_res_fn = "out.csv"
    default_model_fname = "model.xdsl"

    fit_fn = os.path.join(".", "backend", "fit")
    predict_fn = os.path.join(".", "backend", "predict")

    def __init__(self):
        self.cwd = os.getcwd()
        return

    def gen_fit_cfg(self, train_fname, model_fname=None):

        if model_fname is None:
            model_fname = self.default_model_fname

        model_fname = os.path.join(self.cwd, model_fname)

        cfg = {"train_fname": train_fname,
               "model_fname": model_fname}

        return cfg

    def gen_predict_cfg(self, train_fname, targ_idx, miss_idx, out_fname=None, model_fname=None):

        if out_fname is None:
            out_fname = self.default_res_fn
        if model_fname is None:
            model_fname = self.default_model_fname

        out_fname = os.path.join(self.cwd, out_fname)
        model_fname = os.path.join(self.cwd, model_fname)
        
        cfg = {"test_fname":    train_fname,
               "out_fname":     out_fname,
               "model_fname":   model_fname,
               "miss_idx":      miss_idx,
               "targ_idx":      targ_idx}

        return cfg

    def save_config(self, cfg, cfg_fname=None):
        if cfg_fname is None:
            cfg_fname = self.default_cfg_fn

        with open(cfg_fname, 'w') as f:
            json.dump(cfg, f, indent=4)
        return

    def drop_log(self, log_fname, cwd):

        success_log_fnames = [os.path.join(cwd, f) for f in os.listdir(cwd)
                              if log_fname in f
                              if "success" in f]

        for f in success_log_fnames:
            os.remove(f)

        return

    def fit(self,
            train_fname,
            model_fname=None,
            cfg_fname=None,
            log_fname=None,
            timeout=None,
            cwd=None):

        # Parse arguments
        if cwd is None:
            cwd = self.cwd
        if log_fname is None:
            log_fname = self.default_log_fn + "_fit"
        if timeout is None:
            timeout = self.default_to
        if cfg_fname is None:
            cfg_fname = self.default_cfg_fn

        cfg_fname = os.path.join(cwd, cfg_fname)
        log_fname = os.path.join(cwd, log_fname)
        train_fname = os.path.join(cwd, train_fname)

        # Config
        cfg = self.gen_fit_cfg(train_fname, model_fname=model_fname)
        self.save_config(cfg, cfg_fname=cfg_fname)

        # Run - Prelims
        mon = generate_monitor(log_fname, timeout)
        cmd = generate_command(self.fit_fn,
                               cfg_fname,
                               script_prefix="",
                               config_prefix="-c")

        print(cmd)

        # Run
        p = run_process(cmd, monitors=mon, cwd=self.pxs_dir)

        if p == 0:
            os.remove(cfg_fname)
            self.drop_log(log_fname, cwd)

        return p

    def predict(self,
                test_fname,
                targ_idx,
                miss_idx,
                out_fname=None,
                model_fname=None,
                cfg_fname=None,
                log_fname=None,
                timeout=None,
                cwd=None):

        # Parse arguments
        if cwd is None:
            cwd = self.cwd
        if log_fname is None:
            log_fname = self.default_log_fn + "_fit"
        if timeout is None:
            timeout = self.default_to
        if cfg_fname is None:
            cfg_fname = self.default_cfg_fn

        cfg_fname = os.path.join(cwd, cfg_fname)
        log_fname = os.path.join(cwd, log_fname)
        test_fname = os.path.join(cwd, test_fname)

        # Config
        cfg = self.gen_predict_cfg(test_fname,
                                   targ_idx,
                                   miss_idx,
                                   out_fname=out_fname,
                                   model_fname=model_fname)
        self.save_config(cfg, cfg_fname=cfg_fname)

        # Run-Prelims
        mon = generate_monitor(log_fname, timeout)
        cmd = generate_command(self.predict_fn,
                               cfg_fname,
                               script_prefix="",
                               config_prefix="-c")

        # Run
        p = run_process(cmd, monitors=mon, cwd=self.pxs_dir)

        if p == 0:
            os.remove(cfg_fname)
            self.drop_log(log_fname, cwd)

            result = pd.read_csv(cfg["out_fname"], header=None)
            # os.remove(cfg["out_fname"])

            return result.values
        else:
            return p
