import json
import os
import pandas as pd
from timeit import default_timer

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
        self.s = {'model_data': {'ind_time': -1,
                                 'inf_time': -1}}
        return

    def gen_fit_cfg(self, train_fname, model_fname=None):

        if model_fname is None:
            model_fname = self.default_model_fname

        if not os.path.isabs(model_fname):
            model_fname = os.path.join(self.cwd, model_fname)

        cfg = {"train_fname": train_fname,
               "model_fname": model_fname}

        return cfg

    def gen_predict_cfg(self, train_fname, targ_idx, miss_idx, out_fname=None, model_fname=None):

        if out_fname is None:
            out_fname = self.default_res_fn
        if model_fname is None:
            model_fname = self.default_model_fname

        if not os.path.isabs(out_fname):
            out_fname = os.path.join(self.cwd, out_fname)
        if not os.path.isabs(model_fname):
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

    def drop_log(self, log_fname):

        success_log_fnames = [f for f in os.listdir(self.cwd)
                              if os.path.basename(log_fname) in f
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
            cwd=None,
            **kwargs):

        # Parse arguments
        if cwd is None:
            cwd = self.cwd
        if log_fname is None:
            log_fname = self.default_log_fn + "_fit"
        if timeout is None:
            timeout = self.default_to
        if cfg_fname is None:
            cfg_fname = self.default_cfg_fn

        if not os.path.isabs(cfg_fname):
            cfg_fname = os.path.join(cwd, cfg_fname)
        if not os.path.isabs(log_fname):
            log_fname = os.path.join(cwd, log_fname)
        if not os.path.isabs(train_fname):
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

        msg = """
        Generated command: {}
        """.format(cmd)
        debug_print(msg, V=VERBOSITY)

        # Run
        tick = default_timer()
        p = run_process(cmd, monitors=mon, cwd=self.pxs_dir)
        tock = default_timer()
        self.s['model_data']['ind_time'] = tock - tick

        try:
            os.remove(cfg_fname)
            self.drop_log(log_fname)

            return p
        except FileNotFoundError as e:
            msg = """
            Error:                                  {}
            Return code from .backend/predict:      {}                         
            """.format(e.args[-1], p)
            print(msg)
            return p

    def predict(self,
                test_fname,
                targ_idx,
                miss_idx,
                out_fname=None,
                model_fname=None,
                cfg_fname=None,
                log_fname=None,
                q_idx = None,
                timeout=None,
                cwd=None,
                **kwargs):

        # Parse arguments
        if cwd is None:
            cwd = self.cwd
        if log_fname is None:
            log_fname = self.default_log_fn + "_predict"
        if timeout is None:
            timeout = self.default_to
        if cfg_fname is None:
            cfg_fname = self.default_cfg_fn

        if not os.path.isabs(cfg_fname):
            cfg_fname = os.path.join(cwd, cfg_fname)
        if not os.path.isabs(log_fname):
            log_fname = os.path.join(cwd, log_fname)
        if not os.path.isabs(test_fname):
            test_fname = os.path.join(cwd, test_fname)

        if q_idx is not None:
            assert isinstance(q_idx, int)
            log_fname = log_fname + "_Q" + str(q_idx).zfill(4)

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

        msg = """
        Generated command: {}
        """.format(cmd)
        debug_print(msg, V=VERBOSITY)

        # Run
        tick = default_timer()
        p = run_process(cmd, monitors=mon, cwd=self.pxs_dir)
        tock = default_timer()
        self.s['model_data']['inf_time'] = tock - tick

        try:
            os.remove(cfg_fname)
            self.drop_log(log_fname)

            result = pd.read_csv(cfg["out_fname"], header=None)
            os.remove(cfg["out_fname"])

            return result.values

        except FileNotFoundError as e:
            msg = """
            Error:                                  {}
            Return code from .backend/predict:      {}                         
            """.format(e.args[-1], p)
            print(msg)
            return -1