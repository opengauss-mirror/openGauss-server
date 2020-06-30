from . import knobs_ap
from . import knobs_tp
from . import knobs_htap


def get_rl_knobs(scenario):
    if scenario == 'ap':
        return knobs_ap.rl_knobs
    elif scenario == 'tp':
        return knobs_tp.rl_knobs
    else:
        return knobs_htap.rl_knobs


def get_pso_knobs(scenario):
    if scenario == 'ap':
        return knobs_ap.pso_knobs
    elif scenario == 'tp':
        return knobs_tp.pso_knobs
    else:
        return knobs_htap.pso_knobs

