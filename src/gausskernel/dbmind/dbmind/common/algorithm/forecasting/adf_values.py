# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

import numpy as np
from scipy.stats import norm

tau_star_nc = -1.04
tau_min_nc = -19.04
tau_max_nc = np.inf
tau_star_c = -1.61
tau_min_c = -18.83
tau_max_c = 2.74
tau_star_ct = -2.89
tau_min_ct = -16.18
tau_max_ct = 0.7
tau_star_ctt = -3.21
tau_min_ctt = -17.17
tau_max_ctt = 0.54

_tau_maxs = {
    0: tau_max_nc,
    1: tau_max_c,
    2: tau_max_ct,
    3: tau_max_ctt,
}
_tau_mins = {
    0: tau_min_nc,
    1: tau_min_c,
    2: tau_min_ct,
    3: tau_min_ctt,
}
_tau_stars = {
    0: tau_star_nc,
    1: tau_star_c,
    2: tau_star_ct,
    3: tau_star_ctt,
}

small_scaling = np.array([1, 1, 1e-2])
tau_nc_smallp = np.array([0.6344, 1.2378, 3.2496]) * small_scaling
tau_c_smallp = np.array([2.1659, 1.4412, 3.8269]) * small_scaling
tau_ct_smallp = np.array([3.2512, 1.6047, 4.9588]) * small_scaling
tau_ctt_smallp = np.array([4.0003, 1.658, 4.8288]) * small_scaling
_tau_smallps = {
    0: tau_nc_smallp,
    1: tau_c_smallp,
    2: tau_ct_smallp,
    3: tau_ctt_smallp,
}

large_scaling = np.array([1, 1e-1, 1e-1, 1e-2])
tau_nc_largep = np.array([0.4797, 9.3557, -0.6999, 3.3066]) * large_scaling
tau_c_largep = np.array([1.7339, 9.3202, -1.2745, -1.0368]) * large_scaling
tau_ct_largep = np.array([2.5261, 6.1654, -3.7956, -6.0285]) * large_scaling
tau_ctt_largep = np.array([3.0778, 4.9529, -4.1477, -5.9359]) * large_scaling
_tau_largeps = {
    0: tau_nc_largep,
    1: tau_c_largep,
    2: tau_ct_largep,
    3: tau_ctt_largep,
}


def mackinnonp(teststat, trend_order=1):
    if trend_order not in [0, 1, 2, 3]:
        raise ValueError("trend order %s not understood" % trend_order)

    maxstat = _tau_maxs[trend_order]
    minstat = _tau_mins[trend_order]
    starstat = _tau_stars[trend_order]
    if teststat > maxstat:
        return 1.0
    elif teststat < minstat:
        return 0.0

    if teststat <= starstat:
        tau_coef = _tau_smallps[trend_order]
    else:
        tau_coef = _tau_largeps[trend_order]

    return norm.cdf(np.polyval(tau_coef[::-1], teststat))


tau_nc_2010 = np.array([-2.56574, -1.94100, -1.61682])
tau_c_2010 = np.array([-3.43035, -2.86154, -2.56677])
tau_ct_2010 = np.array([-3.95877, -3.41049, -3.12705])
tau_ctt_2010 = np.array([-4.37113, -3.83239, -3.55326])
tau_2010s = {
    0: tau_nc_2010,
    1: tau_c_2010,
    2: tau_ct_2010,
    3: tau_ctt_2010,
}


def mackinnoncrit(trend_order=1):
    if trend_order not in [0, 1, 2, 3]:
        raise ValueError("trend order %s not understood" % trend_order)

    return tau_2010s[trend_order]
