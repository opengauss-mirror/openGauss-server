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

from ...types import Sequence


def merge_contiguous_anomalies_timestamps(sequence_timestamps, anomaly_timestamps):
    temp_queue = list()
    anomaly_timestamp_list = list()
    if not anomaly_timestamps:
        return anomaly_timestamp_list

    p_seq = p_ano = 0
    while p_ano < len(anomaly_timestamps):
        if len(temp_queue) > 0 and sequence_timestamps[p_seq + 1] != anomaly_timestamps[p_ano]:
            anomaly_timestamp_list.append(temp_queue.copy())
            temp_queue.clear()

        while sequence_timestamps[p_seq] < anomaly_timestamps[p_ano]:
            p_seq += 1
        temp_queue.append(anomaly_timestamps[p_ano])
        p_ano += 1

    if len(temp_queue) > 0:
        anomaly_timestamp_list.append(temp_queue.copy())
        temp_queue.clear()

    return anomaly_timestamp_list


def pick_out_anomalies(sequence, anomalies, ignore_head=True, ignore_tail=True):
    """According to the detection results,
    pick out the samples detected as abnormal,
    wrapping them to a sequence object.
    Then merge the contiguous samples to construct a list of sequences.
    """
    sequence_list = list()
    sequence_timestamps = sequence.timestamps
    # Extract the sample while its value is True.
    anomaly_timestamps = [t for t, v in anomalies if v]
    anomaly_timestamp_list = merge_contiguous_anomalies_timestamps(
        sequence_timestamps,
        anomaly_timestamps
    )

    seq_start_ts, seq_end_ts = sequence_timestamps[0], sequence_timestamps[-1]
    for timestamps in anomaly_timestamp_list:
        ano_start_ts, ano_end_ts = timestamps[0], timestamps[-1]
        if (ignore_head and ano_start_ts == seq_start_ts) or (ignore_tail and seq_end_ts == ano_end_ts):
            continue
        sequence_list.append(sequence[ano_start_ts, ano_end_ts])

    return sequence_list


def remove_edge_effect(s: Sequence, window):
    values = list(s.values)
    length = len(values)
    left_idx = window - 1 - (window - 1) // 2
    right_idx = length - 1 - (window - 1) // 2
    for i in range(0, left_idx):  # padding left
        values[i] = False

    for i in range(right_idx, length):  # padding right
        values[i] = False
    return Sequence(timestamps=s.timestamps, values=values)


def over_max_coef(coef, side, threshold):
    if side == "positive":
        return coef >= threshold
    elif side == "negative":
        return abs(coef) >= threshold
