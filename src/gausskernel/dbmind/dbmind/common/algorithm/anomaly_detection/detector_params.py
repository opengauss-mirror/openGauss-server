# According to actual experience, the predicted result
# will be more perfect after the amount of data
# (length of the sequence) exceeds a certain level,
# so we set a threshold here based on experience
# to decide different detection behaviors.

EFFECTIVE_LENGTH = 2500

THRESHOLD = {
    "positive": (0.0, -float("inf")),
    "negative": (float("inf"), 0.0),
    "both": (-float("inf"), float("inf")),
    "neither": (float("inf"), -float("inf"))
}
