"""
One chart style for every EDA and model figure.

Colors follow the job they do:
  - incident types: fixed categorical order, the same color in every chart
  - normal rides: neutral gray (the reference everything is compared with)
  - hard negatives: a lighter gray, they are "normal" too
"""

import matplotlib.pyplot as plt

from ml.config import IMAGES_DIR

INCIDENT_COLORS = {
    "route_deviation": "#2a78d6",
    "long_stop":       "#eb6834",
    "gps_spoofing":    "#1baf7a",
    "wrong_dropoff":   "#6250d6",
    "possible_crash":  "#e34948",
}
NORMAL_COLOR = "#52514e"
HARD_NEGATIVE_COLOR = "#a8a7a1"
ACCENT = "#2a78d6"
INK, INK_MUTED, GRID = "#0b0b0b", "#52514e", "#e6e5e0"


def color_of(name: str) -> str:
    if name in INCIDENT_COLORS:
        return INCIDENT_COLORS[name]
    return HARD_NEGATIVE_COLOR if name.startswith(("innocent", "gps_dropout")) else NORMAL_COLOR


def use():
    plt.rcParams.update({
        "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb", "savefig.facecolor": "#fcfcfb",
        "figure.dpi": 110, "savefig.dpi": 140, "savefig.bbox": "tight",
        "font.size": 10, "axes.titlesize": 11, "axes.titleweight": "bold", "axes.titlelocation": "left",
        "axes.edgecolor": GRID, "axes.labelcolor": INK_MUTED, "text.color": INK,
        "xtick.color": INK_MUTED, "ytick.color": INK_MUTED,
        "axes.grid": True, "axes.axisbelow": True, "grid.color": GRID, "grid.linewidth": 0.8,
        "axes.spines.top": False, "axes.spines.right": False,
        "legend.frameon": False, "lines.linewidth": 2,
    })


def save(fig, name: str):
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(IMAGES_DIR / f"{name}.png")
