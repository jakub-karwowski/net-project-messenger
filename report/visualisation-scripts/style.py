"""Shared visual encoding for every report figure.

Nine variants exceed a categorical palette's eight hues, so the encoding is
composite: hue = concurrency family (threaded, poll, epoll), line style and
marker = socket topology (simplex: solid/circle, duplex: dashed/square,
split: dotted/triangle). pool-sender is told apart from threads by its diamond.
The three hues are the first three slots of the reference palette, which
validate on all pairs for colour-vision deficiency.
"""
import matplotlib as mpl

VARIANTS = ["threads", "pool-sender", "peer-threads",
            "poll", "poll-duplex", "poll-split",
            "epoll", "epoll-duplex", "epoll-split"]

FAMILY_COLOR = {"threaded": "#2a78d6", "poll": "#eb6834", "epoll": "#1baf7a"}
TEXT = "#0b0b0b"
TEXT_2 = "#52514e"
GRID = "#e4e3df"

_FAMILY = {"threads": "threaded", "pool-sender": "threaded", "peer-threads": "threaded",
           "poll": "poll", "poll-duplex": "poll", "poll-split": "poll",
           "epoll": "epoll", "epoll-duplex": "epoll", "epoll-split": "epoll"}
_SHAPE = {"threads": ("-", "o"), "pool-sender": ("-", "D"), "peer-threads": ("--", "s"),
          "poll": ("-", "o"), "poll-duplex": ("--", "s"), "poll-split": (":", "^"),
          "epoll": ("-", "o"), "epoll-duplex": ("--", "s"), "epoll-split": (":", "^")}


def color(variant):
    return FAMILY_COLOR[_FAMILY[variant]]


def line_kw(variant):
    ls, marker = _SHAPE[variant]
    return dict(color=color(variant), linestyle=ls, marker=marker, linewidth=1.6,
                markersize=5, markeredgecolor="white", markeredgewidth=0.8, label=variant)


def setup():
    mpl.rcParams.update({
        "font.family": "serif",
        "font.size": 8.5,
        "axes.titlesize": 9,
        "axes.labelsize": 8.5,
        "axes.edgecolor": TEXT_2,
        "axes.labelcolor": TEXT,
        "axes.linewidth": 0.6,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.color": GRID,
        "grid.linewidth": 0.5,
        "xtick.color": TEXT_2,
        "ytick.color": TEXT_2,
        "xtick.labelsize": 7.5,
        "ytick.labelsize": 7.5,
        "legend.fontsize": 7.5,
        "legend.frameon": False,
        "figure.dpi": 150,
        "savefig.bbox": "tight",
        "pdf.fonttype": 42,
    })


def shared_legend(fig, axes, ncol=9):
    handles, labels = axes.get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=ncol,
               bbox_to_anchor=(0.5, -0.02), handlelength=2.6, columnspacing=1.2)
