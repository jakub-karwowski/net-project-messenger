# Report figures and tables

`make_figures.py` builds every figure and table in `report/tex` from one results
directory (by default `results-0294f37`):

```
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python make_figures.py                       # writes ../tex/figures/*.pdf, ../tex/tables/*.tex
.venv/bin/python make_figures.py --results ../../results-<commit> --out ../tex
```

- Inputs:
  - `summary.csv` (medians and IQR over valid repetitions, from `scripts/analyze.py`);
  - for S4, the per-repetition `rep*.meta.json`, `rep*.stress.json` and
    `rep*.metrics.jsonl` files (`send_failures` against p99).
- `style.py` holds the shared encoding: hue gives the concurrency family (threaded,
  poll, epoll), and line style and marker give the socket topology (simplex, duplex,
  split).

The handshake diagrams are PlantUML sequence diagrams in `handshake.puml`.
`make_handshake.sh` renders them to `../tex/figures/handshake_{simplex,duplex}.pdf`;
it needs `plantuml` and `rsvg-convert` on `PATH`
(for example `conda install -c conda-forge plantuml librsvg`).

Then build the PDF from `report/tex` with any LaTeX engine, for example
`tectonic main.tex` or `latexmk -pdf main.tex`.
