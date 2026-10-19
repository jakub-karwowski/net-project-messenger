# Experiment scripts (python3 stdlib only)

    # run (pins node to cpus 0-1, stressgen to 4-7); resumable, safe to re-run
    scripts/run_matrix.py --app build/app --stressgen build/stressgen --out results
    scripts/run_matrix.py --quick --reps 1          # smoke: 2s runs, small peer sets
    scripts/run_matrix.py --scenarios S1,S5 --variants epoll,poll --reps 3
    scripts/run_matrix.py --fake --out /tmp/fake     # synthetic data, no binaries
    # analyze
    scripts/analyze.py results                       # -> results/summary.csv, results/report.html
    python3 scripts/test_analyze.py                  # helper unit tests

Scenarios (table at top of `run_matrix.py`): S1 fan-in (peers), S2 idle-heavy
(idle_peers), S3 slow consumers (slow_peers), S4 churn, S5 throughput ceiling (size).
`--max-peers` is set to peers+idle+slow+16 so caps never evict.

Layout: `results/<scenario>/<param>=<value>/<variant>/rep<N>.{stress.json,metrics.jsonl,node.log,meta.json,stressgen.txt}`.
A cell is skipped on re-run if its meta.json status is `ok` or `dirty` (node did not
exit cleanly after /quit); `failed` cells are retried. Variant order is shuffled per
(scenario, param, rep) to avoid drift bias.

Analysis: CPU% and ctx-switch rates use the metrics.jsonl window from `warmup` seconds
after frames_in first grows to the last growth; RSS/threads are peaks. Table cells show
median [Q1-Q3] over reps. Failed/dirty/corrupt cells are listed under "Problems" in the report.
