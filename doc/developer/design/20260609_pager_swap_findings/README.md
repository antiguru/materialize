# Raw benchmark data

Backing data for [`../20260609_pager_swap_findings.md`](../20260609_pager_swap_findings.md).
All runs: `correction_v2_pager` bench example, AWS box (247 GiB RAM, 32 cores),
local nvme instance store for `/mnt/scratch` (~1.7 GB/s sustained write), under
`systemd-run --scope -p MemoryMax=… -p MemorySwapMax=…` cgroups.

CSV columns: `config,backend,num_ts,[threads,]pattern,prop,budget_mib,fill_ms,
drain_ms,rss_fill_kib,rss_fill_peak_kib,rss_end_kib,pageouts,out_mib,out_raw_mib,
pageins,pagein_mib,write_mibps` (later sweeps add a `threads` column and use
purpose-built headers — see each file).

| file | what |
|---|---|
| `step1.csv` | write throughput / amplification, file backend, fill-only (EBS-era; superseded by local-nvme runs) |
| `step2.csv`, `step2-tuned.csv` | swap vs file under 256 MiB cap + perf fault counts; vm.dirty_* tuning |
| `step4-large.csv` | RSS/volume scaling 2M–8M ts (RSS flat ~90 MiB; write vol superlinear) |
| `conc.csv` | concurrency, no cap — writes absorbed by page cache (RAM-bandwidth, not disk) |
| `conc2.csv` | concurrency, forced writeback (shared cgroup) — file vs lz4 crossover |
| `conc3-swap.csv` | swap backend under forced-writeback cgroup |
| `fixedcap.csv` | fixed 180 MiB envelope, scale data 4M/8M/32M, swap/file/lz4 |
| `matrix.csv` | baseline-swap + lz4-swap at 180 MiB, 4M/8M/32M |
| `matrix64.csv` | 64M extension (pager-file OOMs; lz4-file survives) |
| `bigN.csv` | 16M/32M scaling (partial; pivoted to parallelism) |
| `threads.csv` | **parallelism sweep, 8M, T=1..32, 180 MiB cap** — the decisive result (file beats swap T≥4; lz4-file best at T=16) |
| `threads_drain.csv` | drain-inclusive parallelism (drain is backend-agnostic) |
| `step2-faults.txt`, `step2-tuned-faults.txt` | `perf stat` page-fault counts |
| `maps.txt` | process maps (ASLR-off) for offline addr2line of swap-in stacks |
| `swap-faults2.bt.txt` | bpftrace `do_swap_page` user-stack attribution (drain readback faults) |

NOT included (too large): samply profiles `swap-contended.json`,
`swap-willneed.json`, `baseline-contended.json`, `file-contended.json` (~4–5 MB
each); their top-functions analysis is inline in the findings doc.
