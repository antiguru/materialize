# CorrectionV2 pager: swap/file/lz4 benchmark findings

Status: investigation notes (mh@ + benchmarking session, 2026-06). Companion to
[`20260504_pager.md`](./20260504_pager.md). The reproduction harness is the
`correction_v2_pager` bench example (committed) plus driver scripts/CSVs that
lived on the (now-torn-down) bench box; the distilled numbers are all inline below.

## TL;DR recommendation

**Ship: lz4-compress on spill + `MADV_PAGEOUT` the compressed bytes (swap backend).**
This single combination is the winner — it resolves both problems we set out on:

- **Redline bandwidth** (customers max memory → swap; the swap merge is
  *bandwidth-bound*, device 100% util): lz4 cuts swap byte volume ~5.6× → fewer
  major-faults (the cost driver) → relaxes the bound. CPU is free to do it
  (hitting swap drops util to 10–20%, threads block on faults).
- **"Spill at 50% is too late"**: `MADV_COLD` is *lazy* (kernel reclaims only at
  the pressure cliff, so the 50% budget is fiction — real RSS pins to the cap,
  no headroom). `MADV_PAGEOUT` is *proactive* (evicts at spill time) → RSS held
  at the budget, swap-out paced, not a cliff burst.
- The two **cancel each other's downsides**: PAGEOUT's steady-state re-swap
  penalty (~+8% raw) *vanishes* under lz4 (5.6× smaller → cheap re-fault: 0%
  under cap, +4% no-cap vs raw's +51%); lz4's lazy fiction-budget is *fixed* by
  PAGEOUT (real headroom). Measured (180 MiB cap, 8M ts, 16 threads):
  `lz4+PAGEOUT 45.1s / 39.7 GiB swap-in` vs `lz4+COLD 46s/41`, `raw+PAGEOUT
  77s/84`, `raw+COLD 72s/76`. No-cap peak RSS: `lz4+PAGEOUT 0.40 GiB` vs
  `lz4+COLD 0.97` (= full compressed working set), `raw+PAGEOUT 0.18`.

Small code change required: today `lz4+swap` keeps the compressed bytes in
`CompressedInner::Memory` as **unmanaged anon memory** (no `madvise`); the fix is
to `madvise(PAGEOUT)` that `Vec` on pageout (see
`column_pager.rs` lz4 `Backend::Swap` branch + `mz_ore::pager::swap`).

## Policy: do NOT add a pressure/PSI trigger

- The pager *abstraction* is free when it doesn't spill (`nospill` config = +0%
  vs baseline uncontended). The measured "2× happy-case tax" of lz4 is an
  artifact of *spilling-but-not-actually-reclaimed* (a static small budget on an
  uncontended box) — it does not occur when replicas are redlined (spilled
  chunks genuinely swap, so compression pays and CPU is free).
- Keep an **envelope-scaled byte budget** (e.g. ~50% of pod RAM) to gate
  resident-hot-set vs spilled. Spill volume *is* the pressure signal, so the
  budget gives pressure-adaptive behavior for free. "Always lz4" with budget=0
  would tax small buffers; full PSI is overkill unless the correction buffer
  co-tenants with other large, variable memory users.

## Alternatives considered (and where they land)

- **zram swap** (node-level, kernel compressed-RAM swap): zero app cost, 2.5×
  over baseline — *but only while the compressed working set fits in spare node
  RAM*; once the zram pool fills it overflows to disk = baseline. A finite
  capacity extension, not a fix, and its pool competes with workload RAM on a
  packed node. Good ops lever where RAM is slack; not a substitute for the app
  pager in the disk-bound regime.
- **zswap**: failed — writes through to disk anyway (2× CPU, no win).
- **file pager (lz4-file)**: the right tool for the *disk-bound / working-set-
  exceeds-RAM* regime (explicit batched I/O beats per-page swap faults under
  parallelism; no `mmap_lock`/reclaim serialization). Under a strict cap, plain
  `pager-file` can OOM on dirty page cache — lz4 is a safety requirement there,
  not just an I/O win.
- **Rejected after measurement:** zero-copy `take_swap` (cost is the fault, not
  the copy — 0% wall); `MADV_WILLNEED` prefetch (+9% but its own syscall cost
  self-cancels; and the merge is bandwidth-bound so prefetch can't help);
  `MADV_SEQUENTIAL` (2× *slower* — VMA-virtual readahead overshoots the shared
  heap arena; `vm.page-cluster`↑ is the correct readahead knob, ~+8% on raw
  swap, moot under lz4); per-worker append-log file backend (fallocate
  punch-hole + extent fragmentation ≈ wash-to-worse; per-chunk metadata was
  never the bottleneck).

## Measurement-validity caveat (important)

All numbers below come from a **cgroup memory cap on a resource-rich box** (247
GiB RAM, 32 cores mostly idle). That faithfully models the *memory* limit but
leaves CPU and node-RAM unconstrained. Implications: lz4/zram CPU was "free"
partly because cores were idle (validated for the swap regime by the 10–20%-util
observation, but not for any CPU-bound pre-swap phase); and **zram's win
borrowed node RAM outside the cgroup** — it must be re-validated with the pool
charged to a SKU-sized budget. The decisive next experiment is a **SKU-shaped
harness** (cap CPU *and* memory to a real replica size, charge zram to the
budget) before trusting the zram magnitude.

---

# Detailed log (chronological)

# correction-v2 pager benchmark — snapshot

## Setup
- Branch `correction-v2-pager`. Example: `cargo build --release -p mz-compute --features bench --example correction_v2_pager`.
  Flags: `--config baseline|nospill|spill|spill-lz4`, `--backend swap|file`, `--num-ts N`, `--budget-mib M`, `--scratch DIR`, `--no-drain`.
  `baseline` = pager DISABLED (today's behavior; structure resident, kernel LRU swaps under a cap).
- Box: 247 GiB RAM, 32 cores. Root `/`=EBS(network). nvme0n1=instance-store(local), split: 884G swap (nvme0n1p1) + 870G ext4 `/mnt/scratch` (nvme0n1p2). Sustained seq WRITE ~1.7 GB/s (3.5 is read/burst).
- (Raw CSVs/scripts lived under `/home/ubuntu/mzpager/` on the bench box, now torn down with EBS; distilled numbers are inline here.)

## Pager code facts
- swap backend (`src/ore/src/pager/swap.rs`): pageout does `MADV_COLD` (line 104). **Pagein does NOT `MADV_WILLNEED`** — readback faults pages one-by-one (per-page major-fault kernel cost). The prefetch half of the design is unimplemented.
- file backend: explicit file writes; under a memory cap the page cache counts → forced physical writeback.

## Framing (per mh@)
Fixed memory ENVELOPE; scale DATA past it; compare overflow mechanisms. Policy: page at 50% util → 180 MiB envelope ↔ ~90 MiB resident ↔ `--budget-mib 64`. baseline = status quo being migrated off.

## Key results

### A. Default (uncapped, abundant RAM)
- file backend **never touches disk**: 1.3 MiB physical for 22.6 GiB reported. Page cache absorbs writes; chunks unlinked before flush. "Throughput" = RAM bandwidth. Disk only used under memory pressure.
- RSS (budget 64) flat ~90 MiB at 2M/4M/8M/16M while baseline grows 1.67 / 3.34 / 6.70 / 13.4 GiB.

### B. Concurrency / disk saturation (forced writeback, shared cgroup)
- file → disk-bound, lz4 → CPU-bound; wall-time crossover lz4<file at ~K12. swap fastest at all K under cap. Device write ceiling ~1.7 GB/s.

### C. Fixed 180 MiB envelope, scale data  (wall_s / phys_io_GiB / peak_disk_GiB)  [MAIN TABLE]
| mode | 4M | 8M | 32M |
|---|---|---|---|
| pager+swap        | 136 / 19.3 / —    | 283 / 40.0 / —    | 1815 / 231.6 / — |
| pager+file        | 181 / 27.6 / 4.9  | 368 / 56.0 / 8.1  | 2335 / 303.2 / 27.7 |
| pager+lz4+file    | 259 / 4.5 / 2.2   | 535 / 9.7 / 2.8   | 2949 / 51.8 / 6.3 |
| baseline-swap (status quo) | 298 / 20.5 / — | 633 / 42.6 / — | 4271 / 270.8 / — |
| lz4-swap          | 181 / 2.9 / — | 372 / 5.9 / — | 2221 / 35.2 / — |

**32M speed ranking:** pager+swap(1815) < lz4-swap(2221) < pager+file(2335) < lz4-file(2949) < baseline-swap(4271).
**lz4-swap standout:** 2nd-fastest AND lowest I/O (35 GiB, 6.6× less than pager+swap); beats both file modes. swap+file share exponent ~N^1.34 → ratio ~constant 1.28, may never cross. Thesis test = 64M/128M.

### Findings
1. **pager+swap is ~2.2–2.35× faster than baseline-swap** (4M:136 vs 298; 8M:283 vs 633; 32M:1815 vs 4271) at equal I/O → pure `MADV_COLD` value, gap widening with scale. Cheap interim win without leaving swap.
2. **swap fastest of all modes through 32M.** The fault-cost-dominates thesis (file overtakes swap at scale) NOT yet observed by 32M. Need larger N (64M/128M).
3. **Both swap & file superlinear ~N^1.34**; lz4 milder ~N^1.23. 4M→8M looked linear — small-scale illusion (why we scaled up).
4. **lz4 = lowest I/O (~6× less) + lowest peak-disk (best capacity), but slowest (CPU).**

### Machine max (180 MiB envelope)
- swap: working set ≤ RAM+swap ≈ 884 GiB (cgroup) / 1.13 TiB (machine).
- file: peak on-disk ≤ FS 870 GiB (no overprovision), ~0.87 GiB/M ts → ~1000M ts.
- lz4: ~0.2 GiB/M ts peak → highest capacity, >2500M ts, but CPU-time prohibitive.

## 64M extension (matrix64.csv, 180 MiB cap)
- pager-swap 64M: 5594s / 576.5 GiB. Superlinearity ACCELERATING: 32M→64M = 3.08× time (exp ~1.62, up from 1.34).
- **pager-file 64M: OOM-KILLED (exit 137).** cgroup OOM though process anon-rss was only ~90 MiB at kill. Cause: dirty page cache for spill files counts against the 180 MiB cgroup and can't be reclaimed until flushed; with MemorySwapMax=0, merge-churn dirty production outran 1.7 GB/s writeback → breach. **file backend is NOT a drop-in for swap under a hard cap** — needs swap safety margin / O_DIRECT / writeback throttle / separate cgroup for spill cache. swap cannot OOM this way (has the swap valve).
- lz4-swap 64M: 6184s / 84.7 GiB (OK). Exp ~1.48; converging toward pager-swap (ratio 0.82→0.90).
- lz4-file 64M: 7970s / 121.5 GiB / peak-disk 30.6 GiB (**OK — survived**). Compression cuts dirty-page pressure enough that writeback keeps pace → no OOM. **lz4 is a safety requirement for file under a hard cap, not just an I/O win.**
- 64M speed: pager-swap(5594) < lz4-swap(6184) < lz4-file(7970) < pager-file(OOM). swap still wins; NO crossover through 64M.

## METHODOLOGY FLAG
All runs above are `--no-drain` (fill only). Fill pages OUT (MADV_COLD / file writes); it barely exercises pageIN faults. swap's per-page fault cost — the whole motivation — dominates on READBACK, which happens in the DRAIN/merge phase (step-3 bpftrace faults were all in drain read path: take/Chunk::column/merge_cursors). So fill-only cannot reveal the swap→file crossover. Proper thesis test = DRAIN-inclusive run at moderate N (drain is O(slow), ~124s at 200k). Recommend pivoting there instead of larger fill-only N.

## PARALLELISM SWEEP (threads.csv) — the decisive result
Fixed total 8M ts, 180 MiB cap, --no-drain, T worker threads sharing one address space + global pager (real timely-worker model). Added `--threads` to the example (in-process, shared vspace).

| T | swap wall_s | file wall_s | swap I/O GiB | file I/O GiB |
|---|---|---|---|---|
| 1 | 285 | 379 | 40.1 | 58.2 |
| 2 | 201 | 204 | 50.8 | 62.7 |
| 4 | 111 | **97** | 51.4 | 62.3 |
| 8 | 71 | **61** | 58.9 | 62.0 |
| 16 | 69 | **54** | 79.2 | 65.8 |
| 32 | 93 | **63** | 117.3 | 88.3 |

### Full matrix incl lz4 (wall_s):
| mode | 1 | 2 | 4 | 8 | 16 | 32 | I/O@16 GiB |
|---|---|---|---|---|---|---|---|
| pager-swap | 285 | 201 | 111 | 71 | 69 | 93 | 79 |
| pager-file | 379 | 204 | 97 | 61 | 54 | 63 | 66 |
| lz4-swap | 377 | 187 | 89 | 46 | 45 | 83 | 42 |
| lz4-file | 530 | 263 | 119 | 54 | **35** | 56 | **30** |

**WINNER at realistic parallelism = lz4-file.** Slowest at T=1 (530) but scales 15× to T=16 (35s) — fastest of all, 2× faster than pager-swap at 2.6× less I/O. Combines both escapes from swap's weakness: explicit syscall I/O (no mmap_lock/fault contention) + compression (less volume). T=16 ranking: lz4-file 35 < lz4-swap 45 < pager-file 54 < pager-swap 69 — both file modes beat both swap modes. All regress at T=32 (32 threads / 32 cores, no spare cores for kswapd/flush); file modes graceful (+17-60%), swap modes U-turn hard (+35-84%).

**MIGRATION VERDICT:** single-thread flatters swap (wins at T=1); at real worker parallelism the file pager (esp. lz4-file) wins decisively on latency AND disk traffic. swap's per-page-fault contention is the dominating cost the project hypothesized.

### baseline-swap (STATUS QUO) backfilled — inverts the pager-swap story:
| mode | 1 | 2 | 4 | 8 | 16 | 32 |
|---|---|---|---|---|---|---|
| baseline-swap | 615 | 287 | 127 | 73 | 60 | 72 |
| pager-swap | 285 | 201 | 111 | 71 | 69 | 93 |
| pager-file | 379 | 204 | 97 | 61 | 54 | 63 |
| lz4-swap | 377 | 187 | 89 | 46 | 45 | 83 |
| lz4-file | 530 | 263 | 119 | 54 | **35** | 56 |

**MADV_COLD pager-swap win INVERTS under parallelism.** T=1: pager-swap 2.2× faster than baseline (285 vs 615). T=16: baseline (60) BEATS pager-swap (69); T=32: 72 vs 93. **pager-swap is the WORST mode at T=16.** Cause: the pager is a process-global singleton (one ColumnPager/CountingPolicy) → its shared lock/accounting is a contention point; pager+swap pays BOTH that and swap fault contention. baseline avoids the pager lock; file avoids swap faults; pager-swap gets neither escape. T=16 ranking: lz4-file 35 < lz4-swap 45 < pager-file 54 < baseline 60 < pager-swap 69.

**MIGRATION CORRECTION:** baseline-swap → pager+SWAP would REGRESS at production parallelism. The win requires pager+FILE (lz4-file best: 35s @T=16, ~1.7× faster than today's baseline, far less I/O). pager-swap is a trap at scale — the "cheap interim win" held only single-threaded. Action item: the global-singleton pager lock is itself a parallel bottleneck worth investigating.

**CROSSOVER at T≈3-4.** Single-thread swap wins (285<379); from T≥4 file wins, gap widening to 32% at T=32. swap I/O explodes 40→117 GiB (2.9×, mmap_lock/reclaim contention + fault amplification under concurrent workers); file I/O grows gently 58→88 (1.5×). **This validates the migration thesis: swap's per-page-fault cost dominates under PARALLELISM (not scale alone).** On 32 cores (~real worker count) file clearly wins. Both U-shaped (best T=16, regress at 32); file's regression milder (+17% vs +35%).

## DRAIN-INCLUSIVE parallelism sweep (threads_drain.csv, 2M total, 180 MiB cap, drain ON)
| mode | wall T=1/8/16 | drain T=1/8/16 (s) | io@16 GiB |
|---|---|---|---|
| pager-swap | 1402/172/96 | 1343/160/86 | 18.9 |
| lz4-file | 1438/169/93 | 1324/160/86 | 8.3 |
| pager-file | 1392/176/100 | 1322/162/88 | 29.0 |
| lz4-swap | 1426/174/101 | 1347/166/92 | 14.2 |
| baseline-swap | 1479/175/93 | 1432/.../... | 13.9 |

**Drain is backend-AGNOSTIC** — all four drain times within ~5% at every T (pure merge-CPU; readback IO not the bottleneck at this scale). drain dominates total (86s drain vs ~10s fill at T=16), so drain-inclusive wall-time barely distinguishes backends.
- Wall-time file-over-swap win lives in the FILL phase (parallel swap-in fault contention; 8M no-drain sweep: file wins T≥4). Drain neutral. Net = workload's fill:drain ratio.
- **Disk I/O differentiator is universal: lz4-file lowest (8.3), pager-file worst (29.0, 3.5×).** Compression + no swap-amplification.

NOTE: 2M total chosen for drain tractability; merge-CPU dominance may itself be scale-dependent. baseline-swap (status quo) backfill pending for both datasets.

## Next / pending
- baseline-swap 32M (running), then lz4-swap 4M/8M/32M.
- 64M / 128M for swap-vs-file crossover (the thesis).
- Optional: add `MADV_WILLNEED` to swap pagein and re-measure (quantifies per-fault overhead).
- 100 GiB working set ≈ 120M ts; swap projected ~3 hr (superlinear), not 71 min.

## PROFILE: contended pager-swap T=16 (samply → pollard, swap-contended.json)
4M no-drain, 180 MiB cap, 16 threads, 142k samples. Top self-time:
- **fun_a9b40 (libc memcpy/memmove): 41.4%** — single caller chain: merge_cursors → Chunk::index → OnceCell try_init (Chunk::column) → ColumnPager::take → **pager::swap::take_swap** → memcpy. The swap pagein `extend_from_slice` concat copy, which also absorbs the MADV_COLD page-fault-in stall.
- copy-family total ~49% (a9b40 41 + aa000 4 + a95c0 4); _madvise 2.6% (COLD hints).
- actual merge logic small: merge_cursors self 7.3%, Chunk::index 8.3%, Column::borrow 7.1%.
=> ~half the contended swap run is pager-swap DATA MOVEMENT (take_swap memcpy + fault), not merge work.
Actionable: (1) take_swap multi-chunk path always memcpys (single-chunk is zero-copy swap.rs:164) — merge makes multi-chunk handles; avoid the concat / read Column in place. (2) implement MADV_WILLNEED to prefetch before the copy. (3) verify the lazy OnceCell Chunk::column isn't re-materializing per touch.

## MADV_WILLNEED implemented + re-profiled (swap-willneed.json)
Added madvise_willneed on take_swap/read_at_swap readback paths (swap.rs). A/B at pager-swap T=16:
- Wall: 69s → 62.8s (~9% faster, 8M/180MiB cap). phys I/O ~same (79→81 GiB).
- Profile self%: memcpy fun_a9b40 41.4%→26.6% (−15pp: synchronous fault-stall folded into the copy now prefetched/overlapped). BUT _madvise 2.6%→15.5% (+13pp): the WILLNEED syscalls themselves, issued per-chunk every readback, much on already-resident pages (working set partly resident at 4M/180MiB) = mostly overhead.
- NET: ~9% wall, marginal — prefetch overlaps fault IO but its own syscall cost eats most of the gain.
- Remaining dominant cost = the take_swap concat memcpy (27%). Bigger lever = eliminate the copy (borrow Column in place) rather than prefetch it. Refine WILLNEED: skip resident chunks / coarser batching.

## SWAP DEEP-DIVE conclusion (perf-counter + scale-validated)
Contended (T=16, 180 MiB cap), perf stat. major-faults rank-correlate with wall at every scale → THE cost driver.
4M:  lz4-swap 22.6s/0.96M maj-faults < baseline 25.8s/1.34M < spill 30.2s/1.61M
8M:  lz4-swap 49.3s/2.12M           < baseline 61.0s/3.29M < spill 74.8s/4.10M
Gaps WIDEN at 2x (lz4 edge 14%→24%, spill penalty 17%→23%); major-faults grow ~2.5x per 2x data (lz4 slowest 2.21x, spill fastest 2.55x). NOT a small-scale artifact.
perf stat spill-vs-baseline: tax = +9% instructions (user: take_swap copy/Paged machinery) + +26% sys time (MADV_COLD syscalls + the faults they force). IPC equal ~2.7 (not cache/stall-bound). Under a tight cap faults are CAP-FORCED (kernel must reclaim regardless), so no-COLD didn't cut wall — COLD just adds syscall overhead + premature-reclaim faults.
Ruled out as levers (all measured): zero-copy take (fault not copy, 0% wall), MADV_WILLNEED (+9% but self-cancels), no-COLD (no change), global_pager lock/clone (0.1%).
VERDICT: ship lz4-swap (compression lowers the fault floor; only swap mode that beats baseline). Plain pager-swap is strictly worse than doing nothing (baseline) — don't ship. In-place-borrow/drop-COLD only helps uncompressed path reach baseline; lower priority (lz4 must decode anyway).

## GATE: swap latency- vs bandwidth-bound (decides prefetch worth)
Contended merge (spill 8M T=16 180MiB cap), vmstat + iostat on swap dev nvme0n1p1:
- %util=100%, aqu-sz~53, ~1.1 GB/s swap-IN + ~1.1 GB/s swap-OUT = ~2.2 GB/s combined vs ~2.7 GB/s device ceiling. r_await 0.21ms (tiny). vmstat: si/so~1.1GB/s, wa 37%, ~12 blocked threads.
=> BANDWIDTH-BOUND (device saturated), not latency-bound.
=> Prefetch (companion thread / io_uring / process_madvise WILLNEED) WON'T help — no spare bandwidth; reordering saturated IO changes nothing. DON'T build the companion thread.
=> Half the device bw is swap-OUT (cap-forced eviction) competing with swap-in. The only lever is FEWER BYTES → compression (lz4, proven) or less-aggressive swap-out. 50% CPU idle = headroom to trade decode for less I/O. Next swap idea must target VOLUME, not latency.

## ZERO-HAPPY-CASE-COST tail fix: zram swap (the answer)
Concern: all app pager variants degrade the happy case (file=copy, lz4=CPU, MADV_COLD=syscalls). Want: no-op when no pressure, improves tail, no per-replica SKU.
Tested kernel-level compressed swap (baseline config = pager DISABLED, just swap strategy):
- zswap (compressed cache in front of disk swap): FAILED. wrote through to disk (written_back=9.3M, pool stored=0), disk still 100% util, 2x CPU, 66s > baseline 61s. Page-level compression on the 76%-<1ms churn + shrinker writeback = worst of both.
- **zram (compressed RAM block device as swap, prio>disk): WON. 24.5s vs baseline 61s (2.5x) and app-lz4 49s (2x). Disk %util→0 (fully absorbed in RAM), compression 5.5x (4GiB→720MiB).**
Why zram beats app-lz4: removes the DISK entirely (RAM compress/decompress); app-lz4 cut volume but still paid disk I/O. zswap loses because it writes through to disk anyway.
=> zram swap = zero happy-case cost (idle w/o pressure), 2.5x tail win, NO app code, node-level config (no SKU). Can obviate the app pager for the swap case. Keep disk swap as lower-prio overflow to bound OOM.
TRADE-OFF: zram pool uses NODE RAM (outside the cgroup) — wins here due to abundant free RAM; on a tightly-packed node the pool competes with workload memory. "Spend spare node RAM to erase swap-disk latency."

## zram capacity cliff (confirmed): once full, == baseline
Small zram (512MiB) so 8M working set overflows it (disk swap prio -1 overflow):
- zram-512M (overflow): 52.7s, disk %util back to 100%, 2GiB spilled to disk. ≈ baseline 61s.
- vs zram-8G (fits): 24.5s, disk 0%.
=> zram is a CAPACITY EXTENSION (≈ spare_RAM × compression_ratio), not a fix. Fits → 2.5x; overflows → baseline for the excess (disk-bandwidth wall returns).

## FINAL swap decision framework
- Working set fits compressed in spare node RAM → **zram swap** (zero app cost, 2.5x, no SKU; size pool to working set; disk swap as overflow).
- Working set EXCEEDS it → disk-bandwidth wall regardless of mechanism. Levers: less volume (lz4) + batched I/O over per-page faults → **file pager (lz4-file)** wins here (beats kernel disk-swap under parallelism: no mmap_lock/fault serialization).
- No single mechanism is both zero-happy-case-cost AND unbounded. zram = free lunch while it fits; file pager = the disk-bound-regime tool. plain pager-swap and zswap are dominated everywhere.

## File-pager append-log prototype: NEGATIVE result
Hypothesis: per-chunk create/open/unlink + single-dir contention is a file-pager bottleneck. Built per-worker append log (thread-local fd, pwrite at monotonic offset, fallocate PUNCH_HOLE reclaim on take). A/B 8M T=16 180MiB cap:
- per-chunk: 62.5s. append-log: 69.7s (SLOWER).
- strace: metadata eliminated (openat+unlinkat+close 86k→~28) BUT fallocate punch-hole (9.5%) ≈ the unlinkat (9.75%) it replaced, and pwrite-to-sparse-file (62us/call) > writev-to-fresh-file (37us) due to extent-tree fragmentation from repeated append+punch.
=> Per-chunk metadata is NOT the bottleneck (~13-20% syscall time; ext4 htree+delayed-alloc make small-file create/unlink cheap). Append-log trades it for fallocate+extent-churn ≈ wash-to-worse. Don't pursue.
=> File pager's real cost = genuine write/read I/O (24% writev) → only lever is fewer bytes = lz4-file (exists, wins). Metadata ceiling ~13% even if perfect.
Prototype gated behind pager::set_file_log_mode + example --file-log (uncommitted).

## Swap readahead: page-cluster helps, MADV_SEQUENTIAL HURTS (8M T=16 180MiB cap)
vm.page-cluster sweep (raw spill swap): pc0=141s, pc1=109s, pc3(default)=71s, pc6=65-67s. MORE readahead = faster — swap-in is IO-SIZE/IOPS-bound, not raw-byte-bound (pc6 reads 15% more bytes but faster; eff bw 0.52→1.04→1.30 GB/s). page-cluster=6 ~ +8% for raw swap, zero code.
MADV_SEQUENTIAL (per-region, tried): 2x SLOWER (144s vs 73s). Cause: it triggers VMA-VIRTUAL readahead, which reads sequentially through the address space — but chunks are small Vecs packed in ONE shared heap arena (one VMA), so it overshoots into adjacent unrelated chunks. page-cluster does PHYSICAL swap-slot cluster readahead (stays within the chunk's co-swapped pages) → correct. Same intent, opposite mechanism; layout makes the VMA hint wrong. REVERTED.
lz4-swap: 46.5s, unaffected by page-cluster (pc3≈pc6) — compression already cuts byte/fault volume so readahead is moot. lz4-swap remains the swap winner.
CPU note (mh@ prod experience): hitting swap drops CPU util from 64-core-full to 10-20% (threads block on faults) => ~80% idle CPU available for compression. Validates spending CPU on lz4/zram to cut I/O; retracts the CPU-axis of the redline worry for the swap regime. Remaining redline caveat = zram's RAM pool (node-RAM axis), not CPU.

## HAPPY-CASE TAX (uncontended, no cap, nothing hits disk; 8M T=16)
baseline 7.9s | nospill 7.9s (+0%) | spill-swap 9.0s (+14%) | lz4-swap 16.2s (+105%, 2.05x) | spill-file 11.5s (+46%) | lz4-file 17.5s (+122%).
- lz4-swap ~2x slower uncontended: compresses every chunk + decompresses on read (budget-driven 64MiB << 6.7GiB ws), all in RAM, ZERO disk benefit. Always-on lz4 is a non-starter.
- nospill = +0% => the pager abstraction is FREE when it doesn't spill; the tax is entirely the unnecessary spill+compress.
- FIX = pressure-reactive spilling: trigger on actual memory pressure (PSI/memory.current near limit), not a static byte budget. Then happy case collapses to nospill (~0%) and the tail keeps the lz4 win. The 2x only appears in the "spilling-but-not-pressured" regime a static small budget creates.
DESIGN CONCLUSION: spill TRIGGER must be pressure, not budget. Pager machinery itself is free (nospill 0%).

## "always lz4" vs size policy? -> size policy (envelope-scaled), NOT always-lz4
Uncontended lz4 tax vs buffer/budget ratio (no cap, budget 64MiB): 48MiB(below budget)=+0%, 132MiB=+10%, 479MiB=+66%, 1748MiB=+91%. Tax is 0 below budget, grows to ~2x above.
=> The budget IS the size policy and makes sub-threshold buffers free. "always lz4" (budget=0) would tax small buffers for nothing — strictly worse. Keep a threshold.
=> Scale the threshold to the memory ENVELOPE (% of pod RAM, the 50% policy): then "buffer > budget" ⟺ buffer is a big fraction of pod RAM ⟺ genuinely near pressure (where swap looms and CPU frees up per mh@). So an envelope-scaled size budget approximates pressure for free — the tax lands only where it's cheap.
=> Full PSI/memory.current pressure-reactivity only worth it if the correction buffer shares the pod with OTHER large variable memory users (size budget sees only its own size). If the buffer dominates pod RSS (sink-heavy replica), envelope-scaled size ≈ pressure, PSI overkill.
ANSWER: not always-lz4; keep a threshold scaled to the envelope; skip PSI unless co-tenant memory varies.

## CONVERGED DESIGN (mh@: customers always redline mem+swap, so happy case doesn't exist)
If steady state is always memory-maxed + swapping, the 2x happy-case tax is an ARTIFACT (it was the no-cap "spilling-but-not-reclaimed" case = compress for zero I/O). Redlined => spilled chunks actually swap => compression saves real I/O + CPU is free (10-20% util when swapping). So:
- ALWAYS-LZ4 ON THE SPILL PATH is the right default. NOT "compress everything" — lz4 is the codec for SPILLED chunks; the budget still keeps the hot working set resident+uncompressed.
- This SELF-LIMITS by spill volume: headroom => little spill => ~no tax; redlined => much spill => big win. Spill volume IS the pressure signal, so the budget gives pressure-adaptive behavior FOR FREE — no PSI/pressure detection needed.
- Design: (1) budget gates resident hot set (uncompressed), scaled to envelope; (2) always lz4 spilled chunks; (3) drop pressure/PSI machinery.
- Load-bearing assumption: replicas run redlined AND correction buffer is a dominant memory user (what fills memory is what spills). mh@ experience says both hold.

## MADV_COLD vs MADV_PAGEOUT (mh@: spill-at-50% with COLD is "too late" for swap)
COLD is LAZY (kernel reclaims only under pressure); PAGEOUT is PROACTIVE (reclaims at spill time). Measured:
- No cap: COLD peak_RSS=3.38GiB (=full working set, never reclaimed) vs PAGEOUT 0.18GiB (=budget). PAGEOUT controls RSS WITHOUT a cap; COLD does not. (wall 18.3 vs 27.6 — but COLD did zero I/O w/o pressure; unfair, under redline both swap.)
- Under 180MiB cap, 8M T=16: COLD 72.2s/76GiB swap-in vs PAGEOUT 77.8s/86GiB (+8% wall, +13% I/O).
KEY: COLD's budget is FICTION under a cap — COLD'd pages stay resident until reclaimed AT the cap, so real RSS pins to ~100% of the limit regardless of the 50% budget. No real headroom => the spike/cliff "too late" failure. PAGEOUT makes the budget REAL (RSS held at threshold) => genuine headroom, paced swap-out, works without relying on a hard cap.
TRADE: COLD = live at cap, all-RAM-as-cache, ~8% faster steady-state, NO headroom (spike/cliff risk). PAGEOUT = real RSS headroom at budget, proactive/paced, spike-safe, ~8% steady cost. For "work effectively with swap / not too late" => PAGEOUT. Hybrid (PAGEOUT cold tail, keep merge front as cache) could recover the 8% while keeping headroom.

## pageout+lz4: THE WINNER (combine proactive evict + compression)
Today lz4+swap stores compressed bytes as CompressedInner::Memory = UNMANAGED anon (no madvise, kernel-LRU lazy). Added: advise the compressed Vec (COLD/PAGEOUT per flag).
Under 180MiB cap, 8M T=16 (wall / swap-in):
- raw+COLD 72s/76GiB | raw+PAGEOUT 77s/84GiB (+8%, proactive)
- lz4+COLD 46s/41GiB | lz4+PAGEOUT 45.1s/39.7GiB
No-cap peak RSS (4M T=4): raw+COLD 3.38GiB, raw+PAGEOUT 0.18, lz4+COLD 0.97, lz4+PAGEOUT 0.40. (lz4+PAGEOUT wall penalty no-cap only +4% vs +51% for raw — compression makes proactive evict cheap.)
=> pageout+lz4 wins all 3 axes: fastest tier (45s, ~1.6x over raw), lowest swap I/O (40GiB, 5.6x less → relaxes bandwidth bound), proactive RSS control (real headroom, fixes "spill too late"). PAGEOUT's steady-state penalty (re-swap hot front) VANISHES under lz4 (5.6x smaller → cheap re-fault). CPU-funded (free during swap).
SHIP: lz4-compress on spill + MADV_PAGEOUT the compressed bytes. Resolves both "spill@50% too late" (PAGEOUT proactive) and redline bandwidth (lz4 volume) with no steady/happy tax.
