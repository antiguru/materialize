---
marp: true
theme: default
paginate: true
title: PR reviews / Cluster unification
author: Moritz Hoffmann
---

<style>
/* Fix titles at the top: top-align content slides (those whose first element
   is an h2) so the title stays put regardless of how much content follows. */
section:has(> h2:first-child) {
  display: flex;
  flex-direction: column;
  justify-content: flex-start;
}
</style>

<!-- _paginate: false -->

# Pull request reviews

Moritz Hoffmann
R&D sync, September 2026

What I changed as an author, why I review anyway, and three techniques.

<!--
* Two agenda items in one deck: PR reviews (~6 min), cluster unification (~7 min).
* Framing for this half: motivational, not a process proposal. Nobody adopts a process from a 6-minute talk; people do adopt one tool and one habit.
* SAY THE LOAD OUT LOUD, FIRST: we moved the bottleneck from writing PRs to reviewing them, and I am the largest single source of that load in this room. If you skip that, the whole talk reads as "please review more of my PRs".
* Order matters: author side FIRST. It buys the rest of the slot.
-->

---

## My side first

A PR is expensive to review when the reviewer has to reconstruct the before state.
Most of that cost is payable by the author.

- **Draft first.** I open it as a draft and review it on GitHub myself, until I am happy and CI is green.
- **Stacks, not mega-PRs.** Large branches land as a chain of small PRs.
- Commits that each compile and each do one thing.
- Description says which **invariant** survives the change, not which code moved.

<!--
* The draft-and-self-review bullet is the credibility line. It is the one that earns the right to ask for anything later. Do not rush it.
* Why it works: reading your own diff in the review UI puts you in the reviewer's seat, where the missing context is obvious. Half of what a reviewer would have asked, you find yourself.
* CI green before asking is part of the same point: a red PR spends someone else's attention on your bug.
* Do NOT turn this into a checklist for the room. It is what I stopped doing, offered as evidence, not policy.
-->

---

## Why it's hard

You can only review a change if you understand the **before** state, and then the **after** state.

"I have no idea how this works" is the normal starting position, not a failure.

<div style="height: 0.6em"></div>

The question is who pays to close that gap.

- Reviewer pays: reads the surrounding code, reconstructs intent, guesses at invariants.
- Author pays: one paragraph naming the invariant and the shape of the change.

<!--
* This resolves the contradiction in my earlier draft, where "you just have to read it" and "ask questions" pulled against each other. The resolution is the author side: a good description means the reviewer does not need the full before-state.
* Concede honestly: for a subsystem you have never touched, there is no substitute for reading. The description shrinks the reading, it does not remove it.
-->

---

## Why I do it anyway

- A licensed excuse to read a part of Mz I don't own. Reviewing is the cheapest way in.
- I find out what is landing on me **before** it lands.
- Reciprocity: reviewers get their own PRs looked at.

<div style="height: 0.6em"></div>

One honest caveat: reciprocity does not clear across domain boundaries.
Some people review far more of my code than I can return in kind.

<!--
* My earlier draft had five reasons, four of which were "I personally enjoy reading code". That does not transfer to anyone. Cut to the three that are structural.
* The caveat is the part with teeth. Say it plainly, without naming anyone unless you want to; the person in question knows.
* If asked how to fix the imbalance: it is not fixable by trade. It is fixable by making each review cheaper, which is the next slide.
-->

---

## Three techniques

- **Ask, don't conclude.** "I read this as X, is that right?" A review made entirely of questions is a complete review, and it puts the burden back on the author.
- **Get out of the browser.** `gh pr checkout` plus rust-analyzer beats reading Rust in a diff view. `git range-diff` after a restack shows only what changed since your last pass. Per-file *viewed* checkboxes.
- **Run `ci-review` in parallel.** Start it, then read the PR yourself. Latency hiding, not delegation.

<div style="height: 0.6em"></div>

Read the tool's output *after* you have formed a view, so it doesn't anchor you.
Comments are usually good, but it does not always converge: **you** decide when the review is done.

<!--
* SHOW ONE THING LIVE: `git range-diff @{upstream}...HEAD` on a restacked PR, or a screenshot of it. It is the single biggest win on stacked PRs and almost nobody uses it.
* Framing for the LLM beat is ADOPTION, not advocacy: ci-review already exists, so the useful content is how to invoke it and where it is weak.
* "Does not converge" concretely: successive passes keep producing new findings rather than settling, so there is no natural stopping point. That is why the stopping rule has to be yours, and why you read it after forming your own view.
-->

---

## Cheat sheet

- Ask, don't conclude
- Check it out locally
- `range-diff` on restack
- Draft and self-review before you ask anyone

<!--
* Four items, all actionable. "It should be fun" was cut on purpose: nobody acts on it.
* Last item is deliberately the author-side one, so the talk ends on what I owe the room rather than what I want from it.
-->

---

<!-- _paginate: false -->

# Cluster unification

Two runtimes, one concept.

<!--
* Second agenda item, ~7 min. This half is technical: R&D sync, not all-hands.
* Arc: why the split was correct, why it no longer is, what one runtime requires, what it costs to get there, and who picks it up.
-->

---

## How we got here

Compute replicas and storage replicas were genuinely different things.

- Storage meant **linked clusters**: one object, one cluster, 1:1.
- Expensive, and no colocation.
- Opposite failure models: storage crashes and restarts fast, compute's state **is** the value.

<div style="height: 0.6em"></div>

Different controller, different protocol, different rendering all follow from that.
Both use Timely, and that is where it ends.

<!--
* Be fair to the original design. The split was not an accident, it followed from linked clusters plus the two failure models. That is what makes the next slide land.
* "State is the value" for compute: losing an arrangement costs a rehydration, so a compute replica is worth restarting carefully. A source can be killed and resumed from persist.
* This is also the seed of the open question on the "It already runs" slide: unification merges the failure models too.
-->

---

## Colocated, still separate

Linked clusters are gone. Sources, sinks, and dataflows now share a binary, a pod, and a heap.

<style scoped>section pre { font-size: 0.7em; line-height: 1.25; }</style>

```
 ┌──────────── clusterd: one binary, one pod, one heap ────────────┐
 │                                                                 │
 │  ┌──── timely runtime A ────┐      ┌──── timely runtime B ────┐  │
 │  │ compute rendering        │      │ storage rendering        │  │
 │  │ command_channel          │      │ internal sequencer       │  │
 │  │ 4 loggers                │ ◄─── │ 1 logger, ids + 1<<48    │  │
 │  │ reconcile() #1           │ log  │ reconcile() #2           │  │
 │  └──────────────────────────┘      └──────────────────────────┘  │
 │              ▲                                 ▲                │
 └──────────────┼─────────────────────────────────┼────────────────┘
         compute protocol                  storage protocol
                ▲                                 ▲
       compute controller                storage controller
```

The only thing the two runtimes exchange is log events, in one direction.

<!--
* One heap is literal: clusterd starts *compute's* memory limiter unconditionally, for storage-only processes too (src/clusterd/src/lib.rs:279, mz_compute::memory_limiter::start_limiter).
* The bridge: src/storage/src/server.rs registers a single "timely" logger whose events are forwarded into compute's logging dataflow. Every operator, channel and address id is shifted by STORAGE_ID_OFFSET = 1 << 48 so it cannot collide with compute's, and every Park event is dropped, because (quoting the comment) compute's park tracking assumes a single timely runtime.
* Compute registers four loggers by comparison: timely, timely/reachability/*, differential/arrange, materialize/compute (src/compute/src/logging/initialize.rs:229-248).
* Two reconcile()s: src/compute/src/server.rs:561 and src/storage/src/storage_state.rs:1014. Same problem, controller reconnect, solved twice.
* This slide replaces the max_sources/max_sinks TODO from the old deck. That TODO is about linked-cluster limits, not the compute/storage split, and explaining why it is funny costs a minute.
-->

---

## What the fork costs

Same replica, same engine, same question. Only one side can answer it.

```sql
-- "which operator is holding the memory?"
SELECT o.name, s.records, s.size
FROM mz_introspection.mz_arrangement_sizes s
JOIN mz_introspection.mz_dataflow_operators o ON o.id = s.operator_id
ORDER BY s.size DESC LIMIT 5;
```

- Materialized view: per-operator bytes, one query.
- Upsert source on that same replica: **0 rows**. Storage never registers a differential logger.
- All you get is `bytes_indexed` / `records_indexed`, one number for the whole collection.

Plus: every fix lands twice, or once and silently not the other. Two protocol invariant sets, two failure models, two runtimes to reason about.

<!--
* DO NOT SAY "no introspection for sources". It is false and someone here knows it: storage operators DO appear in mz_dataflow_operators and mz_scheduling_elapsed, via the bridge on the previous slide. The claim is that the bridge carries timely logging only, so everything derived from differential and compute logging is missing on the storage side.
* Missing, concretely: arrangement sizes, reachability, park histograms, and the whole materialize/compute family (hydration times, operator duration histograms, compute exports).
* bytes_indexed/records_indexed are per-collection gauges from src/storage-client/src/statistics.rs, reset on source restart. Useful for "is upsert state large", useless for "which operator".
* IF YOU HAVE A SCREENSHOT: two psql panes, the query returning rows for an MV and (0 rows) for a source. Strongest evidence in the deck. Backup slide at the end has the setup.
* The area split already groups Compute and Sources & Sinks inside clusterd. The org chart unified; the code did not.
-->

---

## The constraint

A replica processes commands in a **total order**.
Two runtimes means two command streams with no defined interleaving.

<style scoped>section pre { font-size: 0.68em; line-height: 1.25; }</style>

```
before: two lanes, no defined interleaving between them

    compute cmds ──►  command_channel   ──►  render
    storage cmds ──►  internal sequencer ──►  render

after: one lane, total order established receiver-side

    w0 ──inject──┐
    w1 ──inject──┼──►  worker 0: assign global index  ──►  broadcast
    w2 ──inject──┘                                            │
                                                              ▼
                              every worker replays in index order
```

One lane carrying `Either<ComputeCommand, InternalStorageCommand>`.
Storage's controller-facing protocol stays untouched.

<!--
* THIS IS THE SLIDE PEOPLE WILL ASK ABOUT. Land the key insight explicitly: all rendering must follow one cross-worker order. Storage already funnels every construction through its internal sequencer; compute through command_channel. The fix is not a new mechanism, it is generalizing command_channel into a two-hop sequencer and putting both command types in it.
* Two hops: any worker may inject, worker 0 assigns the global index and broadcasts, receivers reorder by that index. Hence "receiver-side total order".
* Objects-first, not protocol-first: we migrate the objects onto the compute runtime and leave the storage controller protocol alone. The earlier protocol-first attempt (#37091) was closed.
* Suspend-and-restart stays replica-local and the controller stays unaware, which is itself an argument for doing it this way round.
-->

---

## It already runs

Prototype: draft PR **#38579**, default-on.

- SLT all shards green. pg-cdc, Kafka, testdrive suites green.
- Topology swap onto an existing catalog: 78 then 240 sources come up, MVs advance.
- `environmentd` SIGKILL mid create-storm, restart recovers. Zero panics.
- Storage dataflows show up natively in compute introspection. The log bridge becomes dead code.

<div style="height: 0.6em"></div>

One architectural failure, `cluster-controller.td`. Blast radius runs both ways:

- **Measured:** a wedged compute worker starves co-hosted sources, and single-replica ingestion scheduling picked the wedged replica because it has no liveness signal. It never needed one.
- **Open:** the failure models merge too. "Crash and restart fast" is cheap only while nothing valuable shares the process.

<!--
* Raise the failure yourself. It is what makes this slide read as engineering rather than optimism.
* The test wedges the compute side of a source cluster with mz_sleep(3600) in an MV. Native: only the compute runtime is wedged, the storage runtime keeps ingesting. Unified: the one shared worker is wedged and the storage guest starves.
* Three candidate answers, all defensible, none chosen: liveness-aware ingestion scheduling; placement by dataflow class (the two-runtime work already has the infrastructure); or accept and document.
* Untested gaps if asked: Kafka sinks and exports, oneshot ingestion, controller reconnect without a process restart, explicit suspend-and-restart, and the idle wakeup cost of the park cap.
* The branch is a throwaway spike. It is a working existence proof, not a mergeable PR.
-->

---

## Sequencing, and what I want

1. **Unify rendering.** One timely runtime. Unblocks introspection, retires the second runtime.
2. **Unify protocols.**
3. **Controllers.** Storage controller splits into a sources/sinks controller and a persist controller.

Rendering first, because it pays off before the rest lands.

<div style="height: 0.6em"></div>

The work belongs to Sources & Sinks, not to me. I have a prototype and I will consult.

**Two things from you:** agree rendering goes first, and tell me whether #38579 stays alive or I close it.

<!--
* Why rendering first: it is the only stage whose payoff arrives on its own. Protocols and controllers are cleanup that pays off once rendering has landed. If someone starts at protocols, stage 1's benefit never shows up.
* Why the ask is small: I do not want the work, and saying so keeps this from reading as a bid. But FYI-only has a failure mode: the spike is on a throwaway branch, and in six months someone re-derives the total-order problem from scratch. A keep-or-close decision costs the room thirty seconds and prevents that.
* If pushed on effort: stage 1 deletes the storage timely cluster, the storage server loop, and the log bridge. It is a subtraction, not a feature.
* THEN the joke: and then we rename Compute to Cluster.
-->

---

# Thanks

Questions?

<!--
* BACKUP SLIDE FOLLOWS. Do not advance past this one in the normal flow.
-->

---

<!-- BACKUP: introspection asymmetry, live or screenshot. -->

## Backup: the asymmetry, live

```sql
-- on a cluster with one MV and one upsert source
SELECT d.dataflow_name, o.name, s.size
FROM mz_introspection.mz_arrangement_sizes s
JOIN mz_introspection.mz_dataflow_operators o ON o.id = s.operator_id
JOIN mz_introspection.mz_dataflow_operator_dataflows d ON d.id = o.id
ORDER BY s.size DESC;

SELECT bytes_indexed, records_indexed, rehydration_latency
FROM mz_internal.mz_source_statistics WHERE id = '<source id>';
```

First query: rows for the MV's dataflow, nothing for the source's.
Second query: the only memory number a source gets, and it covers the whole collection.

<!--
* Run against a local environmentd. Use an upsert source so envelope state is non-empty; a plain append-only source has no state to show either way, which muddies the point.
* Set the cluster with SET cluster = <name> before querying mz_introspection; those relations are per-replica.
-->
