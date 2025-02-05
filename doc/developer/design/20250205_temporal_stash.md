# Efficiently retain future updates for totally ordered timestamps

- Associated: (Insert list of associated epics, issues, or PRs)

<!--
The goal of a design document is to thoroughly discover problems and
examine potential solutions before moving into the delivery phase of
a project. In order to be ready to share, a design document must address
the questions in each of the following sections. Any additional content
is at the discretion of the author.

Note: Feel free to add or remove sections as needed. However, most design
docs should at least keep the suggested sections.
-->

## The Problem

<!--
What is the user problem we want to solve?

The answer to this question should link to at least one open GitHub
issue describing the problem.
-->

Materialize offers temporal filters, that define when a record is part of a result and when it disappears again.
We map a record to an insertion and separate retraction at their respective times, when they first become visible, and when they disappear.
The implementation to handle future updates is not efficient and causes operational problems for customers.
Specifically, the work associated with future updates is linear in the number of updates, performed whenever the clock ticks forward.

We propose an algorithm that avoids the linear CPU overhead and replaces it with a logarithmic overhead.
We do not address the issue of where to store future updates.

## Success Criteria

<!--
What does a solution to this problem need to accomplish in order to
be successful?

The criteria should help us verify that a proposed solution would solve
our problem without naming a specific solution. Instead, focus on the
outcomes we hope result from this work. Feel free to list both qualitative
and quantitative measurements.
-->

## Out of Scope

<!--
What does a solution to this problem not need to address in order to be
successful?

It's important to be clear about what parts of a problem we won't be solving
and why. This leads to crisper designs, and it aids in focusing the reviewer.
-->

## Solution Proposal

<!--
What is your preferred solution, and why have you chosen it over the
alternatives? Start this section with a brief, high-level summary.

This is your opportunity to clearly communicate your chosen design. For any
design document, the appropriate level of technical details depends both on
the target reviewers and the nature of the design that is being proposed.
A good rule of thumb is that you should strive for the minimum level of
detail that fully communicates the proposal to your reviewers. If you're
unsure, reach out to your manager for help.

Remember to document any dependencies that may need to break or change as a
result of this work.
-->

We assume timestamps that are totally ordered, of bounded size, and, for the sake of simplicity, correspond to a linear time scale.
We then divide the whole timestamp space into power-of-two sized buckets.
For a 64-bit timestamp, this results in 2^64 1-timestamp buckets, 2^63 2-timestamp buckets, until one 2^64-timestamp bucket.

Update: A modification of data at a point in time.

Frontier: A lower bound of all future updates.

Bucket chain: A sequence of buckets of increasing size covering the space between a frontier and the upper limit, without gaps and overlaps.

(For any n-bit timestamp, we can cover the space between a frontier and the upper bound with at most n buckets.
TODO: Proof)

Minimal bucket chain: A minimal sequence of buckets forming a bucket chain.

Balanced bucket chain: A bucket chain where consecutive elements are within a factor of 4 in size.
A balanced bucket chain is at most a factor of two larger than a minimal bucket chain.

Splitting: Replacing an 2k-sized bucket in a bucket chain with two k-sized buckets.

Claim: For a n-bit timestamp, given a balanced bucket chain and a frontier f, we can derive a new balanced bucket chain for a new frontier f' in at most n splits, which amortized to log n operations when repeated.

Example 4-bit domain:
```
Value:   0 1 2 3 4 5 6 7 8 9 A B C D E F
Bucket 0:0 1 2 3 4 5 6 7 8 9 A B C D E F
Buctet 1:0   1   2   3   4   5   6   7
Bucket 2:0       1       2       3
Bucket 3:0               1
```



## Minimal Viable Prototype

<!--
Build and share the minimal viable version of your project to validate the
design, value, and user experience. Depending on the project, your prototype
might look like:

- A Figma wireframe, or fuller prototype
- SQL syntax that isn't actually attached to anything on the backend
- A hacky but working live demo of a solution running on your laptop or in a
  staging environment

The best prototypes will be validated by Materialize team members as well
as prospects and customers. If you want help getting your prototype in front
of external folks, reach out to the Product team in #product.

This step is crucial for de-risking the design as early as possible and a
prototype is required in most cases. In _some_ cases it can be beneficial to
get eyes on the initial proposal without a prototype. If you think that
there is a good reason for skipping or delaying the prototype, please
explicitly mention it in this section and provide details on why you'd
like to skip or delay it.
-->

## Alternatives

<!--
What other solutions were considered, and why weren't they chosen?

This is your chance to demonstrate that you've fully discovered the problem.
Alternative solutions can come from many places, like: you or your Materialize
team members, our customers, our prospects, academic research, prior art, or
competitive research. One of our company values is to "do the reading" and
to "write things down." This is your opportunity to demonstrate both!
-->

## Open questions

<!--
What is left unaddressed by this design document that needs to be
closed out?

When a design document is authored and shared, there might still be
open questions that need to be explored. Through the design document
process, you are responsible for getting answers to these open
questions. All open questions should be answered by the time a design
document is merged.
-->
