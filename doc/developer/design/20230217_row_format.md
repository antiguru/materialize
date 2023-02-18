- Feature name: row-format
- Associated: (Insert list of associated epics, issues, or PRs)

# Summary
[summary]: #summary

One paragraph to explain the feature.

The current row format is based on a tag-value format, where each tag determines the type that follows it.
All parts are serialized in memory, which makes row decoding non-trivial and potentially requires unaligned loads.
We analyze the problems and present an updated format to address the shortcomings.

# Motivation
[motivation]: #motivation

Why are we doing this? What problems does it solve? What use cases does it enable? What is the desired outcome?

The row format in Materialize is based on an in-memory serialization format with tags and values.
Conceptually, a row consists of datums of a specific type, following the column order.
Datums can be `NULL` depending on the context, and can be constant or variable length.
This leads to a duplication of information as well as nontrivial decoding to access a row's contents.

In this design we focus on rows as processed within Compute, but the same should apply to other parts of Materialize.

Within Compute, the optimizer knows the types of columns and therefore possible values for datums.
Adding the metadata to the row itself serves two purposes: It allows us to check at runtime that the types are indeed correct, and to encode `NULL` values, which are recognized by a unique tag type and not special values.
In the past, this metadata was useful to detect bugs where types would not align, and this is still true today, for example in [17616][#17616].
Nevertheless, it introduces a significant runtime cost to maintain this metadata.

[#17616]: https://github.com/MaterializeInc/materialize/issues/17616

To decode a row, we need to traverse it sequentially.
A tag determines how subsequent bytes need to be interpreted, for example an integer tag will be followed by a specific number of bytes, or a string tag by a length followed by as many bytes.
It does not allow any operator to access specific columns directly without decoding preceding columns.
This mixed with unaligned memory accesses make it expensive to decode rows.

Rows currently optimize the representation for different size classes of variable-length datums.
Because the type is encoded as part of the data, we can support different tags for the same type depending on the length of the variably-sized portion.
For instance, a string can be tiny, short, or huge, which determines the number of bytes required for the length value (1, 2, or 4 bytes.)
This means any new representation needs to trade-off memory requirements with decoding efficiency.

# Explanation
[explanation]: #explanation

Explain the design as if it were part of Materialize and you were teaching the team about it.
This can mean:

- Introduce new named concepts.
- Explain the feature using examples that demonstrate product-level changes.
- Explain how it builds on the current architecture.
- Explain how engineers and users should think about this change, and how it influences how everyone uses the product.
- If needed, talk though errors, backwards-compatibility, or migration strategies.
- Discuss how this affects maintainability, or whether it introduces concepts that might be hard to change in the future.

We would like to address the shortcoming of _packing_ and _decoding_ rows as outlined in the [motivation](#motivation).
We'll start what information we need to efficiently decode a row, which directly influences how we pack it.

We'd like to be able to decode rows with the following properties:
1. Columns can be decoded independently of decoding the whole row.
2. All datums of a row are naturally aligned based on the size of the largest member.

The first property requires datums within a row to be located at known offsets.
This is trivial for constant-sized types, but less obvious for nullable values as well as variable-length types.

The second property requires a special ordering of columns within a row.
In the past, this was hard to achieve, but it should be significantly easier since Materialize supports permutations for columns to de-duplicate keys from values in arrangements.
The same technique could likely be used to supply additional permutation information during planning.


# Reference explanation
[reference-explanation]: #reference-explanation

Focus on the implementation of the feature.
This is the technical part of the design.

- Is it reasonably clear how the feature is implemented?
- What dependencies does the feature have and introduce?
- Focus on corner cases.
- How can we test the feature and protect against regressions?

## Design of a row

```rust
struct Row {
    data: Vec<u8>,
    null: Vec<bool>,
}
```




# Rollout
[rollout]: #rollout

Describe what steps are necessary to enable this feature for users.

## Testing
[testing]: #testing

Describe how you will test and roll out the implementation.
When the deliverable is a refactoring, the existing tests may be sufficient.
When the deliverable is a new feature, new tests are imperative.

Describe what metrics can be used to monitor and observe the feature.
What information do we need to expose internally, and what information is interesting to the user?

Basic guidelines:

* Nearly every feature requires either Rust unit tests, sqllogictest tests, or testdrive tests.
* Features that interact with Kubernetes additionally need a cloudtest test.
* Features that interact with external systems additionally should be tested manually in a staging environment.
* Features or changes to performance-critical parts of the system should be load tested.

## Lifecycle
[lifecycle]: #lifecycle

If the design is risky or has the potential to be destabilizing, you should plan to roll the implementation out behind a feature flag.
List all feature flags, their behavior and when it is safe to change their value.
Describe the [lifecycle of the feature](https://www.notion.so/Feature-lifecycle-2fb13301803b4b7e9ba0868238bd4cfb).
Will it start as an alpha feature behind a feature flag?
What level of testing will be required to promote to beta?
To stable?

# Drawbacks
[drawbacks]: #drawbacks

Why should we *not* do this?

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

- Why is the design the best to solve the problem?
- What other designs have been considered, and what were the reasons to not pick any other?
- What is the impact of not implementing this design?

# Unresolved questions
[unresolved-questions]: #unresolved-questions

- What questions need to be resolved to finalize the design?
- What questions will need to be resolved during the implementation of the design?
- What questions does this design raise that we should address separately?

# Future work
[future-work]: #future-work

Describe what work could follow from this design, which new aspects it enables, and how it might affect individual parts of Materialize.
Think in larger terms.
This section can also serve as a place to dump ideas that are related but not part of the design.

If you can't think of any, please note this down.
