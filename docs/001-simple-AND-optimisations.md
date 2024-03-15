# 001 Simple AND optimisations

As of commit e9ac36f, docdb executes predicates in the query in the order that
they are passed to `search_index` --- which, as of that commit, only accepts a
single list of predicates, which are all implicitly AND'd together.

Before thinking about more complicated heuristics, such as using sketches like
HyperLogLog to choose indexes with high-cardinality for equality queries to
reduce the result set, I think there are some simpler re-orderings that we
should do. These should put us in a decent place to take better advantage of
more complex optimisations should we get there.

These are:

1. If any scan retrieves zero results, immediately return the empty set.
   - Implemented in 16dd5a6.
1. After we have retrieved the IDs matching the first predicate, never add more
   entries to our result list --- because we are executing within an AND, we can
   never extend the set beyond what we have now (currently the code generates a
   count of the number of times it sees each ID, and filters the set down at the
   end to those IDs that have a count equal to the number of predicates).

   This primarily reduces the amount of memory we need for any given AND. We can
   use other optimisations (eg, execute equality first or use HLL) as heuristics
   to try to keep this set the smallest we can.

   Once this is done, other optimisations make more sense.

   - Implemented in 2c80d66.

1. Reorder clauses to put equality predicates first. These are likely to have
   the fewest index matches, and so make sense to do first to narrow down the
   number of IDs that we are storing in memory once (1) is complete.
   - Implemented in 0f43ad3.
1. Do Optimising single field calculations, below.

It is probably worth adding some code to return "statistics" alongside the
result. In this case, it'd be the number of index scans actually executed. We
can use this to infer in tests that the expected collapsing of predicates
happened.

## Optimising single field calculations

We make the decision that we will only use indexes for predicates where every
returned value from the index will match the predicate (eg, this excludes "value
in array at any index" predicates or regex predicates). This allows us to
relatively easily calculate the smallest range for a given field if we have only
eq, gte, gt, lt, lte predicates to worry about.

We note that eq is still a range scan. It scans the range of keys for a given
path/value prefix.

What we want to do for a given field is generate the smallest range scan that
satisfies a set of predicates.

Examples searching a field `foo` --- for ease, we use integers, and define that
LTE 47 == LT 48:

```
foo GT 12
foo GTE 15
foo LT 47
foo LT 33
=> GTE 15 <=> LT 33

foo GT 12
foo GTE 15
foo LT 47
foo LT 33
foo EQ 12
=> GTE 12 <=> LTE 12

foo GT 12
foo GTE 15
foo LT 5
=> No overlapping range!

foo EQ 12
foo GTE 15
foo LT 50
=> No overlapping range!
```

We need to be careful to collapse queries only within a single field, otherwise
a query like `foo > 15 AND zoo < 45` would include all doc IDs with fields
`goo`, `loo` and so on --- results that shouldn't be in our set. Conversely,
`foo < 15 AND zoo > 45` would result in a range that cannot be satisfied (as
foo/15 will sort lower in the index than zoo/45), meaning we'd erroneously
receive no results!

So we need to:

1. Group our AND predicates by their fields.
1. Within each group, generate the start/end key for each predicate.
1. Iterate the group, taking the highest start key and lowest end key.
1. Either:
   - If the start key ≤ end key, Some(startkey, endkey).
   - If start key > end key, return None (and return the empty set for the AND).

## Further minor optimisations:

1. We could use the max/min of the doc IDs in the result set to create slightly
   smaller ranges for queries. For most use-cases this would not be that
   valuable, but might be useful for queries on low-cardinality fields where the
   docID range bounds might actually remove a decent chunk of the scanned rows.
   For anything high-cardinality, this won't make any difference, however. I
   suspect this wouldn't generally make much difference.
