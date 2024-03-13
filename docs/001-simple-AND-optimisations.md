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
1. For the remaining clauses, order by the field. We may be able to collapse
   queries together, and if not, we might at least end up scanning similar areas
   of files (depending on whether we are using a btree or LSM tree key/value
   store; likely yes for the btree, less likely for the LSM tree).
1. Analyse the predicates to see if individual greater/less than (or equal to)
   clauses can be combined. Ie, `a < 12` and `a > 2` can be combined to a single
   scan of `2 < a < 12`.
1. Conversely, remove predicates which contradict each other -- like `a > 12`
   and `a < 5`, or even `a < 12` and `a > 12`. Ensure we don't accidentally
   catch `a >= 12` and `a <=12` in this.

It is probably worth adding some code to return "statistics" alongside the
result. In this case, it'd be the number of index scans actually executed. We
can use this to infer in tests that the expected collapsing of predicates
happened.

Further minor optimisations:

1. We could use the max/min of the doc IDs in the result set to create slightly
   smaller ranges for queries. For most use-cases this would not be that
   valuable, but might be useful for queries on low-cardinality fields where the
   docID range bounds might actually remove a decent chunk of the scanned rows.
   For anything high-cardinality, this won't make any difference, however. I
   suspect this wouldn't generally make much difference.
