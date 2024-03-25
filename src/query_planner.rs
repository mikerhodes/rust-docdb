// query_planner implements whatever functionality we write as a query
// planner and/or optimisations into rust-docdb. It's not likely a good
// query planner, or even one that meets the usual definition of a
// planner. Instead, it's where we take the query and refactor it for
// more efficient or easier execution.

use std::collections::BTreeMap;

use crate::{
    docdb::DocDbError,
    encoding::{self, Encodable},
    query::{Scan, TaggableValue, QP},
};

// query_plan is the main query planning entry point. It takes in the
// query and outputs a set of index scans that can be used to answer
// the query.
//
// Currently, a query can only be a single conjunction of predicates,
// so we return a vec of Scans where the results of each scan need
// to be intersected to get the final result set.
pub(crate) fn query_plan(q: Vec<QP>) -> Result<Vec<Scan>, DocDbError> {
    //
    // Where there are multiple predicates for a single field,
    // collapse them to a single range scan. Sometimes this cannot
    // be done, for example "a < 12 AND a > 12" cannot form a single
    // range scan. In this case, we actually should fail the query
    // early: no single document can match both predicates, so there
    // is no point carrying out a query.
    //

    // Map the query predicates into individual range scans
    // while grouping them by field. This is a nice structure
    // for later optimisations.
    let mut groups: BTreeMap<Vec<u8>, Vec<Scan>> = BTreeMap::new();
    for qp in q {
        // these matches are awkward, I wonder if it's possible
        // to do better in the match, or if, in fact, I'd be better
        // off with a struct with an `operator` field.
        let path = match &qp {
            QP::E { p, .. }
            | QP::GT { p, .. }
            | QP::GTE { p, .. }
            | QP::LT { p, .. }
            | QP::LTE { p, .. } => p.clone(),
        };
        let (skey, ekey) = match qp {
            QP::E { p, v } => scan_range_eq(p, v),
            QP::GT { p, v } => scan_range_gt(p, v),
            QP::GTE { p, v } => scan_range_gte(p, v),
            QP::LT { p, v } => scan_range_lt(p, v),
            QP::LTE { p, v } => scan_range_lte(p, v),
        };
        let x = (&path).encode();
        groups.entry(x).or_insert(vec![]).push(Scan { skey, ekey })
    }

    // And now collapse each grouped set of Scans into one scan.
    // Note we allow this to create invalid scan ranges for
    // disjoint predicates for a given field; we check for this later.
    let mut collapsed_scans = vec![];
    for (_, scans) in groups {
        let skey = scans.iter().map(|x| x.skey.clone()).max().unwrap();
        let ekey = scans.iter().map(|x| x.ekey.clone()).min().unwrap();
        collapsed_scans.push(Scan { skey, ekey });
    }

    // Check for invalid scan ranges, that is with a start key
    // that is greater than the end key. If found, return error.
    match collapsed_scans.iter().find(|&s| s.skey > s.ekey) {
        Some(_) => Err(DocDbError::InvalidQuery),
        None => Ok(collapsed_scans),
    }
}

//
// These methods are pub(crate) as they are currently used in tests for query.rs
//
// Later I'll hopefully have the reserves of patience to separate out the testing
// of planning and executing, but that isn't done right now.
//

pub(crate) fn scan_range_eq(path: Vec<TaggableValue>, v: TaggableValue) -> (Vec<u8>, Vec<u8>) {
    let start_key = encoding::encode_index_query_pv_start_key(&path, &v);
    let end_key = encoding::encode_index_query_pv_end_key(&path, &v);
    (start_key, end_key)
}

pub(crate) fn scan_range_gte(path: Vec<TaggableValue>, v: TaggableValue) -> (Vec<u8>, Vec<u8>) {
    let start_key = encoding::encode_index_query_pv_start_key(&path, &v);
    let end_key = encoding::encode_index_query_p_end_key(&path);
    (start_key, end_key)
}

pub(crate) fn scan_range_gt(path: Vec<TaggableValue>, v: TaggableValue) -> (Vec<u8>, Vec<u8>) {
    let start_key = encoding::encode_index_query_pv_end_key(&path, &v);
    let end_key = encoding::encode_index_query_p_end_key(&path);
    (start_key, end_key)
}

pub(crate) fn scan_range_lt(path: Vec<TaggableValue>, v: TaggableValue) -> (Vec<u8>, Vec<u8>) {
    let start_key = encoding::encode_index_query_p_start_key(&path);
    let end_key = encoding::encode_index_query_pv_start_key(&path, &v);
    (start_key, end_key)
}

pub(crate) fn scan_range_lte(path: Vec<TaggableValue>, v: TaggableValue) -> (Vec<u8>, Vec<u8>) {
    let start_key = encoding::encode_index_query_p_start_key(&path);
    let end_key = encoding::encode_index_query_pv_end_key(&path, &v);
    (start_key, end_key)
}

#[cfg(test)]
mod tests {
    use rand::distributions::{Distribution, Uniform};
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    use crate::docdb::DocDbError;
    use crate::encoding::query_upper_bound;
    use crate::query::{Scan, TaggableValue};
    use crate::query_planner::query_plan;
    use crate::{
        keypath,
        query::{self, tv},
    };

    // Check that when we create a set of predicates where there is
    // a valid single range query to satisfy all the predicates when
    // AND'd together, we return that range, even with many predicates
    // and using a fuzz testing approach to shuffle them.
    #[test]
    fn collapse_overlapping_predicates() -> Result<(), DocDbError> {
        let p = keypath!["age"];
        let mut preds = vec![
            // The lower bound
            query::QP::GT {
                p: p.clone(),
                v: tv(50),
            },
            // the upper bound
            query::QP::LTE {
                p: p.clone(),
                v: tv(123),
            },
        ];
        let mut rng = thread_rng();

        // Now we add a lot of values that are outside the
        // collapsed range that we want to end up with, so
        // that we can check they are discarded.
        let between = Uniform::try_from(-500..49).unwrap();
        for _ in 1..100 {
            preds.push(query::QP::GT {
                p: p.clone(),
                v: tv(between.sample(&mut rng)),
            });
        }
        let between = Uniform::try_from(124..12345).unwrap();
        for _ in 1..100 {
            preds.push(query::QP::LT {
                p: p.clone(),
                v: tv(between.sample(&mut rng)),
            });
        }

        // At this point we've 202 entries in preds.

        // For the test, we shuffle the preds list to fuzz test the
        // discovery of the correct lower and upper bounds.
        for _ in 1..1001 {
            // skey is upper bound because the query is GT not GTE
            let skey = query_upper_bound(&p.clone(), Some(&tv(50)));
            let ekey = query_upper_bound(&p.clone(), Some(&tv(123)));
            let mut test = preds.clone();
            test.shuffle(&mut rng);
            assert_eq!(
                query_plan(test)?,
                vec![Scan {
                    skey: skey,
                    ekey: ekey,
                }],
            )
        }
        Ok(())
    }

    // Check that when we pass a set of predicates that cannot be satisfied
    // by a single range, we return an error from the query plan function.
    #[test]
    fn fail_non_overlapping_predicates() -> Result<(), DocDbError> {
        let p = keypath!["age"];
        let mut preds = vec![
            // these two predicates cannot be satisfied
            // concurrently.
            query::QP::GT {
                p: p.clone(),
                v: tv(100),
            },
            query::QP::LTE {
                p: p.clone(),
                v: tv(50),
            },
        ];
        let mut rng = thread_rng();

        // Now we add a lot of values that are outside the
        // collapsed range that we want to end up with, so
        // that we can check they are discarded.
        let between = Uniform::try_from(-500..12345).unwrap();
        for _ in 1..200 {
            preds.push(query::QP::GT {
                p: p.clone(),
                v: tv(between.sample(&mut rng)),
            });
            preds.push(query::QP::LT {
                p: p.clone(),
                v: tv(between.sample(&mut rng)),
            });
        }

        // At this point we've 202 entries in preds.

        // For the test, we shuffle the preds list to fuzz test the
        // discovery of the correct lower and upper bounds.
        for _ in 1..1001 {
            let mut test = preds.clone();
            test.shuffle(&mut rng);
            assert!(query_plan(test).is_err());
        }

        Ok(())
    }
}
