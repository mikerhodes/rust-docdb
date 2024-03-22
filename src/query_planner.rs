// query_planner implements whatever functionality we write as a query
// planner and/or optimisations into rust-docdb. It's not likely a good
// query planner, or even one that meets the usual definition of a
// planner. Instead, it's where we take the query and refactor it for
// more efficient or easier execution.

use std::collections::BTreeMap;

use crate::{
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
pub(crate) fn query_plan(q: Vec<QP>) -> Vec<Scan> {
    // group the query predicates by the field
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
            QP::E { p, v } => lookup_eq(p, v),
            QP::GT { p, v } => lookup_gt(p, v),
            QP::GTE { p, v } => lookup_gte(p, v),
            QP::LT { p, v } => lookup_lt(p, v),
            QP::LTE { p, v } => lookup_lte(p, v),
        };
        let x = (&path).encode();
        groups.entry(x).or_insert(vec![]).push(Scan { skey, ekey })
    }

    // and now collapse each grouped set of Scans into one scan
    let mut collapsed_scans = vec![];
    for (_, scans) in groups {
        let mut skey: Vec<u8> = vec![encoding::KEY_INDEX - 1];
        let mut ekey: Vec<u8> = vec![encoding::KEY_INDEX + 1];
        for s in scans {
            if s.skey > skey {
                skey = s.skey;
            }
            if s.ekey < ekey {
                ekey = s.ekey;
            }
        }
        collapsed_scans.push(Scan { skey, ekey });
    }

    println!("collscns: {:?}", &collapsed_scans);
    collapsed_scans
}

//
// These methods are pub(crate) as they are currently used in tests for query.rs
//
// Later I'll hopefully have the reserves of patience to separate out the testing
// of planning and executing, but that isn't done right now.
//

pub(crate) fn lookup_eq(path: Vec<TaggableValue>, v: TaggableValue) -> (Vec<u8>, Vec<u8>) {
    let start_key = encoding::encode_index_query_pv_start_key(&path, &v);
    let end_key = encoding::encode_index_query_pv_end_key(&path, &v);
    (start_key, end_key)
}

pub(crate) fn lookup_gte(path: Vec<TaggableValue>, v: TaggableValue) -> (Vec<u8>, Vec<u8>) {
    let start_key = encoding::encode_index_query_pv_start_key(&path, &v);
    let end_key = encoding::encode_index_query_p_end_key(&path);
    (start_key, end_key)
}

pub(crate) fn lookup_gt(path: Vec<TaggableValue>, v: TaggableValue) -> (Vec<u8>, Vec<u8>) {
    let start_key = encoding::encode_index_query_pv_end_key(&path, &v);
    let end_key = encoding::encode_index_query_p_end_key(&path);
    (start_key, end_key)
}

pub(crate) fn lookup_lt(path: Vec<TaggableValue>, v: TaggableValue) -> (Vec<u8>, Vec<u8>) {
    let start_key = encoding::encode_index_query_p_start_key(&path);
    let end_key = encoding::encode_index_query_pv_start_key(&path, &v);
    (start_key, end_key)
}

pub(crate) fn lookup_lte(path: Vec<TaggableValue>, v: TaggableValue) -> (Vec<u8>, Vec<u8>) {
    let start_key = encoding::encode_index_query_p_start_key(&path);
    let end_key = encoding::encode_index_query_pv_end_key(&path, &v);
    (start_key, end_key)
}
