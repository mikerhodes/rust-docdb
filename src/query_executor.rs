// query_executor contains a simple query executor. It accepts physical operators
// and executes them against the database, returning the set of document IDs that
// match the operators.

use std::collections::BTreeMap;

use sled::Db;

use crate::{
    docdb::DocDbError,
    encoding,
    query::{QueryResult, QueryStats, Scan},
};

// query_execute executes a query plan, which currently consists of a single conjunction
// of scan operations, returning a QueryResult.
pub(crate) fn query_execute(scans: Vec<Scan>, db: &Db) -> Result<QueryResult, DocDbError> {
    let mut result_ids = BTreeMap::new();
    let mut n_preds = 0;
    let mut stats = QueryStats { scans: 0 };
    let mut first_predicate = true;
    for s in scans {
        n_preds += 1;
        let ids = scan(&db, &s.skey, &s.ekey)?;
        stats.scans += 1;

        if ids.is_empty() {
            // Short-circuit evaluation; an empty result set means
            // this conjunction can't have any results. Stop scanning.
            return Ok(QueryResult {
                results: vec![],
                stats,
            });
        }

        for id in ids {
            let e = result_ids.entry(id).and_modify(|c| *c += 1);
            if first_predicate {
                // As no result ID that appears in a later scan but not the
                // first scan can be in the final result set, don't use memory
                // to store them.
                e.or_insert(1);
            }
        }

        first_predicate = false;
    }
    let mut results = vec![];
    for (id, n) in result_ids {
        if n == n_preds {
            results.push(id);
        }
    }
    let query_result = QueryResult { results, stats };
    Ok(query_result)
}

pub(crate) fn scan(db: &Db, start_key: &[u8], end_key: &[u8]) -> Result<Vec<String>, DocDbError> {
    let mut ids = vec![];
    let iter = db.range(start_key..end_key);
    for i in iter {
        let (k, _) = i?;
        match encoding::decode_index_key_docid(&k) {
            Ok(v) => ids.push(v.to_string()),
            Err(_) => println!("Couldn't decode docID from {:?}", &k),
        };
    }
    Ok(ids)
}
