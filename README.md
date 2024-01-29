# dataset_test
just try to handle data set



// clear data
MATCH (n)
DETACH DELETE n;

:auto USING PERIODIC COMMIT 1000
load csv with headers from 'file:///csv_out.csv' as line
MERGE (e:user {uuid: line.uuid})
RETURN count(e);
