DROP TABLE IF EXISTS datatypes.sample;
CREATE TABLE datatypes.sample (
    primaryRowKey text,
    secondaryRowKey text,
    primaryColumnKey timeuuid,
    secondaryColumnKey text,
    atime timestamp,
    aboolean boolean,
    abigint bigint,
    auuid uuid,
    adouble double,
    adecimal decimal,
    aset set<text>,
    amap map <text,text>,
    alist list<text>,
    ablob blob,
    PRIMARY KEY ((primaryRowKey, secondaryRowKey), primaryColumnKey, secondaryColumnKey)
);

INSERT INTO datatypes.sample (
    primaryRowKey, secondaryRowKey,
    primaryColumnKey, secondaryColumnKey,
    atime, aboolean, abigint,
    auuid, adouble, adecimal,
    aset, amap, alist
) VALUES (
    'row1','part1',
    now(),'group1',
    dateof(now()),true,876543210987654321,
    uuid(),1.5,1.1234567890123456789,
    {'abc', 'def', 'ghi'}, {'key1': 'value1', 'key2': 'value2'}, ['xxx', 'jjj', 'ttt']
);