#!/bin/bash

if test -z $1
then
    test_prod=""
else
    test_prod="WHERE p.name=\"$1\""
fi

mysql --table -h percona.lnf.infn.it -P 3306 -u padmeMCDB -p$PADME_MCDB_PASSWD PadmeMCDB <<EOF
SELECT
  p.name                             AS prod,
  j.name                             AS job,
  CASE j.status WHEN 0 THEN '0 Created'
                WHEN 1 THEN '1 Processing'
                WHEN 2 THEN '2 Success'
                WHEN 3 THEN '3 Fail'
                ELSE 'Unknown'
  END                                AS 'job status',
  j.job_dir                          AS directory,
  f.name                             AS file,
  LPAD(FORMAT(j.n_events,0),8," ")   AS events,
  LPAD(FORMAT(f.size,0),15," ")      AS size,
  f.adler32                          AS checksum
FROM file f
  INNER JOIN job j        ON f.job_id=j.id
  INNER JOIN production p ON j.production_id=p.id
$test_prod
ORDER BY p.name,j.name;
EOF
