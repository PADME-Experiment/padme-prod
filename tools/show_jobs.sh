#!/bin/bash

if test -z $1
then
    test_prod=""
else
    test_prod="WHERE p.name=\"$1\""
fi

mysql --table -h percona.lnf.infn.it -P 3306 -u padmeMCDB -p$PADME_MCDB_PASSWD PadmeMCDB <<EOF
SELECT
  p.name                             AS production,
  j.name                             AS job,
  j.random                           AS seeds,
  CASE j.status WHEN 0 THEN '0 Created'
                WHEN 1 THEN '1 Active'
                WHEN 2 THEN '2 Success'
                WHEN 3 THEN '3 Fail'
                ELSE CONCAT(LPAD(j.status,1," "),' ???')
  END                                AS 'job status',
  LPAD(FORMAT(j.n_events,0),8," ")   AS events,
  LPAD(FORMAT(j.n_files,0),8," ")    AS files,
  j.time_create                      AS 'time created',
  j.time_complete                    AS 'time completed'
FROM job j
  INNER JOIN production p ON j.production_id=p.id
$test_prod
ORDER BY p.name,j.name;
EOF
