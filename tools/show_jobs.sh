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
  CASE j.status WHEN 0 THEN '0 Created'
                WHEN 1 THEN '1 Active'
                WHEN 2 THEN '2 Success'
                WHEN 3 THEN '3 Fail'
                ELSE 'Unknown'
  END                                AS 'job status',
  LPAD(FORMAT(j.n_events,0),8," ")   AS events,
  s.submit_index                     AS 'sub idx',
  CASE s.status WHEN   0 THEN '  0 Created'
                WHEN   1 THEN '  1 Registered'
                WHEN   2 THEN '  2 Pending'
                WHEN   3 THEN '  3 Idle'
                WHEN   4 THEN '  4 Running'
                WHEN   5 THEN '  5 Really-Running'
                WHEN   6 THEN '  6 Held'
                WHEN   7 THEN '  7 Done-OK'
                WHEN   8 THEN '  8 Done-Failed'
                WHEN   9 THEN '  9 Cancelled'
                WHEN  10 THEN ' 10 Aborted'
                WHEN  11 THEN ' 11 Unknown'
                WHEN  12 THEN ' 12 Undef'
                WHEN 107 THEN '107 Done-OK - No Out'
                WHEN 108 THEN '108 Done-Failed - No Out'
                WHEN 109 THEN '109 Cancelled - No Out'
                WHEN 207 THEN '207 Done-OK - RC!=0'
                         ELSE '??? Unknown'
  END                                AS 'sub status',
  s.ce_job_id                        AS 'ce job id',
  s.description                      AS 'description',
  s.worker_node                      AS 'worker node',
  s.wn_user                          AS user,
  s.time_submit                      AS 'time submit',
  s.time_complete                    AS 'time complete'
FROM job_submit s
  INNER JOIN job j ON s.job_id=j.id
  INNER JOIN production p ON j.production_id=p.id
$test_prod
ORDER BY p.name,j.name,s.submit_index;
EOF
