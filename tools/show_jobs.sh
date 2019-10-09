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
                WHEN 1 THEN '1 Processing'
                WHEN 2 THEN '2 Success'
                WHEN 3 THEN '3 Fail'
                ELSE 'Unknown'
  END                                AS 'job status',
  LPAD(FORMAT(j.n_events,0),8," ")   AS events,
  s.submit_index                     AS 'sub idx',
  CASE s.status WHEN 0 THEN '0 Created'
                WHEN 1 THEN '1 Submitted'
                WHEN 2 THEN '2 Running'
                WHEN 3 THEN '3 Done'
                WHEN 4 THEN '4 Fail'
                WHEN 5 THEN '5 Done - Bad'
                WHEN 6 THEN '6 Fail - Bad'
                WHEN 7 THEN '7 Cancelled'
                WHEN 8 THEN '8 Undefined'
                ELSE 'Unknown'
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
