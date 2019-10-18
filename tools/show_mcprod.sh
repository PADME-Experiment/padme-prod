#!/bin/bash

if test -z $1
then
    test_prod=""
else
    test_prod="WHERE p.name=\"$1\""
fi

mysql --table -h percona.lnf.infn.it -P 3306 -u padmeMCDB -p$PADME_MCDB_PASSWD PadmeMCDB <<EOF
SELECT
  m.user_req                            AS user,
  LPAD(FORMAT(m.n_events_req,0),10," ") AS "req events",
  m.mc_version                          AS version,
  p.name                                AS production,
  p.prod_ce                             AS CE,
  LPAD(FORMAT(p.n_jobs,0),4," ")        AS 'jobs',
  LPAD(FORMAT(p.n_jobs_ok,0),7," ")     AS 'jobs ok',
  LPAD(FORMAT(p.n_jobs_fail,0),9," ")   AS 'jobs fail',
  LPAD(FORMAT(p.n_events,0),12," ")     AS 'total events',
  p.time_create                         AS 'time create',
  p.time_complete                       AS 'time complete'
FROM production p
  INNER JOIN mc_prod m ON m.production_id=p.id
$test_prod
ORDER BY p.name;
EOF
