#!/usr/bin/env bash
# Prepares a local MySQL 8 server for the Nomios integration tests:
# enables GTID mode (binlog/ROW format are MySQL 8 defaults) and creates
# the replication user + test database the tests expect.
#
# Usage: sudo ./scripts/setup-test-mysql.sh
set -euo pipefail

mysql <<'EOF'
SET PERSIST enforce_gtid_consistency = ON;
SET GLOBAL gtid_mode = OFF_PERMISSIVE;
SET GLOBAL gtid_mode = ON_PERMISSIVE;
SET GLOBAL gtid_mode = ON;
SET PERSIST gtid_mode = ON;

CREATE USER IF NOT EXISTS 'nomios'@'%' IDENTIFIED BY 'nomios-test';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'nomios'@'%';
CREATE DATABASE IF NOT EXISTS nomios_test;
GRANT ALL PRIVILEGES ON nomios_test.* TO 'nomios'@'%';
FLUSH PRIVILEGES;
EOF

mysql -e "SHOW VARIABLES WHERE Variable_name IN ('log_bin','binlog_format','gtid_mode')"
echo "MySQL is ready for: go test -tags integration ./test/integration/..."
