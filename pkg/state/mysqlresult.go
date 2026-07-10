package state

import "github.com/go-mysql-org/go-mysql/mysql"

// mysqlResult is a thin wrapper so the store code reads uniformly.
type mysqlResult struct {
	*mysql.Result
}

func (r *mysqlResult) Close() {
	if r.Result != nil {
		r.Result.Close()
	}
}
