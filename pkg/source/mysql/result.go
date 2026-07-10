package mysql

import gomysql "github.com/go-mysql-org/go-mysql/mysql"

type mysqlResult struct {
	*gomysql.Result
}

func (r *mysqlResult) Close() {
	if r.Result != nil {
		r.Result.Close()
	}
}
