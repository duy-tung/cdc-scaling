package mysql

import "path"

// tableFilter decides which "db.table" streams are captured. Patterns use
// path.Match syntax against the fully qualified name, e.g. "catalog.deals",
// "catalog.*_price", "catalog.*". An empty include list captures everything
// not excluded.
type tableFilter struct {
	include []string
	exclude []string
}

func newTableFilter(include, exclude []string) *tableFilter {
	return &tableFilter{include: include, exclude: exclude}
}

func (f *tableFilter) match(fqtn string) bool {
	for _, p := range f.exclude {
		if ok, _ := path.Match(p, fqtn); ok {
			return false
		}
	}
	if len(f.include) == 0 {
		return true
	}
	for _, p := range f.include {
		if ok, _ := path.Match(p, fqtn); ok {
			return true
		}
	}
	return false
}
