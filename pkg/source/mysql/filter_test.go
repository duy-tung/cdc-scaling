package mysql

import "testing"

func TestTableFilter(t *testing.T) {
	cases := []struct {
		include, exclude []string
		fqtn             string
		want             bool
	}{
		{nil, nil, "any.table", true},
		{[]string{"catalog.deals"}, nil, "catalog.deals", true},
		{[]string{"catalog.deals"}, nil, "catalog.products", false},
		{[]string{"catalog.*"}, nil, "catalog.products", true},
		{[]string{"catalog.*_price"}, nil, "catalog.seller_price", true},
		{[]string{"catalog.*_price"}, nil, "catalog.price", false},
		{[]string{"catalog.*"}, []string{"catalog.tmp_*"}, "catalog.tmp_x", false},
		{nil, []string{"mysql.*"}, "mysql.user", false},
	}
	for _, c := range cases {
		f := newTableFilter(c.include, c.exclude)
		if got := f.match(c.fqtn); got != c.want {
			t.Errorf("include=%v exclude=%v fqtn=%s: got %v want %v",
				c.include, c.exclude, c.fqtn, got, c.want)
		}
	}
}

func TestFormatSID(t *testing.T) {
	sid := []byte{0x3e, 0x11, 0xfa, 0x47, 0x71, 0xca, 0x11, 0xe1, 0x9e, 0x33, 0xc8, 0x0a, 0xa9, 0x42, 0x95, 0x62}
	want := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	if got := formatSID(sid); got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}
