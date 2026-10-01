package pg

import (
	"net/url"
	"strings"
)

// DSNHostPort extracts "host:port" from a PostgreSQL DSN in either the
// URL form (postgres://user:***@host:port/db) or the keyword/value form
// (host=h port=p). Only the host and port are returned — never the user
// or password. A URL form without an explicit port returns host:5432; a
// keyword form without a port likewise defaults to 5432. An empty or
// unparseable DSN returns "".
func DSNHostPort(dsn string) string {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return ""
	}
	if u, err := url.Parse(dsn); err == nil && u.Host != "" {
		return u.Host
	}
	host, port := "", "5432"
	for _, kv := range strings.Fields(dsn) {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		switch k {
		case "host":
			host = v
		case "port":
			port = v
		}
	}
	if host == "" {
		return ""
	}
	return host + ":" + port
}
