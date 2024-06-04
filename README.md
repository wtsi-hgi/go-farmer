# go-farmer

Initial experiment in doing farmer report elastic queries in Go instead of R
for possible speed improvements and caching to a local database.

## Config

You need a config file in YAML format with the following details.

```
elastic:
	host:     "www.elastichost.com"
	username: "user"
	password: "redacted"
	scheme:   "http"
	port:     1234
```

Note that only basic auth is implemented right now, intended for an internal
network elastic deployment with public access.

## Usage

```
go run main.go /path/to/config.yml
```
