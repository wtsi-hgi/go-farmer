# go-farmer

Transparent caching proxy to elastic search for aggregation queries, with
high speed farm-related non-aggregation scroll queries handled by querying a
local database formed by doing daily elastic search queries.

## Config

You need a config file in YAML format with the following details.

```
elastic:
  host: "www.elastichost.com"
  username: "user"
  password: "redacted"
  scheme: "http"
  port: 1234
  index: "indexes-needed-for-all-searches-*"
farmer:
  host: "0.0.0.0"
  port: 1235
  database_dir: "/path"
  pool_size: 0
  file_size: 33554432
  buffer_size: 4194304
  cache_entries: 128
```

The "elastic" section defines how we will connect to the real elastic search;
only basic auth is implemented right now, intended for an internal network
elastic deployment with public access.

The "farmer" section defines the IP and port we will listen on.

* database_dir is where "backfill" local database files are stored.
* pool_size is the initial size of a buffer pool used for processing hit data
  stored on disk. If you set this higher than the expected number of hits in
  your largest scroll query, you'll use a lot of memory, but the first time you
  run that query it will be fast. Defaults to 0, but you could try a value of
  4000000 if you have enough memory.
* file and buffer size are in bytes. file_size determines the desired size
  of local database files within database_dir, and buffer_size is the write and
  read buffer size when creating/parsing those files. The default values for 
  these are given in the example above (32MB and 4MB respectively).
* cache_entries is the number of query results that will be stored in an
  in-memory LRU cache. Defaults to 128.

## Install

Requires Go v1.22 or later.

```
make
```

That will install `farmer` in your `$GOPATH/bin`.

## Usage

First populate the local database:

```
farmer backfill -c /path/to/config.yml -p 1d
```

Now start the server (you can leave it running and repeat the backfill the next
day and it will see the new data automatically):

```
farmer server -c /path/to/config.yml &
```

(Or better, use daemonize to daemonize this process.)

You can now configure https://github.com/wtsi-hgi/farmers-report to use our
configured farmer host:port as its elastic host:port.
