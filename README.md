# promtool-tsdb-analyze-parser

Just a really simple utility to take output from `promtool tsdb analyze /data` and turn it into json.

This is mostly pretty ugly because the source material is also [not exactly structured](https://github.com/prometheus/prometheus/blob/a84c472745123e7c4e319a31dec7e2d14442c2bc/cmd/promtool/tsdb.go#L421-L572).

Usage is pretty basic. Would look something like this:

```
promtool tsdb analyze /data | promtool-tsdb-analyze-parser
```

There are flags to specify both the input and output files, but the default is to use stdin and stdout.
