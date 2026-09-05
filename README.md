# go-parquet

Opinionated Go package for working with Parquet files.

## Motivation

This is not a general purpose Parquet package. Currently it has exactly one function:

```
Iterate[T any](ctx context.Context, uris ...string) iter.Seq2[*T, error]
```

Which is designed to iterate over a series of URIs which may be local files on disk or remote files served over HTTP(S).

I have copy-pasted this code in to different packages enough times that it seemed worth putting it in a common `import`-able package of its own.