//go:build wasmjs

package parquet

import (
	"fmt"
	"strconv"
	"strings"
	"syscall/js"
)

// OpenURI checks file metadata via HEAD to get size, then prepares our Range Reader.
func OpenURI(uri string) (ReadCloserAt, int64, error) {

	if strings.HasPrefix(uri, "file://") || !strings.Contains(uri, "://") {
		return nil, 0, fmt.Errorf("Local file access disabled in WASM targets")
	}

	global := js.Global()
	fetch := global.Get("fetch")

	options := global.Get("Object").New()
	options.Set("method", "HEAD")

	promise := fetch.Invoke(uri, options)
	ch := make(chan js.Value, 1)

	success := js.FuncOf(func(this js.Value, args []js.Value) any {
		ch <- args[0]
		return nil
	})

	defer success.Release()

	promise.Call("then", success)
	resp := <-ch

	var content_len int64

	headers := resp.Get("headers")

	if headers.Type() != js.TypeUndefined && headers.Type() != js.TypeNull {

		v := headers.Call("get", "content-length")

		if v.Type() == js.TypeString {

			parsed, err := strconv.ParseInt(v.String(), 10, 64)

			if err == nil {
				content_len = parsed
			}
		}
	}

	if content_len == 0 {
		return nil, 0, fmt.Errorf("Failed to fetch valid content-length header from server")
	}

	reader := &HTTPRangeReader{
		uri:  uri,
		size: content_len,
	}

	return reader, content_len, nil
}
