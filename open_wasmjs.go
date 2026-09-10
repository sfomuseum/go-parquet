//go:build wasmjs

// UNTESTED

package parquet

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"syscall/js"
)

// jsStreamReader wraps a JavaScript ReadableStreamDefaultReader
// to satisfy the Go io.ReadCloser interface lazily.
type jsStreamReader struct {
	reader js.Value
}

func (j *jsStreamReader) Read(p []byte) (int, error) {
	// Call the JavaScript reader.read() method which returns a Promise
	promise := j.reader.Call("read")

	type readResult struct {
		n   int
		err error
	}
	done_ch := make(chan readResult, 1)

	// Define success handler callback
	on_success := js.FuncOf(func(this js.Value, args []js.Value) any {
		result := args[0]
		done := result.Get("done").Bool()

		if done {
			done_ch <- readResult{0, io.EOF}
			return nil
		}

		// Retrieve the Uint8Array from JavaScript
		value := result.Get("value")
		js_len := value.Get("byteLength").Int()

		if js_len == 0 {
			done_ch <- readResult{0, nil}
			return nil
		}

		// Cap bytes copied to avoid bursting the Go destination slice boundary
		cp_len := len(p)
		if js_len < cp_len {
			cp_len = js_len
		}

		// Safely extract the chunk bytes directly into Go memory space
		js.CopyBytesToGo(p[:cp_len], value)
		done_ch <- readResult{cp_len, nil}
		return nil
	})
	defer on_success.Release()

	// Define error handler callback
	onFailure := js.FuncOf(func(this js.Value, args []js.Value) any {
		jsErr := args[0].Call("toString").String()
		done_ch <- readResult{0, fmt.Errorf("js fetch stream error: %s", jsErr)}
		return nil
	})
	defer onFailure.Release()

	// Bind handlers to the promise
	promise.Call("then", on_success, onFailure)

	// Block until the TinyGo scheduler wakes up this channel on JS callback resolution
	res := <-done_ch
	return res.n, res.err
}

func (j *jsStreamReader) Close() error {
	j.reader.Call("cancel")
	return nil
}

// OpenURI opens a remote parquet asset via Browser Fetch without memory pre-allocations.
func OpenURI(uri string) (ReadCloserAt, int64, error) {

	if strings.HasPrefix(uri, "file://") || !strings.Contains(uri, "://") {
		return nil, 0, fmt.Errorf("parquet.OpenURI: local file access is disabled in WASM environments")
	}

	global := js.Global()
	fetch := global.Get("fetch")

	if fetch.Type() == js.TypeUndefined {
		return nil, 0, fmt.Errorf("parquet.OpenURI: global fetch API not found in this environment")
	}

	// Trigger the asynchronous JS fetch function
	fetch_promise := fetch.Invoke(uri)

	type fetchResult struct {
		resp js.Value
		err  error
	}

	done_ch := make(chan fetchResult, 1)

	fetch_onsuccess := js.FuncOf(func(this js.Value, args []js.Value) any {
		done_ch <- fetchResult{resp: args[0], err: nil}
		return nil

	})

	defer fetch_onsuccess.Release()

	fetch_onfailure := js.FuncOf(func(this js.Value, args []js.Value) any {
		js_err := args[0].Call("toString").String()
		done_ch <- fetchResult{resp: js.Undefined(), err: fmt.Errorf("fetch failed: %s", js_err)}
		return nil
	})

	defer fetch_onfailure.Release()

	fetch_promise.Call("then", fetch_onsuccess, fetch_onfailure)

	// Block until the network response headers settle

	res := <-done_ch

	if res.err != nil {
		return nil, 0, res.err
	}

	// Verify HTTP Ok response codes

	ok := res.resp.Get("ok").Bool()

	if !ok {
		status := res.resp.Get("status").Int()
		return nil, 0, fmt.Errorf("parquet.OpenURI: bad server response status: %d", status)
	}

	// Extract Content-Length header to determine parquet size
	var contentLength int64
	headers := res.resp.Get("headers")
	if headers.Type() != js.TypeUndefined && headers.Type() != js.TypeNull {
		clValue := headers.Call("get", "content-length")
		if clValue.Type() == js.TypeString {
			if parsed, err := strconv.ParseInt(clValue.String(), 10, 64); err == nil {
				contentLength = parsed
			}
		}
	}

	// Pull the streaming body pointer and spin up a reader
	body_str := res.resp.Get("body")

	if body_str.Type() == js.TypeNull || body_str.Type() == js.TypeUndefined {
		return nil, 0, fmt.Errorf("parquet.OpenURI: response body is not readable")
	}

	reader := body_str.Call("getReader")
	lazy_str := &jsStreamReader{reader: reader}

	// Route into your provided cachedReaderAt layout to lazily consume 4KB blocks
	return NewCachedReaderAt(lazy_str), contentLength, nil
}
