//go:build wasmjs

package parquet

import (
	"fmt"
	"io"
	"syscall/js"
)

// Create a wrapper interface that bundles all required behaviors
type ReadCloserAt interface {
	io.ReaderAt
	io.Closer
	Size() int64 // Add this to allow parquet-go to determine file boundaries without type-assertion failures
}

// HTTPRangeReader implements io.ReaderAt by requesting specific chunks on-demand.
type HTTPRangeReader struct {
	uri  string
	size int64
}

// Size returns the file size determined during initialization, satisfying parquet-go's requirements.
func (h *HTTPRangeReader) Size() int64 {
	return h.size
}

func (h *HTTPRangeReader) ReadAt(p []byte, off int64) (int, error) {

	if len(p) == 0 {
		return 0, nil
	}

	global := js.Global()
	fetch := global.Get("fetch")

	options := global.Get("Object").New()
	headers := global.Get("Object").New()

	range_header := fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)

	headers.Set("Range", range_header)
	options.Set("headers", headers)

	promise := fetch.Invoke(h.uri, options)

	type fetchResult struct {
		resp js.Value
		err  error
	}

	ch := make(chan fetchResult, 1)

	success := js.FuncOf(func(this js.Value, args []js.Value) any {
		ch <- fetchResult{resp: args[0], err: nil}
		return nil
	})

	defer success.Release()

	failure := js.FuncOf(func(this js.Value, args []js.Value) any {
		ch <- fetchResult{resp: js.Undefined(), err: fmt.Errorf("Network error, %s", args[0].Call("toString").String())}
		return nil
	})

	defer failure.Release()

	promise.Call("then", success, failure)

	res := <-ch

	if res.err != nil {
		return 0, res.err
	}

	status := res.resp.Get("status").Int()

	if status != 206 && status != 200 {
		return 0, fmt.Errorf("http error status: %d", status)
	}

	buf_promise := res.resp.Call("arrayBuffer")
	buf_ch := make(chan js.Value, 1)

	buf_success := js.FuncOf(func(this js.Value, args []js.Value) any {
		buf_ch <- args[0]
		return nil
	})

	defer buf_success.Release()

	buf_promise.Call("then", buf_success)
	array_buf := <-buf_ch

	uint8_array := global.Get("Uint8Array").New(array_buf)
	bytes_read := uint8_array.Get("byteLength").Int()

	if bytes_read > len(p) {
		bytes_read = len(p)
	}

	js.CopyBytesToGo(p[:bytes_read], uint8_array)

	if bytes_read < len(p) {
		return bytes_read, io.EOF
	}

	return bytes_read, nil
}

func (h *HTTPRangeReader) Close() error {
	return nil
}
