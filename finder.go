package parquet

import (
	"errors"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
)

type Id interface {
	string | int64
}

// FindRecordById scans any parquet schema by an ID field.
// T: Represents the Go struct model to return
// K: Represents the search key type (string or int64)
func FindRecordById[T any, K Id](uri string, id K) (T, error) {

	var zero T

	rsp, err := FindRecordByIdAll[T, K](uri, id)

	if err != nil {
		return zero, err
	}

	switch len(rsp) {
	case 1:
		return rsp[0], nil
	case 0:
		return zero, fmt.Errorf("Not found")
	default:
		return zero, fmt.Errorf("Multiple results")
	}
}

func FindRecordByIdAll[T any, K Id](uri string, id K) ([]T, error) {

	file, sz, err := OpenURI(uri)

	if err != nil {
		return nil, err
	}

	defer file.Close()

	pf, err := parquet.OpenFile(file, sz)

	if err != nil {
		return nil, err
	}

	return FindRecordByIdWithParquetFileAll[T, K](file, pf, id)
}

func FindRecordByIdWithParquetFile[T any, K Id](file io.ReaderAt, pf *parquet.File, id K) (T, error) {

	var zero T

	rsp, err := FindRecordByIdWithParquetFileAll[T, K](file, pf, id)

	if err != nil {
		return zero, err
	}

	switch len(rsp) {
	case 1:
		return rsp[0], nil
	case 0:
		return zero, fmt.Errorf("Not found")
	default:
		return zero, fmt.Errorf("Multiple results")
	}
}

func FindRecordByIdWithParquetFileAll[T any, K Id](file io.ReaderAt, pf *parquet.File, id K) ([]T, error) {

	id_idx := -1

	fields := pf.Schema().Fields()

	for i, field := range fields {

		if field.Name() == "id" {
			id_idx = i
			break
		}
	}

	if id_idx == -1 {
		id_idx = 0
	}

	// Prepare a parquet.Value representation of our target ID for Bloom filter evaluation

	var parquet_v parquet.Value

	switch v := any(id).(type) {
	case int64:
		parquet_v = parquet.Int64Value(v)
	case string:
		parquet_v = parquet.ByteArrayValue([]byte(v))
	}

	var row_idx int64 = 0
	var matched_idx []int64

	for _, rg := range pf.RowGroups() {

		chunks := rg.ColumnChunks()

		if len(chunks) <= id_idx {
			continue
		}

		id_chunk := chunks[id_idx]

		bf := id_chunk.BloomFilter()

		if bf != nil {

			hasValue, err := bf.Check(parquet_v)

			if err == nil && !hasValue {
				row_idx += rg.NumRows()
				continue
			}
		}

		id_pages := id_chunk.Pages()

		for {

			p, err := id_pages.ReadPage()

			if err != nil {

				if errors.Is(err, io.EOF) {
					break
				}

				id_pages.Close()
				return nil, err
			}

			// Branch depending on key type for optimized vector scans
			switch any(id).(type) {
			case int64:

				target := any(id).(int64)

				int64_r, ok := p.Values().(parquet.Int64Reader)

				if ok {

					id_buf := make([]int64, p.NumValues())
					n, _ := int64_r.ReadInt64s(id_buf)

					for i := 0; i < n; i++ {

						if id_buf[i] == target {
							// id_pages.Close()
							// exactRowIndex := row_idx + int64(i)
							// return fetchFullRowAt[T](file, exactRowIndex)

							matched_idx = append(matched_idx, row_idx+int64(i))
						}
					}

					row_idx += int64(n)
					continue
				}

			case string:

				target := any(id).(string)
				// Strings use the generic underlying Value scanner
				values := make([]parquet.Value, p.NumValues())

				n, _ := p.Values().ReadValues(values)

				for i := 0; i < n; i++ {
					if values[i].String() == target {
						// id_pages.Close()
						// exact_idx := row_idx + int64(i)
						// return fetchFullRowAt[T](file, exact_idx)

						matched_idx = append(matched_idx, row_idx+int64(i))
					}
				}
				row_idx += int64(n)
				continue
			}

			// Catch-all structural fallback evaluation
			values := make([]parquet.Value, p.NumValues())
			n, _ := p.Values().ReadValues(values)
			for i := 0; i < n; i++ {
				match := false

				int_v, ok := any(id).(int64)

				if ok && values[i].Int64() == int_v {
					match = true
				} else if str_v, ok := any(id).(string); ok && values[i].String() == str_v {
					match = true
				}

				if match {
					//id_pages.Close()
					//exact_idx := row_idx + int64(i)
					//return fetchFullRowAt[T](file, exact_idx)
					matched_idx = append(matched_idx, row_idx+int64(i))
				}
			}

			row_idx += int64(n)
		}

		id_pages.Close()
	}

	// return zero, fmt.Errorf("id not found in parquet archive")
	return fetchFullRowsAt[T](file, matched_idx)
}

func fetchFullRowsAt[T any](file io.ReaderAt, row_idx []int64) ([]T, error) {

	reader := parquet.NewGenericReader[T](file)
	defer reader.Close()

	results := make([]T, 0, len(row_idx))
	buffer := make([]T, 1)

	for _, idx := range row_idx {

		err := reader.SeekToRow(idx)

		if err != nil {
			return nil, err
		}

		n, err := reader.Read(buffer)

		if n > 0 {
			results = append(results, buffer[0])
		}

		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
	}

	return results, nil
}
