package plan

import (
	"fmt"
	"github.com/ZzzYtl/MyMask/mysql"
	"github.com/ZzzYtl/MyMask/util/hack"
	"strconv"
)

// GenerateSelectResultRowData generate raw RowData from values
// 根据value反向构造RowData
// copy from server.buildResultset()
func GenerateSelectResultRowData(r *mysql.Result) error {
	r.RowDatas = nil
	for i, vs := range r.Values {
		if len(vs) != len(r.Fields) {
			return fmt.Errorf("row %d has %d column not equal %d", i, len(vs), len(r.Fields))
		}

		var row []byte
		for _, value := range vs {
			// build row values
			if value == nil {
				row = append(row, 0xfb)
			} else {
				b, err := formatValue(value)
				if err != nil {
					return err
				}
				row = mysql.AppendLenEncStringBytes(row, b)
			}
		}

		r.RowDatas = append(r.RowDatas, row)
	}

	return nil
}

// copy from server.formatValue()
// formatValue encode value into a string format
func formatValue(value interface{}) ([]byte, error) {
	if value == nil {
		return hack.Slice("NULL"), nil
	}
	switch v := value.(type) {
	case int8:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int16:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int32:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int64:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case uint8:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint16:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint32:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint64:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case float32:
		return strconv.AppendFloat(nil, float64(v), 'f', -1, 64), nil
	case float64:
		return strconv.AppendFloat(nil, float64(v), 'f', -1, 64), nil
	case []byte:
		return v, nil
	case string:
		return hack.Slice(v), nil
	default:
		return nil, fmt.Errorf("invalid type %T", value)
	}
}
