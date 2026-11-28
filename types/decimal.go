package types

import (
	"github.com/datazip-inc/olake/utils"
	"github.com/parquet-go/parquet-go"
)

// MaxDecimalPrecision is the widest decimal Iceberg holds: its decimal(p, s) needs a fixed
// precision of at most 38 digits.
const MaxDecimalPrecision = 38

// decimalFamily registers decimal(%d,%d): a precision of 1 to MaxDecimalPrecision digits, of which
// the scale sit after the point.
var decimalFamily = newTypeFamily(Decimal, validDecimal)

// validDecimal is decimalFamily's parameter check, named so DecimalParams can call it without its
// array escaping through the family's func field.
func validDecimal(p []int) bool {
	return len(p) == 2 && p[0] >= 1 && p[0] <= MaxDecimalPrecision && p[1] >= 0 && p[1] <= p[0]
}

// DecimalOf returns the DataType of an exact number of precision digits, scale of them after the
// point, and false when Iceberg holds no such decimal.
func DecimalOf(precision, scale int) (DataType, bool) {
	params := [2]int{precision, scale}
	if !validDecimal(params[:]) {
		return "", false
	}
	return Decimal.Of(precision, scale), true
}

// DecimalParams returns the precision and scale of a decimal(p,s) DataType. It allocates nothing,
// so a writer can check every value against its column.
func DecimalParams(d DataType) (precision, scale int, ok bool) {
	var params [2]int
	if !utils.ScanType(d, Decimal, params[:]) || !validDecimal(params[:]) {
		return 0, 0, false
	}
	return params[0], params[1], true
}

// icebergDecimal is decimalFamily's iceberg pattern, decimal(%d, %d), read once for
// IcebergDecimalParams.
var icebergDecimal = decimalFamily.icebergPattern()

// IcebergDecimalParams returns the precision and scale of an iceberg decimal(p, s) type.
func IcebergDecimalParams(icebergType string) (precision, scale int, ok bool) {
	var params [2]int
	if !utils.ScanType(icebergType, icebergDecimal, params[:]) || !validDecimal(params[:]) {
		return 0, 0, false
	}
	return params[0], params[1], true
}

// decimalNode stores the unscaled value in an int32 up to 9 digits and an int64 up to 18. parquet-go
// orders fixed-length bytes unsigned in its statistics, mis-pruning negative decimals, so wider
// decimals are written as their exact text.
func decimalNode(params ...any) parquet.Node {
	precision, scale := params[0].(int), params[1].(int)
	switch {
	case precision <= 9:
		return parquet.Decimal(scale, precision, parquet.Int32Type)
	case precision <= 18:
		return parquet.Decimal(scale, precision, parquet.Int64Type)
	default:
		return parquet.String()
	}
}

// widerDecimal returns the narrowest decimal holding every value of two decimals, when Iceberg
// holds one.
func widerDecimal(t1, t2 DataType) (DataType, bool) {
	p1, s1, ok1 := DecimalParams(t1)
	p2, s2, ok2 := DecimalParams(t2)
	if !ok1 || !ok2 {
		return "", false
	}
	scale := max(s1, s2)
	return DecimalOf(max(p1-s1, p2-s2)+scale, scale)
}
