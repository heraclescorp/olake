package typeutils

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/datazip-inc/olake/types"
)

// Decimal is an exact value of a decimal(p,s) column: its unscaled integer times 10^-scale. It
// carries its column type, so destinations derive the column from the value as they do for every
// type. NaN, which a source decimal column may hold, is a Decimal no destination decimal holds.
type Decimal struct {
	unscaled decimal128.Num
	scale    int32
	nan      bool
	dataType types.DataType
}

// NewDecimal is the value unscaled * 10^-scale of a decimal(precision, scale) column, the form
// Arrow stores it in.
func NewDecimal(unscaled decimal128.Num, precision, scale int32) Decimal {
	return Decimal{unscaled: unscaled, scale: scale, dataType: types.Decimal.Of(int(precision), int(scale))}
}

// Type is the decimal(p,s) the value was read as.
func (d Decimal) Type() types.DataType {
	return d.dataType
}

// Unscaled returns the unscaled integer and the scale it sits at, the form Arrow and Parquet store.
func (d Decimal) Unscaled() (decimal128.Num, int32) {
	return d.unscaled, d.scale
}

// IsNaN reports whether the value is the source's NaN.
func (d Decimal) IsNaN() bool {
	return d.nan
}

// String is the exact value with every digit of its scale, the way databases print decimals.
func (d Decimal) String() string {
	if d.nan {
		return "NaN"
	}
	return string(appendDecimalText(nil, d.unscaled, d.scale))
}

// MarshalJSON writes the value as an exact JSON number; NaN, which JSON numbers lack, as a string.
func (d Decimal) MarshalJSON() ([]byte, error) {
	if d.nan {
		return []byte(`"NaN"`), nil
	}
	return appendDecimalText(nil, d.unscaled, d.scale), nil
}

// Value binds the decimal as its exact text, which a database casts to the column's decimal;
// database/sql rejects a struct argument.
func (d Decimal) Value() (driver.Value, error) {
	return d.String(), nil
}

// Float64 is the double the value's text parses to: exactly what a double column held for it
// before decimal columns, so such a column keeps receiving the same values.
func (d Decimal) Float64() float64 {
	if d.nan {
		return math.NaN()
	}
	f, _ := strconv.ParseFloat(d.String(), 64)
	return f
}

// IDKey renders the value inside an olake id. A decimal key has always been rendered as its double,
// so tables keyed on a decimal column keep their row identities.
func (d Decimal) IDKey() string {
	return fmt.Sprint(d.Float64())
}

// Cmp compares two decimals by value whatever their scales; NaN sorts above every number, as in
// PostgreSQL.
func (d Decimal) Cmp(other Decimal) int {
	switch {
	case d.nan || other.nan:
		return boolCompare(d.nan, other.nan)
	case d.scale == other.scale:
		return d.unscaled.Cmp(other.unscaled)
	}
	a, b := d.unscaled.BigInt(), other.unscaled.BigInt()
	if d.scale < other.scale {
		a.Mul(a, decimal128.GetScaleMultiplier(int(other.scale-d.scale)).BigInt())
	} else {
		b.Mul(b, decimal128.GetScaleMultiplier(int(d.scale-other.scale)).BigInt())
	}
	return a.Cmp(b)
}

func boolCompare(a, b bool) int {
	switch {
	case a == b:
		return 0
	case a:
		return 1
	default:
		return -1
	}
}

// ReformatDecimal converts v to a value of dataType, a decimal(p,s). It parses text straight into
// the unscaled integer, never through a float, and fails rather than drop a digit or overflow the
// precision.
func ReformatDecimal(dataType types.DataType, v any) (Decimal, error) {
	if d, isDecimal := v.(Decimal); isDecimal && d.dataType == dataType {
		return d, nil
	}
	precision, scale, isDecimal := types.DecimalParams(dataType)
	if !isDecimal {
		return Decimal{}, fmt.Errorf("%s is not a decimal type", dataType)
	}
	unscaled, _, _, nan, err := decimalOf(v, int32(scale))
	switch {
	case err != nil:
		return Decimal{}, err
	case nan:
		return Decimal{scale: int32(scale), nan: true, dataType: dataType}, nil
	case !unscaled.FitsInPrecision(int32(precision)):
		return Decimal{}, fmt.Errorf("value[%v] does not fit a %s", v, dataType)
	}
	return Decimal{unscaled: unscaled, scale: int32(scale), dataType: dataType}, nil
}

// ParseDecimal reads v exactly at its own scale, so comparing it with a column's values loses
// nothing to rounding.
func ParseDecimal(v any) (Decimal, error) {
	unscaled, scale, digits, nan, err := decimalOf(v, -1)
	if err != nil {
		return Decimal{}, err
	}
	dataType, ok := types.DecimalOf(max(int(digits), int(scale), 1), int(scale))
	if !ok {
		return Decimal{}, fmt.Errorf("value[%v] exceeds %d digits", v, types.MaxDecimalPrecision)
	}
	return Decimal{unscaled: unscaled, scale: scale, nan: nan, dataType: dataType}, nil
}

// decimalOf reads v as an exact number at scale, or at its own scale when scale is negative.
func decimalOf(v any, scale int32) (decimal128.Num, int32, int32, bool, error) {
	switch v := v.(type) {
	case Decimal:
		if v.nan {
			return decimal128.Num{}, max(scale, 0), 0, true, nil
		}
		return parseDecimal(string(appendDecimalText(nil, v.unscaled, v.scale)), scale)
	case string:
		return parseDecimal(v, scale)
	case []byte:
		return parseDecimal(string(v), scale)
	case json.Number:
		return parseDecimal(string(v), scale)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return parseDecimal(fmt.Sprint(v), scale)
	case float32:
		return parseDecimal(strconv.FormatFloat(float64(v), 'f', -1, 32), scale)
	case float64:
		if math.IsNaN(v) {
			return decimal128.Num{}, max(scale, 0), 0, true, nil
		}
		return parseDecimal(strconv.FormatFloat(v, 'f', -1, 64), scale)
	case *any:
		if v == nil {
			return decimal128.Num{}, 0, 0, false, ErrNullValue
		}
		return decimalOf(*v, scale)
	default:
		return decimal128.Num{}, 0, 0, false, fmt.Errorf("failed to change %v (type:%T) to decimal", v, v)
	}
}

var ten = decimal128.FromU64(10)

// parseDecimal reads s, in plain or exponent notation, as an exact unscaled integer at scale, or at
// s's own scale when scale is negative. It returns the scale, the significant digits and whether s
// is NaN. It fails rather than drop a nonzero digit, and past 38 significant digits.
func parseDecimal(s string, scale int32) (decimal128.Num, int32, int32, bool, error) {
	invalid := func() (decimal128.Num, int32, int32, bool, error) {
		return decimal128.Num{}, 0, 0, false, fmt.Errorf("value[%s] is not a decimal number", s)
	}
	if strings.EqualFold(s, "nan") {
		return decimal128.Num{}, max(scale, 0), 0, true, nil
	}
	mantissa, negative := s, false
	if mantissa != "" && (mantissa[0] == '-' || mantissa[0] == '+') {
		negative, mantissa = mantissa[0] == '-', mantissa[1:]
	}
	exponent := 0
	if at := strings.IndexAny(mantissa, "eE"); at >= 0 {
		var err error
		if exponent, err = strconv.Atoi(mantissa[at+1:]); err != nil || exponent > 1000 || exponent < -1000 {
			return invalid()
		}
		mantissa = mantissa[:at]
	}
	integer, fraction, _ := strings.Cut(mantissa, ".")
	if integer == "" && fraction == "" {
		return invalid()
	}
	natural := int32(len(fraction) - exponent)
	target := scale
	if target < 0 {
		target = max(natural, 0)
	}

	// The value is digits * 10^-natural: at target it drops natural-target trailing digits, which
	// must be zeros, or gains target-natural zeros.
	total := len(integer) + len(fraction)
	kept := total - int(max(natural-target, 0))
	var unscaled decimal128.Num
	var significant int32
	for idx := range total {
		var c byte
		if idx < len(integer) {
			c = integer[idx]
		} else {
			c = fraction[idx-len(integer)]
		}
		if c < '0' || c > '9' {
			return invalid()
		}
		if idx >= kept {
			if c != '0' {
				return decimal128.Num{}, 0, 0, false, fmt.Errorf("value[%s] has more than the %d digits after the point a decimal of scale %d keeps", s, target, target)
			}
			continue
		}
		if significant == 0 && c == '0' {
			continue
		}
		if significant++; significant > types.MaxDecimalPrecision {
			return decimal128.Num{}, 0, 0, false, fmt.Errorf("value[%s] exceeds %d digits", s, types.MaxDecimalPrecision)
		}
		unscaled = unscaled.Mul(ten).Add(decimal128.FromU64(uint64(c - '0')))
	}
	if grow := int(max(target-natural, 0)); grow > 0 && significant > 0 {
		if significant += int32(grow); significant > types.MaxDecimalPrecision {
			return decimal128.Num{}, 0, 0, false, fmt.Errorf("value[%s] exceeds %d digits", s, types.MaxDecimalPrecision)
		}
		unscaled = unscaled.Mul(decimal128.GetScaleMultiplier(grow))
	}
	if negative {
		unscaled = unscaled.Negate()
	}
	return unscaled, target, significant, false, nil
}

// appendDecimalText appends an unscaled integer at scale exactly, with every digit of the scale.
func appendDecimalText(dst []byte, unscaled decimal128.Num, scale int32) []byte {
	if unscaled.Sign() < 0 {
		dst = append(dst, '-')
	}
	magnitude := unscaled.Abs()
	var digits []byte
	if magnitude.HighBits() == 0 {
		var buf [20]byte
		digits = strconv.AppendUint(buf[:0], magnitude.LowBits(), 10)
	} else {
		digits = magnitude.BigInt().Append(nil, 10)
	}
	if scale <= 0 {
		return append(dst, digits...)
	}
	if len(digits) <= int(scale) {
		dst = append(dst, '0', '.')
		for range int(scale) - len(digits) {
			dst = append(dst, '0')
		}
		return append(dst, digits...)
	}
	at := len(digits) - int(scale)
	return append(append(append(dst, digits[:at]...), '.'), digits[at:]...)
}
