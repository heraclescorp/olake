package typeutils

import (
	"database/sql/driver"
	"time"
)

// Date is a calendar date, held as midnight UTC of its day: the instant a timestamptz column has
// always stored for it. It carries its type, so destinations create a date column for it.
type Date struct {
	time.Time
}

// DateOf is the date t's wall clock reads, whatever its zone, so no zone shifts the day.
func DateOf(t time.Time) Date {
	year, month, day := t.Date()
	return Date{time.Date(year, month, day, 0, 0, 0, 0, time.UTC)}
}

// Value binds the date as a time.Time; database/sql rejects a struct argument.
func (d Date) Value() (driver.Value, error) {
	return d.Time, nil
}

// TimestampNTZ is a wall-clock date and time without a zone, held in UTC: the instant a timestamptz
// column has always stored for it. It carries its type, so destinations create a timestamp column
// (without a zone) for it.
type TimestampNTZ struct {
	time.Time
}

// TimestampNTZOf is the wall clock t reads, whatever its zone.
func TimestampNTZOf(t time.Time) TimestampNTZ {
	year, month, day := t.Date()
	hour, minute, second := t.Clock()
	return TimestampNTZ{time.Date(year, month, day, hour, minute, second, t.Nanosecond(), time.UTC)}
}

// Value binds the timestamp as a time.Time; database/sql rejects a struct argument.
func (t TimestampNTZ) Value() (driver.Value, error) {
	return t.Time, nil
}

// ReformatCalendarDate converts v to a Date, parsing it as ReformatDate does a timestamp, so
// unparseable and out-of-range values fall back as they do for timestamps.
func ReformatCalendarDate(v any) (Date, error) {
	if d, ok := v.(Date); ok {
		return d, nil
	}
	t, err := ReformatDate(v, true)
	if err != nil {
		return Date{}, err
	}
	return DateOf(t), nil
}

// ReformatTimestampNTZ converts v to a TimestampNTZ, parsing it as ReformatDate does a timestamp,
// so unparseable and out-of-range values fall back as they do for timestamps.
func ReformatTimestampNTZ(v any) (TimestampNTZ, error) {
	if t, ok := v.(TimestampNTZ); ok {
		return t, nil
	}
	t, err := ReformatDate(v, true)
	if err != nil {
		return TimestampNTZ{}, err
	}
	return TimestampNTZOf(t), nil
}
