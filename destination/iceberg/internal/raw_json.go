package internal

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
)

func MarshalRawData(data map[string]any) ([]byte, error) {
	encoded, err := json.Marshal(data)
	if err == nil {
		return encoded, nil
	}
	var unsupported *json.UnsupportedValueError
	if !errors.As(err, &unsupported) {
		return encoded, err
	}
	kind := unsupported.Value.Kind()
	if kind != reflect.Float32 && kind != reflect.Float64 {
		return nil, err
	}
	value := unsupported.Value.Float()
	if !math.IsNaN(value) && !math.IsInf(value, 0) {
		return nil, err
	}
	return json.Marshal(rawJSONCompatibleValue(data, make(map[rawJSONContainer]any)))
}

type rawJSONContainer struct {
	kind    reflect.Kind
	pointer uintptr
	length  int
}

func rawJSONCompatibleValue(value any, copies map[rawJSONContainer]any) any {
	switch value := value.(type) {
	case float32:
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return rawJSONCompatibleValue(float64(value), copies)
		}
		return value
	case float64:
		switch {
		case math.IsNaN(value):
			return "NaN"
		case math.IsInf(value, 1):
			return "Infinity"
		case math.IsInf(value, -1):
			return "-Infinity"
		}
	case map[string]any:
		if value == nil {
			return value
		}
		key := rawJSONContainer{kind: reflect.Map, pointer: uintptr(reflect.ValueOf(value).UnsafePointer())}
		if previous, ok := copies[key]; ok {
			return previous
		}
		result := make(map[string]any, len(value))
		copies[key] = result
		for key, child := range value {
			result[key] = rawJSONCompatibleValue(child, copies)
		}
		return result
	case []any:
		if value == nil {
			return value
		}
		key := rawJSONContainer{kind: reflect.Slice, pointer: reflect.ValueOf(value).Pointer(), length: len(value)}
		if previous, ok := copies[key]; ok {
			return previous
		}
		result := make([]any, len(value))
		copies[key] = result
		for index, child := range value {
			result[index] = rawJSONCompatibleValue(child, copies)
		}
		return result
	}
	return value
}
