package iceberg

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestCanonicalResyncCheckpointNormalizesOnlyKnownCollections(t *testing.T) {
	const expected = `{"type":"MIXED","global":{"state":{"cursor":[2,1],"precise":9007199254740993},"streams":["public.events","public.other"]},"streams":[{"stream":"events","namespace":"public","sync_mode":"incremental","state":{"chunks":[{"min":1,"max":2},{"min":3,"max":4}],"cursor":[9,8],"precise":1.2300e+40}},{"stream":"other","namespace":"public","sync_mode":"full_refresh","state":{"chunks":[],"cursor":[6,5]}}]}`
	const reordered = `{"streams":[{"state":{"cursor":[6,5],"chunks":[]},"sync_mode":"full_refresh","namespace":"public","stream":"other"},{"state":{"precise":1.2300e+40,"cursor":[9,8],"chunks":[{"max":4,"min":3},{"max":2,"min":1}]},"sync_mode":"incremental","namespace":"public","stream":"events"}],"global":{"streams":["public.other","public.events"],"state":{"precise":9007199254740993,"cursor":[2,1]}},"type":"MIXED"}`
	canonical, err := canonicalResyncCheckpoint(reordered, expected)
	require.NoError(t, err)
	require.Contains(t, string(canonical), `"precise":9007199254740993`)
	require.Contains(t, string(canonical), `"precise":1.2300e+40`)
	require.Contains(t, string(canonical), `"cursor":[9,8]`)
	require.Contains(t, string(canonical), `"cursor":[2,1]`)
	for _, change := range []struct {
		name string
		old  string
		new  string
	}{
		{"global-membership", `"public.other"`, `"public.unrelated"`},
		{"stream-membership", `"stream":"other"`, `"stream":"unrelated"`},
		{"chunk-member", `"max":4`, `"max":5`},
		{"global-cursor-order", `"cursor":[2,1]`, `"cursor":[1,2]`},
		{"stream-cursor-order", `"cursor":[9,8]`, `"cursor":[8,9]`},
		{"precise-integer", `9007199254740993`, `9007199254740992`},
		{"numeric-lexeme", `1.2300e+40`, `1.23e+40`},
	} {
		t.Run(change.name, func(t *testing.T) {
			changed := strings.Replace(expected, change.old, change.new, 1)
			_, err := canonicalResyncCheckpoint(changed, expected)
			require.ErrorContains(t, err, "differs from the final source checkpoint")
		})
	}
}

func TestCanonicalResyncCheckpointRequiresFullJSONObject(t *testing.T) {
	for _, checkpoint := range []string{"", "null", "[]", `"checkpoint"`, "42", `{"type":"GLOBAL"} {}`} {
		t.Run(checkpoint, func(t *testing.T) {
			_, err := canonicalResyncCheckpoint(checkpoint, "")
			require.Error(t, err)
		})
	}
}
