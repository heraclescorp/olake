package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type resyncControlClient struct {
	proto.RecordIngestServiceClient
	requests []*proto.IcebergPayload
	send     func(context.Context, *proto.IcebergPayload) (*proto.RecordIngestResponse, error)
}

func (c *resyncControlClient) SendRecords(ctx context.Context, request *proto.IcebergPayload, _ ...grpc.CallOption) (*proto.RecordIngestResponse, error) {
	c.requests = append(c.requests, request)
	return c.send(ctx, request)
}

func resyncTestAdapter(client *resyncControlClient) *Iceberg {
	return &Iceberg{config: &Config{}, server: &serverInstance{client: client}}
}

func resyncTestState() *types.State {
	return &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, Global: &types.GlobalState{
		State:   map[string]any{"lsn": "0/ABC", "large": json.Number("9007199254740993")},
		Streams: types.NewSet[string]("public.events", "public.unrelated"),
	}}
}

func publishedResyncResponse(t *testing.T, checkpoint *types.State) *proto.RecordIngestResponse {
	t.Helper()
	encoded, err := json.Marshal(checkpoint)
	require.NoError(t, err)
	return &proto.RecordIngestResponse{
		Success: true, ResyncPublished: true, ResyncGeneration: "operation-1",
		ResyncCheckpoint: string(encoded), ResyncTableUuid: "table-uuid", SnapshotId: 42,
	}
}

func TestPrepareResyncRecoversFullCommittedCheckpointBeforeAnyWrites(t *testing.T) {
	client := &resyncControlClient{send: func(_ context.Context, request *proto.IcebergPayload) (*proto.RecordIngestResponse, error) {
		require.Equal(t, proto.IcebergPayload_PREPARE_RESYNC, request.GetType())
		require.Equal(t, "operation-1", request.GetMetadata().GetResyncId())
		require.Equal(t, "events", request.GetMetadata().GetDestTableName())
		require.Positive(t, request.GetMetadata().GetResyncDeadlineMs())
		require.Equal(t, `{"global":{"state":{"large":9007199254740993,"lsn":"0/ABC"},"streams":["public.events","public.unrelated"]},"type":"GLOBAL"}`, request.GetMetadata().GetResyncCheckpoint())
		return publishedResyncResponse(t, resyncTestState()), nil
	}}
	adapter := resyncTestAdapter(client)
	ctx, cancel := context.WithTimeout(context.Background(), destination.ResyncMaxDuration)
	defer cancel()
	checkpoint, err := adapter.PrepareResync(ctx, types.NewStream("events", "public", nil).Wrap(0), "operation-1", resyncTestState())
	require.NoError(t, err)
	require.NotNil(t, checkpoint)
	require.True(t, checkpoint.Global.Streams.Exists("public.unrelated"))
	require.Equal(t, json.Number("9007199254740993"), checkpoint.Global.State.(map[string]any)["large"])
	require.True(t, adapter.ResyncPublished())
	uuid, snapshot := adapter.ResyncReceipt()
	require.Equal(t, "table-uuid", uuid)
	require.Equal(t, int64(42), snapshot)
	require.Len(t, client.requests, 1)
	require.Error(t, adapter.AbortResync(ctx))
	require.NoError(t, adapter.FinishResync(ctx))
	uuid, snapshot = adapter.ResyncReceipt()
	require.Equal(t, "table-uuid", uuid)
	require.Equal(t, int64(42), snapshot)
}

func TestPrepareResyncRequiresBoundedLiveContext(t *testing.T) {
	for _, name := range []string{"unbounded", "too-long", "cancelled", "blank-id"} {
		t.Run(name, func(t *testing.T) {
			client := &resyncControlClient{send: func(context.Context, *proto.IcebergPayload) (*proto.RecordIngestResponse, error) {
				t.Fatal("invalid prepare reached the JVM")
				return nil, nil
			}}
			ctx := context.Background()
			id := "operation-1"
			var cancel context.CancelFunc
			switch name {
			case "too-long":
				ctx, cancel = context.WithTimeout(ctx, 13*time.Hour)
			case "cancelled":
				ctx, cancel = context.WithTimeout(ctx, time.Hour)
				cancel()
			case "blank-id":
				ctx, cancel = context.WithTimeout(ctx, time.Hour)
				id = "  "
			}
			if cancel != nil {
				defer cancel()
			}
			_, err := resyncTestAdapter(client).PrepareResync(ctx, types.NewStream("events", "public", nil).Wrap(0), id, resyncTestState())
			require.Error(t, err)
		})
	}
}

func TestPublishResyncRecoversLostResponseFromDurableStatus(t *testing.T) {
	checkpoint := resyncTestState()
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	client := &resyncControlClient{send: func(ctx context.Context, request *proto.IcebergPayload) (*proto.RecordIngestResponse, error) {
		switch request.GetType() {
		case proto.IcebergPayload_PREPARE_RESYNC:
			return &proto.RecordIngestResponse{Success: true}, nil
		case proto.IcebergPayload_PUBLISH_RESYNC:
			require.Equal(t, `{"global":{"state":{"large":9007199254740993,"lsn":"0/ABC"},"streams":["public.events","public.unrelated"]},"type":"GLOBAL"}`, request.GetMetadata().GetResyncCheckpoint())
			cancel()
			return nil, errors.New("publish response lost")
		case proto.IcebergPayload_RESYNC_STATUS:
			require.NoError(t, ctx.Err())
			return publishedResyncResponse(t, checkpoint), nil
		default:
			t.Fatalf("unexpected control %s", request.GetType())
			return nil, nil
		}
	}}
	adapter := resyncTestAdapter(client)
	_, err := adapter.PrepareResync(ctx, types.NewStream("events", "public", nil).Wrap(0), "operation-1", resyncTestState())
	require.NoError(t, err)
	require.NoError(t, adapter.PublishResync(ctx, checkpoint))
	require.True(t, adapter.ResyncPublished())
	require.Len(t, client.requests, 3)
	require.NoError(t, adapter.PublishResync(ctx, checkpoint))
	require.Len(t, client.requests, 3)
}

func TestUnconfirmedPublishCannotClaimAbortOrSuccess(t *testing.T) {
	client := &resyncControlClient{send: func(_ context.Context, request *proto.IcebergPayload) (*proto.RecordIngestResponse, error) {
		if request.GetType() == proto.IcebergPayload_PUBLISH_RESYNC {
			return nil, errors.New("connection lost")
		}
		if request.GetType() == proto.IcebergPayload_ABORT_RESYNC {
			return nil, errors.New("commit outcome unknown; retain attempt files")
		}
		return &proto.RecordIngestResponse{Success: true}, nil
	}}
	adapter := resyncTestAdapter(client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	_, err := adapter.PrepareResync(ctx, types.NewStream("events", "public", nil).Wrap(0), "operation-1", resyncTestState())
	require.NoError(t, err)
	require.ErrorContains(t, adapter.PublishResync(ctx, resyncTestState()), "publication is unconfirmed")
	require.False(t, adapter.ResyncPublished())
	require.ErrorContains(t, adapter.AbortResync(ctx), "unknown")
	uuid, snapshot := adapter.ResyncReceipt()
	require.Empty(t, uuid)
	require.Zero(t, snapshot)
	require.Len(t, client.requests, 4)
}

func TestPublishedReceiptMustContainIdentitySnapshotAndExactCheckpoint(t *testing.T) {
	for _, name := range []string{"missing-uuid", "missing-snapshot", "different-generation", "different-checkpoint", "different-membership"} {
		t.Run(name, func(t *testing.T) {
			response := publishedResyncResponse(t, resyncTestState())
			switch name {
			case "missing-uuid":
				response.ResyncTableUuid = ""
			case "missing-snapshot":
				response.SnapshotId = 0
			case "different-generation":
				response.ResyncGeneration = "other-operation"
			case "different-checkpoint":
				response.ResyncCheckpoint = `{"type":"GLOBAL","global":{"state":{"lsn":"0/OLD"},"streams":[]}}`
			case "different-membership":
				response.ResyncCheckpoint = `{"type":"GLOBAL","global":{"state":{"lsn":"0/ABC","large":9007199254740993},"streams":["public.events","public.other"]}}`
			}
			adapter := resyncTestAdapter(nil)
			adapter.resync = &resyncControl{id: "operation-1"}
			expected, err := json.Marshal(resyncTestState())
			require.NoError(t, err)
			_, err = adapter.acceptPublishedResync(response, string(expected))
			require.Error(t, err)
			require.False(t, adapter.ResyncPublished())
		})
	}
}

func TestPublishedReceiptAcceptsReorderedStreamMembership(t *testing.T) {
	response := publishedResyncResponse(t, resyncTestState())
	response.ResyncCheckpoint = `{"type":"GLOBAL","global":{"state":{"lsn":"0/ABC","large":9007199254740993},"streams":["public.unrelated","public.events"]}}`
	expected := `{"type":"GLOBAL","global":{"state":{"lsn":"0/ABC","large":9007199254740993},"streams":["public.events","public.unrelated"]}}`
	adapter := resyncTestAdapter(nil)
	adapter.resync = &resyncControl{id: "operation-1"}
	state, err := adapter.acceptPublishedResync(response, expected)
	require.NoError(t, err)
	require.True(t, adapter.ResyncPublished())
	require.True(t, state.Global.Streams.Exists("public.unrelated"))
}

func TestFailedPrepareRequiresConfirmedServerAbort(t *testing.T) {
	client := &resyncControlClient{send: func(_ context.Context, request *proto.IcebergPayload) (*proto.RecordIngestResponse, error) {
		if request.GetType() == proto.IcebergPayload_PREPARE_RESYNC {
			return nil, errors.New("prepare response lost")
		}
		require.Equal(t, proto.IcebergPayload_ABORT_RESYNC, request.GetType())
		return &proto.RecordIngestResponse{Success: true}, nil
	}}
	adapter := resyncTestAdapter(client)
	require.Error(t, adapter.AbortResync(context.Background()))
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	_, err := adapter.PrepareResync(ctx, types.NewStream("events", "public", nil).Wrap(0), "operation-1", resyncTestState())
	require.Error(t, err)
	require.NoError(t, adapter.AbortResync(ctx))
	require.Len(t, client.requests, 2)
	require.Error(t, adapter.PublishResync(ctx, resyncTestState()))
}

func TestPublishCannotExtendFreshAttemptDeadline(t *testing.T) {
	client := &resyncControlClient{send: func(_ context.Context, request *proto.IcebergPayload) (*proto.RecordIngestResponse, error) {
		require.Equal(t, proto.IcebergPayload_PREPARE_RESYNC, request.GetType())
		return &proto.RecordIngestResponse{Success: true}, nil
	}}
	adapter := resyncTestAdapter(client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	_, err := adapter.PrepareResync(ctx, types.NewStream("events", "public", nil).Wrap(0), "operation-1", resyncTestState())
	require.NoError(t, err)
	adapter.resync.deadline = time.Now().Add(-time.Second)
	require.ErrorContains(t, adapter.PublishResync(ctx, resyncTestState()), "deadline")
	require.Len(t, client.requests, 1)
}

func TestDefinitivePublishRejectionCanBeFencedByAbort(t *testing.T) {
	client := &resyncControlClient{send: func(_ context.Context, request *proto.IcebergPayload) (*proto.RecordIngestResponse, error) {
		if request.GetType() == proto.IcebergPayload_PUBLISH_RESYNC {
			return nil, errors.New("concurrent catalog update rejected")
		}
		return &proto.RecordIngestResponse{Success: true}, nil
	}}
	adapter := resyncTestAdapter(client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	_, err := adapter.PrepareResync(ctx, types.NewStream("events", "public", nil).Wrap(0), "operation-1", resyncTestState())
	require.NoError(t, err)
	require.Error(t, adapter.PublishResync(ctx, resyncTestState()))
	require.NoError(t, adapter.AbortResync(ctx))
	require.False(t, adapter.ResyncPublished())
	require.Error(t, adapter.PublishResync(ctx, resyncTestState()))
	require.Len(t, client.requests, 4)
}

func TestUnsupportedJvmResyncFailsWithoutFallbackRequests(t *testing.T) {
	client := &resyncControlClient{send: func(_ context.Context, request *proto.IcebergPayload) (*proto.RecordIngestResponse, error) {
		require.Equal(t, proto.IcebergPayload_PREPARE_RESYNC, request.GetType())
		return &proto.RecordIngestResponse{Result: "unsupported payload type"}, nil
	}}
	adapter := resyncTestAdapter(client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	checkpoint, err := adapter.PrepareResync(ctx, types.NewStream("events", "public", nil).Wrap(0), "operation-1", resyncTestState())
	require.ErrorContains(t, err, "unsupported payload")
	require.Nil(t, checkpoint)
	require.False(t, adapter.ResyncPublished())
	require.Len(t, client.requests, 1)
}

func TestPrepareResyncRejectsMissingBaselineWithoutStaging(t *testing.T) {
	client := &resyncControlClient{send: func(context.Context, *proto.IcebergPayload) (*proto.RecordIngestResponse, error) {
		t.Fatal("missing baseline reached the JVM")
		return nil, nil
	}}
	adapter := resyncTestAdapter(client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	_, err := adapter.PrepareResync(ctx, types.NewStream("events", "public", nil).Wrap(0), "operation-1", nil)
	require.ErrorContains(t, err, "full baseline checkpoint")
	require.Nil(t, adapter.resync)
	require.Empty(t, client.requests)
}

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

func TestPrepareAndPublishSendSameCanonicalStreamCheckpoint(t *testing.T) {
	streamState := &types.StreamState{Stream: "events", Namespace: "public", SyncMode: "incremental"}
	streamState.HoldsValue.Store(true)
	streamState.State.Store(types.ChunksKey, types.NewSet[types.Chunk](
		types.Chunk{Min: json.Number("3"), Max: json.Number("4")},
		types.Chunk{Min: json.Number("1"), Max: json.Number("2")},
	))
	streamState.State.Store("cursor", []any{json.Number("9007199254740993"), json.Number("1.2300e+40")})
	checkpoint := &types.State{RWMutex: &sync.RWMutex{}, Type: types.StreamType, Streams: []*types.StreamState{streamState}}
	const expected = `{"streams":[{"namespace":"public","state":{"chunks":[{"max":2,"min":1},{"max":4,"min":3}],"cursor":[9007199254740993,1.2300e+40]},"stream":"events","sync_mode":"incremental"}],"type":"STREAM"}`
	client := &resyncControlClient{send: func(_ context.Context, request *proto.IcebergPayload) (*proto.RecordIngestResponse, error) {
		require.Equal(t, expected, request.GetMetadata().GetResyncCheckpoint())
		switch request.GetType() {
		case proto.IcebergPayload_PREPARE_RESYNC:
			return &proto.RecordIngestResponse{Success: true}, nil
		case proto.IcebergPayload_PUBLISH_RESYNC:
			response := publishedResyncResponse(t, checkpoint)
			response.ResyncCheckpoint = expected
			return response, nil
		default:
			t.Fatalf("unexpected control %s", request.GetType())
			return nil, nil
		}
	}}
	adapter := resyncTestAdapter(client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	_, err := adapter.PrepareResync(ctx, types.NewStream("events", "public", nil).Wrap(0), "operation-1", checkpoint)
	require.NoError(t, err)
	require.NoError(t, adapter.PublishResync(ctx, checkpoint))
	require.Len(t, client.requests, 2)
}
