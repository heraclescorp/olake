package iceberg

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/pkg/indexdb"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	protobuf "google.golang.org/protobuf/proto"
)

type indexScanServer struct {
	proto.UnimplementedTableIndexServiceServer
	batches  []*proto.TableIndexScanBatch
	requests []*proto.TableIndexScanRequest
}

func (s *indexScanServer) ScanTableForIndexing(request *proto.TableIndexScanRequest, stream grpc.ServerStreamingServer[proto.TableIndexScanBatch]) error {
	s.requests = append(s.requests, request)
	for _, batch := range s.batches {
		if err := stream.Send(batch); err != nil {
			return err
		}
	}
	return nil
}

func TestFillTableIndexReceivesLargeScanBatch(t *testing.T) {
	const snapshotID = int64(42)
	filePath := "s3://test-table/data/" + strings.Repeat("partition/", 40) + "00001.parquet"
	batch := &proto.TableIndexScanBatch{SnapshotId: snapshotID}
	for row := range 10_000 {
		batch.Entries = append(batch.Entries, &proto.TableIndexScanBatch_Entry{
			OlakeId:  fmt.Sprintf("%032x", row),
			FilePath: filePath,
			Position: int64(row),
		})
	}
	require.Greater(t, protobuf.Size(batch), 4*1024*1024)
	tail := &proto.TableIndexScanBatch{
		SnapshotId: snapshotID,
		Entries: []*proto.TableIndexScanBatch_Entry{{
			OlakeId:  "tail-row",
			FilePath: "s3://test-table/data/00002.parquet",
			Position: 17,
		}},
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	proto.RegisterTableIndexServiceServer(server, &indexScanServer{batches: []*proto.TableIndexScanBatch{batch, tail}})
	t.Cleanup(server.Stop)
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("serve index scan: %v", err)
		}
	}()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	t.Setenv(constants.IndexDBDir, t.TempDir())
	stream := types.NewStream("events", "public", nil).Wrap(0)
	index, err := indexdb.Open(stream)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })

	writer := &Iceberg{
		options: &destination.Options{ThreadID: "index-recovery"},
		server:  &serverInstance{tableIndexClient: proto.NewTableIndexServiceClient(conn)},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, writer.fillTableIndex(ctx, index, "public.events", nil))

	for _, scanned := range append(batch.Entries, tail.Entries...) {
		location, found, err := index.Lookup(scanned.GetOlakeId())
		require.NoError(t, err)
		require.True(t, found, "missing row %s", scanned.GetOlakeId())
		require.Equal(t, types.RowLocation{FilePath: scanned.GetFilePath(), Position: scanned.GetPosition()}, location)
	}
	checkpoint, err := index.LastCommittedSnapshot()
	require.NoError(t, err)
	require.Equal(t, snapshotID, checkpoint)
}

func TestResyncGenerationForcesFullScanAndDropsOldPhysicalLocations(t *testing.T) {
	scan := &indexScanServer{batches: []*proto.TableIndexScanBatch{{
		SnapshotId: 42,
		Entries: []*proto.TableIndexScanBatch_Entry{{
			OlakeId: "live-row", FilePath: "new.parquet", Position: 8,
		}},
	}}}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	proto.RegisterTableIndexServiceServer(server, scan)
	t.Cleanup(server.Stop)
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("serve index scan: %v", err)
		}
	}()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	t.Setenv(constants.IndexDBDir, t.TempDir())
	stream := types.NewStream("events", "public", nil).Wrap(0)
	index, err := indexdb.Open(stream)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })
	pending := types.NewStreamIndexThread(index)
	pending.Put("deleted-old-row", types.RowLocation{FilePath: "old.parquet", Position: 3})
	oldSnapshot := int64(11)
	require.NoError(t, index.Commit(pending, &oldSnapshot))
	require.NoError(t, indexdb.ReconcileGeneration(index, "completed-resync"))
	writer := &Iceberg{
		options: &destination.Options{ThreadID: "generation-recovery"},
		config:  &Config{}, stream: stream,
		server: &serverInstance{tableIndexClient: proto.NewTableIndexServiceClient(conn)},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, writer.reconcileTableIndex(ctx, index, 42, false))
	require.Len(t, scan.requests, 1)
	require.Nil(t, scan.requests[0].FromSnapshotId)
	_, found, err := index.Lookup("deleted-old-row")
	require.NoError(t, err)
	require.False(t, found)
	location, found, err := index.Lookup("live-row")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, types.RowLocation{FilePath: "new.parquet", Position: 8}, location)
	// The completed full scan must retain the durable generation marker, so the
	// following ordinary writer does not discard its freshly recovered index.
	require.NoError(t, indexdb.ReconcileGeneration(index, "completed-resync"))
	require.NoError(t, writer.reconcileTableIndex(ctx, index, 42, false))
	require.Len(t, scan.requests, 1)
}
