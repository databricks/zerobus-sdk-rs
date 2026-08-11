package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- fake record-count protocol --------------------------------------------
//
// The proto/JSON protocols are atomic: one logical item is always one
// durability unit, so they exercise none of the partial-acknowledgment,
// payload-slicing, or submission-receipt behaviour the core now supports. This
// fake protocol makes a single row the durability unit, so a server can
// acknowledge a prefix of a multi-row item and a transport can submit a prefix
// of one logical Send.

// rowPayload is one logical item whose durability unit is a single row.
type rowPayload struct {
	offset int64
	rows   []string
}

// rowResponse is the fake server response. It carries a cumulative row count
// for the record-count model and a connection-local wire offset for the atomic
// model, so one transport can drive both acknowledgment shapes.
type rowResponse struct {
	ackedRows  uint64
	wireOffset int64
	pause      time.Duration
	hasAck     bool
	hasPause   bool
	unknown    bool
}

func rowEncoderHooks() EncoderHooks[*rowPayload] {
	return EncoderHooks[*rowPayload]{
		EncodeRecord: func(record []byte) (*rowPayload, error) {
			return &rowPayload{rows: []string{string(record)}}, nil
		},
		EncodeBatch: func(records [][]byte) (*rowPayload, error) {
			if len(records) == 0 {
				return nil, fmt.Errorf("row batch must not be empty")
			}
			rows := make([]string, len(records))
			for i, record := range records {
				rows[i] = string(record)
			}
			return &rowPayload{rows: rows}, nil
		},
		StampOffset: func(payload *rowPayload, offset int64) { payload.offset = offset },
		UnitCount:   func(payload *rowPayload) uint64 { return uint64(len(payload.rows)) },
		Slice: func(payload *rowPayload, acknowledgedPrefix uint64) (*rowPayload, error) {
			if acknowledgedPrefix >= uint64(len(payload.rows)) {
				return nil, fmt.Errorf(
					"acknowledged prefix %d covers all %d rows",
					acknowledgedPrefix, len(payload.rows),
				)
			}
			return &rowPayload{
				offset: payload.offset,
				rows:   slices.Clone(payload.rows[acknowledgedPrefix:]),
			}, nil
		},
		Decode: func(payload *rowPayload) [][]byte {
			out := make([][]byte, len(payload.rows))
			for i, row := range payload.rows {
				out[i] = []byte(row)
			}
			return out
		},
		MaxWireSize: func(payload *rowPayload) int {
			size := 0
			for _, row := range payload.rows {
				size += len(row)
			}
			return size
		},
		RetainedSize: func(rawBytes, recordCount int) int64 {
			return int64(rawBytes + recordCount)
		},
	}
}

func rowAckHooks() AckModelHooks[*rowResponse] {
	return AckModelHooks[*rowResponse]{
		Classify: func(resp *rowResponse) ResponseClassification {
			if resp == nil || resp.unknown {
				return ResponseClassification{Status: ResponseUnknown}
			}
			return ResponseClassification{
				Status:        ResponseOK,
				HasAck:        resp.hasAck,
				HasPause:      resp.hasPause,
				PauseDuration: resp.pause,
			}
		},
		Resolve: func(resp *rowResponse, state AckState) (AckResolution, error) {
			return ResolveAcknowledgedUnits(resp.ackedRows, state)
		},
	}
}

// atomicRowAckHooks omits Resolve, the configuration an atomic protocol uses:
// the core resolves the hook-supplied connection-local wire offset itself
// instead of asking the protocol to translate a unit count.
func atomicRowAckHooks() AckModelHooks[*rowResponse] {
	hooks := rowAckHooks()
	hooks.Classify = func(resp *rowResponse) ResponseClassification {
		if resp == nil || resp.unknown {
			return ResponseClassification{Status: ResponseUnknown}
		}
		return ResponseClassification{
			Status:        ResponseOK,
			HasAck:        resp.hasAck,
			LegacyOffset:  resp.wireOffset,
			HasPause:      resp.hasPause,
			PauseDuration: resp.pause,
		}
	}
	hooks.Resolve = nil
	return hooks
}

// rowWire is an in-process transport for the fake row protocol.
type rowWire struct {
	sends     chan *rowPayload
	resps     chan *rowResponse
	delivered chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
}

func newRowWire() *rowWire {
	return &rowWire{
		sends:     make(chan *rowPayload, 64),
		resps:     make(chan *rowResponse, 64),
		delivered: make(chan struct{}, 64),
	}
}

func (w *rowWire) ServerID() string { return "row-wire" }

func (w *rowWire) Send(payload *rowPayload) error {
	if w.closed.Load() {
		return io.EOF
	}
	select {
	case w.sends <- payload:
		return nil
	default:
		return fmt.Errorf("rowWire: sends channel full")
	}
}

func (w *rowWire) Recv() (*rowResponse, error) {
	resp, ok := <-w.resps
	if !ok {
		return nil, io.EOF
	}
	// Signals that the receiver has taken this response, so a test can order a
	// later event after it.
	select {
	case w.delivered <- struct{}{}:
	default:
	}
	return resp, nil
}

func (w *rowWire) CloseSend() error {
	w.shutdown()
	return nil
}

func (w *rowWire) Close() { w.shutdown() }

func (w *rowWire) shutdown() {
	w.closeOnce.Do(func() {
		w.closed.Store(true)
		close(w.resps)
	})
}

// ackRows publishes a cumulative connection-local row acknowledgment.
func (w *rowWire) ackRows(n uint64) {
	if w.closed.Load() {
		return
	}
	w.resps <- &rowResponse{hasAck: true, ackedRows: n}
}

// ackWireOffset publishes a cumulative connection-local wire offset ack.
func (w *rowWire) ackWireOffset(offset int64) {
	if w.closed.Load() {
		return
	}
	w.resps <- &rowResponse{hasAck: true, wireOffset: offset}
}

// nextSend waits for the transport to receive one logical payload.
func (w *rowWire) nextSend(t *testing.T) *rowPayload {
	t.Helper()
	select {
	case payload := <-w.sends:
		return payload
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for a payload to reach the transport")
		return nil
	}
}

// partialRowWire submits only a prefix of the first logical Send and then
// fails, modelling a multi-frame transport whose later frame is rejected.
type partialRowWire struct {
	*rowWire
	submitRows uint64
	failWith   error
	failed     atomic.Bool
}

func (w *partialRowWire) SendWithReceipt(payload *rowPayload) (SubmissionReceipt, error) {
	if w.failed.CompareAndSwap(false, true) {
		prefix := &rowPayload{
			offset: payload.offset,
			rows:   slices.Clone(payload.rows[:w.submitRows]),
		}
		if err := w.rowWire.Send(prefix); err != nil {
			return SubmissionReceipt{}, err
		}
		return SubmissionReceipt{SubmittedUnits: w.submitRows}, w.failWith
	}
	if err := w.rowWire.Send(payload); err != nil {
		return SubmissionReceipt{}, err
	}
	return SubmissionReceipt{SubmittedUnits: uint64(len(payload.rows))}, nil
}

// overReportingRowWire holds one Send open until released, then claims more
// submitted units than the payload contains — the shape of a buggy protocol
// adapter, which must fail the stream rather than wedge it.
type overReportingRowWire struct {
	*rowWire
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
}

func newOverReportingRowWire() *overReportingRowWire {
	return &overReportingRowWire{
		rowWire: newRowWire(),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (w *overReportingRowWire) SendWithReceipt(
	payload *rowPayload,
) (SubmissionReceipt, error) {
	w.startOnce.Do(func() { close(w.started) })
	<-w.release
	if err := w.rowWire.Send(payload); err != nil {
		return SubmissionReceipt{}, err
	}
	return SubmissionReceipt{SubmittedUnits: uint64(len(payload.rows)) + 5}, nil
}

// rowOpener hands out pre-built connections in order.
type rowOpener struct {
	mu     sync.Mutex
	wires  []WireStream[*rowPayload, *rowResponse]
	opened int
}

func newRowOpener(wires ...WireStream[*rowPayload, *rowResponse]) *rowOpener {
	return &rowOpener{wires: wires}
}

func (o *rowOpener) open(
	context.Context, StreamParams,
) (WireStream[*rowPayload, *rowResponse], error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.opened >= len(o.wires) {
		return nil, fmt.Errorf("rowOpener: no connection left")
	}
	wire := o.wires[o.opened]
	o.opened++
	return wire, nil
}

func newRowStream(
	t *testing.T,
	cfg Config,
	opener *rowOpener,
) *CoreStream[*rowPayload, *rowResponse] {
	t.Helper()
	return newRowStreamWithAcks(t, cfg, opener, rowAckHooks())
}

func newRowStreamWithAcks(
	t *testing.T,
	cfg Config,
	opener *rowOpener,
	acks AckModelHooks[*rowResponse],
) *CoreStream[*rowPayload, *rowResponse] {
	t.Helper()
	cs, err := NewCoreStreamWithHooks[*rowPayload, *rowResponse](
		context.Background(),
		testParams(),
		cfg,
		opener.open,
		rowEncoderHooks(),
		acks,
		nil,
	)
	if err != nil {
		t.Fatalf("NewCoreStreamWithHooks: %v", err)
	}
	t.Cleanup(func() { cs.Terminate() })
	return cs
}

func rowConfig() Config {
	cfg := testConfig()
	cfg.DrainTimeout = 2 * time.Second
	return cfg
}

// ---- ResolveAcknowledgedUnits ----------------------------------------------

func TestResolveAcknowledgedUnits(t *testing.T) {
	twoItems := []SubmittedRange{
		{LogicalOffset: 0, UnitStart: 0, UnitEnd: 3, ItemUnitEnd: 3},
		{LogicalOffset: 1, UnitStart: 3, UnitEnd: 5, ItemUnitEnd: 5},
	}

	tests := []struct {
		name       string
		ackedUnits uint64
		state      AckState
		want       AckResolution
		wantErr    string
	}{
		{
			name:       "no new progress",
			ackedUnits: 2,
			state: AckState{
				Ranges: twoItems, AcknowledgedUnits: 2, SubmittedUnits: 5,
			},
			want: AckResolution{
				AcknowledgedUnits: 2, FullyAcknowledgedOffset: -1, PartialOffset: -1,
			},
		},
		{
			// A watermark that advances but lands below every submitted range
			// resolves to no offset, so accepting it would raise the connection's
			// ack watermark without any durable progress behind it.
			name:       "advancing ack below the first submitted range",
			ackedUnits: 2,
			state: AckState{
				SubmittedUnits: 6,
				Ranges: []SubmittedRange{
					{LogicalOffset: 0, UnitStart: 3, UnitEnd: 6, ItemUnitEnd: 6},
				},
			},
			wantErr: "does not intersect an unacknowledged submitted range",
		},
		{
			// The head range is exactly consumed and the next one has not
			// started, which is ordinary progress rather than a gap.
			name:       "ack landing on a later range boundary",
			ackedUnits: 3,
			state:      AckState{Ranges: twoItems, SubmittedUnits: 5},
			want: AckResolution{
				AcknowledgedUnits: 3, FullyAcknowledgedOffset: 0, PartialOffset: -1,
			},
		},
		{
			// The still-sending item resolves like any other range, so an ack
			// arriving mid-send can retire it without the core having to
			// combine it into Ranges first.
			name:       "active range resolves fully",
			ackedUnits: 7,
			state: AckState{
				Ranges:         twoItems,
				Active:         SubmittedRange{LogicalOffset: 2, UnitStart: 5, UnitEnd: 7, ItemUnitEnd: 7},
				HasActive:      true,
				SubmittedUnits: 7,
			},
			want: AckResolution{
				AcknowledgedUnits: 7, FullyAcknowledgedOffset: 2, PartialOffset: -1,
			},
		},
		{
			name:       "active range resolves partially",
			ackedUnits: 6,
			state: AckState{
				Ranges:         twoItems,
				Active:         SubmittedRange{LogicalOffset: 2, UnitStart: 5, UnitEnd: 7, ItemUnitEnd: 7},
				HasActive:      true,
				SubmittedUnits: 7,
			},
			want: AckResolution{
				AcknowledgedUnits: 6, FullyAcknowledgedOffset: 1,
				PartialOffset: 2, PartialUnits: 1,
			},
		},
		{
			// Contiguity is enforced across the Ranges/Active seam too, so a
			// gap there is caught rather than silently resolved.
			name:       "active range not contiguous with ranges",
			ackedUnits: 7,
			state: AckState{
				Ranges:         twoItems,
				Active:         SubmittedRange{LogicalOffset: 2, UnitStart: 6, UnitEnd: 8, ItemUnitEnd: 8},
				HasActive:      true,
				SubmittedUnits: 8,
			},
			wantErr: "submitted unit range starts at 6 after 5",
		},
		{
			// With no Ranges behind it the active item is the whole window.
			name:       "active range alone",
			ackedUnits: 2,
			state: AckState{
				Active:         SubmittedRange{LogicalOffset: 9, UnitStart: 0, UnitEnd: 4, ItemUnitEnd: 4},
				HasActive:      true,
				SubmittedUnits: 4,
			},
			want: AckResolution{
				AcknowledgedUnits: 2, FullyAcknowledgedOffset: -1,
				PartialOffset: 9, PartialUnits: 2,
			},
		},
		{
			name:       "partial prefix of first item",
			ackedUnits: 2,
			state:      AckState{Ranges: twoItems, SubmittedUnits: 5},
			want: AckResolution{
				AcknowledgedUnits: 2, FullyAcknowledgedOffset: -1,
				PartialOffset: 0, PartialUnits: 2,
			},
		},
		{
			name:       "exactly one full item",
			ackedUnits: 3,
			state:      AckState{Ranges: twoItems, SubmittedUnits: 5},
			want: AckResolution{
				AcknowledgedUnits: 3, FullyAcknowledgedOffset: 0, PartialOffset: -1,
			},
		},
		{
			name:       "full item plus partial next",
			ackedUnits: 4,
			state:      AckState{Ranges: twoItems, SubmittedUnits: 5},
			want: AckResolution{
				AcknowledgedUnits: 4, FullyAcknowledgedOffset: 0,
				PartialOffset: 1, PartialUnits: 1,
			},
		},
		{
			name:       "all submitted items",
			ackedUnits: 5,
			state:      AckState{Ranges: twoItems, SubmittedUnits: 5},
			want: AckResolution{
				AcknowledgedUnits: 5, FullyAcknowledgedOffset: 1, PartialOffset: -1,
			},
		},
		{
			name:       "zero ItemUnitEnd defaults to UnitEnd",
			ackedUnits: 2,
			state: AckState{
				Ranges:         []SubmittedRange{{LogicalOffset: 7, UnitStart: 0, UnitEnd: 2}},
				SubmittedUnits: 2,
			},
			want: AckResolution{
				AcknowledgedUnits: 2, FullyAcknowledgedOffset: 7, PartialOffset: -1,
			},
		},
		{
			// A multi-frame Send that failed after submitting a prefix leaves
			// UnitEnd short of ItemUnitEnd. Acking the whole submitted prefix
			// makes the item partially, not fully, durable.
			name:       "submitted prefix of an incomplete item",
			ackedUnits: 2,
			state: AckState{
				Ranges: []SubmittedRange{
					{LogicalOffset: 4, UnitStart: 0, UnitEnd: 2, ItemUnitEnd: 5},
				},
				SubmittedUnits: 2,
			},
			want: AckResolution{
				AcknowledgedUnits: 2, FullyAcknowledgedOffset: -1,
				PartialOffset: 4, PartialUnits: 2,
			},
		},
		{
			name:       "ack beyond submitted units",
			ackedUnits: 6,
			state:      AckState{Ranges: twoItems, SubmittedUnits: 5},
			wantErr:    "only 5 units were submitted",
		},
		{
			name:       "watermark beyond submitted units",
			ackedUnits: 1,
			state: AckState{
				Ranges: twoItems, AcknowledgedUnits: 9, SubmittedUnits: 5,
			},
			wantErr: "watermark 9 exceeds submitted units 5",
		},
		{
			name:       "empty submitted range",
			ackedUnits: 1,
			state: AckState{
				Ranges:         []SubmittedRange{{LogicalOffset: 0, UnitStart: 2, UnitEnd: 2}},
				SubmittedUnits: 4,
			},
			wantErr: "invalid submitted range",
		},
		{
			name:       "item end precedes submitted end",
			ackedUnits: 1,
			state: AckState{
				Ranges: []SubmittedRange{
					{LogicalOffset: 0, UnitStart: 0, UnitEnd: 4, ItemUnitEnd: 2},
				},
				SubmittedUnits: 4,
			},
			wantErr: "precedes submitted range end",
		},
		{
			name:       "non-contiguous ranges",
			ackedUnits: 4,
			state: AckState{
				Ranges: []SubmittedRange{
					{LogicalOffset: 0, UnitStart: 0, UnitEnd: 2, ItemUnitEnd: 2},
					{LogicalOffset: 1, UnitStart: 3, UnitEnd: 5, ItemUnitEnd: 5},
				},
				SubmittedUnits: 5,
			},
			wantErr: "starts at 3 after 2",
		},
		{
			name:       "ack intersects no submitted range",
			ackedUnits: 3,
			state:      AckState{SubmittedUnits: 5},
			wantErr:    "does not intersect",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveAcknowledgedUnits(tc.ackedUnits, tc.state)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("want error containing %q, got resolution %+v", tc.wantErr, got)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %q, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("resolution = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// ---- generic hook seam ------------------------------------------------------

func TestNewCoreStreamWithHooksRejectsIncompleteHooks(t *testing.T) {
	fullEncoder := rowEncoderHooks()
	fullAcks := rowAckHooks()
	opener := newRowOpener(newRowWire())

	tests := []struct {
		name    string
		open    OpenFunc[*rowPayload, *rowResponse]
		enc     EncoderHooks[*rowPayload]
		acks    AckModelHooks[*rowResponse]
		wantErr string
	}{
		{
			name: "missing open", open: nil, enc: fullEncoder, acks: fullAcks,
			wantErr: "protocol Open hook is required",
		},
		{
			name: "missing encoder hook", open: opener.open,
			enc:     EncoderHooks[*rowPayload]{EncodeRecord: fullEncoder.EncodeRecord},
			acks:    fullAcks,
			wantErr: "all encoder hooks are required",
		},
		{
			name: "missing classify", open: opener.open, enc: fullEncoder,
			acks:    AckModelHooks[*rowResponse]{},
			wantErr: "Classify hook is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cs, err := NewCoreStreamWithHooks[*rowPayload, *rowResponse](
				context.Background(), testParams(), rowConfig(),
				tc.open, tc.enc, tc.acks, nil,
			)
			if err == nil {
				cs.Terminate()
				t.Fatalf("want error containing %q, got a stream", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

// TestHookAckModelClassifyTranslatesStatuses covers the adapter that turns a
// protocol's ResponseClassification into the core's internal classification.
// Only the OK+ack shape is reached by the end-to-end tests, so the remaining
// branches — including the guard against a response carrying no signal at all —
// are pinned here.
func TestHookAckModelClassifyTranslatesStatuses(t *testing.T) {
	tests := []struct {
		name         string
		in           ResponseClassification
		wantFailure  responseFailure
		wantAck      bool
		wantOffset   int64
		wantPause    bool
		wantDuration time.Duration
	}{
		{
			name:        "unknown",
			in:          ResponseClassification{Status: ResponseUnknown},
			wantFailure: unknownResponse,
		},
		{
			name:        "malformed",
			in:          ResponseClassification{Status: ResponseMalformed},
			wantFailure: malformedResponse,
		},
		{
			name:       "ack only",
			in:         ResponseClassification{Status: ResponseOK, HasAck: true, LegacyOffset: 7},
			wantAck:    true,
			wantOffset: 7,
		},
		{
			name: "pause only",
			in: ResponseClassification{
				Status: ResponseOK, HasPause: true, PauseDuration: 3 * time.Second,
			},
			wantPause:    true,
			wantDuration: 3 * time.Second,
		},
		{
			name: "ack and pause together",
			in: ResponseClassification{
				Status: ResponseOK, HasAck: true, LegacyOffset: 2,
				HasPause: true, PauseDuration: time.Second,
			},
			wantAck: true, wantOffset: 2,
			wantPause: true, wantDuration: time.Second,
		},
		{
			name:        "ok but no signal",
			in:          ResponseClassification{Status: ResponseOK},
			wantFailure: unknownResponse,
		},
		{
			name:        "unrecognized status",
			in:          ResponseClassification{Status: ResponseStatus(99)},
			wantFailure: unknownResponse,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			model := hookAckModel[*rowResponse]{
				hooks: AckModelHooks[*rowResponse]{
					Classify: func(*rowResponse) ResponseClassification { return tc.in },
				},
			}
			got := model.classify(nil)
			if got.failure != tc.wantFailure {
				t.Fatalf("failure = %v, want %v", got.failure, tc.wantFailure)
			}
			if got.hasAck != tc.wantAck {
				t.Fatalf("hasAck = %v, want %v", got.hasAck, tc.wantAck)
			}
			if got.hasAck && got.legacyOffset != tc.wantOffset {
				t.Fatalf("legacyOffset = %d, want %d", got.legacyOffset, tc.wantOffset)
			}
			if (got.pause != nil) != tc.wantPause {
				t.Fatalf("pause = %v, want pause %v", got.pause, tc.wantPause)
			}
			if got.pause != nil && got.pause.duration != tc.wantDuration {
				t.Fatalf("pause duration = %v, want %v", got.pause.duration, tc.wantDuration)
			}
		})
	}
}

// TestHookProtocolWithoutResolveUsesWireOffsets covers the other half of the
// acknowledgment seam: a protocol that omits Resolve is atomic, so the core
// resolves its wire offsets itself and uses the connection-wide ack-silence
// budget rather than the per-item deadline.
func TestHookProtocolWithoutResolveUsesWireOffsets(t *testing.T) {
	wire := newRowWire()
	cs := newRowStreamWithAcks(t, rowConfig(), newRowOpener(wire), atomicRowAckHooks())

	for _, record := range []string{"a", "b", "c"} {
		if _, err := cs.Ingest(context.Background(), []byte(record)); err != nil {
			t.Fatalf("Ingest %q: %v", record, err)
		}
		wire.nextSend(t)
	}

	// Wire offset 1 makes the first two items durable, not the third.
	wire.ackWireOffset(1)
	if err := cs.WaitForOffset(context.Background(), 1); err != nil {
		t.Fatalf("WaitForOffset(1): %v", err)
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := cs.Flush(flushCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Flush before final ack = %v, want DeadlineExceeded", err)
	}

	wire.ackWireOffset(2)
	if err := cs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after final ack: %v", err)
	}
	if got := cs.buf.inFlight(); got != 0 {
		t.Fatalf("in-flight items after full ack = %d, want 0", got)
	}
}

// TestHookProtocolIngestAndAckByRows covers the ordinary path: a multi-row item
// becomes durable only once every one of its rows is acknowledged.
func TestHookProtocolIngestAndAckByRows(t *testing.T) {
	wire := newRowWire()
	cs := newRowStream(t, rowConfig(), newRowOpener(wire))

	offset, err := cs.IngestBatch(context.Background(), [][]byte{
		[]byte("a"), []byte("b"), []byte("c"),
	})
	if err != nil {
		t.Fatalf("IngestBatch: %v", err)
	}
	if offset != 0 {
		t.Fatalf("offset = %d, want 0", offset)
	}
	if sent := wire.nextSend(t); len(sent.rows) != 3 {
		t.Fatalf("transport received %d rows, want 3", len(sent.rows))
	}

	// A prefix acknowledgment must not advance the logical watermark.
	wire.ackRows(2)
	flushCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := cs.Flush(flushCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Flush after partial ack = %v, want DeadlineExceeded", err)
	}

	wire.ackRows(3)
	if err := cs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after full ack: %v", err)
	}

	// A single record is just a one-unit item on the same connection, so its
	// rows continue the cumulative count.
	single, err := cs.Ingest(context.Background(), []byte("d"))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if single != 1 {
		t.Fatalf("single-record offset = %d, want 1", single)
	}
	if sent := wire.nextSend(t); !slices.Equal(sent.rows, []string{"d"}) {
		t.Fatalf("transport received %v, want [d]", sent.rows)
	}
	wire.ackRows(4)
	if err := cs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after single record: %v", err)
	}
}

// TestHookProtocolPartialAckReplaysOnlyRemainder is the core guarantee of the
// record-count model: rows the server already made durable are not resent.
func TestHookProtocolPartialAckReplaysOnlyRemainder(t *testing.T) {
	first := newRowWire()
	second := newRowWire()
	cs := newRowStream(t, rowConfig(), newRowOpener(first, second))

	if _, err := cs.IngestBatch(context.Background(), [][]byte{
		[]byte("r0"), []byte("r1"), []byte("r2"), []byte("r3"),
	}); err != nil {
		t.Fatalf("IngestBatch: %v", err)
	}
	if sent := first.nextSend(t); len(sent.rows) != 4 {
		t.Fatalf("first connection received %d rows, want 4", len(sent.rows))
	}

	first.ackRows(2)
	// Drop the connection so the supervisor recovers onto the second wire.
	first.shutdown()

	replayed := second.nextSend(t)
	if got := replayed.rows; !slices.Equal(got, []string{"r2", "r3"}) {
		t.Fatalf("replayed rows = %v, want [r2 r3]", got)
	}

	second.ackRows(2)
	if err := cs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after replay: %v", err)
	}
}

// TestHookProtocolSubmissionReceiptPreservesPrefixAck covers a multi-frame Send
// that fails after earlier frames were accepted: an acknowledgment for the
// submitted prefix stays authoritative, so recovery replays only the rest.
func TestHookProtocolSubmissionReceiptPreservesPrefixAck(t *testing.T) {
	failing := &partialRowWire{
		rowWire:    newRowWire(),
		submitRows: 2,
		failWith:   fmt.Errorf("frame rejected"),
	}
	second := newRowWire()
	cs := newRowStream(t, rowConfig(), newRowOpener(failing, second))

	if _, err := cs.IngestBatch(context.Background(), [][]byte{
		[]byte("r0"), []byte("r1"), []byte("r2"),
	}); err != nil {
		t.Fatalf("IngestBatch: %v", err)
	}

	submitted := failing.nextSend(t)
	if got := submitted.rows; !slices.Equal(got, []string{"r0", "r1"}) {
		t.Fatalf("submitted prefix = %v, want [r0 r1]", got)
	}
	// The server acknowledges the frames that did land before the Send failed.
	failing.ackRows(2)

	replayed := second.nextSend(t)
	if got := replayed.rows; !slices.Equal(got, []string{"r2"}) {
		t.Fatalf("replayed rows = %v, want [r2]", got)
	}

	second.ackRows(1)
	if err := cs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after receipt-based recovery: %v", err)
	}
}

// TestHookProtocolBadReceiptDoesNotWedgeTeardown covers a transport that
// reports an impossible submission receipt while an acknowledgment is buffered
// against the still-active Send. Rejecting the receipt must also release that
// buffered state, or teardown waits forever for a completion event that was
// already consumed.
func TestHookProtocolBadReceiptDoesNotWedgeTeardown(t *testing.T) {
	wire := newOverReportingRowWire()
	cfg := rowConfig()
	cfg.Recovery = RecoveryDisabled
	cs := newRowStreamWithAcks(t, cfg, newRowOpener(wire), rowAckHooks())

	if _, err := cs.IngestBatch(context.Background(), [][]byte{
		[]byte("r0"), []byte("r1"),
	}); err != nil {
		t.Fatalf("IngestBatch: %v", err)
	}

	// Buffer an ack against the in-progress Send: the sender is parked inside
	// SendWithReceipt, so the receiver cannot see a completion event first.
	<-wire.started
	wire.ackRows(1)
	<-wire.delivered
	close(wire.release)

	done := make(chan error, 1)
	go func() { done <- cs.Terminate() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Terminate hung after an invalid submission receipt")
	}
}

// TestHookProtocolGetUnackedSlicesAcknowledgedPrefix checks that recovering
// unacknowledged work after teardown excludes rows already made durable.
func TestHookProtocolGetUnackedSlicesAcknowledgedPrefix(t *testing.T) {
	wire := newRowWire()
	cfg := rowConfig()
	cfg.Recovery = RecoveryDisabled
	cs := newRowStream(t, cfg, newRowOpener(wire))

	if _, err := cs.IngestBatch(context.Background(), [][]byte{
		[]byte("r0"), []byte("r1"), []byte("r2"),
	}); err != nil {
		t.Fatalf("IngestBatch: %v", err)
	}
	wire.nextSend(t)
	wire.ackRows(1)
	waitCondition(t, func() bool {
		unacked, err := cs.GetUnackedBatches()
		return err == nil && len(unacked) > 0
	}, 3*time.Second)

	unacked, err := cs.GetUnackedBatches()
	if err != nil {
		t.Fatalf("GetUnackedBatches: %v", err)
	}
	if len(unacked) != 1 {
		t.Fatalf("unacked groups = %d, want 1", len(unacked))
	}
	got := make([]string, len(unacked[0]))
	for i, record := range unacked[0] {
		got[i] = string(record)
	}
	if !slices.Equal(got, []string{"r1", "r2"}) {
		t.Fatalf("unacked rows = %v, want [r1 r2]", got)
	}
}

// ---- typed payload admission ------------------------------------------------

func TestEnqueuePayloadValidatesUnitCount(t *testing.T) {
	wire := newRowWire()
	cs := newRowStream(t, rowConfig(), newRowOpener(wire))

	if _, err := cs.EnqueuePayload(
		context.Background(), &rowPayload{rows: []string{"a"}}, 0, 8,
	); err == nil {
		t.Fatal("zero-unit payload was admitted")
	}
	if _, err := cs.EnqueuePayload(
		context.Background(), &rowPayload{rows: []string{"a", "b"}}, 5, 8,
	); err == nil {
		t.Fatal("payload with a mismatched unit count was admitted")
	}

	offset, err := cs.EnqueuePayload(
		context.Background(), &rowPayload{rows: []string{"a", "b"}}, 2, 8,
	)
	if err != nil {
		t.Fatalf("EnqueuePayload: %v", err)
	}
	if sent := wire.nextSend(t); len(sent.rows) != 2 || sent.offset != offset {
		t.Fatalf("transport received %+v, want 2 rows at offset %d", sent, offset)
	}
	wire.ackRows(2)
	if err := cs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestEnqueuePayloadBuilderReconcilesReservation(t *testing.T) {
	wire := newRowWire()
	cs := newRowStream(t, rowConfig(), newRowOpener(wire))

	if _, err := cs.EnqueuePayloadBuilder(context.Background(), 16, nil); err == nil {
		t.Fatal("nil builder was accepted")
	}

	buildErr := fmt.Errorf("materialization failed")
	if _, err := cs.EnqueuePayloadBuilder(
		context.Background(), 16,
		func() (*rowPayload, uint64, int64, error) { return nil, 0, 0, buildErr },
	); !errors.Is(err, buildErr) {
		t.Fatalf("builder error = %v, want %v", err, buildErr)
	}

	// A builder that reports a different unit count than the payload carries is
	// a protocol bug and must not reach the buffer.
	if _, err := cs.EnqueuePayloadBuilder(
		context.Background(), 16,
		func() (*rowPayload, uint64, int64, error) {
			return &rowPayload{rows: []string{"a"}}, 4, 8, nil
		},
	); err == nil {
		t.Fatal("builder with a mismatched unit count was admitted")
	}

	// The rejected attempts must not have leaked buffer capacity.
	if items, bytes := cs.buf.usage(); items != 0 || bytes != 0 {
		t.Fatalf("buffer usage after failed builds = (%d,%d), want (0,0)", items, bytes)
	}

	if _, err := cs.EnqueuePayloadBuilder(
		context.Background(), 16,
		func() (*rowPayload, uint64, int64, error) {
			return &rowPayload{rows: []string{"a", "b", "c"}}, 3, 6, nil
		},
	); err != nil {
		t.Fatalf("EnqueuePayloadBuilder: %v", err)
	}
	if sent := wire.nextSend(t); len(sent.rows) != 3 {
		t.Fatalf("transport received %d rows, want 3", len(sent.rows))
	}
	wire.ackRows(3)
	if err := cs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}
