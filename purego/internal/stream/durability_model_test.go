package stream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestResolveAcknowledgedUnitsToLogicalOffsets(t *testing.T) {
	state := AckState{
		Ranges: []SubmittedRange{
			{WireOffset: 0, LogicalOffset: 10, UnitStart: 0, UnitEnd: 3},
			{WireOffset: 1, LogicalOffset: 11, UnitStart: 3, UnitEnd: 8},
			{WireOffset: 2, LogicalOffset: 12, UnitStart: 8, UnitEnd: 10},
		},
		SubmittedUnits: 10,
	}
	tests := []struct {
		name         string
		acked        uint64
		full         int64
		partial      int64
		partialUnits uint64
	}{
		{name: "none", acked: 0, full: -1, partial: -1},
		{name: "partial first", acked: 2, full: -1, partial: 10, partialUnits: 2},
		{name: "first complete", acked: 3, full: 10, partial: -1},
		{name: "middle partial", acked: 6, full: 10, partial: 11, partialUnits: 3},
		{name: "middle complete", acked: 8, full: 11, partial: -1},
		{name: "all complete", acked: 10, full: 12, partial: -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveAcknowledgedUnits(tc.acked, state)
			if err != nil {
				t.Fatalf("ResolveAcknowledgedUnits: %v", err)
			}
			if got.FullyAcknowledgedOffset != tc.full ||
				got.PartialOffset != tc.partial ||
				got.PartialUnits != tc.partialUnits {
				t.Fatalf(
					"resolution = %+v, want full=%d partial=%d partialUnits=%d",
					got, tc.full, tc.partial, tc.partialUnits,
				)
			}
		})
	}

	if _, err := ResolveAcknowledgedUnits(11, state); err == nil {
		t.Fatal("ack beyond submitted units succeeded")
	}
}

func TestResolveAcknowledgedUnitsReceiptBoundedRangeStaysPartial(t *testing.T) {
	state := AckState{
		Ranges: []SubmittedRange{{
			WireOffset:    0,
			LogicalOffset: 7,
			UnitStart:     0,
			UnitEnd:       2,
			ItemUnitEnd:   5,
		}},
		SubmittedUnits: 2,
	}
	resolution, err := ResolveAcknowledgedUnits(2, state)
	if err != nil {
		t.Fatalf("ResolveAcknowledgedUnits: %v", err)
	}
	if resolution.FullyAcknowledgedOffset != -1 ||
		resolution.PartialOffset != 7 ||
		resolution.PartialUnits != 2 {
		t.Fatalf("resolution = %+v, want two-unit partial offset 7", resolution)
	}
}

type countPayload struct {
	offset  int64
	records [][]byte
}

type countResponse struct {
	ackedUnits uint64
}

type countWire struct {
	sends     chan *countPayload
	frames    chan []byte
	responses chan countResponse
	closed    chan struct{}
	closeOnce sync.Once
	sendCalls atomic.Int64
}

type failingReceiptWire struct {
	*countWire
	firstSubmitted chan struct{}
	failSend       chan struct{}
	sendFailed     chan struct{}
	firstOnce      sync.Once
	failedOnce     sync.Once
}

type ackBeforeRecvErrorWire struct {
	*countWire
	firstSubmitted chan struct{}
	sendCompleted  chan struct{}
	recvErrors     chan error
	firstOnce      sync.Once
	completedOnce  sync.Once
}

func newFailingReceiptWire() *failingReceiptWire {
	return &failingReceiptWire{
		countWire:      newCountWire(),
		firstSubmitted: make(chan struct{}),
		failSend:       make(chan struct{}),
		sendFailed:     make(chan struct{}),
	}
}

func newAckBeforeRecvErrorWire() *ackBeforeRecvErrorWire {
	return &ackBeforeRecvErrorWire{
		countWire:      newCountWire(),
		firstSubmitted: make(chan struct{}),
		sendCompleted:  make(chan struct{}),
		recvErrors:     make(chan error, 1),
	}
}

func (w *failingReceiptWire) Send(payload *countPayload) error {
	_, err := w.SendWithReceipt(payload)
	return err
}

func (w *failingReceiptWire) SendWithReceipt(
	payload *countPayload,
) (SubmissionReceipt, error) {
	w.sendCalls.Add(1)
	cloned := cloneCountPayload(payload)
	if len(cloned.records) < 2 {
		return SubmissionReceipt{}, fmt.Errorf("test send requires multiple frames")
	}
	select {
	case w.frames <- bytes.Clone(cloned.records[0]):
	case <-w.closed:
		return SubmissionReceipt{}, io.ErrClosedPipe
	}
	w.firstOnce.Do(func() { close(w.firstSubmitted) })
	select {
	case <-w.failSend:
		w.failedOnce.Do(func() { close(w.sendFailed) })
		return SubmissionReceipt{SubmittedUnits: 1}, io.ErrUnexpectedEOF
	case <-w.closed:
		return SubmissionReceipt{SubmittedUnits: 1}, io.ErrClosedPipe
	}
}

func (w *ackBeforeRecvErrorWire) Send(payload *countPayload) error {
	_, err := w.SendWithReceipt(payload)
	return err
}

func (w *ackBeforeRecvErrorWire) SendWithReceipt(
	payload *countPayload,
) (SubmissionReceipt, error) {
	w.sendCalls.Add(1)
	cloned := cloneCountPayload(payload)
	if len(cloned.records) < 2 {
		return SubmissionReceipt{}, fmt.Errorf("test send requires multiple frames")
	}
	select {
	case w.frames <- bytes.Clone(cloned.records[0]):
	case <-w.closed:
		return SubmissionReceipt{}, &classifiedError{retryable: false}
	}
	w.firstOnce.Do(func() { close(w.firstSubmitted) })
	<-w.closed
	w.completedOnce.Do(func() { close(w.sendCompleted) })
	return SubmissionReceipt{SubmittedUnits: 1}, &classifiedError{retryable: false}
}

func (w *ackBeforeRecvErrorWire) Recv() (countResponse, error) {
	select {
	case response := <-w.responses:
		return response, nil
	case err := <-w.recvErrors:
		return countResponse{}, err
	case <-w.closed:
		return countResponse{}, io.EOF
	}
}

func newCountWire() *countWire {
	return &countWire{
		sends:     make(chan *countPayload, 8),
		frames:    make(chan []byte, 32),
		responses: make(chan countResponse, 8),
		closed:    make(chan struct{}),
	}
}

func (*countWire) ServerID() string { return "count-wire" }

// Send deliberately expands one logical item into one transport frame per
// record. The core still observes exactly one Send completion and one submitted
// logical range.
func (w *countWire) Send(payload *countPayload) error {
	w.sendCalls.Add(1)
	cloned := cloneCountPayload(payload)
	for _, record := range cloned.records {
		select {
		case w.frames <- bytes.Clone(record):
		case <-w.closed:
			return io.ErrClosedPipe
		}
	}
	select {
	case w.sends <- cloned:
		return nil
	case <-w.closed:
		return io.ErrClosedPipe
	}
}

func (w *countWire) Recv() (countResponse, error) {
	select {
	case response := <-w.responses:
		return response, nil
	case <-w.closed:
		return countResponse{}, io.EOF
	}
}

func (w *countWire) CloseSend() error {
	w.Close()
	return nil
}

func (w *countWire) Close() {
	w.closeOnce.Do(func() { close(w.closed) })
}

func (w *countWire) ack(units uint64) {
	w.responses <- countResponse{ackedUnits: units}
}

func cloneCountPayload(payload *countPayload) *countPayload {
	if payload == nil {
		return nil
	}
	cloned := &countPayload{offset: payload.offset, records: make([][]byte, len(payload.records))}
	for i, record := range payload.records {
		cloned.records[i] = bytes.Clone(record)
	}
	return cloned
}

func countEncoderHooks() EncoderHooks[*countPayload] {
	build := func(records [][]byte) *countPayload {
		return cloneCountPayload(&countPayload{records: records})
	}
	return EncoderHooks[*countPayload]{
		EncodeRecord: func(record []byte) (*countPayload, error) {
			return build([][]byte{record}), nil
		},
		EncodeBatch: func(records [][]byte) (*countPayload, error) {
			if len(records) == 0 {
				return nil, fmt.Errorf("empty count payload")
			}
			return build(records), nil
		},
		StampOffset: func(payload *countPayload, offset int64) {
			payload.offset = offset
		},
		UnitCount: func(payload *countPayload) uint64 {
			return uint64(len(payload.records))
		},
		Slice: func(payload *countPayload, acknowledgedPrefix uint64) (*countPayload, error) {
			if acknowledgedPrefix > uint64(len(payload.records)) {
				return nil, fmt.Errorf(
					"prefix %d exceeds %d records",
					acknowledgedPrefix, len(payload.records),
				)
			}
			return cloneCountPayload(&countPayload{
				offset:  payload.offset,
				records: payload.records[acknowledgedPrefix:],
			}), nil
		},
		Decode: func(payload *countPayload) [][]byte {
			return cloneCountPayload(payload).records
		},
		MaxWireSize: func(*countPayload) int { return 1 },
		RetainedSize: func(rawBytes, recordCount int) int64 {
			return int64(rawBytes + recordCount)
		},
	}
}

func countAckHooks() AckModelHooks[countResponse] {
	return AckModelHooks[countResponse]{
		Classify: func(countResponse) ResponseClassification {
			return ResponseClassification{
				Status: ResponseOK,
				HasAck: true,
			}
		},
		Resolve: func(response countResponse, state AckState) (AckResolution, error) {
			return ResolveAcknowledgedUnits(response.ackedUnits, state)
		},
	}
}

func newCountCore(
	t *testing.T,
	wires ...*countWire,
) *CoreStream[*countPayload, countResponse] {
	adapted := make([]WireStream[*countPayload, countResponse], len(wires))
	for index := range wires {
		adapted[index] = wires[index]
	}
	return newCountCoreWithHooks(t, testConfig(), countAckHooks(), adapted...)
}

func newCountCoreWithHooks(
	t *testing.T,
	cfg Config,
	acks AckModelHooks[countResponse],
	wires ...WireStream[*countPayload, countResponse],
) *CoreStream[*countPayload, countResponse] {
	t.Helper()
	var openMu sync.Mutex
	next := 0
	open := OpenFunc[*countPayload, countResponse](func(
		context.Context,
		StreamParams,
	) (WireStream[*countPayload, countResponse], error) {
		openMu.Lock()
		defer openMu.Unlock()
		if next >= len(wires) {
			return nil, fmt.Errorf("no count wire for open %d", next)
		}
		wire := wires[next]
		next++
		return wire, nil
	})
	cfg.RecoveryRetries = len(wires) - 1
	core, err := NewCoreStreamWithHooks(
		context.Background(),
		testParams(),
		cfg,
		open,
		countEncoderHooks(),
		acks,
		nil,
	)
	if err != nil {
		t.Fatalf("NewCoreStreamWithHooks: %v", err)
	}
	return core
}

func countRecords(n int) [][]byte {
	records := make([][]byte, n)
	for i := range records {
		records[i] = fmt.Appendf(nil, "record-%d", i)
	}
	return records
}

func TestCountModelPartialFirstItemGetUnacked(t *testing.T) {
	wire := newCountWire()
	core := newCountCore(t, wire)
	records := countRecords(5)
	offset, err := core.EnqueuePayload(
		context.Background(),
		cloneCountPayload(&countPayload{records: records}),
		uint64(len(records)),
		64,
	)
	if err != nil {
		t.Fatalf("EnqueuePayload: %v", err)
	}
	sent := <-wire.sends
	if sent.offset != 0 || len(sent.records) != 5 {
		t.Fatalf("first send = %+v, want offset 0 with 5 records", sent)
	}
	if got := wire.sendCalls.Load(); got != 1 {
		t.Fatalf("wire Send calls = %d, want 1", got)
	}
	if got := len(wire.frames); got != 5 {
		t.Fatalf("transport frames = %d, want 5 behind one Send", got)
	}

	wire.ack(2)
	waitCondition(t, func() bool { return core.durableProgress.Load() == 1 }, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := core.WaitForOffset(ctx, offset); err == nil {
		t.Fatal("partial first-item ack advanced the logical watermark")
	}

	if err := core.Terminate(); err != nil {
		t.Fatalf("Terminate: %v", err)
	}
	unacked, err := core.GetUnacked()
	if err != nil {
		t.Fatalf("GetUnacked: %v", err)
	}
	if !slices.EqualFunc(unacked, records[2:], bytes.Equal) {
		t.Fatalf("GetUnacked = %q, want unacknowledged suffix %q", unacked, records[2:])
	}
}

func TestCountModelRecoverySlicesPartiallyAckedItem(t *testing.T) {
	first := newCountWire()
	second := newCountWire()
	core := newCountCore(t, first, second)
	t.Cleanup(func() { _ = core.Terminate() })

	records := countRecords(5)
	offset, err := core.EnqueuePayload(
		context.Background(),
		cloneCountPayload(&countPayload{records: records}),
		uint64(len(records)),
		64,
	)
	if err != nil {
		t.Fatalf("EnqueuePayload: %v", err)
	}
	<-first.sends
	first.ack(2)
	waitCondition(t, func() bool { return core.durableProgress.Load() == 1 }, time.Second)
	first.Close()

	var replay *countPayload
	select {
	case replay = <-second.sends:
	case <-time.After(2 * time.Second):
		t.Fatal("partially acknowledged item was not replayed")
	}
	if replay.offset != 0 {
		t.Fatalf("replay wire offset = %d, want connection-local offset 0", replay.offset)
	}
	if !slices.EqualFunc(replay.records, records[2:], bytes.Equal) {
		t.Fatalf("replay = %q, want sliced suffix %q", replay.records, records[2:])
	}
	if got := len(second.frames); got != 3 {
		t.Fatalf("replay transport frames = %d, want 3", got)
	}

	second.ack(3)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := core.WaitForOffset(ctx, offset); err != nil {
		t.Fatalf("WaitForOffset after sliced replay: %v", err)
	}
}

func TestCountModelPartialSendReceiptPreservesAckedPrefix(t *testing.T) {
	first := newFailingReceiptWire()
	second := newCountWire()
	ackClassified := make(chan struct{})
	var ackOnce sync.Once
	acks := countAckHooks()
	classify := acks.Classify
	acks.Classify = func(response countResponse) ResponseClassification {
		classified := classify(response)
		ackOnce.Do(func() { close(ackClassified) })
		return classified
	}
	core := newCountCoreWithHooks(
		t,
		testConfig(),
		acks,
		first,
		second,
	)
	t.Cleanup(func() { _ = core.Terminate() })

	records := countRecords(5)
	offset, err := core.EnqueuePayload(
		context.Background(),
		cloneCountPayload(&countPayload{records: records}),
		uint64(len(records)),
		64,
	)
	if err != nil {
		t.Fatalf("EnqueuePayload: %v", err)
	}
	select {
	case <-first.firstSubmitted:
	case <-time.After(time.Second):
		t.Fatal("first transport frame was not submitted")
	}
	first.ack(1)
	select {
	case <-ackClassified:
	case <-time.After(time.Second):
		t.Fatal("first-frame acknowledgment was not classified before send failure")
	}
	close(first.failSend)

	var replay *countPayload
	select {
	case replay = <-second.sends:
	case <-time.After(2 * time.Second):
		t.Fatal("unacknowledged suffix was not replayed")
	}
	if !slices.EqualFunc(replay.records, records[1:], bytes.Equal) {
		t.Fatalf("replay = %q, want only unacknowledged suffix %q", replay.records, records[1:])
	}
	if got := len(second.frames); got != 4 {
		t.Fatalf("replay transport frames = %d, want 4", got)
	}

	second.ack(4)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := core.WaitForOffset(ctx, offset); err != nil {
		t.Fatalf("WaitForOffset after partial-send recovery: %v", err)
	}
}

func TestCountModelAckBeforeRecvErrorReconcilesLaterSendReceipt(t *testing.T) {
	first := newAckBeforeRecvErrorWire()
	second := newCountWire()
	ackClassified := make(chan struct{})
	var ackOnce sync.Once
	acks := countAckHooks()
	classify := acks.Classify
	acks.Classify = func(response countResponse) ResponseClassification {
		classified := classify(response)
		ackOnce.Do(func() { close(ackClassified) })
		return classified
	}
	core := newCountCoreWithHooks(
		t,
		testConfig(),
		acks,
		first,
		second,
	)
	t.Cleanup(func() { _ = core.Terminate() })

	records := countRecords(5)
	offset, err := core.EnqueuePayload(
		context.Background(),
		cloneCountPayload(&countPayload{records: records}),
		uint64(len(records)),
		64,
	)
	if err != nil {
		t.Fatalf("EnqueuePayload: %v", err)
	}
	select {
	case <-first.firstSubmitted:
	case <-time.After(time.Second):
		t.Fatal("first transport frame was not submitted")
	}

	first.ack(1)
	select {
	case <-ackClassified:
	case <-time.After(time.Second):
		t.Fatal("first-frame acknowledgment was not buffered during Send")
	}
	select {
	case <-first.sendCompleted:
		t.Fatal("multi-frame Send completed before the receiver error")
	default:
	}

	// The receiver failure is retryable while the later Send failure is not.
	// Reaching the second wire therefore also proves the receiver error stayed
	// authoritative after receipt reconciliation.
	first.recvErrors <- &classifiedError{retryable: true}
	select {
	case <-first.sendCompleted:
	case <-time.After(time.Second):
		t.Fatal("multi-frame Send did not return its receipt after receiver teardown")
	}

	var replay *countPayload
	select {
	case replay = <-second.sends:
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not recover from the authoritative receiver error")
	}
	if !slices.EqualFunc(replay.records, records[1:], bytes.Equal) {
		t.Fatalf(
			"replay = %q, want only unacknowledged suffix %q",
			replay.records,
			records[1:],
		)
	}

	second.ack(4)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := core.WaitForOffset(ctx, offset); err != nil {
		t.Fatalf("WaitForOffset after receipt reconciliation: %v", err)
	}
}

func TestCountModelAppliesAckAfterPartialSendFailure(t *testing.T) {
	first := newFailingReceiptWire()
	second := newCountWire()
	cfg := testConfig()
	cfg.DrainTimeout = time.Second
	core := newCountCoreWithHooks(
		t,
		cfg,
		countAckHooks(),
		first,
		second,
	)
	t.Cleanup(func() { _ = core.Terminate() })

	records := countRecords(5)
	offset, err := core.EnqueuePayload(
		context.Background(),
		cloneCountPayload(&countPayload{records: records}),
		uint64(len(records)),
		64,
	)
	if err != nil {
		t.Fatalf("EnqueuePayload: %v", err)
	}
	select {
	case <-first.firstSubmitted:
	case <-time.After(time.Second):
		t.Fatal("first transport frame was not submitted")
	}

	close(first.failSend)
	select {
	case <-first.sendFailed:
	case <-time.After(time.Second):
		t.Fatal("multi-frame Send did not fail")
	}
	// The ACK is deliberately delivered only after Send selected its error.
	// The receipt-bounded range must remain resolvable until teardown.
	first.ack(1)

	var replay *countPayload
	select {
	case replay = <-second.sends:
	case <-time.After(2 * time.Second):
		t.Fatal("unacknowledged suffix was not replayed after late ACK")
	}
	if !slices.EqualFunc(replay.records, records[1:], bytes.Equal) {
		t.Fatalf("replay = %q, want suffix %q", replay.records, records[1:])
	}

	second.ack(4)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := core.WaitForOffset(ctx, offset); err != nil {
		t.Fatalf("WaitForOffset after late-ACK recovery: %v", err)
	}
}

func TestCountModelPartialAckDoesNotExtendOldestDeadline(t *testing.T) {
	wire := newCountWire()
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cfg.LackOfAckTimeout = 500 * time.Millisecond
	core := newCountCoreWithHooks(t, cfg, countAckHooks(), wire)
	t.Cleanup(func() { _ = core.Terminate() })

	records := countRecords(3)
	if _, err := core.EnqueuePayload(
		context.Background(),
		cloneCountPayload(&countPayload{records: records}),
		uint64(len(records)),
		64,
	); err != nil {
		t.Fatalf("EnqueuePayload: %v", err)
	}
	select {
	case <-wire.sends:
	case <-time.After(time.Second):
		t.Fatal("payload was not submitted")
	}

	time.Sleep(300 * time.Millisecond)
	wire.ack(1)
	waitCondition(
		t,
		func() bool { return core.durableProgress.Load() == 1 },
		time.Second,
	)
	partialAppliedAt := time.Now()

	select {
	case <-core.done:
		if elapsed := time.Since(partialAppliedAt); elapsed > 300*time.Millisecond {
			t.Fatalf("lack-of-ack teardown took %v after partial ACK", elapsed)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("partial row ACK extended the oldest pending batch deadline")
	}
	if err := core.terminalErr(); err == nil ||
		!strings.Contains(err.Error(), "no ack from server") {
		t.Fatalf("terminal error = %v, want lack-of-ack timeout", err)
	}
}
