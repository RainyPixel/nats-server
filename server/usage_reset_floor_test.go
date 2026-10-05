package server

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Exercise the real consumer store and atomic file writer while its disk-IO
// semaphore holds a snapshot that predates Reset. No ACK or floor is mocked.
func TestUsageResetCannotReloadSnapshotWrittenBeforeReset(t *testing.T) {
	for _, syncAlways := range []bool{false, true} {
		for _, queuedFlush := range []bool{false, true} {
			t.Run(fmt.Sprintf("sync=%v/queued-flush=%v", syncAlways, queuedFlush), func(t *testing.T) {
				testUsageResetWithBlockedWriter(t, syncAlways, queuedFlush)
			})
		}
	}
}

func testUsageResetWithBlockedWriter(t *testing.T, syncAlways, queuedFlush bool) {
	io := newDiskIOSemaphore(4)
	store := &consumerFileStore{
		fs:  &fileStore{dios: io},
		cfg: &FileConsumerInfo{ConsumerConfig: ConsumerConfig{AckPolicy: AckExplicit}},
		ifn: filepath.Join(t.TempDir(), "consumer-state"),
	}
	store.fs.syncAlways.Store(syncAlways)
	for seq := uint64(1); seq <= 4; seq++ {
		if err := store.UpdateDelivered(seq, seq, 1, time.Now().UnixNano()); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.UpdateAcks(2, 2); err != nil {
		t.Fatal(err)
	}
	before, err := store.State()
	if err != nil {
		t.Fatal(err)
	}
	if before.Delivered.Stream != 4 || before.AckFloor.Stream != 0 || len(before.Pending) != 3 {
		t.Fatalf("invalid delivered fixture: %+v", before)
	}
	bytes := encodeConsumerState(before)
	if err := store.writeState(bytes); err != nil {
		t.Fatal(err)
	}
	for range io.cap() {
		io.acquire()
	}
	held := true
	release := func() {
		if held {
			for range io.cap() {
				io.release()
			}
			held = false
		}
	}
	defer release()
	writeDone := make(chan error, 1)
	go func() { writeDone <- store.writeState(bytes) }()
	deadline := time.Now().Add(time.Second)
	for io.waiters.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("old snapshot did not reach actual disk-IO barrier")
		}
		time.Sleep(time.Millisecond)
	}
	var flushDone chan struct{}
	if queuedFlush {
		store.fch, store.qch = make(chan struct{}, 1), make(chan struct{})
		flushDone = make(chan struct{})
		go func() {
			store.flushLoop(store.fch, store.qch)
			close(flushDone)
		}()
		store.fch <- struct{}{}
		deadline := time.Now().Add(time.Second)
		for len(store.fch) != 0 {
			if time.Now().After(deadline) {
				t.Fatal("real flusher did not consume its queued snapshot request")
			}
			time.Sleep(time.Millisecond)
		}
	}
	resetDone := make(chan error, 1)
	go func() { resetDone <- store.Reset(0) }()
	returnedBeforeWrite := false
	select {
	case err := <-resetDone:
		if err != nil {
			t.Fatal(err)
		}
		returnedBeforeWrite = true
	case <-time.After(50 * time.Millisecond):
		// A coherent implementation may wait for the old write before reset.
	}
	release()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("old snapshot writer did not finish")
	}
	if !returnedBeforeWrite {
		select {
		case err := <-resetDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("reset did not finish after disk-IO release")
		}
	}
	if queuedFlush {
		close(store.qch)
		select {
		case <-flushDone:
		case <-time.After(time.Second):
			t.Fatal("queued flusher did not finish after disk-IO release")
		}
	}
	after, err := store.BorrowState()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("reset returned before old write: %v; delivered=%+v floor=%+v pending=%d",
		returnedBeforeWrite, after.Delivered, after.AckFloor, len(after.Pending))
	if after.Delivered.Stream != 0 || after.Delivered.Consumer != 0 ||
		after.AckFloor.Stream != 0 || after.AckFloor.Consumer != 0 || len(after.Pending) != 0 {
		t.Fatalf("Reset(0) reloaded an obsolete snapshot after returning success: %+v", after)
	}
	reopened := &consumerFileStore{fs: store.fs, ifn: store.ifn}
	persisted, err := reopened.State()
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Delivered != (SequencePair{}) || persisted.AckFloor != (SequencePair{}) || len(persisted.Pending) != 0 {
		t.Fatalf("reopened reset state is obsolete: %+v", persisted)
	}
}

// A successfully loaded zero state must remain available when unrelated disk
// writers saturate the IO semaphore. Zero delivered sequences are valid state.
func TestUsageConsumerStoreLoadedZeroDoesNotReload(t *testing.T) {
	for _, present := range []bool{false, true} {
		t.Run(fmt.Sprintf("state-file=%v", present), func(t *testing.T) {
			io := newDiskIOSemaphore(4)
			store := &consumerFileStore{fs: &fileStore{dios: io}, ifn: filepath.Join(t.TempDir(), "state")}
			if present {
				if err := os.WriteFile(store.ifn, encodeConsumerState(&ConsumerState{}), defaultFilePerms); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.loadState(); err != nil {
				t.Fatal(err)
			}
			for range io.cap() {
				io.acquire()
			}
			done := make(chan error, 1)
			go func() {
				state, err := store.BorrowState()
				if err == nil && (state.Delivered != (SequencePair{}) || state.AckFloor != (SequencePair{}) || len(state.Pending) != 0) {
					err = fmt.Errorf("loaded zero state changed: %+v", state)
				}
				done <- err
			}()
			returned := false
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
				returned = true
			case <-time.After(100 * time.Millisecond):
			}
			for range io.cap() {
				io.release()
			}
			if !returned {
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("state read did not finish after IO release")
				}
				t.Error("a loaded zero state tried to reread disk")
			}
		})
	}
}

// Exercise actual disk errors at the existing public reset/update boundary and
// committed-entry apply boundary. The store is real; its writer cannot replace
// a checkpoint file in its read-only directory.
func TestUsageConsumerResetWriteErrorsReachCaller(t *testing.T) {
	for _, operation := range []string{"reset-api", "sourcing-update", "replicated-apply"} {
		t.Run(operation, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()
			nc, js := jsClientConnect(t, s)
			defer nc.Close()
			_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
			require_NoError(t, err)
			cfg := ConsumerConfig{Durable: "CONSUMER", AckPolicy: AckExplicit, Sourcing: operation == "sourcing-update"}
			actual, err := jsConsumerCreate(t, nc, "TEST", cfg, false)
			require_NoError(t, err)
			mset, err := s.globalAccount().lookupStream("TEST")
			require_NoError(t, err)
			o := mset.lookupConsumer("CONSUMER")
			require_NotNil(t, o)
			cfs := o.store.(*consumerFileStore)
			// The prior state remains readable, so an ignored write error can
			// produce a positive INFO response instead of a masking read error.
			blockedDir := filepath.Join(t.TempDir(), "read-only-state")
			require_NoError(t, os.Mkdir(blockedDir, defaultDirPerms))
			blocked := filepath.Join(blockedDir, "state")
			require_NoError(t, os.WriteFile(blocked, encodeConsumerState(&ConsumerState{}), defaultFilePerms))
			require_NoError(t, os.Chmod(blockedDir, 0o555))
			defer os.Chmod(blockedDir, defaultDirPerms)
			cfs.mu.Lock()
			original := cfs.ifn
			cfs.ifn = blocked
			cfs.mu.Unlock()
			defer func() {
				cfs.mu.Lock()
				cfs.ifn = original
				cfs.mu.Unlock()
			}()
			switch operation {
			case "reset-api":
				msg, err := nc.Request("$JS.API.CONSUMER.RESET.TEST.CONSUMER", nil, time.Second)
				require_NoError(t, err)
				var response JSApiConsumerResetResponse
				require_NoError(t, json.Unmarshal(msg.Data, &response))
				if response.Error == nil || response.Error.ErrCode != uint16(JSConsumerInvalidResetErr) || response.ResetSeq != 0 {
					t.Fatalf("failed checkpoint write produced reset success: %+v", response)
				}
			case "sourcing-update":
				actual.Description = "updated source"
				_, err := jsConsumerCreate(t, nc, "TEST", *actual, false)
				if err == nil {
					t.Fatal("failed source reset produced update success")
				}
			case "replicated-apply":
				buf := make([]byte, 9)
				buf[0] = byte(resetSeqOp)
				binary.LittleEndian.PutUint64(buf[1:], 1)
				entry := &CommittedEntry{Entries: []*Entry{{Type: EntryNormal, Data: buf}}}
				err := o.js.applyConsumerEntries(o, entry, true)
				var pathErr *os.PathError
				if !errors.As(err, &pathErr) {
					t.Fatalf("failed reset apply did not return actual disk error: %v", err)
				}
			}
		})
	}
}

// Recovery can read retained metadata/state from a directory whose writes now
// fail. A source consumer must fail its constructor reset before publishing a
// ready consumer; cleanup retains the store instead of deleting its evidence.
func TestUsageSourcingConstructorReportsResetWriteError(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)
	cfg := ConsumerConfig{Durable: "CONSUMER", AckPolicy: AckExplicit, Sourcing: true}
	_, err = jsConsumerCreate(t, nc, "TEST", cfg, false)
	require_NoError(t, err)
	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)
	cfs := o.store.(*consumerFileStore)
	odir := cfs.odir
	require_NoError(t, o.stop())
	require_NoError(t, os.Chmod(odir, 0o555))
	defer os.Chmod(odir, defaultDirPerms)
	_, err = jsConsumerCreate(t, nc, "TEST", cfg, false)
	if err == nil {
		t.Fatal("failed constructor reset produced create success")
	}
	if mset.lookupConsumer("CONSUMER") != nil {
		t.Fatal("failed source constructor left a registered consumer")
	}
	if _, err := os.Stat(cfs.ifn); err != nil {
		t.Fatalf("failed constructor deleted retained state: %v", err)
	}
}
