package core

import (
	"context"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0xPolygon/go-ibft/messages"
	"github.com/0xPolygon/go-ibft/messages/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTryGetConsensusState_Inactive(t *testing.T) {
	t.Parallel()

	i := NewIBFT(mockLogger{}, mockBackend{
		idFn: func() []byte { return []byte("node-a") },
		isProposerFn: func(id []byte, _, _ uint64) bool {
			return string(id) == "node-a"
		},
	}, mockTransport{})

	snap := i.TryGetConsensusState()
	require.NotNil(t, snap)
	require.NotNil(t, snap.Current)
	assert.Equal(t, SequenceInactive, snap.Current.Status)
	assert.True(t, snap.Complete)
	assert.Equal(t, []byte("node-a"), snap.NodeID)
	assert.Equal(t, "new_round", snap.Current.Phase)
}

func TestTryGetConsensusState_PhaseArchive(t *testing.T) {
	t.Parallel()

	nodeA := []byte("node-a")
	nodeB := []byte("node-b")
	nodeC := []byte("node-c")
	proposalHash := []byte("hash-1")
	rawProposal := []byte("block-bytes")

	backend := mockBackend{
		idFn: func() []byte { return nodeA },
		isProposerFn: func(id []byte, _, _ uint64) bool {
			return string(id) == "node-a"
		},
		getVotingPowerFn: func(uint64) (map[string]*big.Int, error) {
			return map[string]*big.Int{
				string(nodeA): big.NewInt(1),
				string(nodeB): big.NewInt(1),
				string(nodeC): big.NewInt(1),
			}, nil
		},
	}

	i := NewIBFT(mockLogger{}, backend, mockTransport{})
	require.NoError(t, i.validatorManager.Init(1))

	i.state.reset(1)
	started := time.Now().Add(-2 * time.Second)
	i.state.markSequenceStarted(started)
	i.archive.beginHeight(1, started)
	i.state.newRound()

	preprepare := &proto.IbftMessage{
		View: &proto.View{Height: 1, Round: 0},
		From: nodeA,
		Type: proto.MessageType_PREPREPARE,
		Payload: &proto.IbftMessage_PreprepareData{
			PreprepareData: &proto.PrePrepareMessage{
				Proposal: &proto.Proposal{
					RawProposal: rawProposal,
					Round:       0,
				},
				ProposalHash: proposalHash,
			},
		},
	}
	i.state.setProposalMessage(preprepare)

	msgStore := messages.NewMessages()
	i.messages = msgStore
	msgStore.AddMessage(preprepare)
	msgStore.AddMessage(&proto.IbftMessage{
		View: &proto.View{Height: 1, Round: 0},
		From: nodeB,
		Type: proto.MessageType_PREPARE,
		Payload: &proto.IbftMessage_PrepareData{
			PrepareData: &proto.PrepareMessage{ProposalHash: proposalHash},
		},
	})

	i.recordPhaseEnd(i.state.changeState(prepare))

	snap := i.TryGetConsensusState()
	require.NotNil(t, snap)
	require.NotNil(t, snap.Current)
	assert.Equal(t, SequenceRunning, snap.Current.Status)
	assert.Equal(t, "prepare", snap.Current.Phase)
	require.GreaterOrEqual(t, len(snap.Current.PhaseSnapshots), 2)

	var foundCompletedNewRound bool
	var foundInProgressPrepare bool
	for _, p := range snap.Current.PhaseSnapshots {
		if p.Phase == "new_round" && p.Status == phaseStatusCompleted {
			foundCompletedNewRound = true
			assert.Equal(t, uint64(1), p.Height)
			assert.True(t, p.Messages["preprepare"].Available)
			assert.Equal(t, 1, len(p.Messages["preprepare"].Messages))
		}
		if p.Phase == "prepare" && p.Status == phaseStatusInProgress {
			foundInProgressPrepare = true
		}
	}
	assert.True(t, foundCompletedNewRound)
	assert.True(t, foundInProgressPrepare)
}

func TestTryGetConsensusState_LastFinalizedRetained(t *testing.T) {
	t.Parallel()

	i := NewIBFT(mockLogger{}, mockBackend{
		idFn:         func() []byte { return []byte("n1") },
		isProposerFn: func([]byte, uint64, uint64) bool { return false },
		getVotingPowerFn: func(uint64) (map[string]*big.Int, error) {
			return map[string]*big.Int{"n1": big.NewInt(1)}, nil
		},
	}, mockTransport{})

	i.state.reset(5)
	i.state.markSequenceStarted(time.Now())
	i.archive.beginHeight(5, time.Now())
	i.state.newRound()
	i.recordPhaseEnd(i.state.changeState(prepare))
	i.finalizeSequenceArchive(i.state.markSequenceCompleted(RoundEndCommitted))

	// Start next height; last finalized must remain.
	i.state.reset(6)
	i.state.markSequenceStarted(time.Now())
	i.archive.beginHeight(6, time.Now())

	snap := i.TryGetConsensusState()
	require.NotNil(t, snap.LastFinalized)
	assert.Equal(t, uint64(5), snap.LastFinalized.Height)
	assert.Equal(t, SequenceCompleted, snap.LastFinalized.Status)
	assert.Equal(t, RoundEndCommitted, snap.LastFinalized.LastRoundEndReason)
	require.NotEmpty(t, snap.LastFinalized.PhaseSnapshots)
}

func TestTryGetConsensusState_ConcurrentSafe(t *testing.T) {
	t.Parallel()

	backend := mockBackend{
		idFn:         func() []byte { return []byte("n1") },
		isProposerFn: func([]byte, uint64, uint64) bool { return true },
		getVotingPowerFn: func(uint64) (map[string]*big.Int, error) {
			return map[string]*big.Int{
				"n1": big.NewInt(1),
				"n2": big.NewInt(1),
				"n3": big.NewInt(1),
			}, nil
		},
		buildPrePrepareMessageFn: func(raw []byte, _ *proto.RoundChangeCertificate, view *proto.View) *proto.IbftMessage {
			return &proto.IbftMessage{
				View: view,
				From: []byte("n1"),
				Type: proto.MessageType_PREPREPARE,
				Payload: &proto.IbftMessage_PreprepareData{
					PreprepareData: &proto.PrePrepareMessage{
						Proposal:     &proto.Proposal{RawProposal: raw, Round: view.Round},
						ProposalHash: []byte("h"),
					},
				},
			}
		},
		buildPrepareMessageFn: func(hash []byte, view *proto.View) *proto.IbftMessage {
			return &proto.IbftMessage{
				View: view,
				From: []byte("n1"),
				Type: proto.MessageType_PREPARE,
				Payload: &proto.IbftMessage_PrepareData{
					PrepareData: &proto.PrepareMessage{ProposalHash: hash},
				},
			}
		},
		buildCommitMessageFn: func(hash []byte, view *proto.View) *proto.IbftMessage {
			return &proto.IbftMessage{
				View: view,
				From: []byte("n1"),
				Type: proto.MessageType_COMMIT,
				Payload: &proto.IbftMessage_CommitData{
					CommitData: &proto.CommitMessage{ProposalHash: hash, CommittedSeal: []byte("seal")},
				},
			}
		},
		buildProposalFn:        func(uint64) []byte { return []byte("block") },
		isValidProposalFn:      func([]byte) bool { return true },
		isValidProposalHashFn:  func(*proto.Proposal, []byte) bool { return true },
		isValidCommittedSealFn: func([]byte, *messages.CommittedSeal) bool { return true },
	}

	i := NewIBFT(mockLogger{}, backend, mockTransport{})
	i.baseRoundTimeout = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		i.RunSequence(ctx, 10)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap := i.TryGetConsensusState()
		require.NotNil(t, snap)
		_ = snap.Complete
		_ = snap.Current
		_ = snap.LastFinalized
		time.Sleep(time.Millisecond)
	}

	cancel()
	wg.Wait()
}

func TestDiagnosticsEvents_CoalescesWithoutBlocking(t *testing.T) {
	t.Parallel()

	i := NewIBFT(mockLogger{}, mockBackend{
		idFn: func() []byte { return []byte("node-a") },
	}, mockTransport{})

	const notifications = 100_000
	done := make(chan struct{})

	go func() {
		for j := 0; j < notifications; j++ {
			i.notifyDiagnosticsChanged()
		}

		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("diagnostics notification blocked the producer")
	}

	require.Len(t, i.diagnosticsEvents, 1)
	<-i.DiagnosticsEvents()

	select {
	case <-i.DiagnosticsEvents():
		t.Fatal("rapid diagnostics events were not coalesced")
	default:
	}
}

func TestDiagnosticsEvents_PhaseArchivePublishedBeforeWakeup(t *testing.T) {
	t.Parallel()

	i := NewIBFT(mockLogger{}, mockBackend{
		idFn: func() []byte { return []byte("node-a") },
	}, mockTransport{})

	startedAt := time.Now().Add(-time.Millisecond)
	i.archive.beginHeight(7, startedAt)
	i.state.reset(7)
	i.state.markSequenceStarted(startedAt)
	i.state.setRoundStarted(true)

	// Drain setup notifications, then create a real phase archive mutation.
	select {
	case <-i.DiagnosticsEvents():
	default:
	}

	i.recordPhaseEnd(i.state.changeState(prepare))

	select {
	case <-i.DiagnosticsEvents():
	case <-time.After(time.Second):
		t.Fatal("phase transition did not publish a diagnostics event")
	}

	snap := i.TryGetConsensusState()
	require.NotNil(t, snap.Current)
	require.NotEmpty(t, snap.Current.PhaseSnapshots)
	assert.Equal(t, "new_round", snap.Current.PhaseSnapshots[0].Phase)
	assert.Equal(t, phaseStatusCompleted, snap.Current.PhaseSnapshots[0].Status)
}

func TestMessages_TryGetViewMessages_NoAddMessageCost(t *testing.T) {
	t.Parallel()

	ms := messages.NewMessages()
	view := &proto.View{Height: 1, Round: 0}

	for i := 0; i < 5; i++ {
		ms.AddMessage(&proto.IbftMessage{
			View: view,
			From: []byte{byte(i)},
			Type: proto.MessageType_PREPARE,
			Payload: &proto.IbftMessage_PrepareData{
				PrepareData: &proto.PrepareMessage{ProposalHash: []byte("h")},
			},
		})
	}

	msgs, ok, truncated := ms.TryGetViewMessages(view, proto.MessageType_PREPARE, 3)
	require.True(t, ok)
	assert.True(t, truncated)
	assert.Len(t, msgs, 3)
}

func TestTryGetConsensusState_IdleDoesNotQueryProposer(t *testing.T) {
	t.Parallel()

	var isProposerCalls int32

	i := NewIBFT(mockLogger{}, mockBackend{
		idFn: func() []byte { return []byte("n1") },
		isProposerFn: func([]byte, uint64, uint64) bool {
			atomic.AddInt32(&isProposerCalls, 1)

			return false
		},
		getVotingPowerFn: func(uint64) (map[string]*big.Int, error) {
			return map[string]*big.Int{"n1": big.NewInt(1)}, nil
		},
	}, mockTransport{})

	snap := i.TryGetConsensusState()
	require.NotNil(t, snap.Current)
	assert.Equal(t, SequenceInactive, snap.Current.Status)
	// Idle node at height 0: backend.IsProposer must not be consulted (it
	// would look up a non-existent previous header and log errors).
	assert.Equal(t, int32(0), atomic.LoadInt32(&isProposerCalls))
}

func TestTryGetConsensusState_ProposerResolvedOnceAndCached(t *testing.T) {
	t.Parallel()

	var isProposerCalls int32

	i := NewIBFT(mockLogger{}, mockBackend{
		idFn: func() []byte { return []byte("n2") },
		isProposerFn: func(id []byte, _, _ uint64) bool {
			atomic.AddInt32(&isProposerCalls, 1)

			return string(id) == "n2"
		},
		getVotingPowerFn: func(uint64) (map[string]*big.Int, error) {
			return map[string]*big.Int{
				"n1": big.NewInt(1),
				"n2": big.NewInt(1),
				"n3": big.NewInt(1),
			}, nil
		},
	}, mockTransport{})
	require.NoError(t, i.validatorManager.Init(3))

	i.state.reset(3)
	i.state.markSequenceStarted(time.Now())
	i.archive.beginHeight(3, time.Now())
	i.state.newRound()

	first := i.TryGetConsensusState()
	require.NotNil(t, first.Current)
	assert.Equal(t, []byte("n2"), first.Current.Proposer)
	assert.True(t, first.Current.IsProposer)
	// Validators are sorted, so n1 (false) then n2 (true): 2 calls.
	assert.Equal(t, int32(2), atomic.LoadInt32(&isProposerCalls))

	second := i.TryGetConsensusState()
	assert.Equal(t, []byte("n2"), second.Current.Proposer)
	assert.Equal(t, int32(2), atomic.LoadInt32(&isProposerCalls), "proposer must be served from cache")

	// Validators must be deterministically ordered.
	ids := make([]string, 0, len(second.Current.Validators))
	for _, v := range second.Current.Validators {
		ids = append(ids, string(v.ID))
	}
	assert.Equal(t, []string{"n1", "n2", "n3"}, ids)
}

func TestFinalizeSequenceArchive_CompletedUnderReadContention(t *testing.T) {
	t.Parallel()

	i := NewIBFT(mockLogger{}, mockBackend{
		idFn:         func() []byte { return []byte("n1") },
		isProposerFn: func([]byte, uint64, uint64) bool { return true },
		getVotingPowerFn: func(uint64) (map[string]*big.Int, error) {
			return map[string]*big.Int{"n1": big.NewInt(1)}, nil
		},
	}, mockTransport{})
	require.NoError(t, i.validatorManager.Init(7))

	i.state.reset(7)
	i.state.markSequenceStarted(time.Now())
	i.archive.beginHeight(7, time.Now())
	i.state.newRound()
	i.recordPhaseEnd(i.state.changeState(prepare))

	// Hammer the state with readers while the sequence is finalized so that a
	// try-lock would frequently fail; the archive must still be correct.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = i.TryGetConsensusState()
				}
			}
		}()
	}

	i.finalizeSequenceArchive(i.state.markSequenceCompleted(RoundEndCommitted))
	close(stop)
	wg.Wait()

	_, last, ok := i.archive.trySnapshot()
	require.True(t, ok)
	require.NotNil(t, last)
	assert.Equal(t, uint64(7), last.Height)
	assert.Equal(t, SequenceCompleted, last.Status)
	assert.Equal(t, RoundEndCommitted, last.LastRoundEndReason)
	require.Len(t, last.RoundHistory, 1)
	assert.Equal(t, RoundEndCommitted, last.RoundHistory[0].EndReason)
}

func TestMessages_GetViewMessages_DeterministicOrder(t *testing.T) {
	t.Parallel()

	ms := messages.NewMessages()
	view := &proto.View{Height: 1, Round: 0}

	for _, from := range []byte{9, 3, 7, 1, 5} {
		ms.AddMessage(&proto.IbftMessage{
			View: view,
			From: []byte{from},
			Type: proto.MessageType_COMMIT,
			Payload: &proto.IbftMessage_CommitData{
				CommitData: &proto.CommitMessage{
					ProposalHash:  []byte("h"),
					CommittedSeal: []byte("s"),
				},
			},
		})
	}

	for n := 0; n < 20; n++ {
		msgs, truncated := ms.GetViewMessages(view, proto.MessageType_COMMIT, 3)
		assert.True(t, truncated)
		require.Len(t, msgs, 3)
		assert.Equal(t, []byte{1}, msgs[0].From)
		assert.Equal(t, []byte{3}, msgs[1].From)
		assert.Equal(t, []byte{5}, msgs[2].From)
	}
}
