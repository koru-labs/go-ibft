package core

import (
	"bytes"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/0xPolygon/go-ibft/messages"
	"github.com/0xPolygon/go-ibft/messages/proto"
)

const (
	// maxRoundHistory is the number of completed rounds retained for the current height.
	maxRoundHistory = 32
	// maxMessagesPerType caps how many messages of one type are copied into a snapshot.
	maxMessagesPerType = 256
)

// SequenceStatus describes whether a consensus sequence is live on this node.
type SequenceStatus string

const (
	SequenceInactive  SequenceStatus = "inactive"
	SequenceRunning   SequenceStatus = "running"
	SequenceCompleted SequenceStatus = "completed"
	SequenceCancelled SequenceStatus = "cancelled"
)

// RoundEndReason is why the previous round ended.
type RoundEndReason string

const (
	RoundEndTimeout    RoundEndReason = "timeout"
	RoundEndFutureProp RoundEndReason = "future_proposal"
	RoundEndRCC        RoundEndReason = "round_change_certificate"
	RoundEndCommitted  RoundEndReason = "committed"
	RoundEndCancelled  RoundEndReason = "cancelled"
	RoundEndUnknown    RoundEndReason = "unknown"
)

// ConsensusState is an immutable, race-safe snapshot of this node's IBFT view.
// It includes the live/current height and the last finalized height archive.
type ConsensusState struct {
	CapturedAt          time.Time
	Complete            bool
	UnavailableSections []string
	NodeID              []byte
	Current             *HeightState
	LastFinalized       *HeightArchive
}

// HeightState is the live (or just-completed) view of one height, including
// frozen per-phase end snapshots accumulated in memory.
type HeightState struct {
	Status             SequenceStatus
	Height             uint64
	Round              uint64
	Phase              string
	RoundStarted       bool
	LastRoundEndReason RoundEndReason

	SequenceStartedAt time.Time
	RoundStartedAt    time.Time
	PhaseStartedAt    time.Time
	RoundTimeout      time.Duration
	RoundDeadline     time.Time

	PhaseElapsedMs    int64
	SequenceElapsedMs int64
	RoundElapsedMs    int64

	CompletedPhaseDurationsMs map[string]int64
	RoundHistory              []RoundSummary
	PhaseSnapshots            []PhaseSnapshot

	Proposal   *ProposalSnapshot
	IsProposer bool
	Proposer   []byte

	Validators       []ValidatorSnapshot
	TotalVotingPower string
	QuorumSize       string
	Quorum           map[string]QuorumProgress
	Messages         map[string]MessageTypeSnapshot
	LatestPC         *PreparedCertificateSnapshot
	CommittedSeals   []CommittedSealSnapshot
}

// RoundSummary is a completed round within the current height.
type RoundSummary struct {
	Round            uint64
	EndReason        RoundEndReason
	StartedAt        time.Time
	EndedAt          time.Time
	DurationMs       int64
	PhaseDurationsMs map[string]int64
}

// ProposalSnapshot describes the accepted proposal for the current round.
type ProposalSnapshot struct {
	Available   bool
	Hash        []byte
	Round       uint64
	RawSize     int
	RawProposal []byte // present for backend enrichment; strip before external JSON
}

// ValidatorSnapshot is one validator's voting power.
type ValidatorSnapshot struct {
	ID          []byte
	VotingPower string
}

// QuorumProgress is received vs required voting power for a message type.
//
// Count is the number of accepted messages of that type. For PREPARE the
// proposer's PREPREPARE counts towards the quorum without a PREPARE message
// (as in go-ibft's quorum rule); in that case ProposerImplied is true and
// ReceivedPower includes the proposer's power even though Count does not.
type QuorumProgress struct {
	Available       bool
	Count           int
	ProposerImplied bool
	ReceivedPower   string
	RequiredPower   string
	HasQuorum       bool
}

// MessageTypeSnapshot is the set of accepted messages of one type for a view.
type MessageTypeSnapshot struct {
	Available  bool
	Truncated  bool
	ViewHeight uint64
	ViewRound  uint64
	Messages   []MessageSnapshot
}

// MessageSnapshot is a compact accepted IBFT message.
type MessageSnapshot struct {
	From          []byte
	Type          string
	Height        uint64
	Round         uint64
	ProposalHash  []byte
	Signature     []byte
	CommittedSeal []byte
}

// PreparedCertificateSnapshot is the latest prepared certificate if any.
type PreparedCertificateSnapshot struct {
	Available      bool
	ProposalHash   []byte
	PrepareCount   int
	ProposalFrom   []byte
	PrepareSenders [][]byte
}

// CommittedSealSnapshot is one aggregated commit seal.
type CommittedSealSnapshot struct {
	Signer    []byte
	Signature []byte
}

// messageSnapshotter is implemented by *messages.Messages; optional on the Messages interface.
type messageSnapshotter interface {
	// TryGetViewMessages never waits: ok=false when the lock is busy.
	TryGetViewMessages(view *proto.View, messageType proto.MessageType, max int) (msgs []*proto.IbftMessage, ok bool, truncated bool)
	// GetViewMessages waits for the lock; used from the consensus goroutine only.
	GetViewMessages(view *proto.View, messageType proto.MessageType, max int) (msgs []*proto.IbftMessage, truncated bool)
}

// proposerCache memoizes the proposer for one (height, round) so diagnostics
// do not repeatedly call backend.IsProposer (which may be expensive, e.g. an
// ecrecover on the previous header) for every validator on every snapshot.
type proposerCache struct {
	mu       sync.Mutex
	height   uint64
	round    uint64
	valid    bool
	proposer []byte
}

func (c *proposerCache) get(height, round uint64) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.valid || c.height != height || c.round != round {
		return nil, false
	}

	return append([]byte(nil), c.proposer...), true
}

func (c *proposerCache) set(height, round uint64, proposer []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.height = height
	c.round = round
	c.proposer = append([]byte(nil), proposer...)
	c.valid = true
}

// DiagnosticsEvents returns a coalesced change-notification channel.
//
// A notification means that a later TryGetConsensusState call may observe a
// different consensus view. The channel has capacity one: rapid changes are
// deliberately coalesced so diagnostics consumers can never apply backpressure
// to the consensus path.
func (i *IBFT) DiagnosticsEvents() <-chan struct{} {
	return i.diagnosticsEvents
}

// notifyDiagnosticsChanged wakes an out-of-band diagnostics consumer without
// waiting. It does not allocate, acquire locks, or build a snapshot.
func (i *IBFT) notifyDiagnosticsChanged() {
	select {
	case i.diagnosticsEvents <- struct{}{}:
	default:
	}
}

// TryGetConsensusState returns a non-blocking snapshot of live consensus state.
// It never waits on a contended lock: busy sections are omitted and listed in UnavailableSections.
func (i *IBFT) TryGetConsensusState() *ConsensusState {
	now := time.Now()
	snap := &ConsensusState{
		CapturedAt:          now,
		Complete:            true,
		UnavailableSections: make([]string, 0, 4),
	}

	if i.backend != nil {
		snap.NodeID = append([]byte(nil), i.backend.ID()...)
	}

	current, unavailable := i.buildHeightState(now, false)
	if current == nil {
		snap.Complete = false
		snap.UnavailableSections = append(snap.UnavailableSections, unavailable...)
	} else {
		snap.Current = current
		if len(unavailable) > 0 {
			snap.Complete = false
			snap.UnavailableSections = append(snap.UnavailableSections, unavailable...)
		}
	}

	archivedCurrent, lastFinalized, aok := i.archive.trySnapshot()
	if !aok {
		snap.Complete = false
		snap.UnavailableSections = append(snap.UnavailableSections, "archive")
	} else {
		snap.LastFinalized = lastFinalized
		if snap.Current != nil && archivedCurrent != nil {
			snap.Current.PhaseSnapshots = archivedCurrent.PhaseSnapshots
		}
	}

	// Attach a lightweight in-progress marker. Full live votes already live on
	// current.quorum / current.messages — avoid duplicating them here.
	if snap.Current != nil && snap.Current.Status == SequenceRunning {
		snap.Current.PhaseSnapshots = append(snap.Current.PhaseSnapshots, PhaseSnapshot{
			Phase:      snap.Current.Phase,
			Status:     phaseStatusInProgress,
			Height:     snap.Current.Height,
			Round:      snap.Current.Round,
			StartedAt:  snap.Current.PhaseStartedAt,
			DurationMs: snap.Current.PhaseElapsedMs,
			Proposer:   append([]byte(nil), snap.Current.Proposer...),
			Proposal:   cloneProposalSnapshot(snap.Current.Proposal),
			LatestPC:   clonePreparedCertificate(snap.Current.LatestPC),
		})
	}

	return snap
}

// buildHeightState assembles the live view of the current height.
//
// With blocking=false (API/pusher path) every lock is acquired with a try-lock
// and busy sections are reported in the returned slice. With blocking=true
// (consensus goroutine, at sequence end) locks are acquired normally so the
// archived height is never a partial or bogus fallback.
func (i *IBFT) buildHeightState(now time.Time, blocking bool) (*HeightState, []string) {
	unavailable := make([]string, 0, 4)

	viewSnap, ok := i.state.snapshot(blocking)
	if !ok {
		return nil, []string{"state"}
	}

	height := &HeightState{
		Status:                    viewSnap.status,
		Height:                    viewSnap.height,
		Round:                     viewSnap.round,
		Phase:                     viewSnap.phase,
		RoundStarted:              viewSnap.roundStarted,
		LastRoundEndReason:        viewSnap.lastRoundEndReason,
		SequenceStartedAt:         viewSnap.sequenceStartedAt,
		RoundStartedAt:            viewSnap.roundStartedAt,
		PhaseStartedAt:            viewSnap.phaseStartedAt,
		CompletedPhaseDurationsMs: viewSnap.phaseDurationsMs,
		RoundHistory:              viewSnap.roundHistory,
		Proposal:                  viewSnap.proposal,
		LatestPC:                  viewSnap.latestPC,
		CommittedSeals:            viewSnap.seals,
		Quorum:                    make(map[string]QuorumProgress, 4),
		Messages:                  make(map[string]MessageTypeSnapshot, 4),
	}

	if !viewSnap.sequenceStartedAt.IsZero() {
		height.SequenceElapsedMs = now.Sub(viewSnap.sequenceStartedAt).Milliseconds()
	}

	if !viewSnap.roundStartedAt.IsZero() {
		height.RoundElapsedMs = now.Sub(viewSnap.roundStartedAt).Milliseconds()
	}

	if !viewSnap.phaseStartedAt.IsZero() {
		height.PhaseElapsedMs = now.Sub(viewSnap.phaseStartedAt).Milliseconds()
	}

	timeout := getRoundTimeout(i.baseRoundTimeout, i.additionalTimeout, viewSnap.round)
	height.RoundTimeout = timeout

	if !viewSnap.roundStartedAt.IsZero() {
		height.RoundDeadline = viewSnap.roundStartedAt.Add(timeout)
	}

	validators, quorumSize, totalPower, vok := i.validatorManager.snapshot(blocking)
	if !vok {
		unavailable = append(unavailable, "validators")
	} else {
		height.Validators = validators
		height.QuorumSize = quorumSize
		height.TotalVotingPower = totalPower
	}

	// Proposer resolution. Only meaningful while a sequence is live: when idle
	// (e.g. height 0 before the first sequence) backend.IsProposer would look up
	// a non-existent previous header and log errors.
	if i.backend != nil && viewSnap.status == SequenceRunning {
		height.Proposer = i.resolveProposer(viewSnap.height, viewSnap.round, viewSnap.proposalFrom, validators)
		if len(height.Proposer) > 0 {
			height.IsProposer = bytes.Equal(height.Proposer, i.backend.ID())
		}
	}

	view := &proto.View{Height: viewSnap.height, Round: viewSnap.round}
	powerIndex := buildPowerIndex(validators)

	snapper, hasSnapper := i.messages.(messageSnapshotter)
	if !hasSnapper {
		unavailable = append(unavailable, "messages")
	} else {
		for _, mt := range []proto.MessageType{
			proto.MessageType_PREPREPARE,
			proto.MessageType_PREPARE,
			proto.MessageType_COMMIT,
			proto.MessageType_ROUND_CHANGE,
		} {
			name := messageTypeName(mt)

			var (
				msgs      []*proto.IbftMessage
				ok        bool
				truncated bool
			)

			if blocking {
				msgs, truncated = snapper.GetViewMessages(view, mt, maxMessagesPerType)
				ok = true
			} else {
				msgs, ok, truncated = snapper.TryGetViewMessages(view, mt, maxMessagesPerType)
			}

			if !ok {
				unavailable = append(unavailable, "messages."+name)
				height.Messages[name] = MessageTypeSnapshot{Available: false}
				height.Quorum[name] = QuorumProgress{Available: false}

				continue
			}

			if truncated {
				unavailable = append(unavailable, "messages."+name+".truncated")
			}

			entry := MessageTypeSnapshot{
				Available:  true,
				Truncated:  truncated,
				ViewHeight: view.Height,
				ViewRound:  view.Round,
				Messages:   make([]MessageSnapshot, 0, len(msgs)),
			}
			for _, m := range msgs {
				entry.Messages = append(entry.Messages, toMessageSnapshot(m))
			}

			height.Messages[name] = entry

			if vok {
				height.Quorum[name] = computeQuorumProgress(
					mt,
					msgs,
					viewSnap.proposalFrom,
					powerIndex,
					quorumSize,
				)
			} else {
				height.Quorum[name] = QuorumProgress{Available: false, Count: len(msgs)}
			}
		}
	}

	return height, unavailable
}

// resolveProposer returns the proposer for (height, round). The accepted
// proposal's sender is authoritative; otherwise the result is derived from the
// validator set via backend.IsProposer and memoized per (height, round).
func (i *IBFT) resolveProposer(height, round uint64, proposalFrom []byte, validators []ValidatorSnapshot) []byte {
	if len(proposalFrom) > 0 {
		i.proposerCache.set(height, round, proposalFrom)

		return append([]byte(nil), proposalFrom...)
	}

	if p, ok := i.proposerCache.get(height, round); ok {
		return p
	}

	if i.backend == nil {
		return nil
	}

	for _, v := range validators {
		if i.backend.IsProposer(v.ID, height, round) {
			i.proposerCache.set(height, round, v.ID)

			return append([]byte(nil), v.ID...)
		}
	}

	return nil
}

type stateSnapshot struct {
	status             SequenceStatus
	height             uint64
	round              uint64
	phase              string
	roundStarted       bool
	lastRoundEndReason RoundEndReason
	sequenceStartedAt  time.Time
	roundStartedAt     time.Time
	phaseStartedAt     time.Time
	phaseDurationsMs   map[string]int64
	roundHistory       []RoundSummary
	proposal           *ProposalSnapshot
	proposalFrom       []byte
	latestPC           *PreparedCertificateSnapshot
	seals              []CommittedSealSnapshot
}

// snapshot copies the diagnostics-relevant state. With blocking=false it
// returns ok=false instead of waiting on a contended lock.
func (s *state) snapshot(blocking bool) (stateSnapshot, bool) {
	if blocking {
		s.RLock()
	} else if !s.TryRLock() {
		return stateSnapshot{}, false
	}
	defer s.RUnlock()

	out := stateSnapshot{
		status:             s.sequenceStatus,
		height:             s.view.Height,
		round:              s.view.Round,
		phase:              canonicalPhaseName(s.name),
		roundStarted:       s.roundStarted,
		lastRoundEndReason: s.lastRoundEndReason,
		sequenceStartedAt:  s.sequenceStartedAt,
		roundStartedAt:     s.roundStartedAt,
		phaseStartedAt:     s.phaseStartedAt,
		phaseDurationsMs:   make(map[string]int64, 4),
	}

	for st := stateType(0); st <= fin; st++ {
		if d := s.phaseDurations[st]; d > 0 {
			out.phaseDurationsMs[canonicalPhaseName(st)] = d.Milliseconds()
		}
	}

	if s.roundHistoryLen > 0 {
		out.roundHistory = make([]RoundSummary, s.roundHistoryLen)
		copy(out.roundHistory, s.roundHistory[:s.roundHistoryLen])
	}

	if s.proposalMessage != nil {
		hash := messages.ExtractProposalHash(s.proposalMessage)
		proposal := messages.ExtractProposal(s.proposalMessage)
		ps := &ProposalSnapshot{Available: true}
		if hash != nil {
			ps.Hash = append([]byte(nil), hash...)
		}

		if proposal != nil {
			ps.Round = proposal.Round
			ps.RawSize = len(proposal.RawProposal)
			// Share the existing immutable raw slice; callers must not mutate.
			ps.RawProposal = proposal.RawProposal
		}

		out.proposal = ps
		out.proposalFrom = append([]byte(nil), s.proposalMessage.From...)
	} else {
		out.proposal = &ProposalSnapshot{Available: false}
	}

	if s.latestPC != nil {
		pc := &PreparedCertificateSnapshot{Available: true}
		if s.latestPC.ProposalMessage != nil {
			pc.ProposalFrom = append([]byte(nil), s.latestPC.ProposalMessage.From...)
			h := messages.ExtractProposalHash(s.latestPC.ProposalMessage)
			if h != nil {
				pc.ProposalHash = append([]byte(nil), h...)
			}
		}

		pc.PrepareCount = len(s.latestPC.PrepareMessages)
		pc.PrepareSenders = make([][]byte, 0, len(s.latestPC.PrepareMessages))
		for _, m := range s.latestPC.PrepareMessages {
			if m != nil {
				pc.PrepareSenders = append(pc.PrepareSenders, append([]byte(nil), m.From...))
			}
		}

		out.latestPC = pc
	}

	if len(s.seals) > 0 {
		out.seals = make([]CommittedSealSnapshot, 0, len(s.seals))
		for _, seal := range s.seals {
			if seal == nil {
				continue
			}

			out.seals = append(out.seals, CommittedSealSnapshot{
				Signer:    append([]byte(nil), seal.Signer...),
				Signature: append([]byte(nil), seal.Signature...),
			})
		}
	}

	return out, true
}

// snapshot returns validators sorted by ID for deterministic output.
func (vm *ValidatorManager) snapshot(blocking bool) (validators []ValidatorSnapshot, quorumSize, totalPower string, ok bool) {
	if blocking {
		vm.vpLock.RLock()
	} else if !vm.vpLock.TryRLock() {
		return nil, "", "", false
	}
	defer vm.vpLock.RUnlock()

	if vm.validatorsVotingPower == nil {
		return []ValidatorSnapshot{}, "0", "0", true
	}

	validators = make([]ValidatorSnapshot, 0, len(vm.validatorsVotingPower))
	total := big.NewInt(0)

	for id, power := range vm.validatorsVotingPower {
		p := "0"
		if power != nil {
			p = power.String()
			total = new(big.Int).Add(total, power)
		}

		validators = append(validators, ValidatorSnapshot{
			ID:          []byte(id),
			VotingPower: p,
		})
	}

	sort.Slice(validators, func(a, b int) bool {
		return bytes.Compare(validators[a].ID, validators[b].ID) < 0
	})

	qs := "0"
	if vm.quorumSize != nil {
		qs = vm.quorumSize.String()
	}

	return validators, qs, total.String(), true
}

func buildPowerIndex(validators []ValidatorSnapshot) map[string]*big.Int {
	idx := make(map[string]*big.Int, len(validators))
	for _, v := range validators {
		p, ok := new(big.Int).SetString(v.VotingPower, 10)
		if !ok {
			p = big.NewInt(0)
		}

		idx[string(v.ID)] = p
	}

	return idx
}

func computeQuorumProgress(
	mt proto.MessageType,
	msgs []*proto.IbftMessage,
	proposalFrom []byte,
	powerIndex map[string]*big.Int,
	quorumSize string,
) QuorumProgress {
	progress := QuorumProgress{
		Available:     true,
		Count:         len(msgs),
		RequiredPower: quorumSize,
		ReceivedPower: "0",
	}

	switch mt {
	case proto.MessageType_PREPREPARE:
		progress.RequiredPower = "1"
		if len(msgs) >= 1 {
			progress.ReceivedPower = "1"
			progress.HasQuorum = true
		}

		return progress
	case proto.MessageType_PREPARE:
		received := big.NewInt(0)
		seen := make(map[string]struct{}, len(msgs)+1)

		if len(proposalFrom) > 0 {
			seen[string(proposalFrom)] = struct{}{}
			if p, ok := powerIndex[string(proposalFrom)]; ok {
				received = new(big.Int).Add(received, p)
				progress.ProposerImplied = true
			}
		}

		for _, m := range msgs {
			if m == nil {
				continue
			}

			key := string(m.From)
			if _, dup := seen[key]; dup {
				continue
			}

			seen[key] = struct{}{}
			if p, ok := powerIndex[key]; ok {
				received = new(big.Int).Add(received, p)
			}
		}

		progress.ReceivedPower = received.String()
		req, ok := new(big.Int).SetString(quorumSize, 10)
		if ok {
			progress.HasQuorum = received.Cmp(req) >= 0
		}

		return progress
	default:
		received := big.NewInt(0)
		seen := make(map[string]struct{}, len(msgs))

		for _, m := range msgs {
			if m == nil {
				continue
			}

			key := string(m.From)
			if _, dup := seen[key]; dup {
				continue
			}

			seen[key] = struct{}{}
			if p, ok := powerIndex[key]; ok {
				received = new(big.Int).Add(received, p)
			}
		}

		progress.ReceivedPower = received.String()
		req, ok := new(big.Int).SetString(quorumSize, 10)
		if ok {
			progress.HasQuorum = received.Cmp(req) >= 0
		}

		return progress
	}
}

func toMessageSnapshot(m *proto.IbftMessage) MessageSnapshot {
	if m == nil {
		return MessageSnapshot{}
	}

	out := MessageSnapshot{
		From:      append([]byte(nil), m.From...),
		Type:      messageTypeName(m.Type),
		Signature: append([]byte(nil), m.Signature...),
	}
	if m.View != nil {
		out.Height = m.View.Height
		out.Round = m.View.Round
	}

	switch m.Type {
	case proto.MessageType_PREPREPARE:
		out.ProposalHash = append([]byte(nil), messages.ExtractProposalHash(m)...)
	case proto.MessageType_PREPARE:
		out.ProposalHash = append([]byte(nil), messages.ExtractPrepareHash(m)...)
	case proto.MessageType_COMMIT:
		out.ProposalHash = append([]byte(nil), messages.ExtractCommitHash(m)...)
		if seal := messages.ExtractCommittedSeal(m); seal != nil {
			out.CommittedSeal = append([]byte(nil), seal.Signature...)
		}
	}

	return out
}

func messageTypeName(mt proto.MessageType) string {
	switch mt {
	case proto.MessageType_PREPREPARE:
		return "preprepare"
	case proto.MessageType_PREPARE:
		return "prepare"
	case proto.MessageType_COMMIT:
		return "commit"
	case proto.MessageType_ROUND_CHANGE:
		return "round_change"
	default:
		return mt.String()
	}
}

func canonicalPhaseName(s stateType) string {
	switch s {
	case newRound:
		return "new_round"
	case prepare:
		return "prepare"
	case commit:
		return "commit"
	case fin:
		return "fin"
	default:
		return s.String()
	}
}
