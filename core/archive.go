package core

import (
	"sync"
	"time"

	"github.com/0xPolygon/go-ibft/messages"
	"github.com/0xPolygon/go-ibft/messages/proto"
)

const (
	// maxPhaseSnapshots bounds retained per-phase snapshots for one height.
	// A round has at most four phases (new_round, prepare, commit, fin), so
	// this keeps phase history and round history (maxRoundHistory) aligned.
	maxPhaseSnapshots = 4 * maxRoundHistory

	phaseStatusCompleted  = "completed"
	phaseStatusInProgress = "in_progress"
)

// PhaseSnapshot is the frozen end-state of one consensus phase.
type PhaseSnapshot struct {
	Phase      string
	Status     string // completed | in_progress
	Height     uint64
	Round      uint64
	StartedAt  time.Time
	EndedAt    time.Time
	DurationMs int64
	Proposer   []byte
	Proposal   *ProposalSnapshot
	Quorum     map[string]QuorumProgress
	Messages   map[string]MessageTypeSnapshot
	LatestPC   *PreparedCertificateSnapshot
}

// HeightArchive is the retained diagnostics for one consensus height.
type HeightArchive struct {
	Status             SequenceStatus
	Height             uint64
	Round              uint64
	Phase              string
	RoundStarted       bool
	LastRoundEndReason RoundEndReason
	SequenceStartedAt  time.Time
	SequenceEndedAt    time.Time
	RoundStartedAt     time.Time
	PhaseStartedAt     time.Time
	NodeID             []byte
	Proposer           []byte
	IsProposer         bool
	Proposal           *ProposalSnapshot
	Validators         []ValidatorSnapshot
	TotalVotingPower   string
	QuorumSize         string
	RoundHistory       []RoundSummary
	PhaseSnapshots     []PhaseSnapshot
	LatestPC           *PreparedCertificateSnapshot
	CommittedSeals     []CommittedSealSnapshot
}

type phaseEndEvent struct {
	hasEvent  bool
	phase     stateType
	height    uint64
	round     uint64
	startedAt time.Time
	endedAt   time.Time
	duration  time.Duration
	proposer  []byte
	proposal  *ProposalSnapshot
	latestPC  *PreparedCertificateSnapshot
}

type diagnosticsArchive struct {
	mu            sync.RWMutex
	current       *HeightArchive
	lastFinalized *HeightArchive
}

func newDiagnosticsArchive() *diagnosticsArchive {
	return &diagnosticsArchive{}
}

func (a *diagnosticsArchive) beginHeight(height uint64, startedAt time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.current = &HeightArchive{
		Status:            SequenceRunning,
		Height:            height,
		Phase:             canonicalPhaseName(newRound),
		SequenceStartedAt: startedAt,
		RoundStartedAt:    startedAt,
		PhaseStartedAt:    startedAt,
		PhaseSnapshots:    make([]PhaseSnapshot, 0, 8),
	}
}

func (a *diagnosticsArchive) appendPhase(snap PhaseSnapshot) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.current == nil {
		return
	}

	if len(a.current.PhaseSnapshots) >= maxPhaseSnapshots {
		copy(a.current.PhaseSnapshots, a.current.PhaseSnapshots[1:])
		a.current.PhaseSnapshots[maxPhaseSnapshots-1] = snap
		a.current.PhaseSnapshots = a.current.PhaseSnapshots[:maxPhaseSnapshots]
	} else {
		a.current.PhaseSnapshots = append(a.current.PhaseSnapshots, snap)
	}

	if len(snap.Proposer) > 0 {
		a.current.Proposer = append([]byte(nil), snap.Proposer...)
	}

	if snap.Proposal != nil && snap.Proposal.Available {
		a.current.Proposal = cloneProposalSnapshot(snap.Proposal)
	}

	if snap.LatestPC != nil && snap.LatestPC.Available {
		a.current.LatestPC = clonePreparedCertificate(snap.LatestPC)
	}
}

func (a *diagnosticsArchive) finalizeCurrent(
	status SequenceStatus,
	reason RoundEndReason,
	endedAt time.Time,
	final *HeightArchive,
) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.current == nil {
		return
	}

	if final != nil {
		a.current.Status = status
		a.current.Height = final.Height
		a.current.Round = final.Round
		a.current.Phase = final.Phase
		a.current.RoundStarted = final.RoundStarted
		a.current.LastRoundEndReason = reason
		a.current.SequenceEndedAt = endedAt

		if !final.SequenceStartedAt.IsZero() {
			a.current.SequenceStartedAt = final.SequenceStartedAt
		}

		if !final.RoundStartedAt.IsZero() {
			a.current.RoundStartedAt = final.RoundStartedAt
		}
		// Prefer the live phase start; if the round already closed it (zero),
		// fall back to the last completed phase snapshot so we don't keep the
		// sequence-start timestamp as a misleading phaseStartedAt.
		switch {
		case !final.PhaseStartedAt.IsZero():
			a.current.PhaseStartedAt = final.PhaseStartedAt
		case len(a.current.PhaseSnapshots) > 0:
			a.current.PhaseStartedAt = a.current.PhaseSnapshots[len(a.current.PhaseSnapshots)-1].StartedAt
		default:
			a.current.PhaseStartedAt = time.Time{}
		}

		a.current.NodeID = append([]byte(nil), final.NodeID...)
		a.current.Proposer = append([]byte(nil), final.Proposer...)
		a.current.IsProposer = final.IsProposer
		a.current.Proposal = cloneProposalSnapshot(final.Proposal)
		a.current.Validators = cloneValidators(final.Validators)
		a.current.TotalVotingPower = final.TotalVotingPower
		a.current.QuorumSize = final.QuorumSize
		a.current.LatestPC = clonePreparedCertificate(final.LatestPC)
		a.current.CommittedSeals = cloneCommittedSeals(final.CommittedSeals)
		// state.roundHistory is the single source of truth for completed rounds.
		a.current.RoundHistory = append([]RoundSummary(nil), final.RoundHistory...)
	} else {
		a.current.Status = status
		a.current.LastRoundEndReason = reason
		a.current.SequenceEndedAt = endedAt
	}

	// Promote immutable copy to lastFinalized.
	a.lastFinalized = cloneHeightArchive(a.current)
}

//nolint:revive // try-lock returns current, last, ok
func (a *diagnosticsArchive) trySnapshot() (current, last *HeightArchive, ok bool) {
	if !a.mu.TryRLock() {
		return nil, nil, false
	}
	defer a.mu.RUnlock()

	return cloneHeightArchive(a.current), cloneHeightArchive(a.lastFinalized), true
}

func cloneHeightArchive(src *HeightArchive) *HeightArchive {
	if src == nil {
		return nil
	}

	out := *src
	out.NodeID = append([]byte(nil), src.NodeID...)
	out.Proposer = append([]byte(nil), src.Proposer...)
	out.Proposal = cloneProposalSnapshot(src.Proposal)
	out.Validators = cloneValidators(src.Validators)
	out.LatestPC = clonePreparedCertificate(src.LatestPC)
	out.CommittedSeals = cloneCommittedSeals(src.CommittedSeals)

	if src.RoundHistory != nil {
		out.RoundHistory = append([]RoundSummary{}, src.RoundHistory...)
	}

	if src.PhaseSnapshots != nil {
		out.PhaseSnapshots = make([]PhaseSnapshot, len(src.PhaseSnapshots))
		for i := range src.PhaseSnapshots {
			out.PhaseSnapshots[i] = clonePhaseSnapshot(src.PhaseSnapshots[i])
		}
	}

	return &out
}

func clonePhaseSnapshot(src PhaseSnapshot) PhaseSnapshot {
	out := src
	out.Proposer = append([]byte(nil), src.Proposer...)
	out.Proposal = cloneProposalSnapshot(src.Proposal)
	out.LatestPC = clonePreparedCertificate(src.LatestPC)

	if src.Quorum != nil {
		out.Quorum = make(map[string]QuorumProgress, len(src.Quorum))
		for k, v := range src.Quorum {
			out.Quorum[k] = v
		}
	}

	if src.Messages != nil {
		out.Messages = make(map[string]MessageTypeSnapshot, len(src.Messages))
		for k, v := range src.Messages {
			out.Messages[k] = compactMessageTypeSnapshot(v)
		}
	}

	return out
}

func cloneProposalSnapshot(src *ProposalSnapshot) *ProposalSnapshot {
	if src == nil {
		return nil
	}

	out := *src
	out.Hash = append([]byte(nil), src.Hash...)
	// RawProposal is treated as immutable after accept; share the backing array.
	out.RawProposal = src.RawProposal

	return &out
}

func clonePreparedCertificate(src *PreparedCertificateSnapshot) *PreparedCertificateSnapshot {
	if src == nil {
		return nil
	}

	out := *src
	out.ProposalHash = append([]byte(nil), src.ProposalHash...)
	out.ProposalFrom = append([]byte(nil), src.ProposalFrom...)

	if len(src.PrepareSenders) > 0 {
		out.PrepareSenders = make([][]byte, len(src.PrepareSenders))
		for i, s := range src.PrepareSenders {
			out.PrepareSenders[i] = append([]byte(nil), s...)
		}
	}

	return &out
}

func cloneValidators(src []ValidatorSnapshot) []ValidatorSnapshot {
	if len(src) == 0 {
		return nil
	}

	out := make([]ValidatorSnapshot, len(src))
	for i, v := range src {
		out[i] = ValidatorSnapshot{
			ID:          append([]byte(nil), v.ID...),
			VotingPower: v.VotingPower,
		}
	}

	return out
}

func cloneCommittedSeals(src []CommittedSealSnapshot) []CommittedSealSnapshot {
	if len(src) == 0 {
		return nil
	}

	out := make([]CommittedSealSnapshot, len(src))
	for i, s := range src {
		out[i] = CommittedSealSnapshot{
			Signer:    append([]byte(nil), s.Signer...),
			Signature: append([]byte(nil), s.Signature...),
		}
	}

	return out
}

// compactMessageTypeSnapshot keeps voter identity/hashes for archives but drops
// bulky signature/seal bytes (those remain available on live current.messages
// and lastFinalized.committedSeals).
func compactMessageTypeSnapshot(src MessageTypeSnapshot) MessageTypeSnapshot {
	out := MessageTypeSnapshot{
		Available:  src.Available,
		Truncated:  src.Truncated,
		ViewHeight: src.ViewHeight,
		ViewRound:  src.ViewRound,
	}
	if len(src.Messages) == 0 {
		return out
	}

	out.Messages = make([]MessageSnapshot, 0, len(src.Messages))
	for _, m := range src.Messages {
		out.Messages = append(out.Messages, MessageSnapshot{
			From:         append([]byte(nil), m.From...),
			Type:         m.Type,
			Height:       m.Height,
			Round:        m.Round,
			ProposalHash: append([]byte(nil), m.ProposalHash...),
		})
	}

	return out
}

func proposalSnapshotFromMessage(
	proposalMessage *proto.IbftMessage,
) (from []byte, proposal *ProposalSnapshot) {
	if proposalMessage == nil {
		return nil, &ProposalSnapshot{Available: false}
	}

	from = append([]byte(nil), proposalMessage.From...)
	proposal = &ProposalSnapshot{Available: true}

	if hash := messages.ExtractProposalHash(proposalMessage); len(hash) > 0 {
		proposal.Hash = append([]byte(nil), hash...)
	}

	if p := messages.ExtractProposal(proposalMessage); p != nil {
		proposal.Round = p.Round
		proposal.RawSize = len(p.RawProposal)
		proposal.RawProposal = p.RawProposal
	}

	return from, proposal
}
