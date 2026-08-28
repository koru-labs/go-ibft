package core

import (
	"sync"
	"time"

	"github.com/0xPolygon/go-ibft/messages"
	"github.com/0xPolygon/go-ibft/messages/proto"
)

type stateType uint8

const (
	newRound stateType = iota
	prepare
	commit
	fin
)

func (s stateType) String() string {
	switch s {
	case newRound:
		return "new round"
	case prepare:
		return phaseNamePrepare
	case commit:
		return phaseNameCommit
	case fin:
		return phaseNameFin
	}

	return ""
}

type state struct {
	sync.RWMutex

	//	current view (sequence, round)
	view *proto.View

	// latestPC is the latest prepared certificate
	latestPC *proto.PreparedCertificate

	// latestPreparedProposal is the proposal
	// for which Q(N)-1 PREPARE messages were received
	latestPreparedProposal *proto.Proposal

	//	accepted proposal for current round
	proposalMessage *proto.IbftMessage

	//	validated commit seals
	seals []*messages.CommittedSeal

	//	flags for different states
	roundStarted bool

	name stateType

	// diagnostics — written only on existing state transitions under Lock.
	sequenceStatus     SequenceStatus
	lastRoundEndReason RoundEndReason
	sequenceStartedAt  time.Time
	roundStartedAt     time.Time
	phaseStartedAt     time.Time
	phaseDurations     [fin + 1]time.Duration
	roundHistory       [maxRoundHistory]RoundSummary
	roundHistoryLen    int
}

func (s *state) getView() *proto.View {
	s.RLock()
	defer s.RUnlock()

	return &proto.View{
		Height: s.view.Height,
		Round:  s.view.Round,
	}
}

func (s *state) reset(height uint64) {
	s.Lock()
	defer s.Unlock()

	s.seals = nil
	s.roundStarted = false
	s.name = newRound
	s.proposalMessage = nil
	s.latestPC = nil
	s.latestPreparedProposal = nil

	s.view = &proto.View{
		Height: height,
		Round:  0,
	}

	// Clear per-height diagnostics. Sequence timestamps are set by markSequenceStarted.
	s.lastRoundEndReason = ""
	s.roundStartedAt = time.Time{}
	s.phaseStartedAt = time.Time{}
	s.phaseDurations = [fin + 1]time.Duration{}
	s.roundHistoryLen = 0
}

func (s *state) getLatestPC() *proto.PreparedCertificate {
	s.RLock()
	defer s.RUnlock()

	return s.latestPC
}

func (s *state) getLatestPreparedProposal() *proto.Proposal {
	s.RLock()
	defer s.RUnlock()

	return s.latestPreparedProposal
}

func (s *state) getProposalMessage() *proto.IbftMessage {
	s.RLock()
	defer s.RUnlock()

	return s.proposalMessage
}

func (s *state) getProposalHash() []byte {
	s.RLock()
	defer s.RUnlock()

	return messages.ExtractProposalHash(s.proposalMessage)
}

func (s *state) setProposalMessage(proposalMessage *proto.IbftMessage) {
	s.Lock()
	defer s.Unlock()

	s.proposalMessage = proposalMessage
}

func (s *state) getRound() uint64 {
	s.RLock()
	defer s.RUnlock()

	return s.view.Round
}

func (s *state) getHeight() uint64 {
	s.RLock()
	defer s.RUnlock()

	return s.view.Height
}

func (s *state) getProposal() *proto.Proposal {
	s.RLock()
	defer s.RUnlock()

	if s.proposalMessage != nil {
		return messages.ExtractProposal(s.proposalMessage)
	}

	return nil
}

func (s *state) getRawDataFromProposal() []byte {
	proposal := s.getProposal()

	if proposal != nil {
		return proposal.RawProposal
	}

	return nil
}

func (s *state) getCommittedSeals() []*messages.CommittedSeal {
	s.RLock()
	defer s.RUnlock()

	return s.seals
}

func (s *state) getStateName() stateType {
	s.RLock()
	defer s.RUnlock()

	return s.name
}

func (s *state) changeState(name stateType) phaseEndEvent {
	s.Lock()
	defer s.Unlock()

	return s.transitionPhaseLocked(time.Now(), name)
}

func (s *state) setRoundStarted(started bool) {
	s.Lock()
	defer s.Unlock()

	s.roundStarted = started
}

func (s *state) setView(view *proto.View) {
	s.Lock()
	defer s.Unlock()

	s.view = view
}

func (s *state) setCommittedSeals(seals []*messages.CommittedSeal) {
	s.Lock()
	defer s.Unlock()

	s.seals = seals
}

func (s *state) newRound() {
	s.Lock()
	defer s.Unlock()

	if !s.roundStarted {
		// Round is not yet started, kick the round off.
		// Preserve timers set by markSequenceStarted / moveToRound.
		now := time.Now()
		s.name = newRound
		s.roundStarted = true

		if s.roundStartedAt.IsZero() {
			s.roundStartedAt = now
		}

		if s.phaseStartedAt.IsZero() {
			s.phaseStartedAt = now
		}
	}
}

func (s *state) finalizePrepare(
	certificate *proto.PreparedCertificate,
	latestPPB *proto.Proposal,
) phaseEndEvent {
	s.Lock()
	defer s.Unlock()

	s.latestPC = certificate
	s.latestPreparedProposal = latestPPB

	// Move to the commit state
	return s.transitionPhaseLocked(time.Now(), commit)
}

// markSequenceStarted records sequence start. Called once at RunSequence entry.
func (s *state) markSequenceStarted(at time.Time) {
	s.Lock()
	defer s.Unlock()

	s.sequenceStatus = SequenceRunning
	s.sequenceStartedAt = at
	s.roundStartedAt = at
	s.phaseStartedAt = at
	s.lastRoundEndReason = ""
}

type sequenceEndResult struct {
	phaseEvent phaseEndEvent
}

// markSequenceCompleted records successful finalization of the height.
func (s *state) markSequenceCompleted(reason RoundEndReason) sequenceEndResult {
	s.Lock()
	defer s.Unlock()

	now := time.Now()
	_, phaseEvent := s.finishRoundLocked(now, reason)
	s.sequenceStatus = SequenceCompleted
	s.lastRoundEndReason = reason

	return sequenceEndResult{phaseEvent: phaseEvent}
}

// markSequenceCancelled records that the sequence was aborted (syncer/shutdown).
func (s *state) markSequenceCancelled() sequenceEndResult {
	s.Lock()
	defer s.Unlock()

	now := time.Now()
	_, phaseEvent := s.finishRoundLocked(now, RoundEndCancelled)
	s.sequenceStatus = SequenceCancelled
	s.lastRoundEndReason = RoundEndCancelled

	return sequenceEndResult{phaseEvent: phaseEvent}
}

// beginRoundLocked starts timing for a new round. Caller must hold Lock.
func (s *state) beginRoundLocked(now time.Time) {
	s.roundStartedAt = now
	s.phaseStartedAt = now
	s.phaseDurations = [fin + 1]time.Duration{}
	s.name = newRound
	s.roundStarted = false
	s.proposalMessage = nil
}

func (s *state) transitionPhaseLocked(now time.Time, next stateType) phaseEndEvent {
	ev := phaseEndEvent{}

	if !s.phaseStartedAt.IsZero() {
		duration := now.Sub(s.phaseStartedAt)
		s.phaseDurations[s.name] += duration

		ev.hasEvent = true
		ev.phase = s.name
		ev.height = s.view.Height
		ev.round = s.view.Round
		ev.startedAt = s.phaseStartedAt
		ev.endedAt = now
		ev.duration = duration
		ev.proposer, ev.proposal = proposalSnapshotFromMessage(s.proposalMessage)

		if s.latestPC != nil {
			ev.latestPC = clonePreparedCertificate(preparedCertificateFromState(s.latestPC))
		}
	}

	s.name = next
	s.phaseStartedAt = now

	return ev
}

func (s *state) finishRoundLocked(now time.Time, reason RoundEndReason) (RoundSummary, phaseEndEvent) {
	var (
		summary RoundSummary
		ev      phaseEndEvent
	)

	if s.view == nil {
		return summary, ev
	}

	if !s.phaseStartedAt.IsZero() {
		duration := now.Sub(s.phaseStartedAt)
		s.phaseDurations[s.name] += duration

		ev.hasEvent = true
		ev.phase = s.name
		ev.height = s.view.Height
		ev.round = s.view.Round
		ev.startedAt = s.phaseStartedAt
		ev.endedAt = now
		ev.duration = duration
		ev.proposer, ev.proposal = proposalSnapshotFromMessage(s.proposalMessage)

		if s.latestPC != nil {
			ev.latestPC = clonePreparedCertificate(preparedCertificateFromState(s.latestPC))
		}

		s.phaseStartedAt = time.Time{}
	}

	summary = RoundSummary{
		Round:            s.view.Round,
		EndReason:        reason,
		StartedAt:        s.roundStartedAt,
		EndedAt:          now,
		PhaseDurationsMs: make(map[string]int64, 4),
	}
	if !s.roundStartedAt.IsZero() {
		summary.DurationMs = now.Sub(s.roundStartedAt).Milliseconds()
	}

	for st := stateType(0); st <= fin; st++ {
		if d := s.phaseDurations[st]; d > 0 {
			summary.PhaseDurationsMs[canonicalPhaseName(st)] = d.Milliseconds()
		}
	}

	if s.roundHistoryLen < maxRoundHistory {
		s.roundHistory[s.roundHistoryLen] = summary
		s.roundHistoryLen++
	} else {
		// Drop oldest; keep the newest maxRoundHistory entries.
		copy(s.roundHistory[:], s.roundHistory[1:])
		s.roundHistory[maxRoundHistory-1] = summary
	}

	return summary, ev
}

// moveToRound advances the view to a new round after recording the previous one.
// Caller must NOT hold the lock; this acquires it.
func (s *state) moveToRound(round uint64, reason RoundEndReason) (RoundSummary, phaseEndEvent) {
	s.Lock()
	defer s.Unlock()

	now := time.Now()
	summary, ev := s.finishRoundLocked(now, reason)
	s.lastRoundEndReason = reason
	s.view = &proto.View{
		Height: s.view.Height,
		Round:  round,
	}
	s.beginRoundLocked(now)

	return summary, ev
}

func preparedCertificateFromState(pc *proto.PreparedCertificate) *PreparedCertificateSnapshot {
	if pc == nil {
		return nil
	}

	out := &PreparedCertificateSnapshot{Available: true}
	if pc.ProposalMessage != nil {
		out.ProposalFrom = append([]byte(nil), pc.ProposalMessage.From...)
		if h := messages.ExtractProposalHash(pc.ProposalMessage); h != nil {
			out.ProposalHash = append([]byte(nil), h...)
		}
	}

	out.PrepareCount = len(pc.PrepareMessages)
	out.PrepareSenders = make([][]byte, 0, len(pc.PrepareMessages))

	for _, m := range pc.PrepareMessages {
		if m != nil {
			out.PrepareSenders = append(out.PrepareSenders, append([]byte(nil), m.From...))
		}
	}

	return out
}
