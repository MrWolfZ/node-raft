import { cancellationToken, CancellationToken } from '../../util/cancellation-token.js'
import { taggedUnion } from '../../util/tagged-unions.js'
import {
  GetCommittedState,
  MessageHandlerResponseType,
  messages,
  RaftMessage,
  WaitForLogReplication,
  WaitForMessage,
} from '../messages.js'
import { LogEntry, NodeState, NOOP_COMMAND, StateMachine } from '../state.js'
import type { WaitForElectionTimeout } from '../types.js'
import { becomeCandidate, BecomeCandidate } from './behavior.js'

export function onMessage<TMessage extends RaftMessage>(
  state: NodeState,
  stateMachine: StateMachine,
  message: TMessage,
) {
  type ResponseType = Exclude<MessageHandlerResponseType<TMessage>, GetCommittedState | WaitForLogReplication>

  const [newState, response] = taggedUnion.switch(message as RaftMessage, {
    RequestVote: ({ term, candidateId, lastLogTerm, lastLogIndex }) => {
      const { currentTerm } = state

      if (term < currentTerm) {
        return [state, messages.requestVoteResponse({ term: currentTerm, voteGranted: false })] as const
      }

      const voteGranted = messages.requestVoteResponse({ term, voteGranted: true })
      const voteNotGranted = messages.requestVoteResponse({ term, voteGranted: false })

      const votedFor = state.votedFor
      const hasVotedForOtherCandidateInSameTerm = votedFor !== null && votedFor !== candidateId && currentTerm === term

      if (hasVotedForOtherCandidateInSameTerm) {
        return [state, voteNotGranted] as const
      }

      const lastLogEntry = state.log[state.log.length - 1]
      const newState = { ...state, currentTerm: term }

      const hasLastTermMismatch = (lastLogEntry?.term || 0) > lastLogTerm
      const hasLastIndexMismatch = (lastLogEntry?.index || 0) > lastLogIndex

      if (hasLastTermMismatch || hasLastIndexMismatch) {
        return [newState, voteNotGranted] as const
      }

      return [{ ...newState, votedFor: candidateId }, voteGranted] as const
    },

    AppendEntries: ({ term, prevLogTerm, prevLogIndex, leaderId, leaderCommit, entries }) => {
      if (term < state.currentTerm) {
        return [state, messages.appendEntriesResponse({ term: state.currentTerm, success: false })] as const
      }

      const successful = messages.appendEntriesResponse({ term, success: true })
      const notSuccessful = messages.appendEntriesResponse({ term, success: false })

      const newState: NodeState = {
        ...state,
        currentTerm: term,
        votedFor: null,
        lastKnownLeaderId: leaderId,
      }

      const prevLogEntry = state.log[prevLogIndex - 1]
      const hasPrevTermMismatch = (prevLogEntry?.term || 0) !== prevLogTerm
      const hasPrevIndexMismatch = (prevLogEntry?.index || 0) !== prevLogIndex

      if (hasPrevTermMismatch || hasPrevIndexMismatch) {
        return [newState, notSuccessful] as const
      }

      let log = dropMismatchingEntriesFromLog(newState.log, entries)
      const newEntries = ignoreExistingIncomingEntries(log, entries)

      if (newEntries.length > 0) {
        log = [...log, ...newEntries]
      }

      const lastNewEntryIndex = newEntries[newEntries.length - 1]?.index || Number.MAX_VALUE
      const incomingCommitIndex = Math.min(leaderCommit, lastNewEntryIndex)
      const commitIndex = Math.max(incomingCommitIndex, newState.commitIndex)
      const lastApplied = applyCommittedCommandsToStateMachine(log, newState.lastApplied, commitIndex)

      return [{ ...newState, log, commitIndex, lastApplied }, successful] as const
    },

    ExecuteCommand: () => {
      const response = state.lastKnownLeaderId
        ? messages.redirectClient({ leaderId: state.lastKnownLeaderId })
        : messages.noLeaderError()

      return [state, response] as const
    },

    GetState: () => {
      const response = state.lastKnownLeaderId
        ? messages.redirectClient({ leaderId: state.lastKnownLeaderId })
        : messages.noLeaderError()

      return [state, response] as const
    },
  })

  return [newState, response as ResponseType] as const

  function dropMismatchingEntriesFromLog(log: LogEntry[], incomingEntries: LogEntry[]) {
    for (const { index, term } of incomingEntries) {
      const entry = state.log[index - 1]

      if (entry?.term !== term) {
        return log.filter((e) => e.index < index)
      }
    }

    return log
  }

  function ignoreExistingIncomingEntries(log: LogEntry[], incomingEntries: LogEntry[]) {
    return incomingEntries.filter((e) => log[e.index - 1]?.term !== e.term)
  }

  function applyCommittedCommandsToStateMachine(log: LogEntry[], lastApplied: number, commitIndex: number) {
    const entriesToApply = log.filter((e) => e.index > lastApplied && e.index <= commitIndex)

    for (const { command } of entriesToApply) {
      if (command === NOOP_COMMAND) {
        continue
      }

      stateMachine.applyCommand(command)
    }

    return commitIndex
  }
}

export async function runFollower(
  state: NodeState,
  stateMachine: StateMachine,
  waitForElectionTimeout: WaitForElectionTimeout,
  waitForMessage: WaitForMessage,
  onUpdateState: (state: NodeState) => Promise<NodeState>,
  token: CancellationToken,
): Promise<[NodeState, BecomeCandidate]> {
  while (!token.isCancelled) {
    const childTokenSource = cancellationToken.createSource(token)

    const result = await Promise.race([
      waitForElectionTimeout(childTokenSource.token),
      waitForMessage(childTokenSource.token),
    ])

    childTokenSource.cancel()

    if (!result || token.isCancelled) {
      break
    }

    const [updatedState, response] = onMessage(state, stateMachine, result.message)
    state = await onUpdateState(updatedState)
    result.respond(response)
  }

  return [state, becomeCandidate()]
}
