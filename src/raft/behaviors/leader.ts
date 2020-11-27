import { cancellationToken, CancellationToken } from '../../util/cancellation-token.js'
import { taggedUnion } from '../../util/tagged-unions.js'
import {
  ExecuteCommandResponse,
  GetStateResponse,
  MessageHandlerResponseType,
  messages,
  NoLeaderError,
  RaftMessage,
  RedirectClient,
  ResponseChannel,
  SendAppendEntries,
  WaitForMessage,
} from '../messages.js'
import { LogEntry, NodeState, NOOP_COMMAND, StateMachine } from '../state.js'
import type { LogIndex, NodeId, WaitForHeartbeatInterval } from '../types.js'
import { becomeFollower, BecomeFollower } from './behavior.js'

export function onMessage<TMessage extends RaftMessage>(state: NodeState, message: TMessage) {
  type ResponseType = Exclude<MessageHandlerResponseType<TMessage>, NoLeaderError | RedirectClient>

  const result = taggedUnion.switch(message as RaftMessage, {
    RequestVote: ({ term }) => {
      if (term <= state.currentTerm) {
        return [state, messages.requestVoteResponse({ term: state.currentTerm, voteGranted: false })] as const
      }

      return becomeFollower()
    },

    AppendEntries: ({ term }) => {
      if (term <= state.currentTerm) {
        return [state, messages.appendEntriesResponse({ term: state.currentTerm, success: false })] as const
      }

      return becomeFollower()
    },

    ExecuteCommand: ({ command }) => {
      const newLogEntry: LogEntry = { index: state.log.length + 1, term: state.currentTerm, command }
      const newState: NodeState = { ...state, log: [...state.log, newLogEntry] }

      return [newState, messages.waitForLogReplication({ logIndex: newState.log.length })] as const
    },

    GetState: () => [state, messages.getCommittedState()] as const,
  })

  return result as [NodeState, ResponseType] | BecomeFollower
}

export type LogReplicationState = {
  [nodeId in NodeId]: {
    nextIndex: LogIndex
    matchIndex: LogIndex
  }
}

export function runLogReplication(
  nodeId: NodeId,
  getState: () => NodeState,
  otherNodeIds: NodeId[],
  setCommitIndex: (index: LogIndex) => Promise<void>,
  waitForHeartbeatInterval: WaitForHeartbeatInterval,
  sendLogEntries: SendAppendEntries,
  parentToken: CancellationToken,
  logReplicationState: LogReplicationState = {}, // allow passing value for testing
) {
  const cancellationTokenSource = cancellationToken.createSource(parentToken)
  const { token } = cancellationTokenSource

  const newLogEntryListeners = new Set<() => void>()

  function onNewLogEntry() {
    for (const listener of newLogEntryListeners) {
      listener()
    }
  }

  const matchIndices = otherNodeIds.reduce(
    (r, id) => ({ ...r, [id]: logReplicationState[id]?.matchIndex ?? 0 }),
    {} as Record<NodeId, LogIndex>,
  )

  return {
    onNewLogEntry,
    promise: run(),
  }

  async function run() {
    const result = await Promise.race(otherNodeIds.map(replicateToNode))
    cancellationTokenSource.cancel()
    return result
  }

  async function replicateToNode(id: NodeId): Promise<BecomeFollower> {
    const state = getState()
    const lastLogEntry = state.log[state.log.length - 1]

    let nextIndex = logReplicationState[id]?.nextIndex ?? (lastLogEntry?.index ?? 0) + 1
    let matchIndex = logReplicationState[id]?.matchIndex ?? 0

    let isStillLeader = await sendEntries()

    while (isStillLeader && !token.isCancelled) {
      if (getState().log.length < nextIndex) {
        await Promise.race([waitForHeartbeatInterval(token), waitForNewEntry()])
      }

      isStillLeader = await sendEntries()
    }

    return becomeFollower()

    async function sendEntries() {
      const state = getState()
      const entriesToSend = state.log.slice(nextIndex - 1)
      const lastLogEntry = entriesToSend[entriesToSend.length - 1]
      const prevLogEntry = state.log[nextIndex - 2]
      const message = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: entriesToSend,
        leaderCommit: state.commitIndex,
        prevLogTerm: prevLogEntry?.term || 0,
        prevLogIndex: prevLogEntry?.index || 0,
      })

      try {
        const { term, success } = await sendLogEntries(id, message, token)

        if (term > state.currentTerm) {
          return false
        }

        if (success) {
          if (lastLogEntry) {
            nextIndex = lastLogEntry.index + 1
            matchIndex = lastLogEntry.index

            await onUpdateMatchIndex(id, matchIndex)
          }
        } else {
          nextIndex -= 1
        }
      } catch {
        // we ignore errors since the next loop iteration will
        // try to send the same log entries
      }

      return true
    }

    function waitForNewEntry() {
      return new Promise<void>((resolve) => {
        const listener = () => {
          newLogEntryListeners.delete(listener)
          resolve()
        }

        newLogEntryListeners.add(listener)
      })
    }
  }

  async function onUpdateMatchIndex(id: NodeId, matchIndex: LogIndex) {
    const { commitIndex } = getState()

    matchIndices[id] = matchIndex

    const majority = otherNodeIds.length / 2
    let newCommitIndex = commitIndex

    while (newCommitIndex <= matchIndex) {
      const nrOfNodesWithGreaterIndex = otherNodeIds.filter((id) => matchIndices[id] > newCommitIndex).length

      if (nrOfNodesWithGreaterIndex < majority) {
        break
      }

      newCommitIndex += 1
    }

    if (newCommitIndex > commitIndex) {
      await setCommitIndex(newCommitIndex)
    }
  }
}

export async function getCommittedStateMachineState(
  nodeId: NodeId,
  state: NodeState,
  stateMachine: StateMachine,
  otherNodeIds: NodeId[],
  sendHeartbeat: SendAppendEntries,
  parentToken: CancellationToken,
): Promise<BecomeFollower | GetStateResponse> {
  const cancellationTokenSource = cancellationToken.createSource(parentToken)
  const { token } = cancellationTokenSource

  const prevLogEntry = state.log[state.commitIndex - 1]
  const message = messages.appendEntries({
    term: state.currentTerm,
    leaderId: nodeId,
    entries: [],
    leaderCommit: state.commitIndex,
    prevLogTerm: prevLogEntry?.term || 0,
    prevLogIndex: prevLogEntry?.index || 0,
  })

  const pendingResponses = new Map(otherNodeIds.map((id) => [id, sendHeartbeatWithRetry(id)]))

  let requiredVotes = otherNodeIds.length / 2

  while (requiredVotes > 0 && !token.isCancelled) {
    const result = await Promise.race(pendingResponses.values())

    if (becomeFollower.is(result)) {
      return result
    }

    pendingResponses.delete(result.id)
    requiredVotes -= 1
  }

  cancellationTokenSource.cancel()

  return messages.getStateResponse({ state: stateMachine.getState() })

  async function sendHeartbeatWithRetry(id: NodeId) {
    while (!token.isCancelled) {
      try {
        const { term, success } = await sendHeartbeat(id, message, token)

        if (term > state.currentTerm) {
          return becomeFollower()
        }

        if (success) {
          return { id }
        }
      } catch {
        // we retry indefinitely (or until cancelled)
      }
    }

    return becomeFollower()
  }
}

export async function runLeader(
  nodeId: NodeId,
  state: NodeState,
  stateMachine: StateMachine,
  otherNodeIds: NodeId[],
  waitForMessage: WaitForMessage,
  waitForHeartbeatInterval: WaitForHeartbeatInterval,
  sendLogEntries: SendAppendEntries,
  onUpdateState: (state: NodeState) => Promise<NodeState>,
  parentToken: CancellationToken,
): Promise<[NodeState, BecomeFollower]> {
  const childTokenSource = cancellationToken.createSource(parentToken)
  const { token } = childTokenSource

  let notifyReplicationOfNewEntry: () => void
  const pendingLogReplications = new Map<LogIndex, ResponseChannel<ExecuteCommandResponse | NoLeaderError>>()

  const result = await Promise.race([logReplication(), processMessages()])

  for (const respond of pendingLogReplications.values()) {
    respond(messages.noLeaderError())
  }

  childTokenSource.cancel()

  return [state, result]

  async function logReplication() {
    const setCommitIndex = async (logIndex: LogIndex) => {
      state = await onUpdateState({ ...state, commitIndex: logIndex })

      const committedIndices = [...pendingLogReplications.keys()].filter((idx) => idx <= logIndex).sort()

      for (const idx of committedIndices) {
        const respond = pendingLogReplications.get(idx) || (() => void 0)
        pendingLogReplications.delete(idx)
        stateMachine.applyCommand(state.log[idx - 1].command)
        respond(messages.executeCommandResponse())
      }
    }

    const { onNewLogEntry, promise } = runLogReplication(
      nodeId,
      () => state,
      otherNodeIds,
      setCommitIndex,
      waitForHeartbeatInterval,
      sendLogEntries,
      token,
    )

    notifyReplicationOfNewEntry = onNewLogEntry

    // as outlined in chapter 8 of the RAFT paper a leader inserts a no-op entry into the
    // log at the start of its term to ensure it knows the commit state of all followers
    const [newState] = (onMessage(state, messages.executeCommand({ command: NOOP_COMMAND })) as unknown) as [NodeState]

    state = await onUpdateState({ ...newState, lastKnownLeaderId: undefined })
    notifyReplicationOfNewEntry()

    return await promise
  }

  async function processMessages() {
    while (!token.isCancelled) {
      const result = await waitForMessage(token)

      if (becomeFollower.is(result)) {
        return result
      }

      const { message, respond, cancellationToken } = result

      const messageResult = onMessage(state, message)

      if (becomeFollower.is(messageResult)) {
        return becomeFollower({ triggeringMessage: result })
      }

      const [newState, response] = messageResult

      const hasNewLogEntries = newState.log.length !== state.log.length

      state = await onUpdateState(newState)

      if (hasNewLogEntries) {
        notifyReplicationOfNewEntry()
      }

      if (messages.waitForLogReplication.is(response)) {
        pendingLogReplications.set(response.logIndex, respond)
      } else if (messages.getCommittedState.is(response)) {
        // getCommittedStateMachineState should never throw, so we ignore errors here
        getCommittedStateForClientRequest(respond, cancellationToken).catch(() => void 0)
      } else {
        respond(response)
      }
    }

    return becomeFollower()
  }

  async function getCommittedStateForClientRequest(
    respond: ResponseChannel<GetStateResponse | NoLeaderError>,
    requestToken: CancellationToken,
  ) {
    // if we get cancelled while we are waiting for the state (e.g. because we lost the leader role),
    // we return an error to the client, which should cause it to retry its operation with the new
    // leader
    const unregister = token.register(() => respond(messages.noLeaderError()))

    const combinedToken = cancellationToken.createSource(token, requestToken).token

    try {
      const result = await getCommittedStateMachineState(
        nodeId,
        state,
        stateMachine,
        otherNodeIds,
        sendLogEntries,
        combinedToken,
      )

      if (token.isCancelled) {
        return
      }

      if (becomeFollower.is(result)) {
        respond(messages.noLeaderError())
      } else {
        respond(result)
      }
    } finally {
      unregister()
    }
  }
}
