import { cancellationToken, CancellationToken } from '../../util/cancellation-token.js'
import { taggedUnion } from '../../util/tagged-unions.js'
import {
  GetCommittedState,
  MessageHandlerResponseType,
  messages,
  RaftMessage,
  SendRequestVote,
  WaitForLogReplication,
  WaitForMessage,
} from '../messages.js'
import type { NodeState } from '../state.js'
import type { NodeId, WaitForElectionTimeout } from '../types.js'
import { becomeFollower, BecomeFollower, becomeLeader, BecomeLeader } from './behavior.js'

export function onMessage<TMessage extends RaftMessage>(state: NodeState, message: TMessage) {
  type ResponseType = Exclude<MessageHandlerResponseType<TMessage>, GetCommittedState | WaitForLogReplication>

  return taggedUnion.switch(message as RaftMessage, {
    RequestVote: ({ term }) => {
      if (term <= state.currentTerm) {
        return messages.requestVoteResponse({ term: state.currentTerm, voteGranted: false })
      }

      return becomeFollower()
    },

    AppendEntries: ({ term }) => {
      if (term < state.currentTerm) {
        return messages.appendEntriesResponse({ term: state.currentTerm, success: false })
      }

      return becomeFollower()
    },

    ExecuteCommand: () => messages.noLeaderError(),
    GetState: () => messages.noLeaderError(),
  }) as ResponseType | BecomeFollower
}

export async function runElection(
  nodeId: NodeId,
  state: NodeState,
  otherNodeIds: NodeId[],
  waitForElectionTimeout: WaitForElectionTimeout,
  requestVote: SendRequestVote,
  parentToken: CancellationToken,
): Promise<BecomeLeader | BecomeFollower | 'timeout'> {
  const { token, cancel } = cancellationToken.createSource(parentToken)

  const pendingResponses = new Map(otherNodeIds.map((id) => [id, requestVoteWithRetry(id)]))

  let requiredVotes = otherNodeIds.length / 2

  const timeout = waitForElectionTimeout(token)

  while (requiredVotes > 0 && !token.isCancelled) {
    const result = await Promise.race([timeout, ...pendingResponses.values()])

    if (!result) {
      cancel()

      return 'timeout'
    }

    if (becomeFollower.is(result)) {
      return result
    }

    const { id, voteGranted } = result

    pendingResponses.delete(id)

    if (voteGranted) {
      requiredVotes -= 1
    }
  }

  cancel()

  return becomeLeader()

  async function requestVoteWithRetry(id: NodeId) {
    const lastLogEntry = state.log[state.log.length - 1]

    const message = messages.requestVote({
      term: state.currentTerm,
      candidateId: nodeId,
      lastLogTerm: lastLogEntry?.term || 0,
      lastLogIndex: lastLogEntry?.index || 0,
    })

    while (!token.isCancelled) {
      try {
        const { term, voteGranted } = await requestVote(id, message, token)

        if (term > state.currentTerm) {
          return becomeFollower()
        }

        return { id, voteGranted }
      } catch {
        // we retry indefinitely (or until cancelled)
      }
    }

    return becomeFollower()
  }
}

export async function runCandidate(
  nodeId: NodeId,
  state: NodeState,
  otherNodeIds: NodeId[],
  waitForElectionTimeout: WaitForElectionTimeout,
  waitForMessage: WaitForMessage,
  requestVote: SendRequestVote,
  onUpdateState: (state: NodeState) => Promise<NodeState>,
  token: CancellationToken,
) {
  const childTokenSource = cancellationToken.createSource(token)

  const result = await run()

  childTokenSource.cancel()

  return result

  async function run(): Promise<[NodeState, BecomeLeader | BecomeFollower]> {
    const elections = runElections()

    while (!token.isCancelled) {
      const result = await Promise.race([elections, waitForMessage(childTokenSource.token)])

      if (becomeFollower.is(result) || becomeLeader.is(result)) {
        return [state, result]
      }

      const { message, respond } = result

      const messageResult = onMessage(state, message)

      if (becomeFollower.is(messageResult)) {
        return [state, becomeFollower({ triggeringMessage: result })]
      }

      respond(messageResult)
    }

    return [state, becomeFollower()]
  }

  async function runElections() {
    while (!token.isCancelled) {
      state = await onUpdateState({ ...state, currentTerm: state.currentTerm + 1, lastKnownLeaderId: undefined })

      const result = await runElection(
        nodeId,
        state,
        otherNodeIds,
        waitForElectionTimeout,
        requestVote,
        childTokenSource.token,
      )

      if (result !== 'timeout') {
        return result
      }
    }

    return becomeFollower()
  }
}
