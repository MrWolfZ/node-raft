import { CancellationToken, cancellationToken } from '../../util/cancellation-token.js'
import {
  messages,
  MessageWithResponseChannel,
  RaftMessage,
  RequestVote,
  ResponseChannel,
  SendRequestVote,
} from '../messages.js'
import { createNodeState } from '../state.js'
import type { NodeId } from '../types.js'
import { becomeFollower, becomeLeader } from './behavior.js'
import { onMessage, runCandidate, runElection } from './candidate.js'

describe('candidate', () => {
  describe(onMessage.name, () => {
    describe('when receiving a request for vote', () => {
      describe('and candidate term is equal to or lower than current term', () => {
        it('does not grant vote if other term is lower than candidate term', () => {
          const state = createNodeState({ currentTerm: 2 })
          const message = messages.requestVote({ term: 1, candidateId: 1, lastLogTerm: 0, lastLogIndex: 0 })

          const result = onMessage(state, message)

          if (becomeFollower.is(result)) {
            fail('expected result to not be becoming a follower')
          }

          expect(result.voteGranted).toBe(false)
        })

        it('does not grant vote if other term is equal to candidate term', () => {
          const state = createNodeState({ currentTerm: 2 })
          const message = messages.requestVote({ term: 2, candidateId: 1, lastLogTerm: 0, lastLogIndex: 0 })

          const result = onMessage(state, message)

          if (becomeFollower.is(result)) {
            fail('expected result to not be becoming a follower')
          }

          expect(result.voteGranted).toBe(false)
        })

        it('sets response term to current term', () => {
          const state = createNodeState({ currentTerm: 2 })
          const message = messages.requestVote({ term: 1, candidateId: 1, lastLogTerm: 0, lastLogIndex: 0 })

          const result = onMessage(state, message)

          if (becomeFollower.is(result)) {
            fail('expected result to not be becoming a follower')
          }

          expect(result.term).toBe(state.currentTerm)
        })
      })

      describe('and candidate term is higher than current term', () => {
        it('becomes a follower', () => {
          const state = createNodeState({ currentTerm: 2 })
          const message = messages.requestVote({ term: 3, candidateId: 1, lastLogTerm: 0, lastLogIndex: 0 })

          const result = onMessage(state, message)

          expect(result).toEqual(becomeFollower())
        })
      })
    })

    describe('when receiving a request to append entries', () => {
      describe('and leader term is lower than current term', () => {
        it('responds with success=false', () => {
          const state = createNodeState({ currentTerm: 2 })
          const message = messages.appendEntries({
            term: 1,
            entries: [],
            prevLogIndex: 0,
            prevLogTerm: 0,
            leaderId: 1,
            leaderCommit: 0,
          })

          const result = onMessage(state, message)

          if (becomeFollower.is(result)) {
            fail('expected result to not be becoming a follower')
          }

          expect(result.success).toBe(false)
        })

        it('sets response term to current term', () => {
          const state = createNodeState({ currentTerm: 2 })
          const message = messages.appendEntries({
            term: 1,
            entries: [],
            prevLogIndex: 0,
            prevLogTerm: 0,
            leaderId: 1,
            leaderCommit: 0,
          })

          const result = onMessage(state, message)

          if (becomeFollower.is(result)) {
            fail('expected result to not be becoming a follower')
          }

          expect(result.term).toBe(state.currentTerm)
        })
      })

      describe('and leader term is equal to or higher than current term', () => {
        it('becomes a follower if leader term is equal to current term', () => {
          const state = createNodeState({ currentTerm: 2 })
          const message = messages.appendEntries({
            term: 2,
            entries: [],
            prevLogIndex: 0,
            prevLogTerm: 0,
            leaderId: 1,
            leaderCommit: 0,
          })

          const result = onMessage(state, message)

          expect(result).toEqual(becomeFollower())
        })

        it('becomes a follower if leader term is higher than current term', () => {
          const state = createNodeState({ currentTerm: 2 })
          const message = messages.appendEntries({
            term: 3,
            entries: [],
            prevLogIndex: 0,
            prevLogTerm: 0,
            leaderId: 1,
            leaderCommit: 0,
          })

          const result = onMessage(state, message)

          expect(result).toEqual(becomeFollower())
        })
      })
    })

    describe('when receiving a client request', () => {
      it('returns an error to client for execution request', () => {
        const state = createNodeState({ currentTerm: 1 })
        const message = messages.executeCommand({ command: 1 })

        const result = onMessage(state, message)

        expect(result).toEqual(messages.noLeaderError())
      })

      it('returns an error to client for get state request', () => {
        const state = createNodeState({ currentTerm: 1 })
        const message = messages.getState()

        const result = onMessage(state, message)

        expect(result).toEqual(messages.noLeaderError())
      })
    })
  })

  const never = <T>() => new Promise<T>(() => void 0)
  const immediately = <T>(value?: T) => Promise.resolve(value as T)
  const sendMessageNever: SendRequestVote = () => never()

  // during some tasks we need to wait for some promise continuations to
  // be evaluated; to do this we return a promise that returns after a 0
  // timeout, which schedules an execution on the macrotask queue, and
  // therefore causes the microtask queue to be drained
  const drainMicrotaskQueue = () => new Promise((r) => setTimeout(r, 0))

  const nodeId = 1
  const otherNodeIds = [2, 3]

  describe(runElection.name, () => {
    it('sends a request for vote to all other nodes if log is empty', () => {
      const state = createNodeState({ currentTerm: 2 })

      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const requestVote = messages.requestVote({
        term: state.currentTerm,
        candidateId: nodeId,
        lastLogTerm: 0,
        lastLogIndex: 0,
      })

      const sentMessages: [NodeId, RequestVote][] = []

      const sendMessage: SendRequestVote = (nodeId, message) => {
        sentMessages.push([nodeId, message])
        return never()
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runElection(nodeId, state, otherNodeIds, waitForElectionTimeout, sendMessage, cancellationToken.never)

      expect(sentMessages).toEqual([
        [2, requestVote],
        [3, requestVote],
      ])
    })

    it('sends a request for vote to all other nodes if log has entries', () => {
      const state = createNodeState({
        currentTerm: 2,
        log: [
          { index: 1, command: 1, term: 1 },
          { index: 2, command: 2, term: 1 },
          { index: 3, command: 3, term: 2 },
        ],
      })

      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const requestVote = messages.requestVote({
        term: state.currentTerm,
        candidateId: nodeId,
        lastLogTerm: 2,
        lastLogIndex: 3,
      })

      const sentMessages: [NodeId, RequestVote][] = []

      const sendMessage: SendRequestVote = (nodeId, message) => {
        sentMessages.push([nodeId, message])
        return never()
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runElection(nodeId, state, otherNodeIds, waitForElectionTimeout, sendMessage, cancellationToken.never)

      expect(sentMessages).toEqual([
        [2, requestVote],
        [3, requestVote],
      ])
    })

    describe('when receiving a response with a term that is higher than current term', () => {
      it('becomes a follower', async () => {
        const state = createNodeState({ currentTerm: 2 })

        const waitForElectionTimeout = jest.fn().mockReturnValue(never())
        const requestVote = messages.requestVote({
          term: state.currentTerm,
          candidateId: nodeId,
          lastLogTerm: 0,
          lastLogIndex: 0,
        })

        const sendMessage: SendRequestVote = () => {
          return immediately(messages.requestVoteResponse({ term: requestVote.term + 1, voteGranted: false }))
        }

        const result = await runElection(
          nodeId,
          state,
          otherNodeIds,
          waitForElectionTimeout,
          sendMessage,
          cancellationToken.never,
        )

        expect(result).toEqual(becomeFollower())
      })
    })

    describe('when receiving votes from majority', () => {
      it('becomes leader', async () => {
        const state = createNodeState({ currentTerm: 2 })

        const waitForElectionTimeout = jest.fn().mockReturnValue(never())

        const sendMessage: SendRequestVote = (nodeId, message) =>
          immediately(messages.requestVoteResponse({ term: message.term, voteGranted: nodeId === 3 }))

        const result = await runElection(
          nodeId,
          state,
          otherNodeIds,
          waitForElectionTimeout,
          sendMessage,
          cancellationToken.never,
        )

        expect(result).toEqual(becomeLeader())
      })

      it('cancels in-flight operations', async () => {
        const state = createNodeState({ currentTerm: 2 })

        const waitForElectionTimeout = jest.fn().mockReturnValue(never())

        let token: CancellationToken | undefined

        const sendMessage: SendRequestVote = (nodeId, message, t) => {
          if (nodeId === 2) {
            token = t
            return never()
          }

          return immediately(messages.requestVoteResponse({ term: message.term, voteGranted: true }))
        }

        await runElection(nodeId, state, otherNodeIds, waitForElectionTimeout, sendMessage, cancellationToken.never)

        expect(token?.isCancelled).toBe(true)
      })
    })

    describe('when election times out', () => {
      it('signals timeout', async () => {
        const state = createNodeState({ currentTerm: 2 })

        const waitForElectionTimeout = jest.fn().mockReturnValue(immediately())

        const result = await runElection(
          nodeId,
          state,
          otherNodeIds,
          waitForElectionTimeout,
          sendMessageNever,
          cancellationToken.never,
        )

        expect(result).toBe('timeout')
      })

      it('cancels in-flight operations', async () => {
        const state = createNodeState({ currentTerm: 2 })

        const waitForElectionTimeout = jest.fn().mockReturnValue(immediately())
        const sendMessage = jest.fn().mockReturnValue(never())

        await runElection(nodeId, state, otherNodeIds, waitForElectionTimeout, sendMessage, cancellationToken.never)

        expect(sendMessage.mock.calls[0][2].isCancelled).toBe(true)
        expect(sendMessage.mock.calls[1][2].isCancelled).toBe(true)
      })
    })

    it('runs only a single election timeout', async () => {
      const state = createNodeState({ currentTerm: 2 })

      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const requestVote = messages.requestVote({
        term: state.currentTerm,
        candidateId: nodeId,
        lastLogTerm: 0,
        lastLogIndex: 0,
      })

      const sendMessage: SendRequestVote = (nodeId) =>
        immediately(messages.requestVoteResponse({ term: requestVote.term, voteGranted: nodeId === 3 }))

      await runElection(nodeId, state, otherNodeIds, waitForElectionTimeout, sendMessage, cancellationToken.never)

      expect(waitForElectionTimeout).toHaveBeenCalledTimes(1)
    })

    it('retries requests when they fail', async () => {
      const state = createNodeState({ currentTerm: 2 })

      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const requestVote = messages.requestVote({
        term: state.currentTerm,
        candidateId: nodeId,
        lastLogTerm: 0,
        lastLogIndex: 0,
      })

      const sendMessage = jest
        .fn()
        .mockReturnValueOnce(Promise.reject(new Error()))
        .mockReturnValueOnce(Promise.reject(new Error()))
        .mockReturnValue(immediately(messages.requestVoteResponse({ term: requestVote.term, voteGranted: true })))

      const result = await runElection(
        nodeId,
        state,
        otherNodeIds,
        waitForElectionTimeout,
        sendMessage,
        cancellationToken.never,
      )

      expect(result).toEqual(becomeLeader())
    })

    it('cancels in-flight operations when cancelled', () => {
      const state = createNodeState({ currentTerm: 2 })
      const waitForElectionTimeout = jest.fn().mockReturnValueOnce(never())

      const cancellationTokenSource = cancellationToken.createSource()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runElection(nodeId, state, otherNodeIds, waitForElectionTimeout, sendMessageNever, cancellationTokenSource.token)

      cancellationTokenSource.cancel()

      expect(waitForElectionTimeout.mock.calls[0][0].isCancelled).toBe(true)
    })
  })

  describe(runCandidate.name, () => {
    const messageWithResponseChannel = <T extends RaftMessage>(
      message: T,
      respond: ResponseChannel<unknown>,
      token = cancellationToken.never,
    ): MessageWithResponseChannel<T, unknown> => ({ message, respond, cancellationToken: token })

    it('increments current term', async () => {
      const state = createNodeState({ currentTerm: 2 })
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const waitForMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const sendMessage = jest
        .fn()
        .mockReturnValue(immediately(messages.requestVoteResponse({ term: state.currentTerm, voteGranted: true })))

      const [newState] = await runCandidate(
        nodeId,
        state,
        otherNodeIds,
        waitForElectionTimeout,
        waitForMessage,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(newState.currentTerm).toBe(state.currentTerm + 1)
      expect(onUpdateState).toHaveBeenCalledWith(expect.objectContaining({ currentTerm: state.currentTerm + 1 }))
    })

    it('forgets about any previous leader', async () => {
      const state = createNodeState({ currentTerm: 2, lastKnownLeaderId: 3 })
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const waitForMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const sendMessage = jest
        .fn()
        .mockReturnValue(immediately(messages.requestVoteResponse({ term: state.currentTerm, voteGranted: true })))

      const [newState] = await runCandidate(
        nodeId,
        state,
        otherNodeIds,
        waitForElectionTimeout,
        waitForMessage,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(newState.lastKnownLeaderId).toBeUndefined()
      expect(onUpdateState).toHaveBeenCalledWith(expect.objectContaining({ lastKnownLeaderId: undefined }))
    })

    it('runs an election', async () => {
      const state = createNodeState({ currentTerm: 2 })
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const waitForMessage = jest.fn().mockReturnValue(never())
      const sendMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runCandidate(
        nodeId,
        state,
        otherNodeIds,
        waitForElectionTimeout,
        waitForMessage,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(waitForElectionTimeout).toHaveBeenCalled()
      expect(sendMessage).toHaveBeenCalledTimes(otherNodeIds.length)
    })

    it('runs a new election if an election times out', async () => {
      const state = createNodeState({ currentTerm: 2 })
      const waitForElectionTimeout = jest.fn().mockReturnValueOnce(immediately()).mockReturnValue(never())
      const waitForMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const sendMessage = jest
        .fn()
        .mockReturnValueOnce(never())
        .mockReturnValueOnce(never())
        .mockReturnValue(immediately(messages.requestVoteResponse({ term: state.currentTerm, voteGranted: true })))

      await runCandidate(
        nodeId,
        state,
        otherNodeIds,
        waitForElectionTimeout,
        waitForMessage,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(waitForElectionTimeout).toHaveBeenCalledTimes(2)
      expect(sendMessage).toHaveBeenCalledTimes(otherNodeIds.length * 2)
    })

    it('increments current term again after election times out', async () => {
      const state = createNodeState({ currentTerm: 2 })
      const waitForElectionTimeout = jest.fn().mockReturnValueOnce(immediately()).mockReturnValue(never())
      const waitForMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const sendMessage = jest
        .fn()
        .mockReturnValueOnce(never())
        .mockReturnValueOnce(never())
        .mockReturnValue(immediately(messages.requestVoteResponse({ term: state.currentTerm, voteGranted: true })))

      const [newState] = await runCandidate(
        nodeId,
        state,
        otherNodeIds,
        waitForElectionTimeout,
        waitForMessage,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(newState.currentTerm).toBe(state.currentTerm + 2)
      expect(onUpdateState).toHaveBeenCalledWith(expect.objectContaining({ currentTerm: state.currentTerm + 2 }))
    })

    it('becomes a leader if it wins an election', async () => {
      const state = createNodeState({ currentTerm: 2 })
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const waitForMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const sendMessage = jest
        .fn()
        .mockReturnValue(immediately(messages.requestVoteResponse({ term: state.currentTerm, voteGranted: true })))

      const [, behaviorChange] = await runCandidate(
        nodeId,
        state,
        otherNodeIds,
        waitForElectionTimeout,
        waitForMessage,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(behaviorChange).toEqual(becomeLeader())
    })

    it('becomes a follower if it loses an election due to a vote response', async () => {
      const state = createNodeState({ currentTerm: 2 })
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const waitForMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const sendMessage = jest
        .fn()
        .mockReturnValue(immediately(messages.requestVoteResponse({ term: state.currentTerm + 2, voteGranted: false })))

      const [, behaviorChange] = await runCandidate(
        nodeId,
        state,
        otherNodeIds,
        waitForElectionTimeout,
        waitForMessage,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(behaviorChange).toEqual(becomeFollower())
    })

    it('becomes a follower if it receives a message from the new leader', async () => {
      const state = createNodeState({ currentTerm: 2 })
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const message = messages.appendEntries({
        term: state.currentTerm + 1,
        leaderId: 2,
        leaderCommit: 0,
        prevLogIndex: 0,
        prevLogTerm: 0,
        entries: [],
      })

      const messageWithChannel = messageWithResponseChannel(message, jest.fn())
      const waitForMessage = jest.fn().mockReturnValue(immediately(messageWithChannel))

      const [, behaviorChange] = await runCandidate(
        nodeId,
        state,
        otherNodeIds,
        waitForElectionTimeout,
        waitForMessage,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(behaviorChange).toEqual(becomeFollower({ triggeringMessage: messageWithChannel }))
    })

    it('returns an error for client requests', async () => {
      const state = createNodeState({ currentTerm: 2 })
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const message = messages.getState()
      const requestVote = messages.requestVote({ term: 99, candidateId: 2, lastLogIndex: 0, lastLogTerm: 0 })
      const respond = jest.fn()
      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, respond)))
        .mockReturnValue(immediately(messageWithResponseChannel(requestVote, jest.fn())))

      await runCandidate(
        nodeId,
        state,
        otherNodeIds,
        waitForElectionTimeout,
        waitForMessage,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(respond).toHaveBeenCalledWith(messages.noLeaderError())
    })

    it('can be cancelled', async () => {
      const state = createNodeState({ currentTerm: 2 })

      const waitForElectionTimeout = jest.fn().mockReturnValueOnce(never())
      const waitForMessage = jest.fn().mockReturnValue(never())
      const sendMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const cancellationTokenSource = cancellationToken.createSource()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runCandidate(
        nodeId,
        state,
        otherNodeIds,
        waitForElectionTimeout,
        waitForMessage,
        sendMessage,
        onUpdateState,
        cancellationTokenSource.token,
      )

      await drainMicrotaskQueue()

      cancellationTokenSource.cancel()

      expect(waitForElectionTimeout.mock.calls[0][0].isCancelled).toBe(true)
    })

    it('stops waiting for timeouts when behavior changes due to message', async () => {
      const state = createNodeState({ currentTerm: 2 })
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const requestVote = messages.requestVote({ term: 99, candidateId: 2, lastLogIndex: 0, lastLogTerm: 0 })
      const waitForMessage = jest.fn().mockReturnValue(immediately(messageWithResponseChannel(requestVote, jest.fn())))

      await runCandidate(
        nodeId,
        state,
        otherNodeIds,
        waitForElectionTimeout,
        waitForMessage,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(waitForElectionTimeout.mock.calls[0][0].isCancelled).toBe(true)
    })

    it('stops waiting for messages when behavior changes due to election', async () => {
      const state = createNodeState({ currentTerm: 2 })
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const sendMessage: SendRequestVote = (nodeId) =>
        immediately(messages.requestVoteResponse({ term: state.currentTerm + 1, voteGranted: nodeId === 3 }))

      const waitForMessage = jest.fn().mockReturnValue(never())

      await runCandidate(
        nodeId,
        state,
        otherNodeIds,
        waitForElectionTimeout,
        waitForMessage,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(waitForMessage.mock.calls[0][0].isCancelled).toBe(true)
    })
  })
})
