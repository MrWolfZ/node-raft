import { cancellationToken } from '../util/cancellation-token.js'
import {
  messages,
  MessageWithResponseChannel,
  RaftMessage,
  RequestVote,
  RequestVoteResponse,
  ResponseChannel,
} from './messages.js'
import { runNode } from './node.js'
import { createNodeState, noopStateMachine, StateMachine } from './state.js'
import type { NodeId } from './types.js'

describe(runNode.name, () => {
  const never = <T>() => new Promise<T>(() => void 0)
  const immediately = <T>(value?: T) => Promise.resolve(value as T)

  // during some tasks we need to wait for some promise continuations to
  // be evaluated; to do this we return a promise that returns after a 0
  // timeout, which schedules an execution on the macrotask queue, and
  // therefore causes the microtask queue to be drained
  const drainMicrotaskQueue = () => new Promise((r) => setTimeout(r, 0))

  const nodeId = 1
  const otherNodeIds = [2, 3]

  const messageWithResponseChannel = <T extends RaftMessage>(
    message: T,
    respond: ResponseChannel<unknown>,
    token = cancellationToken.never,
  ): MessageWithResponseChannel<T, unknown> => ({ message, respond, cancellationToken: token })

  describe('as a follower', () => {
    it('becomes a candidate after the election timeout', async () => {
      const state = createNodeState({})

      const waitForMessage = jest.fn().mockReturnValue(never())
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValueOnce(immediately()).mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())

      const sendAppendEntries = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        noopStateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(onChangeBehavior).toHaveBeenCalledWith('candidate')
    })

    it('votes for candidates', async () => {
      const state = createNodeState({})

      const message = messages.requestVote({ candidateId: 2, term: 1, lastLogTerm: 0, lastLogIndex: 1 })
      const respond = jest.fn()
      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, respond)))
        .mockReturnValue(never())

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())

      const sendAppendEntries = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        noopStateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(respond).toHaveBeenCalledWith(messages.requestVoteResponse({ term: message.term, voteGranted: true }))
    })

    it('applies committed commands to state machine', async () => {
      const state = createNodeState({ currentTerm: 1, log: [{ index: 1, term: 1, command: 1 }] })

      const message = messages.appendEntries({
        term: 1,
        entries: [],
        leaderId: 1,
        leaderCommit: 1,
        prevLogIndex: 1,
        prevLogTerm: 1,
      })

      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, jest.fn())))
        .mockReturnValue(never())

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())

      const sendAppendEntries = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      const applyCommand = jest.fn()
      const stateMachine: StateMachine<number, number> = {
        applyCommand,
        getState: () => 0,
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        stateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(applyCommand).toHaveBeenCalledWith(1)
    })

    it('redirects client requests to leader', async () => {
      const state = createNodeState({ currentTerm: 1, lastKnownLeaderId: 2 })

      const message = messages.getState()
      const respond = jest.fn()
      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, respond)))
        .mockReturnValue(never())

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())

      const sendAppendEntries = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        noopStateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(respond).toHaveBeenCalledWith(messages.redirectClient({ leaderId: state.lastKnownLeaderId || 0 }))
    })

    it('can be cancelled', async () => {
      const state = createNodeState({})

      const waitForMessage = jest.fn().mockReturnValue(never())
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())

      const sendAppendEntries = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      const { token, cancel } = cancellationToken.createSource()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        noopStateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        token,
      )

      await drainMicrotaskQueue()

      cancel()

      expect(waitForElectionTimeout.mock.calls[0][0].isCancelled).toBe(true)
    })
  })

  describe('as a candidate', () => {
    it('becomes a leader if it wins an election', async () => {
      const state = createNodeState({})

      const waitForMessage = jest.fn().mockReturnValue(never())
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest
        .fn<Promise<RequestVoteResponse>, [NodeId, RequestVote]>()
        .mockImplementation((_, message) =>
          immediately(messages.requestVoteResponse({ term: message.term, voteGranted: true })),
        )

      const sendAppendEntries = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        noopStateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        cancellationToken.never,
        'candidate',
      )

      await drainMicrotaskQueue()

      expect(onChangeBehavior).toHaveBeenCalledWith('leader')
    })

    it('becomes a follower if a different node becomes leader', async () => {
      const state = createNodeState({ currentTerm: 1 })

      const message = messages.appendEntries({
        term: state.currentTerm + 1,
        entries: [],
        leaderId: 1,
        leaderCommit: 1,
        prevLogIndex: 1,
        prevLogTerm: 1,
      })

      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, jest.fn())))
        .mockReturnValue(never())

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())
      const sendAppendEntries = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        noopStateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        cancellationToken.never,
        'candidate',
      )

      await drainMicrotaskQueue()

      expect(onChangeBehavior).toHaveBeenCalledWith('follower')
    })

    it('becomes a follower if a different candidate requests votes for higher term', async () => {
      const state = createNodeState({})

      const message = messages.requestVote({
        term: state.currentTerm + 2,
        candidateId: 2,
        lastLogIndex: 0,
        lastLogTerm: 0,
      })

      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, jest.fn())))
        .mockReturnValue(never())

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())
      const sendAppendEntries = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        noopStateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        cancellationToken.never,
        'candidate',
      )

      await drainMicrotaskQueue()

      expect(onChangeBehavior).toHaveBeenCalledWith('follower')
    })

    it('processes message as follower after becoming follower', async () => {
      const state = createNodeState({})

      const message = messages.requestVote({
        term: state.currentTerm + 2,
        candidateId: 2,
        lastLogIndex: 0,
        lastLogTerm: 0,
      })

      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, jest.fn())))
        .mockReturnValue(never())

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())
      const sendAppendEntries = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        noopStateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        cancellationToken.never,
        'candidate',
      )

      await drainMicrotaskQueue()

      expect(onUpdateState).toHaveBeenCalledWith(expect.objectContaining({ currentTerm: message.term }))
    })

    it('can be cancelled', async () => {
      const state = createNodeState({})

      const waitForMessage = jest.fn().mockReturnValue(never())
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())

      const sendAppendEntries = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      const { token, cancel } = cancellationToken.createSource()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        noopStateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        token,
        'candidate',
      )

      await drainMicrotaskQueue()

      cancel()

      expect(waitForElectionTimeout.mock.calls[0][0].isCancelled).toBe(true)
    })

    it('becomes a leader if it is the only node in the cluster', async () => {
      const state = createNodeState({})

      const waitForMessage = jest.fn().mockReturnValue(never())
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())

      const sendAppendEntries = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        [],
        state,
        noopStateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        cancellationToken.never,
        'candidate',
      )

      await drainMicrotaskQueue()

      expect(onChangeBehavior).toHaveBeenCalledWith('leader')
    })
  })

  describe('as a leader', () => {
    it('sends heartbeats to followers', async () => {
      const state = createNodeState({ currentTerm: 1 })

      const waitForMessage = jest.fn().mockReturnValue(never())
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())
      const sendAppendEntries = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      const heartbeat = messages.appendEntries({
        term: state.currentTerm,
        entries: [],
        leaderId: nodeId,
        leaderCommit: 0,
        prevLogTerm: 0,
        prevLogIndex: 0,
      })

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        noopStateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        cancellationToken.never,
        'leader',
      )

      await drainMicrotaskQueue()

      expect(sendAppendEntries).toHaveBeenCalledWith(2, heartbeat, expect.anything())
      expect(sendAppendEntries).toHaveBeenCalledWith(3, heartbeat, expect.anything())
    })

    it('sends new entries to followers', async () => {
      const state = createNodeState({ currentTerm: 1 })

      const message = messages.executeCommand({ command: 3 })
      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, jest.fn())))
        .mockReturnValue(never())

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())

      const appendEntriesResponse = messages.appendEntriesResponse({ term: state.currentTerm, success: true })
      const sendAppendEntries = jest.fn().mockReturnValue(immediately(appendEntriesResponse))

      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        noopStateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        cancellationToken.never,
        'leader',
      )

      await drainMicrotaskQueue()

      expect(sendAppendEntries).toHaveBeenCalledWith(
        2,
        expect.objectContaining({
          entries: expect.arrayContaining([expect.objectContaining({ command: message.command })]),
        }),
        expect.anything(),
      )
    })

    it('applies committed commands to state machine', async () => {
      const state = createNodeState({ currentTerm: 1 })

      const message = messages.executeCommand({ command: 3 })
      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, jest.fn())))
        .mockReturnValue(never())

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())

      const appendEntriesResponse = messages.appendEntriesResponse({ term: state.currentTerm, success: true })
      const sendAppendEntries = jest.fn().mockReturnValue(immediately(appendEntriesResponse))

      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      const applyCommand = jest.fn()
      const stateMachine: StateMachine<number, number> = {
        applyCommand,
        getState: () => 0,
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        stateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        cancellationToken.never,
        'leader',
      )

      await drainMicrotaskQueue()

      expect(applyCommand).toHaveBeenCalledWith(message.command)
    })

    it('responds to client execution request after committing command', async () => {
      const state = createNodeState({ currentTerm: 1 })

      const message = messages.executeCommand({ command: 3 })
      const respond = jest.fn()
      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, respond)))
        .mockReturnValue(never())

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())

      const appendEntriesResponse = messages.appendEntriesResponse({ term: state.currentTerm, success: true })
      const sendAppendEntries = jest.fn().mockReturnValue(immediately(appendEntriesResponse))

      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        noopStateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        cancellationToken.never,
        'leader',
      )

      await drainMicrotaskQueue()

      expect(respond).toHaveBeenCalledWith(messages.executeCommandResponse())
    })

    it('responds to client get request after verifying leadership', async () => {
      const state = createNodeState({ currentTerm: 1 })

      const message = messages.getState()
      const respond = jest.fn()
      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, respond)))
        .mockReturnValue(never())

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())

      const appendEntriesResponse = messages.appendEntriesResponse({ term: state.currentTerm, success: true })
      const sendAppendEntries = jest.fn().mockReturnValue(immediately(appendEntriesResponse))

      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      const getState = jest.fn().mockReturnValue(3)
      const stateMachine: StateMachine<number, number> = {
        applyCommand: () => void 0,
        getState,
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        stateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        cancellationToken.never,
        'leader',
      )

      await drainMicrotaskQueue()

      expect(respond).toHaveBeenCalledWith(messages.getStateResponse({ state: 3 }))
    })

    it('becomes a follower if a different candidate requests votes for higher term', async () => {
      const state = createNodeState({})

      const message = messages.requestVote({
        term: state.currentTerm + 2,
        candidateId: 2,
        lastLogIndex: 0,
        lastLogTerm: 0,
      })

      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, jest.fn())))
        .mockReturnValue(never())

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())
      const sendAppendEntries = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        noopStateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        cancellationToken.never,
        'leader',
      )

      await drainMicrotaskQueue()

      expect(onChangeBehavior).toHaveBeenCalledWith('follower')
    })

    it('processes message as follower after becoming follower', async () => {
      const state = createNodeState({})

      const message = messages.requestVote({
        term: state.currentTerm + 1,
        candidateId: 2,
        lastLogIndex: 0,
        lastLogTerm: 0,
      })

      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, jest.fn())))
        .mockReturnValue(never())

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())
      const sendAppendEntries = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        noopStateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        cancellationToken.never,
        'leader',
      )

      await drainMicrotaskQueue()

      expect(onUpdateState).toHaveBeenCalledWith(expect.objectContaining({ currentTerm: message.term }))
    })

    it('can be cancelled', async () => {
      const state = createNodeState({})

      const waitForMessage = jest.fn().mockReturnValue(never())
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForElectionTimeout = jest.fn().mockReturnValue(never())
      const sendRequestVote = jest.fn().mockReturnValue(never())

      const sendAppendEntries = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const onChangeBehavior = jest.fn()

      const { token, cancel } = cancellationToken.createSource()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runNode(
        nodeId,
        otherNodeIds,
        state,
        noopStateMachine,
        waitForMessage,
        waitForHeartbeatInterval,
        waitForElectionTimeout,
        sendRequestVote,
        sendAppendEntries,
        onUpdateState,
        onChangeBehavior,
        token,
        'leader',
      )

      await drainMicrotaskQueue()

      cancel()

      expect(waitForMessage.mock.calls[0][0].isCancelled).toBe(true)
    })
  })
})
