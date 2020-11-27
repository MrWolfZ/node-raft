import { cancellationToken } from '../../util/cancellation-token.js'
import {
  AppendEntries,
  messages,
  MessageWithResponseChannel,
  RaftMessage,
  ResponseChannel,
  SendAppendEntries,
} from '../messages.js'
import { createNodeState, LogEntry, noopStateMachine, NOOP_COMMAND, StateMachine } from '../state.js'
import type { NodeId } from '../types.js'
import { becomeFollower } from './behavior.js'
import {
  getCommittedStateMachineState,
  LogReplicationState,
  onMessage,
  runLeader,
  runLogReplication,
} from './leader.js'

describe('leader', () => {
  describe(onMessage.name, () => {
    describe('when receiving a request for vote', () => {
      describe('and candidate term is equal to or lower than current term', () => {
        it('does not grant vote if other term is lower than candidate term', () => {
          const state = createNodeState({ currentTerm: 2 })
          const message = messages.requestVote({ term: 1, candidateId: 1, lastLogTerm: 0, lastLogIndex: 0 })

          const result = onMessage(state, message)

          if (!Array.isArray(result)) {
            fail('expected result to be an array')
          }

          const [newState, response] = result

          expect(response.voteGranted).toBe(false)
          expect(newState).toBe(state)
        })

        it('does not grant vote if other term is equal to candidate term', () => {
          const state = createNodeState({ currentTerm: 2 })
          const message = messages.requestVote({ term: 2, candidateId: 1, lastLogTerm: 0, lastLogIndex: 0 })

          const result = onMessage(state, message)

          if (!Array.isArray(result)) {
            fail('expected result to be an array')
          }

          const [newState, response] = result

          expect(response.voteGranted).toBe(false)
          expect(newState).toBe(state)
        })

        it('sets response term to current term', () => {
          const state = createNodeState({ currentTerm: 2 })
          const message = messages.requestVote({ term: 1, candidateId: 1, lastLogTerm: 0, lastLogIndex: 0 })

          const result = onMessage(state, message)

          if (!Array.isArray(result)) {
            fail('expected result to be an array')
          }

          const [, response] = result

          expect(response.term).toBe(state.currentTerm)
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
      describe('and new leader term is equal to or lower than current term', () => {
        it('responds with success=false when new leader term is equal to current term', () => {
          const state = createNodeState({ currentTerm: 2 })
          const message = messages.appendEntries({
            term: 2,
            entries: [],
            prevLogIndex: 0,
            prevLogTerm: 0,
            leaderId: 1,
            leaderCommit: state.commitIndex,
          })

          const result = onMessage(state, message)

          if (!Array.isArray(result)) {
            fail('expected result to be an array')
          }

          const [newState, response] = result

          expect(response.success).toBe(false)
          expect(newState).toBe(state)
        })

        it('responds with success=false when new leader term is lower than current term', () => {
          const state = createNodeState({ currentTerm: 2 })
          const message = messages.appendEntries({
            term: 1,
            entries: [],
            prevLogIndex: 0,
            prevLogTerm: 0,
            leaderId: 1,
            leaderCommit: state.commitIndex,
          })

          const result = onMessage(state, message)

          if (!Array.isArray(result)) {
            fail('expected result to be an array')
          }

          const [newState, response] = result

          expect(response.success).toBe(false)
          expect(newState).toBe(state)
        })

        it('sets response term to current term', () => {
          const state = createNodeState({ currentTerm: 2 })
          const message = messages.appendEntries({
            term: 1,
            entries: [],
            prevLogIndex: 0,
            prevLogTerm: 0,
            leaderId: 1,
            leaderCommit: state.commitIndex,
          })

          const result = onMessage(state, message)

          if (!Array.isArray(result)) {
            fail('expected result to be an array')
          }

          const [newState, response] = result

          expect(response.term).toBe(state.currentTerm)
          expect(newState).toBe(state)
        })
      })

      describe('and new leader term is higher than current term', () => {
        it('becomes a follower', () => {
          const state = createNodeState({ currentTerm: 2 })
          const message = messages.appendEntries({
            term: 3,
            entries: [],
            prevLogIndex: 0,
            prevLogTerm: 0,
            leaderId: 1,
            leaderCommit: state.commitIndex,
          })

          const result = onMessage(state, message)

          expect(result).toEqual(becomeFollower())
        })
      })
    })

    describe('when receiving a client request to execute command', () => {
      it('it appends a log entry to the local log', () => {
        const state = createNodeState({ currentTerm: 1, log: [{ index: 1, term: 1, command: 1 }] })
        const message = messages.executeCommand({ command: 2 })

        const result = onMessage(state, message)

        if (!Array.isArray(result)) {
          fail('expected result to be an array')
        }

        const [newState] = result
        const lastLogEntry = newState.log[newState.log.length - 1]

        expect(newState.log.length).toBe(state.log.length + 1)
        expect(lastLogEntry).toEqual({ index: newState.log.length, term: state.currentTerm, command: message.command })
      })

      it('it notifies log replication', () => {
        const state = createNodeState({ currentTerm: 1, log: [{ index: 1, term: 1, command: 1 }] })
        const message = messages.executeCommand({ command: 2 })

        const result = onMessage(state, message)

        if (!Array.isArray(result)) {
          fail('expected result to be an array')
        }

        const [, response] = result

        expect(response).toEqual(messages.waitForLogReplication({ logIndex: state.log.length + 1 }))
      })
    })

    describe('when receiving a client request to get state', () => {
      it('it notifies to fetch committed state machine state', () => {
        const state = createNodeState({ currentTerm: 1, log: [{ index: 1, term: 1, command: 1 }] })
        const message = messages.getState()

        const result = onMessage(state, message)

        if (!Array.isArray(result)) {
          fail('expected result to be an array')
        }

        const [, response] = result

        expect(response).toEqual(messages.getCommittedState())
      })
    })
  })

  const never = <T>() => new Promise<T>(() => void 0)
  const immediately = <T>(value?: T) => Promise.resolve(value as T)

  // during some tasks we need to wait for some promise continuations to
  // be evaluated; to do this we return a promise that returns after a 0
  // timeout, which schedules an execution on the macrotask queue, and
  // therefore causes the microtask queue to be drained
  const drainMicrotaskQueue = () => new Promise((r) => setTimeout(r, 0))

  const nodeId = 1
  const otherNodeIds = [2, 3]

  describe(runLogReplication.name, () => {
    let unhandledRejections: unknown[] = []

    function onUnhandledRejection(err: unknown) {
      unhandledRejections.push(err)
    }

    beforeEach(() => {
      unhandledRejections = []
      process.on('unhandledRejection', onUnhandledRejection)
    })

    afterEach(() => process.off('unhandledRejection', onUnhandledRejection))

    it('sends heartbeats immediately to all other nodes', async () => {
      const state = createNodeState({ currentTerm: 2, commitIndex: 1, log: [{ index: 1, term: 1, command: 1 }] })

      const setCommitIndex = jest.fn()
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())

      const heartbeat = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [],
        leaderCommit: 1,
        prevLogTerm: 1,
        prevLogIndex: 1,
      })

      const sentMessages: [NodeId, AppendEntries][] = []

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        sentMessages.push([nodeId, message])
        return never()
      }

      runLogReplication(
        nodeId,
        () => state,
        otherNodeIds,
        setCommitIndex,
        waitForHeartbeatInterval,
        sendMessage,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(sentMessages).toEqual([
        [2, heartbeat],
        [3, heartbeat],
      ])
    })

    it('sends heartbeats immediately to all other nodes if log is empty', () => {
      const state = createNodeState({ currentTerm: 2 })

      const setCommitIndex = jest.fn()
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())

      const heartbeat = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [],
        leaderCommit: state.commitIndex,
        prevLogTerm: 0,
        prevLogIndex: 0,
      })

      const sentMessages: [NodeId, AppendEntries][] = []

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        sentMessages.push([nodeId, message])
        return never()
      }

      runLogReplication(
        nodeId,
        () => state,
        otherNodeIds,
        setCommitIndex,
        waitForHeartbeatInterval,
        sendMessage,
        cancellationToken.never,
      )

      expect(sentMessages).toEqual([
        [2, heartbeat],
        [3, heartbeat],
      ])
    })

    it('sends heartbeats to all other nodes after heartbeat interval elapses', async () => {
      const state = createNodeState({ currentTerm: 2, commitIndex: 1, log: [{ index: 1, term: 1, command: 1 }] })

      const setCommitIndex = jest.fn()
      const waitForHeartbeatInterval = jest
        .fn()
        .mockReturnValueOnce(immediately())
        .mockReturnValueOnce(immediately())
        .mockReturnValue(never())

      const heartbeat = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [],
        leaderCommit: 1,
        prevLogTerm: 1,
        prevLogIndex: 1,
      })

      const sentMessages: [NodeId, AppendEntries][] = []

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        sentMessages.push([nodeId, message])
        return immediately(messages.appendEntriesResponse({ term: message.term, success: true }))
      }

      runLogReplication(
        nodeId,
        () => state,
        otherNodeIds,
        setCommitIndex,
        waitForHeartbeatInterval,
        sendMessage,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(sentMessages).toEqual([
        [2, heartbeat],
        [3, heartbeat],
        [2, heartbeat],
        [3, heartbeat],
      ])
    })

    it('sends heartbeat to single node after heartbeat interval elapses for it', async () => {
      const state = createNodeState({ currentTerm: 2, commitIndex: 1, log: [{ index: 1, term: 1, command: 1 }] })

      const setCommitIndex = jest.fn()
      const waitForHeartbeatInterval = jest.fn().mockReturnValueOnce(immediately()).mockReturnValue(never())

      const heartbeat = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [],
        leaderCommit: 1,
        prevLogTerm: 1,
        prevLogIndex: 1,
      })

      const sentMessages: [NodeId, AppendEntries][] = []

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        sentMessages.push([nodeId, message])
        return immediately(messages.appendEntriesResponse({ term: message.term, success: true }))
      }

      runLogReplication(
        nodeId,
        () => state,
        otherNodeIds,
        setCommitIndex,
        waitForHeartbeatInterval,
        sendMessage,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(sentMessages).toEqual([
        [2, heartbeat],
        [3, heartbeat],
        [2, heartbeat],
      ])
    })

    it('sends heartbeats with latest state', async () => {
      let state = createNodeState({ currentTerm: 2, commitIndex: 0, log: [{ index: 1, term: 1, command: 1 }] })

      const setCommitIndex = jest.fn()
      const waitForHeartbeatInterval = jest
        .fn()
        .mockReturnValueOnce(immediately())
        .mockReturnValueOnce(immediately())
        .mockReturnValue(never())

      const getState = jest.fn().mockImplementation(() => state)

      const heartbeat = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [],
        leaderCommit: state.commitIndex,
        prevLogTerm: 1,
        prevLogIndex: 1,
      })

      const heartbeat2: typeof heartbeat = { ...heartbeat, leaderCommit: 1 }

      const sentMessages: [NodeId, AppendEntries][] = []

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        sentMessages.push([nodeId, message])

        if (nodeId === 3) {
          state = { ...state, commitIndex: 1 }
        }

        return immediately(messages.appendEntriesResponse({ term: message.term, success: true }))
      }

      runLogReplication(
        nodeId,
        getState,
        otherNodeIds,
        setCommitIndex,
        waitForHeartbeatInterval,
        sendMessage,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(sentMessages).toEqual([
        [2, heartbeat],
        [3, heartbeat],
        [2, heartbeat2],
        [3, heartbeat2],
      ])
    })

    it('does not slow down heartbeats due to slow follower', async () => {
      const state = createNodeState({ currentTerm: 2, commitIndex: 1, log: [{ index: 1, term: 1, command: 1 }] })

      const setCommitIndex = jest.fn()
      const waitForHeartbeatInterval = jest.fn().mockReturnValueOnce(immediately()).mockReturnValue(never())

      const heartbeat = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [],
        leaderCommit: 1,
        prevLogTerm: 1,
        prevLogIndex: 1,
      })

      const sentMessages: [NodeId, AppendEntries][] = []

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        sentMessages.push([nodeId, message])
        return nodeId === 2
          ? immediately(messages.appendEntriesResponse({ term: state.currentTerm, success: true }))
          : never()
      }

      runLogReplication(
        nodeId,
        () => state,
        otherNodeIds,
        setCommitIndex,
        waitForHeartbeatInterval,
        sendMessage,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(sentMessages).toEqual([
        [2, heartbeat],
        [3, heartbeat],
        [2, heartbeat],
      ])
    })

    describe('when receiving a heartbeat response with a term that is higher than current term', () => {
      it('becomes a follower', async () => {
        const state = createNodeState({ currentTerm: 2, commitIndex: 1, log: [{ index: 1, term: 1, command: 1 }] })

        const setCommitIndex = jest.fn()
        const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())

        const sendMessage: SendAppendEntries = () =>
          immediately(messages.appendEntriesResponse({ term: state.currentTerm + 1, success: false }))

        const { promise } = runLogReplication(
          nodeId,
          () => state,
          otherNodeIds,
          setCommitIndex,
          waitForHeartbeatInterval,
          sendMessage,
          cancellationToken.never,
        )

        const result = await promise

        expect(result).toEqual(becomeFollower())
      })
    })

    it('ignores errors while sending heartbeat', async () => {
      const state = createNodeState({ currentTerm: 2, commitIndex: 1, log: [{ index: 1, term: 1, command: 1 }] })

      const setCommitIndex = jest.fn()
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())

      const sendMessage: SendAppendEntries = (nodeId) => {
        if (nodeId === 2) {
          return Promise.reject(new Error())
        }

        return immediately(messages.appendEntriesResponse({ term: state.currentTerm, success: true }))
      }

      runLogReplication(
        nodeId,
        () => state,
        otherNodeIds,
        setCommitIndex,
        waitForHeartbeatInterval,
        sendMessage,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(unhandledRejections.length).toBe(0)
      expect(waitForHeartbeatInterval.mock.calls[0][0].isCancelled).toBe(false)
    })

    it('sends single entry to single follower whose nextIndex <= last log index', () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, commitIndex: 0, log: [entry] })

      const logReplicationState: LogReplicationState = {
        2: {
          nextIndex: 1,
          matchIndex: 0,
        },
        3: {
          nextIndex: 2,
          matchIndex: 0,
        },
      }

      const setCommitIndex = jest.fn()
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())

      const message = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [entry],
        leaderCommit: state.commitIndex,
        prevLogTerm: 0,
        prevLogIndex: 0,
      })

      const sentMessages: [NodeId, AppendEntries][] = []

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        if (message.entries.length > 0) {
          sentMessages.push([nodeId, message])
        }

        return never()
      }

      runLogReplication(
        nodeId,
        () => state,
        otherNodeIds,
        setCommitIndex,
        waitForHeartbeatInterval,
        sendMessage,
        cancellationToken.never,
        logReplicationState,
      )

      expect(sentMessages).toEqual([[2, message]])
    })

    it('sends multiple entries to single follower whose nextIndex <= last log index', () => {
      const entry1: LogEntry = { index: 1, term: 1, command: 1 }
      const entry2: LogEntry = { index: 2, term: 2, command: 2 }
      const state = createNodeState({ currentTerm: 2, commitIndex: 0, log: [entry1, entry2] })

      const logReplicationState: LogReplicationState = {
        2: {
          nextIndex: 1,
          matchIndex: 0,
        },
        3: {
          nextIndex: 3,
          matchIndex: 0,
        },
      }

      const setCommitIndex = jest.fn()
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())

      const message = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [entry1, entry2],
        leaderCommit: state.commitIndex,
        prevLogTerm: 0,
        prevLogIndex: 0,
      })

      const sentMessages: [NodeId, AppendEntries][] = []

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        if (message.entries.length > 0) {
          sentMessages.push([nodeId, message])
        }

        return never()
      }

      runLogReplication(
        nodeId,
        () => state,
        otherNodeIds,
        setCommitIndex,
        waitForHeartbeatInterval,
        sendMessage,
        cancellationToken.never,
        logReplicationState,
      )

      expect(sentMessages).toEqual([[2, message]])
    })

    it('sends single entry to all followers whose nextIndex <= last log index', () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, commitIndex: 0, log: [entry] })

      const logReplicationState: LogReplicationState = {
        2: {
          nextIndex: 1,
          matchIndex: 0,
        },
        3: {
          nextIndex: 1,
          matchIndex: 0,
        },
      }

      const setCommitIndex = jest.fn()
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())

      const message = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [entry],
        leaderCommit: state.commitIndex,
        prevLogTerm: 0,
        prevLogIndex: 0,
      })

      const sentMessages: [NodeId, AppendEntries][] = []

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        if (message.entries.length > 0) {
          sentMessages.push([nodeId, message])
        }

        return never()
      }

      runLogReplication(
        nodeId,
        () => state,
        otherNodeIds,
        setCommitIndex,
        waitForHeartbeatInterval,
        sendMessage,
        cancellationToken.never,
        logReplicationState,
      )

      expect(sentMessages).toEqual([
        [2, message],
        [3, message],
      ])
    })

    it('sends multiple entries to all followers based on their nextIndex', () => {
      const entry1: LogEntry = { index: 1, term: 1, command: 1 }
      const entry2: LogEntry = { index: 2, term: 1, command: 2 }
      const entry3: LogEntry = { index: 3, term: 2, command: 3 }
      const state = createNodeState({ currentTerm: 2, commitIndex: 1, log: [entry1, entry2, entry3] })

      const logReplicationState: LogReplicationState = {
        2: {
          nextIndex: 1,
          matchIndex: 0,
        },
        3: {
          nextIndex: 2,
          matchIndex: 0,
        },
      }

      const setCommitIndex = jest.fn()
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())

      const messageTo2 = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [entry1, entry2, entry3],
        leaderCommit: 1,
        prevLogTerm: 0,
        prevLogIndex: 0,
      })

      const messageTo3 = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [entry2, entry3],
        leaderCommit: 1,
        prevLogTerm: 1,
        prevLogIndex: 1,
      })

      const sentMessages: [NodeId, AppendEntries][] = []

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        if (message.entries.length > 0) {
          sentMessages.push([nodeId, message])
        }

        return never()
      }

      runLogReplication(
        nodeId,
        () => state,
        otherNodeIds,
        setCommitIndex,
        waitForHeartbeatInterval,
        sendMessage,
        cancellationToken.never,
        logReplicationState,
      )

      expect(sentMessages).toEqual([
        [2, messageTo2],
        [3, messageTo3],
      ])
    })

    it('waits for new entries to be added', async () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const newEntry: LogEntry = { index: 2, term: 2, command: 2 }
      const state = createNodeState({ currentTerm: 2, commitIndex: 1, log: [entry] })

      const logReplicationState: LogReplicationState = {
        2: {
          nextIndex: 2,
          matchIndex: 0,
        },
        3: {
          nextIndex: 2,
          matchIndex: 0,
        },
      }

      const setCommitIndex = jest.fn()
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())

      const message = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [newEntry],
        leaderCommit: state.commitIndex,
        prevLogTerm: entry.term,
        prevLogIndex: entry.index,
      })

      const sentMessages: [NodeId, AppendEntries][] = []

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        if (message.entries.length > 0) {
          sentMessages.push([nodeId, message])
        }

        return immediately(messages.appendEntriesResponse({ term: message.term, success: true }))
      }

      const { onNewLogEntry } = runLogReplication(
        nodeId,
        () => state,
        otherNodeIds,
        setCommitIndex,
        waitForHeartbeatInterval,
        sendMessage,
        cancellationToken.never,
        logReplicationState,
      )

      state.log.push(newEntry)

      onNewLogEntry()

      await drainMicrotaskQueue()

      expect(sentMessages).toEqual([
        [2, message],
        [3, message],
      ])
    })

    it('can handle new entries being added while replication is in progress', async () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const newEntry: LogEntry = { index: 2, term: 2, command: 2 }
      const state = createNodeState({ currentTerm: 2, commitIndex: 1, log: [entry] })

      const logReplicationState: LogReplicationState = {
        2: {
          nextIndex: 1,
          matchIndex: 0,
        },
        3: {
          nextIndex: 2,
          matchIndex: 0,
        },
      }

      const setCommitIndex = jest.fn()
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())

      const heartbeat = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [],
        leaderCommit: state.commitIndex,
        prevLogTerm: 1,
        prevLogIndex: 1,
      })

      const message1 = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [entry],
        leaderCommit: state.commitIndex,
        prevLogTerm: 0,
        prevLogIndex: 0,
      })

      const message2 = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [newEntry],
        leaderCommit: state.commitIndex,
        prevLogTerm: entry.term,
        prevLogIndex: entry.index,
      })

      const sentMessages: [NodeId, AppendEntries][] = []

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        sentMessages.push([nodeId, message])
        return immediately(messages.appendEntriesResponse({ term: message.term, success: true }))
      }

      const { onNewLogEntry } = runLogReplication(
        nodeId,
        () => state,
        otherNodeIds,
        setCommitIndex,
        waitForHeartbeatInterval,
        sendMessage,
        cancellationToken.never,
        logReplicationState,
      )

      state.log.push(newEntry)

      onNewLogEntry()

      await drainMicrotaskQueue()

      expect(sentMessages).toEqual([
        [2, message1],
        [3, heartbeat],
        [3, message2],
        [2, message2],
      ])
    })

    it('handles failures when sending entries gracefully', async () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, commitIndex: 0, log: [entry] })

      const logReplicationState: LogReplicationState = {
        2: {
          nextIndex: 1,
          matchIndex: 0,
        },
        3: {
          nextIndex: 2,
          matchIndex: 0,
        },
      }

      const setCommitIndex = jest.fn()
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())

      const message = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [entry],
        leaderCommit: state.commitIndex,
        prevLogTerm: 0,
        prevLogIndex: 0,
      })

      const sentMessages: [NodeId, AppendEntries][] = []
      let hasThrown = false

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        if (!hasThrown) {
          hasThrown = true
          return Promise.reject(new Error())
        }

        if (message.entries.length > 0) {
          sentMessages.push([nodeId, message])
        }

        return never()
      }

      runLogReplication(
        nodeId,
        () => state,
        otherNodeIds,
        setCommitIndex,
        waitForHeartbeatInterval,
        sendMessage,
        cancellationToken.never,
        logReplicationState,
      )

      await drainMicrotaskQueue()

      expect(sentMessages).toEqual([[2, message]])
    })

    describe('when receiving a response with a term that is higher than current term', () => {
      it('becomes a follower', async () => {
        const entry: LogEntry = { index: 1, term: 1, command: 1 }
        const state = createNodeState({ currentTerm: 2, commitIndex: 0, log: [entry] })

        const logReplicationState: LogReplicationState = {
          2: {
            nextIndex: 1,
            matchIndex: 0,
          },
          3: {
            nextIndex: 2,
            matchIndex: 0,
          },
        }

        const setCommitIndex = jest.fn()
        const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())

        const sentMessages: [NodeId, AppendEntries][] = []

        const sendMessage: SendAppendEntries = (nodeId, message) => {
          if (message.entries.length > 0) {
            sentMessages.push([nodeId, message])
            return immediately(messages.appendEntriesResponse({ term: message.term + 1, success: false }))
          }

          return never()
        }

        const { promise } = runLogReplication(
          nodeId,
          () => state,
          otherNodeIds,
          setCommitIndex,
          waitForHeartbeatInterval,
          sendMessage,
          cancellationToken.never,
          logReplicationState,
        )

        const result = await promise

        expect(result).toEqual(becomeFollower())
      })
    })

    it('notifies of new committed index for single new entry once majority is reached', async () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, commitIndex: 0, log: [entry] })

      const logReplicationState: LogReplicationState = {
        2: {
          nextIndex: 1,
          matchIndex: 0,
        },
        3: {
          nextIndex: 1,
          matchIndex: 0,
        },
      }

      const setCommitIndex = jest.fn()
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())

      const sendMessage: SendAppendEntries = (nodeId, message) =>
        nodeId === 2 ? immediately(messages.appendEntriesResponse({ term: message.term, success: true })) : never()

      runLogReplication(
        nodeId,
        () => state,
        otherNodeIds,
        setCommitIndex,
        waitForHeartbeatInterval,
        sendMessage,
        cancellationToken.never,
        logReplicationState,
      )

      await drainMicrotaskQueue()

      expect(setCommitIndex).toHaveBeenCalledTimes(1)
      expect(setCommitIndex).toHaveBeenCalledWith(entry.index)
    })

    it('notifies of new committed index for multiple new entries once majority is reached', async () => {
      const entry1: LogEntry = { index: 1, term: 1, command: 1 }
      const entry2: LogEntry = { index: 2, term: 2, command: 2 }
      const state = createNodeState({ currentTerm: 2, commitIndex: 0, log: [entry1, entry2] })

      const logReplicationState: LogReplicationState = {
        2: {
          nextIndex: 1,
          matchIndex: 0,
        },
        3: {
          nextIndex: 1,
          matchIndex: 0,
        },
        4: {
          nextIndex: 1,
          matchIndex: 0,
        },
        5: {
          nextIndex: 2,
          matchIndex: 1,
        },
      }

      const setCommitIndex = jest.fn()
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())

      const sendMessage: SendAppendEntries = (nodeId, message) =>
        nodeId === 2 || nodeId === 4
          ? immediately(messages.appendEntriesResponse({ term: message.term, success: true }))
          : never()

      runLogReplication(
        nodeId,
        () => state,
        [2, 3, 4, 5],
        setCommitIndex,
        waitForHeartbeatInterval,
        sendMessage,
        cancellationToken.never,
        logReplicationState,
      )

      await drainMicrotaskQueue()

      expect(setCommitIndex).toHaveBeenCalledTimes(2)
      expect(setCommitIndex).toHaveBeenCalledWith(entry1.index)
      expect(setCommitIndex).toHaveBeenCalledWith(entry2.index)
    })

    describe('when conflict occurs', () => {
      it('tries again with decremented next index', async () => {
        const entry1: LogEntry = { index: 1, term: 1, command: 1 }
        const entry2: LogEntry = { index: 2, term: 2, command: 2 }
        const state = createNodeState({ currentTerm: 2, commitIndex: 0, log: [entry1, entry2] })

        const logReplicationState: LogReplicationState = {
          2: {
            nextIndex: 2,
            matchIndex: 0,
          },
          3: {
            nextIndex: 3,
            matchIndex: 0,
          },
        }

        const setCommitIndex = jest.fn()
        const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())

        const message1 = messages.appendEntries({
          term: state.currentTerm,
          leaderId: nodeId,
          entries: [entry2],
          leaderCommit: state.commitIndex,
          prevLogTerm: 1,
          prevLogIndex: 1,
        })

        const message2 = messages.appendEntries({
          term: state.currentTerm,
          leaderId: nodeId,
          entries: [entry1, entry2],
          leaderCommit: state.commitIndex,
          prevLogTerm: 0,
          prevLogIndex: 0,
        })

        const sentMessages: [NodeId, AppendEntries][] = []

        const sendMessage: SendAppendEntries = (nodeId, message) => {
          if (message.entries.length > 0) {
            sentMessages.push([nodeId, message])
            return nodeId === 2 && message.prevLogIndex === 1
              ? immediately(messages.appendEntriesResponse({ term: message.term, success: false }))
              : never()
          }

          return never()
        }

        runLogReplication(
          nodeId,
          () => state,
          otherNodeIds,
          setCommitIndex,
          waitForHeartbeatInterval,
          sendMessage,
          cancellationToken.never,
          logReplicationState,
        )

        await drainMicrotaskQueue()

        expect(sentMessages).toEqual([
          [2, message1],
          [2, message2],
        ])
      })

      it('tries again with decremented next index multiple times', async () => {
        const entry1: LogEntry = { index: 1, term: 1, command: 1 }
        const entry2: LogEntry = { index: 2, term: 1, command: 2 }
        const entry3: LogEntry = { index: 3, term: 2, command: 3 }
        const state = createNodeState({ currentTerm: 2, commitIndex: 0, log: [entry1, entry2, entry3] })

        const logReplicationState: LogReplicationState = {
          2: {
            nextIndex: 3,
            matchIndex: 0,
          },
          3: {
            nextIndex: 4,
            matchIndex: 0,
          },
        }

        const setCommitIndex = jest.fn()
        const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())

        const heartbeat = messages.appendEntries({
          term: state.currentTerm,
          leaderId: nodeId,
          entries: [],
          leaderCommit: state.commitIndex,
          prevLogTerm: 2,
          prevLogIndex: 3,
        })

        const message1 = messages.appendEntries({
          term: state.currentTerm,
          leaderId: nodeId,
          entries: [entry3],
          leaderCommit: state.commitIndex,
          prevLogTerm: 1,
          prevLogIndex: 2,
        })

        const message2 = messages.appendEntries({
          term: state.currentTerm,
          leaderId: nodeId,
          entries: [entry2, entry3],
          leaderCommit: state.commitIndex,
          prevLogTerm: 1,
          prevLogIndex: 1,
        })

        const message3 = messages.appendEntries({
          term: state.currentTerm,
          leaderId: nodeId,
          entries: [entry1, entry2, entry3],
          leaderCommit: state.commitIndex,
          prevLogTerm: 0,
          prevLogIndex: 0,
        })

        const sentMessages: [NodeId, AppendEntries][] = []

        const sendMessage: SendAppendEntries = (nodeId, message) => {
          sentMessages.push([nodeId, message])
          return nodeId === 2 && message.prevLogIndex > 0
            ? immediately(messages.appendEntriesResponse({ term: message.term, success: false }))
            : never()
        }

        runLogReplication(
          nodeId,
          () => state,
          otherNodeIds,
          setCommitIndex,
          waitForHeartbeatInterval,
          sendMessage,
          cancellationToken.never,
          logReplicationState,
        )

        await drainMicrotaskQueue()

        expect(sentMessages).toEqual([
          [2, message1],
          [3, heartbeat],
          [2, message2],
          [2, message3],
        ])
      })

      it('tries again with decremented next index for heartbeat failure', async () => {
        const entry1: LogEntry = { index: 1, term: 1, command: 1 }
        const entry2: LogEntry = { index: 2, term: 2, command: 2 }
        const state = createNodeState({ currentTerm: 2, commitIndex: 0, log: [entry1, entry2] })

        const logReplicationState: LogReplicationState = {
          2: {
            nextIndex: 3,
            matchIndex: 0,
          },
          3: {
            nextIndex: 2,
            matchIndex: 0,
          },
        }

        const setCommitIndex = jest.fn()
        const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())

        const heartbeat1 = messages.appendEntries({
          term: state.currentTerm,
          leaderId: nodeId,
          entries: [],
          leaderCommit: state.commitIndex,
          prevLogTerm: 2,
          prevLogIndex: 2,
        })

        const heartbeat2 = messages.appendEntries({
          term: state.currentTerm,
          leaderId: nodeId,
          entries: [entry2],
          leaderCommit: state.commitIndex,
          prevLogTerm: 1,
          prevLogIndex: 1,
        })

        const sentMessages: [NodeId, AppendEntries][] = []

        const sendMessage: SendAppendEntries = (nodeId, message) => {
          sentMessages.push([nodeId, message])
          return nodeId === 2 && message.prevLogIndex === 2
            ? immediately(messages.appendEntriesResponse({ term: message.term, success: false }))
            : never()
        }

        runLogReplication(
          nodeId,
          () => state,
          otherNodeIds,
          setCommitIndex,
          waitForHeartbeatInterval,
          sendMessage,
          cancellationToken.never,
          logReplicationState,
        )

        await drainMicrotaskQueue()

        expect(sentMessages).toEqual([
          [2, heartbeat1],
          [3, heartbeat2],
          [2, heartbeat2],
        ])
      })
    })

    it('cancels in-flight operations when canceled', () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, commitIndex: 0, log: [entry] })

      const logReplicationState: LogReplicationState = {
        2: {
          nextIndex: 1,
          matchIndex: 0,
        },
        3: {
          nextIndex: 2,
          matchIndex: 0,
        },
      }

      const setCommitIndex = jest.fn()
      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const sendMessage = jest.fn().mockReturnValue(never())

      const cancellationTokenSource = cancellationToken.createSource()

      runLogReplication(
        nodeId,
        () => state,
        otherNodeIds,
        setCommitIndex,
        waitForHeartbeatInterval,
        sendMessage,
        cancellationTokenSource.token,
        logReplicationState,
      )

      cancellationTokenSource.cancel()

      expect(sendMessage.mock.calls[0][2].isCancelled).toBe(true)
    })
  })

  describe(getCommittedStateMachineState.name, () => {
    let unhandledRejections: unknown[] = []

    function onUnhandledRejection(err: unknown) {
      unhandledRejections.push(err)
    }

    beforeEach(() => {
      unhandledRejections = []
      process.on('unhandledRejection', onUnhandledRejection)
    })

    afterEach(() => process.off('unhandledRejection', onUnhandledRejection))

    it('sends a heartbeat to all other nodes', () => {
      const state = createNodeState({ currentTerm: 2, commitIndex: 1, log: [{ index: 1, term: 1, command: 1 }] })

      const heartbeat = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [],
        leaderCommit: 1,
        prevLogTerm: 1,
        prevLogIndex: 1,
      })

      const sentMessages: [NodeId, AppendEntries][] = []

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        sentMessages.push([nodeId, message])
        return never()
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      getCommittedStateMachineState(nodeId, state, noopStateMachine, otherNodeIds, sendMessage, cancellationToken.never)

      expect(sentMessages).toEqual([
        [2, heartbeat],
        [3, heartbeat],
      ])
    })

    it('returns state machine state once majority responds successfully to heartbeats', async () => {
      const state = createNodeState({ currentTerm: 2, commitIndex: 1, log: [{ index: 1, term: 1, command: 1 }] })

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        return nodeId === 2
          ? immediately(messages.appendEntriesResponse({ term: message.term, success: true }))
          : never()
      }

      const stateMachineState = { value: 10 }
      const stateMachine: StateMachine<typeof stateMachineState, number> = {
        applyCommand: () => void 0,
        getState: () => stateMachineState,
      }

      const result = await getCommittedStateMachineState(
        nodeId,
        state,
        stateMachine,
        otherNodeIds,
        sendMessage,
        cancellationToken.never,
      )

      if (!messages.getStateResponse.is(result)) {
        fail('expected result to be a getState response')
      }

      expect(result.state).toBe(stateMachineState)
    })

    it('becomes a follower if another leader takes over', async () => {
      const state = createNodeState({ currentTerm: 2, commitIndex: 1, log: [{ index: 1, term: 1, command: 1 }] })

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        return nodeId === 2
          ? immediately(messages.appendEntriesResponse({ term: message.term + 1, success: false }))
          : never()
      }

      const result = await getCommittedStateMachineState(
        nodeId,
        state,
        noopStateMachine,
        otherNodeIds,
        sendMessage,
        cancellationToken.never,
      )

      expect(result).toEqual(becomeFollower())
    })

    it('retries heartbeats until majority has responded', async () => {
      const state = createNodeState({ currentTerm: 2, commitIndex: 1, log: [{ index: 1, term: 1, command: 1 }] })

      const heartbeat = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [],
        leaderCommit: 1,
        prevLogTerm: 1,
        prevLogIndex: 1,
      })

      const sentMessages: [NodeId, AppendEntries][] = []

      let hasThrown = false

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        sentMessages.push([nodeId, message])

        if (nodeId !== 2) {
          return never()
        }

        if (!hasThrown) {
          hasThrown = true
          return Promise.reject(new Error())
        }

        return immediately(messages.appendEntriesResponse({ term: message.term, success: true }))
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      getCommittedStateMachineState(nodeId, state, noopStateMachine, otherNodeIds, sendMessage, cancellationToken.never)

      await drainMicrotaskQueue()

      expect(unhandledRejections).toEqual([])
      expect(sentMessages).toEqual([
        [2, heartbeat],
        [3, heartbeat],
        [2, heartbeat],
      ])
    })

    it('cancels in-flight operations when canceled', () => {
      const state = createNodeState({ currentTerm: 2, commitIndex: 1, log: [{ index: 1, term: 1, command: 1 }] })

      const sendMessage = jest.fn().mockReturnValue(never())

      const cancellationTokenSource = cancellationToken.createSource()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      getCommittedStateMachineState(
        nodeId,
        state,
        noopStateMachine,
        otherNodeIds,
        sendMessage,
        cancellationTokenSource.token,
      )

      cancellationTokenSource.cancel()

      expect(sendMessage.mock.calls[0][2].isCancelled).toBe(true)
      expect(sendMessage.mock.calls[1][2].isCancelled).toBe(true)
    })
  })

  describe(runLeader.name, () => {
    const messageWithResponseChannel = <T extends RaftMessage>(
      message: T,
      respond: ResponseChannel<unknown>,
      token = cancellationToken.never,
    ): MessageWithResponseChannel<T, unknown> => ({ message, respond, cancellationToken: token })

    it('starts sending heartbeats', async () => {
      const state = createNodeState({ currentTerm: 2 })

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const heartbeat = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [],
        leaderCommit: 0,
        prevLogTerm: 0,
        prevLogIndex: 0,
      })

      const sentMessages: [NodeId, AppendEntries][] = []

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        sentMessages.push([nodeId, message])
        return never()
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runLeader(
        nodeId,
        state,
        noopStateMachine,
        otherNodeIds,
        waitForMessage,
        waitForHeartbeatInterval,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(sentMessages).toEqual([
        [2, heartbeat],
        [3, heartbeat],
      ])
    })

    it('starts log replication when log is not empty', async () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, log: [entry] })

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const noop: LogEntry = { index: 2, term: 2, command: NOOP_COMMAND }

      const message = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [entry, noop],
        leaderCommit: 0,
        prevLogTerm: 0,
        prevLogIndex: 0,
      })

      const sentMessages: [NodeId, AppendEntries][] = []
      const sendMessage: SendAppendEntries = (nodeId, message) => {
        if (message.entries.length > 0) {
          sentMessages.push([nodeId, message])
          return never()
        }

        if (message.prevLogTerm === 1) {
          return immediately(messages.appendEntriesResponse({ term: message.term, success: false }))
        }

        return never()
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runLeader(
        nodeId,
        state,
        noopStateMachine,
        otherNodeIds,
        waitForMessage,
        waitForHeartbeatInterval,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(sentMessages).toEqual([
        [2, message],
        [3, message],
      ])

      expect(onUpdateState).toHaveBeenCalledWith(expect.objectContaining({ log: [entry, noop] }))
    })

    it('starts log replication when log is empty', async () => {
      const state = createNodeState({ currentTerm: 2 })

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const waitForMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const noop: LogEntry = { index: 1, term: 2, command: NOOP_COMMAND }

      const message = messages.appendEntries({
        term: state.currentTerm,
        leaderId: nodeId,
        entries: [noop],
        leaderCommit: 0,
        prevLogTerm: 0,
        prevLogIndex: 0,
      })

      const sentMessages: [NodeId, AppendEntries][] = []
      const sendMessage: SendAppendEntries = (nodeId, message) => {
        if (message.entries.length > 0) {
          sentMessages.push([nodeId, message])
          return never()
        }

        return immediately(messages.appendEntriesResponse({ term: message.term, success: true }))
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runLeader(
        nodeId,
        state,
        noopStateMachine,
        otherNodeIds,
        waitForMessage,
        waitForHeartbeatInterval,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(sentMessages).toEqual([
        [2, message],
        [3, message],
      ])
    })

    it('becomes a follower if another leader takes over', async () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, log: [entry] })

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const sendMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const message = messages.appendEntries({
        term: state.currentTerm + 1,
        leaderId: nodeId,
        entries: [],
        leaderCommit: 1,
        prevLogTerm: 1,
        prevLogIndex: 1,
      })

      const messageWithChannel = messageWithResponseChannel(message, jest.fn())
      const waitForMessage = jest.fn().mockReturnValueOnce(immediately(messageWithChannel)).mockReturnValue(never())

      const [, response] = await runLeader(
        nodeId,
        state,
        noopStateMachine,
        otherNodeIds,
        waitForMessage,
        waitForHeartbeatInterval,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(response).toEqual(becomeFollower({ triggeringMessage: messageWithChannel }))
    })

    it('applies single committed command to state machine', async () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, log: [entry] })

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const sendMessage: SendAppendEntries = (_, message) => {
        return immediately(messages.appendEntriesResponse({ term: message.term, success: true }))
      }

      const message = messages.executeCommand({ command: 3 })

      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, jest.fn())))
        .mockReturnValue(never())

      const applyCommand = jest.fn()
      const stateMachine: StateMachine<number, number> = {
        applyCommand,
        getState: () => 10,
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runLeader(
        nodeId,
        state,
        stateMachine,
        otherNodeIds,
        waitForMessage,
        waitForHeartbeatInterval,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(applyCommand).toHaveBeenCalledTimes(1)
      expect(applyCommand).toHaveBeenCalledWith(message.command)
    })

    it('applies multiple committed commands to state machine', async () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, log: [entry] })

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const sendMessage: SendAppendEntries = (_, message) => {
        return immediately(messages.appendEntriesResponse({ term: message.term, success: true }))
      }

      const message1 = messages.executeCommand({ command: 3 })
      const message2 = messages.executeCommand({ command: 5 })

      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message1, jest.fn())))
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message2, jest.fn())))
        .mockReturnValue(never())

      const applyCommand = jest.fn()
      const stateMachine: StateMachine<number, number> = {
        applyCommand,
        getState: () => 10,
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runLeader(
        nodeId,
        state,
        stateMachine,
        otherNodeIds,
        waitForMessage,
        waitForHeartbeatInterval,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(applyCommand).toHaveBeenCalledTimes(2)
      expect(applyCommand).toHaveBeenCalledWith(message1.command)
      expect(applyCommand).toHaveBeenCalledWith(message2.command)
    })

    it('responds to client execution request after committing command', async () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, log: [entry] })

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const sendMessage: SendAppendEntries = (_, message) => {
        return immediately(messages.appendEntriesResponse({ term: message.term, success: true }))
      }

      const message = messages.executeCommand({ command: 3 })

      const respond = jest.fn()
      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, respond)))
        .mockReturnValue(never())

      const applyCommand = jest.fn()
      const stateMachine: StateMachine<number, number> = {
        applyCommand,
        getState: () => 10,
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runLeader(
        nodeId,
        state,
        stateMachine,
        otherNodeIds,
        waitForMessage,
        waitForHeartbeatInterval,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(respond).toHaveBeenCalledTimes(1)
      expect(respond).toHaveBeenCalledWith(messages.executeCommandResponse())
    })

    it('responds to client execution request with no leader error if new leader takes over', async () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, log: [entry] })

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const sendMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const executeCommand = messages.executeCommand({ command: 3 })
      const requestVote = messages.requestVote({
        term: state.currentTerm + 1,
        candidateId: 2,
        lastLogIndex: 1,
        lastLogTerm: 1,
      })

      const respond = jest.fn()
      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(executeCommand, respond)))
        .mockReturnValueOnce(immediately(messageWithResponseChannel(requestVote, respond)))
        .mockReturnValue(never())

      const stateMachine: StateMachine<number, number> = {
        applyCommand: jest.fn(),
        getState: () => 10,
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runLeader(
        nodeId,
        state,
        stateMachine,
        otherNodeIds,
        waitForMessage,
        waitForHeartbeatInterval,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(respond).toHaveBeenCalledTimes(1)
      expect(respond).toHaveBeenCalledWith(messages.noLeaderError())
    })

    it('responds to client get request after verifying leadership', async () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, log: [entry] })

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const sendMessage: SendAppendEntries = (_, message) =>
        immediately(messages.appendEntriesResponse({ term: message.term, success: true }))

      const message = messages.getState()

      const respond = jest.fn()
      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, respond)))
        .mockReturnValue(never())

      const stateMachineState = { value: 10 }
      const stateMachine: StateMachine<typeof stateMachineState, number> = {
        applyCommand: () => void 0,
        getState: () => stateMachineState,
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runLeader(
        nodeId,
        state,
        stateMachine,
        otherNodeIds,
        waitForMessage,
        waitForHeartbeatInterval,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(respond).toHaveBeenCalledTimes(1)
      expect(respond).toHaveBeenCalledWith(messages.getStateResponse({ state: stateMachineState }))
    })

    it('responds to client get request with no leader error if new leader takes over', async () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, log: [entry] })

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const sendMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const getState = messages.getState()
      const requestVote = messages.requestVote({
        term: state.currentTerm + 1,
        candidateId: 2,
        lastLogIndex: 1,
        lastLogTerm: 1,
      })

      const respond = jest.fn()
      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(getState, respond)))
        .mockReturnValueOnce(immediately(messageWithResponseChannel(requestVote, respond)))
        .mockReturnValue(never())

      const stateMachine: StateMachine<number, number> = {
        applyCommand: jest.fn(),
        getState: () => 10,
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runLeader(
        nodeId,
        state,
        stateMachine,
        otherNodeIds,
        waitForMessage,
        waitForHeartbeatInterval,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(respond).toHaveBeenCalledTimes(1)
      expect(respond).toHaveBeenCalledWith(messages.noLeaderError())
    })

    it('responds to client get request with no leader error leader verification fails', async () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, log: [entry] })

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const sendMessage: SendAppendEntries = (_, message) => {
        if (message.entries.length > 0) {
          return never()
        }

        return immediately(messages.appendEntriesResponse({ term: message.term + 1, success: true }))
      }

      const getState = messages.getState()

      const respond = jest.fn()
      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(getState, respond)))
        .mockReturnValue(never())

      const stateMachine: StateMachine<number, number> = {
        applyCommand: jest.fn(),
        getState: () => 10,
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runLeader(
        nodeId,
        state,
        stateMachine,
        otherNodeIds,
        waitForMessage,
        waitForHeartbeatInterval,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      expect(respond).toHaveBeenCalledTimes(1)
      expect(respond).toHaveBeenCalledWith(messages.noLeaderError())
    })

    it('stops trying to verify leadership if client get request is cancelled', async () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, log: [entry] })

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const sendMessage = jest.fn().mockReturnValue(never())

      const message = messages.getState()

      const respond = jest.fn()
      const tokenSource = cancellationToken.createSource()
      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, respond, tokenSource.token)))
        .mockReturnValue(never())

      const stateMachineState = { value: 10 }
      const stateMachine: StateMachine<typeof stateMachineState, number> = {
        applyCommand: () => void 0,
        getState: () => stateMachineState,
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runLeader(
        nodeId,
        state,
        stateMachine,
        otherNodeIds,
        waitForMessage,
        waitForHeartbeatInterval,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      await drainMicrotaskQueue()

      tokenSource.cancel()

      expect(sendMessage.mock.calls[2][2].isCancelled).toBe(true)
      expect(sendMessage.mock.calls[3][2].isCancelled).toBe(true)
    })

    it('stops trying to verify leadership if cancelled', async () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, log: [entry] })

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))
      const sendMessage = jest.fn().mockReturnValue(never())

      const message = messages.getState()

      const respond = jest.fn()
      const tokenSource = cancellationToken.createSource()
      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, respond)))
        .mockReturnValue(never())

      const stateMachineState = { value: 10 }
      const stateMachine: StateMachine<typeof stateMachineState, number> = {
        applyCommand: () => void 0,
        getState: () => stateMachineState,
      }

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runLeader(
        nodeId,
        state,
        stateMachine,
        otherNodeIds,
        waitForMessage,
        waitForHeartbeatInterval,
        sendMessage,
        onUpdateState,
        tokenSource.token,
      )

      await drainMicrotaskQueue()

      tokenSource.cancel()

      expect(sendMessage.mock.calls[2][2].isCancelled).toBe(true)
      expect(sendMessage.mock.calls[3][2].isCancelled).toBe(true)
    })

    it('returns updated state', async () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, log: [entry] })

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      let resolveRequestVotePromise: () => void
      const requestVotePromise = new Promise((resolve) => {
        const requestVote = messages.requestVote({
          term: state.currentTerm + 1,
          candidateId: 2,
          lastLogIndex: 1,
          lastLogTerm: 1,
        })

        resolveRequestVotePromise = () => resolve(messageWithResponseChannel(requestVote, jest.fn()))
      })

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        if (nodeId === 3) {
          return never()
        }

        resolveRequestVotePromise()
        return immediately(messages.appendEntriesResponse({ term: message.term, success: true }))
      }

      const executeCommand = messages.executeCommand({ command: 3 })

      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(executeCommand, jest.fn())))
        .mockReturnValueOnce(requestVotePromise)
        .mockReturnValue(never())

      const [newState] = await runLeader(
        nodeId,
        state,
        noopStateMachine,
        otherNodeIds,
        waitForMessage,
        waitForHeartbeatInterval,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(newState.log.length).toBe(3)

      expect(onUpdateState).toHaveBeenCalledWith(
        expect.objectContaining({ log: newState.log, commitIndex: state.commitIndex }),
      )
    })

    it('updates commit index in state when committing commands', async () => {
      const entry: LogEntry = { index: 1, term: 1, command: 1 }
      const state = createNodeState({ currentTerm: 2, log: [entry] })

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      let resolveRequestVotePromise: () => void
      const requestVotePromise = new Promise((resolve) => {
        const requestVote = messages.requestVote({
          term: state.currentTerm + 1,
          candidateId: 2,
          lastLogIndex: 1,
          lastLogTerm: 1,
        })

        resolveRequestVotePromise = () => resolve(messageWithResponseChannel(requestVote, jest.fn()))
      })

      const sendMessage: SendAppendEntries = (nodeId, message) => {
        if (nodeId === 3) {
          return never()
        }

        // eslint-disable-next-line @typescript-eslint/no-floating-promises
        drainMicrotaskQueue().then(resolveRequestVotePromise)

        return immediately(messages.appendEntriesResponse({ term: message.term, success: true }))
      }

      const executeCommand = messages.executeCommand({ command: 3 })

      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(executeCommand, jest.fn())))
        .mockReturnValueOnce(requestVotePromise)
        .mockReturnValue(never())

      const [newState] = await runLeader(
        nodeId,
        state,
        noopStateMachine,
        otherNodeIds,
        waitForMessage,
        waitForHeartbeatInterval,
        sendMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(newState.commitIndex).toBe(3)
      expect(onUpdateState).toHaveBeenCalledWith(expect.objectContaining({ commitIndex: 3 }))
    })

    it('cancels in-flight operations when canceled', () => {
      const state = createNodeState({ currentTerm: 2, commitIndex: 1, log: [{ index: 1, term: 1, command: 1 }] })

      const waitForHeartbeatInterval = jest.fn().mockReturnValue(never())
      const sendMessage = jest.fn().mockReturnValue(never())
      const waitForMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const cancellationTokenSource = cancellationToken.createSource()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runLeader(
        nodeId,
        state,
        noopStateMachine,
        otherNodeIds,
        waitForMessage,
        waitForHeartbeatInterval,
        sendMessage,
        onUpdateState,
        cancellationTokenSource.token,
      )

      cancellationTokenSource.cancel()

      expect(sendMessage.mock.calls[0][2].isCancelled).toBe(true)
      expect(sendMessage.mock.calls[1][2].isCancelled).toBe(true)
      expect(waitForMessage.mock.calls[0][0].isCancelled).toBe(true)
    })
  })
})
