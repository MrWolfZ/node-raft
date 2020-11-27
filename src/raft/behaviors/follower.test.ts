import { cancellationToken } from '../../util/cancellation-token.js'
import { messages, MessageWithResponseChannel, RaftMessage, ResponseChannel } from '../messages.js'
import { createNodeState, noopStateMachine, NOOP_COMMAND, StateMachine } from '../state.js'
import { onMessage, runFollower } from './follower.js'

describe('follower', () => {
  describe(onMessage.name, () => {
    describe('when receiving a request for vote', () => {
      describe('and candidate term is lower than current term', () => {
        it('does not grant vote', () => {
          const state = createNodeState({ currentTerm: 2 })
          const message = messages.requestVote({ term: 1, candidateId: 1, lastLogTerm: 0, lastLogIndex: 0 })

          const [newState, response] = onMessage(state, noopStateMachine, message)

          expect(response.voteGranted).toBe(false)
          expect(newState).toBe(state)
        })

        it('sets response term to current term', () => {
          const state = createNodeState({ currentTerm: 2 })
          const message = messages.requestVote({ term: 1, candidateId: 1, lastLogTerm: 0, lastLogIndex: 0 })

          const [, response] = onMessage(state, noopStateMachine, message)

          expect(response.term).toBe(state.currentTerm)
        })
      })

      describe('and candidate term is equal to or higher than current term', () => {
        it('sets current term to candidate term', () => {
          const state = createNodeState({ currentTerm: 1 })
          const message = messages.requestVote({ term: 2, candidateId: 1, lastLogTerm: 0, lastLogIndex: 0 })

          const [newState] = onMessage(state, noopStateMachine, message)

          expect(newState.currentTerm).toBe(message.term)
        })

        it('sets response term to candidate term', () => {
          const state = createNodeState({ currentTerm: 1 })
          const message = messages.requestVote({ term: 2, candidateId: 1, lastLogTerm: 0, lastLogIndex: 0 })

          const [, response] = onMessage(state, noopStateMachine, message)

          expect(response.term).toBe(message.term)
        })

        describe('and follower has already voted for a different candidate', () => {
          it('does not grant vote', () => {
            const state = createNodeState({ votedFor: 2, currentTerm: 1 })
            const message = messages.requestVote({ term: 1, candidateId: 1, lastLogTerm: 0, lastLogIndex: 0 })

            const [, response] = onMessage(state, noopStateMachine, message)

            expect(response.voteGranted).toBe(false)
          })

          it('does grant vote if voted for other candidate in lower term', () => {
            const state = createNodeState({ votedFor: 2 })
            const message = messages.requestVote({ term: 1, candidateId: 1, lastLogTerm: 0, lastLogIndex: 0 })

            const [newState, response] = onMessage(state, noopStateMachine, message)

            expect(response.voteGranted).toBe(true)
            expect(newState.votedFor).toBe(message.candidateId)
            expect(newState.currentTerm).toBe(message.term)
          })
        })

        describe('and follower has not yet voted for different candidate', () => {
          describe('and log of candidate is not up to date', () => {
            it('does not grant vote if last term is lower', () => {
              const state = createNodeState({ currentTerm: 1, log: [{ index: 2, term: 2, command: undefined }] })
              const message = messages.requestVote({ term: 2, candidateId: 1, lastLogTerm: 1, lastLogIndex: 2 })

              const [newState, response] = onMessage(state, noopStateMachine, message)

              expect(response.voteGranted).toBe(false)
              expect(newState.currentTerm).toBe(message.term)
            })

            it('does not grant vote if last index is lower', () => {
              const state = createNodeState({ currentTerm: 1, log: [{ index: 3, term: 1, command: undefined }] })
              const message = messages.requestVote({ term: 2, candidateId: 1, lastLogTerm: 1, lastLogIndex: 2 })

              const [newState, response] = onMessage(state, noopStateMachine, message)

              expect(response.voteGranted).toBe(false)
              expect(newState.currentTerm).toBe(message.term)
            })
          })

          describe('and log of candidate is up to date', () => {
            it('does grant vote if log is not empty', () => {
              const state = createNodeState({ currentTerm: 1, log: [{ index: 2, term: 1, command: undefined }] })
              const message = messages.requestVote({ term: 2, candidateId: 1, lastLogTerm: 1, lastLogIndex: 3 })

              const [newState, response] = onMessage(state, noopStateMachine, message)

              expect(response.voteGranted).toBe(true)
              expect(newState.currentTerm).toBe(message.term)
            })

            it('does grant vote if log is empty', () => {
              const state = createNodeState({ currentTerm: 1 })
              const message = messages.requestVote({ term: 2, candidateId: 1, lastLogTerm: 0, lastLogIndex: 0 })

              const [newState, response] = onMessage(state, noopStateMachine, message)

              expect(response.voteGranted).toBe(true)
              expect(newState.currentTerm).toBe(message.term)
            })

            it('sets votedFor to candidate ID', () => {
              const state = createNodeState({ currentTerm: 1, log: [{ index: 2, term: 1, command: undefined }] })
              const message = messages.requestVote({ term: 2, candidateId: 1, lastLogTerm: 1, lastLogIndex: 3 })

              const [newState] = onMessage(state, noopStateMachine, message)

              expect(newState.votedFor).toBe(message.candidateId)
              expect(newState.currentTerm).toBe(message.term)
            })
          })
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

          const [newState, response] = onMessage(state, noopStateMachine, message)

          expect(response.success).toBe(false)
          expect(response.term).toBe(state.currentTerm)
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
            leaderCommit: 0,
          })

          const [, response] = onMessage(state, noopStateMachine, message)

          expect(response.term).toBe(state.currentTerm)
        })
      })

      describe('and candidate term is equal to or higher than current term', () => {
        it('sets current term to leader term', () => {
          const state = createNodeState({ currentTerm: 1 })
          const message = messages.appendEntries({
            term: 2,
            entries: [],
            prevLogIndex: 0,
            prevLogTerm: 0,
            leaderId: 1,
            leaderCommit: 0,
          })

          const [newState] = onMessage(state, noopStateMachine, message)

          expect(newState.currentTerm).toBe(message.term)
        })

        it('sets response term to leader term', () => {
          const state = createNodeState({ currentTerm: 1 })
          const message = messages.appendEntries({
            term: 2,
            entries: [],
            prevLogIndex: 0,
            prevLogTerm: 0,
            leaderId: 1,
            leaderCommit: 0,
          })

          const [, response] = onMessage(state, noopStateMachine, message)

          expect(response.term).toBe(message.term)
        })

        it('clears votedFor', () => {
          const state = createNodeState({ currentTerm: 1, votedFor: 1 })
          const message = messages.appendEntries({
            term: 2,
            entries: [],
            prevLogIndex: 0,
            prevLogTerm: 0,
            leaderId: 1,
            leaderCommit: 0,
          })

          const [newState] = onMessage(state, noopStateMachine, message)

          expect(newState.votedFor).toBe(null)
        })

        it('sets the leader ID', () => {
          const state = createNodeState({ currentTerm: 1, votedFor: 1 })
          const message = messages.appendEntries({
            term: 2,
            entries: [],
            prevLogIndex: 0,
            prevLogTerm: 0,
            leaderId: 1,
            leaderCommit: 0,
          })

          const [newState] = onMessage(state, noopStateMachine, message)

          expect(newState.lastKnownLeaderId).toBe(message.leaderId)
        })

        describe('and log does not contain matching prevLogIndex entry with matching prevLogTerm', () => {
          it('responds with success=false if term at prev index does not match prev term', () => {
            const state = createNodeState({
              currentTerm: 1,
              log: [
                { index: 1, term: 1, command: undefined },
                { index: 2, term: 2, command: undefined },
              ],
            })

            const message = messages.appendEntries({
              term: 2,
              entries: [],
              prevLogIndex: 1,
              prevLogTerm: 2,
              leaderId: 1,
              leaderCommit: 0,
            })

            const [newState, response] = onMessage(state, noopStateMachine, message)

            expect(response.success).toBe(false)
            expect(response.term).toBe(message.term)
            expect(newState.currentTerm).toBe(message.term)
            expect(newState.votedFor).toBe(null)
          })

          it('responds with success=false if entry at prev index does not exist', () => {
            const state = createNodeState({ currentTerm: 1, log: [{ index: 1, term: 1, command: undefined }] })
            const message = messages.appendEntries({
              term: 2,
              entries: [],
              prevLogIndex: 2,
              prevLogTerm: 2,
              leaderId: 1,
              leaderCommit: 0,
            })

            const [newState, response] = onMessage(state, noopStateMachine, message)

            expect(response.success).toBe(false)
            expect(response.term).toBe(message.term)
            expect(newState.currentTerm).toBe(message.term)
            expect(newState.votedFor).toBe(null)
          })
        })

        describe('and log contains entry with matching prev term at prev index', () => {
          it('responds with success=true', () => {
            const state = createNodeState({ currentTerm: 1, log: [{ index: 1, term: 2, command: undefined }] })
            const message = messages.appendEntries({
              term: 2,
              entries: [],
              prevLogIndex: 1,
              prevLogTerm: 2,
              leaderId: 1,
              leaderCommit: 0,
            })

            const [newState, response] = onMessage(state, noopStateMachine, message)

            expect(response.success).toBe(true)
            expect(response.term).toBe(message.term)
            expect(newState.currentTerm).toBe(message.term)
            expect(newState.votedFor).toBe(null)
          })

          it('appends new entries to log', () => {
            const state = createNodeState({
              currentTerm: 1,
              log: [
                { index: 1, term: 1, command: undefined },
                { index: 2, term: 1, command: undefined },
              ],
            })

            const message = messages.appendEntries({
              term: 2,
              entries: [
                { index: 3, term: 2, command: undefined },
                { index: 4, term: 2, command: undefined },
              ],
              prevLogIndex: 2,
              prevLogTerm: 1,
              leaderId: 1,
              leaderCommit: 0,
            })

            const [newState, response] = onMessage(state, noopStateMachine, message)

            expect(newState.log).toEqual([...state.log, ...message.entries])
            expect(response.success).toBe(true)
            expect(response.term).toBe(message.term)
            expect(newState.currentTerm).toBe(message.term)
            expect(newState.votedFor).toBe(null)
          })

          it('appends new entries after deleting conflicting entries', () => {
            const state = createNodeState({
              currentTerm: 1,
              log: [
                { index: 1, term: 1, command: undefined },
                { index: 2, term: 1, command: undefined },
                { index: 3, term: 1, command: undefined },
              ],
            })

            const message = messages.appendEntries({
              term: 2,
              entries: [
                { index: 2, term: 2, command: undefined },
                { index: 3, term: 2, command: undefined },
              ],
              prevLogIndex: 1,
              prevLogTerm: 1,
              leaderId: 1,
              leaderCommit: 0,
            })

            const [newState, response] = onMessage(state, noopStateMachine, message)

            expect(newState.log).toEqual([state.log[0], ...message.entries])
            expect(response.success).toBe(true)
            expect(response.term).toBe(message.term)
            expect(newState.currentTerm).toBe(message.term)
            expect(newState.votedFor).toBe(null)
          })

          it('ignores existing entries', () => {
            const state = createNodeState({
              currentTerm: 1,
              log: [
                { index: 1, term: 1, command: undefined },
                { index: 2, term: 1, command: undefined },
                { index: 3, term: 1, command: undefined },
              ],
            })

            const message = messages.appendEntries({
              term: 2,
              entries: [
                { index: 2, term: 1, command: undefined },
                { index: 3, term: 2, command: undefined },
              ],
              prevLogIndex: 1,
              prevLogTerm: 1,
              leaderId: 1,
              leaderCommit: 0,
            })

            const [newState, response] = onMessage(state, noopStateMachine, message)

            expect(newState.log).toEqual([...state.log.slice(0, 2), message.entries[1]])
            expect(response.success).toBe(true)
            expect(response.term).toBe(message.term)
            expect(newState.currentTerm).toBe(message.term)
            expect(newState.votedFor).toBe(null)
          })

          it('does not update the log if the request is empty', () => {
            const state = createNodeState({ currentTerm: 1, log: [{ index: 1, term: 2, command: undefined }] })
            const message = messages.appendEntries({
              term: 2,
              entries: [],
              prevLogIndex: 1,
              prevLogTerm: 2,
              leaderId: 1,
              leaderCommit: 0,
            })

            const [newState] = onMessage(state, noopStateMachine, message)

            expect(newState.log).toBe(state.log)
          })

          describe('and leader commit index is higher than own commitIndex', () => {
            it('sets its commit index to min of leader commit index and last request entry index when leader commit index is lower', () => {
              const state = createNodeState({
                currentTerm: 1,
                commitIndex: 1,
                log: [{ index: 1, term: 1, command: undefined }],
              })

              const message = messages.appendEntries({
                term: 2,
                entries: [
                  { index: 2, term: 1, command: undefined },
                  { index: 3, term: 2, command: undefined },
                ],
                prevLogIndex: 1,
                prevLogTerm: 1,
                leaderId: 1,
                leaderCommit: 2,
              })

              const [newState] = onMessage(state, noopStateMachine, message)

              expect(newState.commitIndex).toBe(message.leaderCommit)
            })

            it('sets its commit index to min of leader commit index and last request entry index when last request entry index is lower', () => {
              const state = createNodeState({
                currentTerm: 1,
                commitIndex: 1,
                log: [{ index: 1, term: 1, command: undefined }],
              })

              const message = messages.appendEntries({
                term: 2,
                entries: [
                  { index: 2, term: 1, command: undefined },
                  { index: 3, term: 2, command: undefined },
                ],
                prevLogIndex: 1,
                prevLogTerm: 1,
                leaderId: 1,
                leaderCommit: 4,
              })

              const [newState] = onMessage(state, noopStateMachine, message)

              expect(newState.commitIndex).toBe(3)
            })
          })

          describe('and leader commit index is not higher than own commitIndex', () => {
            it('keeps own commit index', () => {
              const state = createNodeState({
                currentTerm: 1,
                commitIndex: 3,
                log: [{ index: 1, term: 1, command: undefined }],
              })

              const message = messages.appendEntries({
                term: 2,
                entries: [
                  { index: 2, term: 1, command: undefined },
                  { index: 3, term: 2, command: undefined },
                ],
                prevLogIndex: 1,
                prevLogTerm: 1,
                leaderId: 1,
                leaderCommit: 2,
              })

              const [newState] = onMessage(state, noopStateMachine, message)

              expect(newState.commitIndex).toBe(state.commitIndex)
            })
          })

          describe('and leader commit index is higher than lastApplied', () => {
            it('applies all pending commands to state machine', () => {
              const state = createNodeState({
                currentTerm: 1,
                commitIndex: 1,
                lastApplied: 1,
                log: [{ index: 1, term: 1, command: 1 }],
              })

              const message = messages.appendEntries({
                term: 2,
                entries: [
                  { index: 2, term: 1, command: 2 },
                  { index: 3, term: 2, command: 3 },
                  { index: 4, term: 2, command: 3 },
                ],
                prevLogIndex: 1,
                prevLogTerm: 1,
                leaderId: 1,
                leaderCommit: 3,
              })

              const appliedCommands: number[] = []
              const stateMachine: StateMachine<number, number> = {
                applyCommand: (command) => appliedCommands.push(command),
                getState: () => 0,
              }

              const [newState] = onMessage(state, stateMachine, message)

              expect(appliedCommands).toEqual(message.entries.slice(0, 2).map((e) => e.command))
              expect(newState.lastApplied).toBe(message.entries[1].index)
            })

            it('ignores no-op log entries', () => {
              const state = createNodeState({
                currentTerm: 1,
                commitIndex: 1,
                lastApplied: 1,
                log: [
                  { index: 1, term: 1, command: 1 },
                  { index: 2, term: 2, command: NOOP_COMMAND },
                ],
              })

              const message = messages.appendEntries({
                term: 2,
                entries: [],
                prevLogIndex: 2,
                prevLogTerm: 2,
                leaderId: 1,
                leaderCommit: 2,
              })

              const applyCommand = jest.fn()
              const stateMachine: StateMachine<number, number> = {
                applyCommand,
                getState: () => 0,
              }

              const [newState] = onMessage(state, stateMachine, message)

              expect(applyCommand).not.toHaveBeenCalled()
              expect(newState.lastApplied).toBe(state.log[1].index)
            })
          })
        })
      })
    })

    describe('when receiving a client request', () => {
      describe('and follower knows a leader', () => {
        it('redirects client to leader for execution request', () => {
          const state = createNodeState({ currentTerm: 1, lastKnownLeaderId: 1 })
          const message = messages.executeCommand({ command: 1 })

          const [newState, response] = onMessage(state, noopStateMachine, message)

          expect(newState).toBe(state)
          expect(response).toEqual(messages.redirectClient({ leaderId: state.lastKnownLeaderId || -1 }))
        })

        it('redirects client to leader for get state request', () => {
          const state = createNodeState({ currentTerm: 1, lastKnownLeaderId: 1 })
          const message = messages.getState()

          const [newState, response] = onMessage(state, noopStateMachine, message)

          expect(newState).toBe(state)
          expect(response).toEqual(messages.redirectClient({ leaderId: state.lastKnownLeaderId || -1 }))
        })
      })

      describe('and follower does not know a leader', () => {
        it('returns an error to client for execution request', () => {
          const state = createNodeState({ currentTerm: 1, lastKnownLeaderId: undefined })
          const message = messages.executeCommand({ command: 1 })

          const [newState, response] = onMessage(state, noopStateMachine, message)

          expect(newState).toBe(state)
          expect(response).toEqual(messages.noLeaderError())
        })

        it('returns an error to client for get state request', () => {
          const state = createNodeState({ currentTerm: 1, lastKnownLeaderId: undefined })
          const message = messages.getState()

          const [newState, response] = onMessage(state, noopStateMachine, message)

          expect(newState).toBe(state)
          expect(response).toEqual(messages.noLeaderError())
        })
      })
    })
  })

  describe(runFollower.name, () => {
    const messageWithResponseChannel = <T extends RaftMessage>(
      message: T,
      respond: ResponseChannel<unknown>,
      token = cancellationToken.never,
    ): MessageWithResponseChannel<T, unknown> => ({ message, respond, cancellationToken: token })

    const never = <T>() => new Promise<T>(() => void 0)
    const immediately = <T>(value?: T) => Promise.resolve(value)

    it('becomes a candidate if no message received before the election timeout elapses', async () => {
      const state = createNodeState({})
      const waitForElectionTimeout = jest.fn().mockReturnValueOnce(Promise.resolve())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const [, behaviorChange] = await runFollower(
        state,
        noopStateMachine,
        waitForElectionTimeout,
        never,
        onUpdateState,
        cancellationToken.never,
      )

      expect(behaviorChange.$type).toBe('BecomeCandidate')
      expect(waitForElectionTimeout).toHaveBeenCalled()
    })

    it('resets the election timeout after each message', async () => {
      const state = createNodeState({})

      const waitForElectionTimeout = jest.fn().mockReturnValueOnce(never()).mockReturnValueOnce(immediately())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const waitForMessage = jest
        .fn()
        .mockReturnValue(immediately(messageWithResponseChannel(messages.getState(), jest.fn())))

      await runFollower(
        state,
        noopStateMachine,
        waitForElectionTimeout,
        waitForMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(waitForElectionTimeout).toHaveBeenCalledTimes(2)
      expect(waitForElectionTimeout.mock.calls[0][0].isCancelled).toBe(true)
    })

    it('stops waiting for messages when election timeout elapses', async () => {
      const state = createNodeState({})

      const waitForElectionTimeout = jest.fn().mockReturnValueOnce(immediately())
      const waitForMessage = jest.fn().mockReturnValue(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      await runFollower(
        state,
        noopStateMachine,
        waitForElectionTimeout,
        waitForMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(waitForMessage.mock.calls[0][0].isCancelled).toBe(true)
    })

    it('returns updated state after processing messages', async () => {
      const state = createNodeState({})
      const waitForElectionTimeout = jest.fn().mockReturnValueOnce(never()).mockReturnValueOnce(immediately())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const message = messages.requestVote({ term: 1, candidateId: 1, lastLogIndex: 1, lastLogTerm: 1 })
      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, jest.fn())))
        .mockReturnValueOnce(never())

      const [updatedState] = await runFollower(
        state,
        noopStateMachine,
        waitForElectionTimeout,
        waitForMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(updatedState).not.toBe(state)
      expect(updatedState.currentTerm).toBe(message.term)
    })

    it('redirects client request', async () => {
      const state = createNodeState({ lastKnownLeaderId: 2 })
      const waitForElectionTimeout = jest.fn().mockReturnValueOnce(never()).mockReturnValueOnce(immediately())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const respond = jest.fn()
      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(messages.getState(), respond)))
        .mockReturnValueOnce(never())

      await runFollower(
        state,
        noopStateMachine,
        waitForElectionTimeout,
        waitForMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(respond).toHaveBeenCalledWith(messages.redirectClient({ leaderId: 2 }))
    })

    it('notifies when the state is updated', async () => {
      const state = createNodeState({})
      const waitForElectionTimeout = jest.fn().mockReturnValueOnce(never()).mockReturnValueOnce(immediately())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const message = messages.requestVote({ term: 1, candidateId: 1, lastLogIndex: 1, lastLogTerm: 1 })
      const waitForMessage = jest
        .fn()
        .mockReturnValueOnce(immediately(messageWithResponseChannel(message, jest.fn())))
        .mockReturnValueOnce(never())

      await runFollower(
        state,
        noopStateMachine,
        waitForElectionTimeout,
        waitForMessage,
        onUpdateState,
        cancellationToken.never,
      )

      expect(onUpdateState).toHaveBeenCalledWith(expect.objectContaining({ currentTerm: message.term }))
    })

    it('can be cancelled', () => {
      const state = createNodeState({ lastKnownLeaderId: 2 })
      const waitForElectionTimeout = jest.fn().mockReturnValueOnce(never())
      const waitForMessage = jest.fn().mockReturnValueOnce(never())
      const onUpdateState = jest.fn().mockImplementation((s) => Promise.resolve(s))

      const cancellationTokenSource = cancellationToken.createSource()

      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      runFollower(
        state,
        noopStateMachine,
        waitForElectionTimeout,
        waitForMessage,
        onUpdateState,
        cancellationTokenSource.token,
      )

      cancellationTokenSource.cancel()

      expect(waitForElectionTimeout.mock.calls[0][0].isCancelled).toBe(true)
      expect(waitForMessage.mock.calls[0][0].isCancelled).toBe(true)
    })
  })
})
