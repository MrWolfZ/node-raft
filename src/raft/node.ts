import { cancellationToken, CancellationToken } from '../util/cancellation-token.js'
import { switch$ } from '../util/switch.js'
import { taggedUnion } from '../util/tagged-unions.js'
import type { Timespan } from '../util/timespan.js'
import { wait } from '../util/wait.js'
import { becomeFollower } from './behaviors/behavior.js'
import { runCandidate } from './behaviors/candidate.js'
import { runFollower } from './behaviors/follower.js'
import { runLeader } from './behaviors/leader.js'
import type {
  MessageWithResponseChannel,
  RaftMessage,
  SendAppendEntries,
  SendRequestVote,
  WaitForMessage,
} from './messages.js'
import type { NodeMessaging } from './messaging.js'
import type { NodeStatePersistence } from './persistence.js'
import { createNodeState, NodeState, StateMachine } from './state.js'
import type { NodeId, WaitForElectionTimeout, WaitForHeartbeatInterval } from './types.js'

export type ActiveNodeBehavior = 'leader' | 'candidate' | 'follower'

export async function runNode(
  nodeId: number,
  otherNodeIds: number[],
  initialState: NodeState,
  stateMachine: StateMachine,
  waitForMessage: WaitForMessage,
  waitForHeartbeatInterval: WaitForHeartbeatInterval,
  waitForElectionTimeout: WaitForElectionTimeout,
  sendRequestVote: SendRequestVote,
  sendAppendEntries: SendAppendEntries,
  onUpdateState: (state: NodeState) => Promise<NodeState>,
  onChangeBehavior: (activeBehavior: ActiveNodeBehavior) => void,
  token: CancellationToken,
  activeBehavior: ActiveNodeBehavior = 'follower',
) {
  let currentState = initialState

  let cachedMessage: MessageWithResponseChannel<RaftMessage, unknown> | undefined
  const waitForMessageWithCache = <TMessage extends RaftMessage, TResponse>(
    token: CancellationToken,
  ): Promise<MessageWithResponseChannel<TMessage, TResponse>> => {
    if (cachedMessage) {
      const result = Promise.resolve(cachedMessage as MessageWithResponseChannel<TMessage, TResponse>)
      cachedMessage = undefined
      return result
    }

    return waitForMessage<TMessage, TResponse>(token)
  }

  while (!token.isCancelled) {
    const [updatedState, behaviorChange] = await switch$(activeBehavior, {
      leader: () =>
        runLeader(
          nodeId,
          currentState,
          stateMachine,
          otherNodeIds,
          waitForMessageWithCache,
          waitForHeartbeatInterval,
          sendAppendEntries,
          onUpdateState,
          token,
        ),
      candidate: () =>
        runCandidate(
          nodeId,
          currentState,
          otherNodeIds,
          waitForElectionTimeout,
          waitForMessageWithCache,
          sendRequestVote,
          onUpdateState,
          token,
        ),
      follower: () =>
        runFollower(currentState, stateMachine, waitForElectionTimeout, waitForMessageWithCache, onUpdateState, token),
    })

    if (token.isCancelled) {
      break
    }

    currentState = updatedState

    activeBehavior = taggedUnion.switch(behaviorChange, {
      BecomeLeader: () => 'leader' as ActiveNodeBehavior,
      BecomeCandidate: () => 'candidate' as ActiveNodeBehavior,
      BecomeFollower: () => 'follower' as ActiveNodeBehavior,
    })

    if (becomeFollower.is(behaviorChange)) {
      cachedMessage = behaviorChange.triggeringMessage
    }

    onChangeBehavior(activeBehavior)
  }
}

export interface NodeOptions<TState, TCommand> {
  nodeId: NodeId
  otherNodeIds: NodeId[]
  electionTimeout: Timespan
  heartbeatInterval: Timespan
  stateMachineFactory: () => StateMachine<TState, TCommand>
  statePersistence: NodeStatePersistence
  messaging: NodeMessaging
  onUpdateState?: (state: NodeState) => void
  onUpdateBehavior?: (behavior: ActiveNodeBehavior) => void
  onUpdateIsRunning?: (isRunning: boolean) => void
}

export interface Node<TState, TCommand> {
  isRunning: boolean

  start: () => void
  stop: () => void
  executeCommand: (command: TCommand) => Promise<void>
  getState: () => Promise<TState>
}

export function createNode<TState, TCommand>({
  nodeId,
  otherNodeIds,
  electionTimeout,
  heartbeatInterval,
  stateMachineFactory,
  statePersistence,
  messaging,
  onUpdateState,
  onUpdateBehavior,
  onUpdateIsRunning,
}: NodeOptions<TState, TCommand>): Node<TState, TCommand> {
  let stop: (() => void) | undefined

  return {
    start: () => {
      if (stop) {
        throw new Error(`node ${nodeId} is already started`)
      }

      stop = runInstance()
      onUpdateIsRunning?.(true)
    },

    stop: () => {
      stop?.()
      stop = undefined
      onUpdateIsRunning?.(false)
    },

    executeCommand: () => new Promise<void>(() => void 0),
    getState: () => new Promise<TState>(() => void 0),

    get isRunning() {
      return !!stop
    },
  }

  function runInstance() {
    const { token, cancel } = cancellationToken.createSource()

    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    run()

    return cancel

    async function run() {
      const persistentState = await statePersistence.get(nodeId)
      const nodeState = createNodeState(persistentState || {})

      await runNode(
        nodeId,
        otherNodeIds,
        nodeState,
        stateMachineFactory(),
        (t) => messaging.waitForMessage(nodeId, t),
        wait(heartbeatInterval),
        wait(electionTimeout),
        messaging.sendMessage,
        messaging.sendMessage,
        storeState,
        onUpdateBehavior || (() => void 0),
        token,
      )
    }
  }

  async function storeState(state: NodeState) {
    await statePersistence.put(nodeId, { currentTerm: state.currentTerm, log: state.log, votedFor: state.votedFor })
    onUpdateState?.(state)
    return state
  }
}
