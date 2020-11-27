import type { Dispose } from '../util/disposable.js'
import { timespan, Timespan } from '../util/timespan.js'
import { Client, createClient } from './client.js'
import type { NodeMessaging } from './messaging.js'
import { ActiveNodeBehavior, createNode, Node } from './node.js'
import type { NodeStatePersistence } from './persistence.js'
import type { NodeState, StateMachine } from './state.js'
import type { NodeId } from './types.js'

export interface ClusterOptions<TState, TCommand> {
  numberOfNodes: number
  electionTimeoutMin: Timespan
  electionTimeoutMax: Timespan
  heartbeatInterval: Timespan
  stateMachineFactory: (nodeId: NodeId) => StateMachine<TState, TCommand>
  statePersistence: NodeStatePersistence
  messaging: NodeMessaging
  onUpdateState?: (nodeId: NodeId, state: NodeState) => void
  onUpdateBehavior?: (nodeId: NodeId, behavior: ActiveNodeBehavior) => void
  onUpdateIsRunning?: (nodeId: NodeId, isRunning: boolean) => void
}

export interface Cluster<TState, TCommand> {
  start: () => Dispose
  toggleNode: (nodeId: NodeId) => boolean
  createClient: () => Client<TState, TCommand>
}

export function createCluster<TState, TCommand>({
  numberOfNodes,
  electionTimeoutMin,
  electionTimeoutMax,
  heartbeatInterval,
  stateMachineFactory,
  statePersistence,
  messaging,
  onUpdateState,
  onUpdateBehavior,
  onUpdateIsRunning,
}: ClusterOptions<TState, TCommand>): Cluster<TState, TCommand> {
  const nodeIds = Array.from({ length: numberOfNodes }).map((_, idx) => idx + 1)
  const nodes = {} as Record<NodeId, Node<TState, TCommand>>

  for (const nodeId of nodeIds) {
    const otherNodeIds = nodeIds.filter((id) => id !== nodeId)
    nodes[nodeId] = createNode({
      nodeId,
      otherNodeIds,
      electionTimeout: getElectionTimeout(),
      heartbeatInterval,
      stateMachineFactory: () => stateMachineFactory(nodeId),
      statePersistence,
      messaging,
      onUpdateState: (state) => onUpdateState?.(nodeId, state),
      onUpdateBehavior: (behavior) => onUpdateBehavior?.(nodeId, behavior),
      onUpdateIsRunning: (isRunning) => onUpdateIsRunning?.(nodeId, isRunning),
    })
  }

  return {
    start: () => {
      for (const node of Object.values(nodes)) {
        node.start()
      }

      return () => {
        for (const node of Object.values(nodes)) {
          node.stop()
        }
      }
    },

    toggleNode: (nodeId: NodeId) => {
      const node = nodes[nodeId]
      node.isRunning ? node.stop() : node.start()
      return node.isRunning
    },

    createClient: () => createClient({ nodeIds, messaging }),
  }

  function getElectionTimeout() {
    const electionMinMs = timespan.totalMilliseconds(electionTimeoutMin)
    const electionMaxMs = timespan.totalMilliseconds(electionTimeoutMax)
    const electionTimeoutMs = Math.random() * (electionMaxMs - electionMinMs) + electionMinMs
    return timespan.fromMilliseconds(electionTimeoutMs)
  }
}
