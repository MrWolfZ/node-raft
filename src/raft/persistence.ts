import type { PersistentNodeState } from './state.js'
import type { NodeId } from './types.js'

export interface NodeStatePersistence {
  get: (nodeId: NodeId) => Promise<PersistentNodeState | undefined>
  put: (nodeId: NodeId, state: PersistentNodeState) => Promise<void>
}

export function createInMemoryNodeStatePersistence(): NodeStatePersistence {
  const nodeStates = {} as Record<NodeId, PersistentNodeState>

  return {
    get: (nodeId) => Promise.resolve(nodeStates[nodeId]),
    put: (nodeId, state) => {
      nodeStates[nodeId] = state
      return Promise.resolve()
    },
  }
}
