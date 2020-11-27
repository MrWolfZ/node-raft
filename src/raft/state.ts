import type { LogIndex, NodeId, Term } from './types.js'

export interface PersistentNodeState {
  readonly currentTerm: Term
  readonly votedFor: NodeId | null
  readonly log: LogEntry[]
}

export interface NodeState extends PersistentNodeState {
  readonly commitIndex: LogIndex
  readonly lastApplied: LogIndex
  readonly lastKnownLeaderId: NodeId | undefined
}

export const NOOP_COMMAND = '$no-op$'

export interface LogEntry<TCommand = unknown> {
  term: Term
  index: LogIndex
  command: TCommand | typeof NOOP_COMMAND
}

export const initialNodeState: NodeState = {
  currentTerm: 0,
  commitIndex: 0,
  lastApplied: 0,
  lastKnownLeaderId: undefined,
  log: [],
  votedFor: null,
}

export function createNodeState(partial: Partial<NodeState>): NodeState {
  return {
    ...initialNodeState,
    ...partial,
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface StateMachine<TState = any, TCommand = any> {
  applyCommand: (command: TCommand) => void
  getState: () => TState
}

export const noopStateMachine: StateMachine = {
  applyCommand: () => void 0,
  getState: () => void 0,
}
