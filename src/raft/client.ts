import type { CancellationToken } from '../util/cancellation-token.js'
import { messages } from './messages.js'
import type { NodeMessaging } from './messaging.js'
import type { NodeId } from './types.js'

export interface ClientOptions {
  nodeIds: NodeId[]
  messaging: NodeMessaging
}

export interface Client<TState, TCommand> {
  executeCommand: (command: TCommand, token: CancellationToken) => Promise<void>
  getState: (token: CancellationToken) => Promise<TState>
}

export function createClient<TState, TCommand>({ nodeIds, messaging }: ClientOptions): Client<TState, TCommand> {
  let leaderId = nodeIds[0]

  return {
    executeCommand,
    getState,
  }

  async function executeCommand(command: TCommand, token: CancellationToken): Promise<void> {
    try {
      const result = await messaging.sendMessage(leaderId, messages.executeCommand({ command }), token)

      if (messages.noLeaderError.is(result)) {
        useNextNodeAsLeader()
        return executeCommand(command, token)
      }

      if (messages.redirectClient.is(result)) {
        leaderId = result.leaderId
        return executeCommand(command, token)
      }
    } catch {
      useNextNodeAsLeader()
      return executeCommand(command, token)
    }
  }

  async function getState(token: CancellationToken): Promise<TState> {
    try {
      const result = await messaging.sendMessage(leaderId, messages.getState(), token)

      if (messages.noLeaderError.is(result)) {
        useNextNodeAsLeader()
        return getState(token)
      }

      if (messages.redirectClient.is(result)) {
        leaderId = result.leaderId
        return getState(token)
      }

      return result.state as TState
    } catch {
      useNextNodeAsLeader()
      return getState(token)
    }
  }

  function useNextNodeAsLeader() {
    const currentLeaderIdx = nodeIds.indexOf(leaderId)
    const nextLeaderIdx = currentLeaderIdx === nodeIds.length - 1 ? 0 : currentLeaderIdx + 1
    leaderId = nodeIds[nextLeaderIdx]
  }
}
