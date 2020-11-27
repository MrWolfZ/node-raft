import type { CancellationToken } from '../util/cancellation-token.js'

type NodeId = number
type Term = number
type LogIndex = number

type WaitForElectionTimeout = (token: CancellationToken) => Promise<void>
type WaitForHeartbeatInterval = (token: CancellationToken) => Promise<void>
