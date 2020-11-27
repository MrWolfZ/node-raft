import { taggedUnion } from '../../util/tagged-unions.js'
import type { MessageWithResponseChannel, RaftMessage } from '../messages.js'

export type BehaviorChange = BecomeFollower | BecomeCandidate | BecomeLeader

export type BecomeFollower = {
  $type: 'BecomeFollower'
  triggeringMessage?: MessageWithResponseChannel<RaftMessage, unknown>
}

export type BecomeCandidate = {
  $type: 'BecomeCandidate'
}

export type BecomeLeader = {
  $type: 'BecomeLeader'
}

export const becomeFollower = taggedUnion.create<BecomeFollower>('BecomeFollower')
export const becomeCandidate = taggedUnion.create<BecomeCandidate>('BecomeCandidate')
export const becomeLeader = taggedUnion.create<BecomeLeader>('BecomeLeader')
