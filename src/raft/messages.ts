import type { CancellationToken } from '../util/cancellation-token.js'
import { taggedUnion } from '../util/tagged-unions.js'
import type { LogEntry } from './state.js'
import type { LogIndex, NodeId, Term } from './types.js'

export type RaftMessage = RequestVote | AppendEntries | ExecuteCommand | GetState

export type MessageResponseType<TMessage extends RaftMessage> = TMessage extends RequestVote
  ? RequestVoteResponse
  : TMessage extends AppendEntries
  ? AppendEntriesResponse
  : TMessage extends ExecuteCommand
  ? ExecuteCommandResponse | RedirectClient | NoLeaderError
  : TMessage extends GetState
  ? GetStateResponse | RedirectClient | NoLeaderError
  : never

export type MessageHandlerResponseType<TMessage extends RaftMessage> = TMessage extends RequestVote
  ? RequestVoteResponse
  : TMessage extends AppendEntries
  ? AppendEntriesResponse
  : TMessage extends ExecuteCommand
  ? WaitForLogReplication | RedirectClient | NoLeaderError
  : TMessage extends GetState
  ? GetCommittedState | RedirectClient | NoLeaderError
  : never

export interface RequestVote {
  $type: 'RequestVote'
  term: Term
  candidateId: NodeId
  lastLogIndex: LogIndex
  lastLogTerm: Term
}

export interface RequestVoteResponse {
  $type: 'RequestVoteResponse'
  term: Term
  voteGranted: boolean
}

export interface AppendEntries {
  $type: 'AppendEntries'
  term: Term
  leaderId: NodeId
  prevLogIndex: LogIndex
  prevLogTerm: Term
  entries: LogEntry[]
  leaderCommit: LogIndex
}

export interface AppendEntriesResponse {
  $type: 'AppendEntriesResponse'
  term: Term
  success: boolean
}

export interface ExecuteCommand<TCommand = unknown> {
  $type: 'ExecuteCommand'
  command: TCommand
}

export interface WaitForLogReplication {
  $type: 'WaitForLogReplication'
  logIndex: LogIndex
}

export interface ExecuteCommandResponse {
  $type: 'ExecuteCommandResponse'
}

export interface GetState {
  $type: 'GetState'
}

export interface GetCommittedState {
  $type: 'GetCommittedState'
}

export interface GetStateResponse<TState = unknown> {
  $type: 'GetStateResponse'
  state: TState
}

export interface RedirectClient {
  $type: 'RedirectClient'
  leaderId: NodeId
}

export interface NoLeaderError {
  $type: 'NoLeaderError'
}

type ExecuteCommandFactory = <TCommand>(payload: { command: TCommand }) => ExecuteCommand<TCommand>
type GetStateResponseFactory = <TState>(payload: { state: TState }) => GetStateResponse<TState>

export const messages = {
  requestVote: taggedUnion.create<RequestVote>('RequestVote'),
  requestVoteResponse: taggedUnion.create<RequestVoteResponse>('RequestVoteResponse'),
  appendEntries: taggedUnion.create<AppendEntries>('AppendEntries'),
  appendEntriesResponse: taggedUnion.create<AppendEntriesResponse>('AppendEntriesResponse'),
  executeCommand: taggedUnion.create<ExecuteCommand, ExecuteCommandFactory>('ExecuteCommand'),
  executeCommandResponse: taggedUnion.create<ExecuteCommandResponse>('ExecuteCommandResponse'),
  waitForLogReplication: taggedUnion.create<WaitForLogReplication>('WaitForLogReplication'),
  getState: taggedUnion.create<GetState>('GetState'),
  getStateResponse: taggedUnion.create<GetStateResponse, GetStateResponseFactory>('GetStateResponse'),
  getCommittedState: taggedUnion.create<GetCommittedState>('GetCommittedState'),
  redirectClient: taggedUnion.create<RedirectClient>('RedirectClient'),
  noLeaderError: taggedUnion.create<NoLeaderError>('NoLeaderError'),
}

export type SendRequestVote = SendMessage<RequestVote, RequestVoteResponse>
export type SendAppendEntries = SendMessage<AppendEntries, AppendEntriesResponse>

export type SendMessage<TMessage extends RaftMessage, TResponse> = (
  nodeId: NodeId,
  message: TMessage,
  cancellationToken: CancellationToken,
) => Promise<TResponse>

export interface MessageWithResponseChannel<TMessage extends RaftMessage, TResponse> {
  message: TMessage
  respond: ResponseChannel<TResponse>
  cancellationToken: CancellationToken
}

export type ResponseChannel<TResponse> = (response: TResponse) => void

export type WaitForMessage = <TMessage extends RaftMessage, TResponse>(
  cancellationToken: CancellationToken,
) => Promise<MessageWithResponseChannel<TMessage, TResponse>>
