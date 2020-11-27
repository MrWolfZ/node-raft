import { blockingQueue, BlockingQueue } from '../util/blocking-queue.js'
import { cancellationToken, CancellationToken } from '../util/cancellation-token.js'
import { timespan, Timespan } from '../util/timespan.js'
import { wait } from '../util/wait.js'
import type { MessageResponseType, MessageWithResponseChannel, RaftMessage } from './messages.js'
import type { NodeId } from './types.js'

export interface NodeMessaging {
  waitForMessage: <TMessage extends RaftMessage, TResponse>(
    nodeId: NodeId,
    token: CancellationToken,
  ) => Promise<MessageWithResponseChannel<TMessage, TResponse>>

  sendMessage: <TMessage extends RaftMessage>(
    nodeId: NodeId,
    message: TMessage,
    token: CancellationToken,
  ) => Promise<MessageResponseType<TMessage>>
}

export interface InMemoryNodeMessagingOptions {
  messageTimeout?: Timespan
  messageDelay?: Timespan
}

export function createInMemoryNodeMessaging({
  messageTimeout,
  messageDelay,
}: InMemoryNodeMessagingOptions): NodeMessaging {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const nodeMailboxes = new Map<NodeId, BlockingQueue<MessageWithResponseChannel<any, any>>>()

  return {
    waitForMessage: (nodeId, token) => addOrGetMailbox(nodeId).dequeue(token),
    sendMessage,
  }

  async function sendMessage<TMessage extends RaftMessage>(
    nodeId: NodeId,
    message: TMessage,
    token: CancellationToken,
  ) {
    const timeoutSource = cancellationToken.createSource()

    if (messageTimeout) {
      timeoutSource.cancelAfter(messageTimeout)
    }

    await wait(messageDelay || timespan.fromMilliseconds(0), token)

    return await new Promise<MessageResponseType<TMessage>>((resolve, reject) => {
      const unregister = timeoutSource.token.register(() => reject('timeout'))

      function respond(response: MessageResponseType<TMessage>) {
        unregister()
        timeoutSource.cancel()
        resolve(response)
      }

      const messageWithResponseChannel: MessageWithResponseChannel<TMessage, MessageResponseType<TMessage>> = {
        message,
        respond,
        cancellationToken: token,
      }

      const mailbox = addOrGetMailbox(nodeId)
      mailbox.enqueue(messageWithResponseChannel, token)
    })
  }

  function addOrGetMailbox(nodeId: NodeId) {
    let mailbox = nodeMailboxes.get(nodeId)

    if (!mailbox) {
      mailbox = blockingQueue.create()
      nodeMailboxes.set(nodeId, mailbox)
    }

    return mailbox
  }
}
