import bodyParser from 'body-parser'
import compression from 'compression'
import errorHandler from 'errorhandler'
import express from 'express'
import lusca from 'lusca'
import { CONFIG } from './infrastructure/config.js'
import { log } from './infrastructure/logging.js'
import { createCluster } from './raft/cluster.js'
import { createInMemoryNodeMessaging } from './raft/messaging.js'
import { createInMemoryNodeStatePersistence } from './raft/persistence.js'
import type { StateMachine } from './raft/state.js'
import type { NodeId } from './raft/types.js'
import { cancellationToken } from './util/cancellation-token.js'
import { combineDisposables, Dispose } from './util/disposable.js'
import { isFailure, isFailureWithMessages, isFailureWithPayload } from './util/result.js'
import { timespan } from './util/timespan.js'

const host = express()

host.use(compression())
host.use(bodyParser.json())
host.use(bodyParser.urlencoded({ extended: true }))
host.use(lusca.xframe('SAMEORIGIN'))
host.use(lusca.xssProtection(true))
host.use(errorHandler())

const disposables: Dispose[] = []

function createAddingStateMachine(nodeId: NodeId): StateMachine<number, number> {
  let sum = 0

  return {
    applyCommand: (n) => {
      sum += n
      log.named(`node ${nodeId}`).info(`updated state machine sum to ${sum}`)
    },
    getState: () => sum,
  }
}

const numberOfNodes = 3
const cluster = createCluster({
  numberOfNodes,
  electionTimeoutMin: timespan.fromMilliseconds(150),
  electionTimeoutMax: timespan.fromMilliseconds(300),
  heartbeatInterval: timespan.fromMilliseconds(50),
  stateMachineFactory: createAddingStateMachine,
  statePersistence: createInMemoryNodeStatePersistence(),
  messaging: createInMemoryNodeMessaging({
    messageDelay: timespan.fromMilliseconds(10),
    messageTimeout: timespan.fromMilliseconds(50),
  }),

  onUpdateBehavior: (nodeId, behavior) => {
    log.named(`node ${nodeId}`).info(`changed behavior to ${behavior}`)
  },

  onUpdateIsRunning: (nodeId, isRunning) => {
    log.named(`node ${nodeId}`).info(isRunning ? 'starting...' : 'stopping...')
  },
})

disposables.push(cluster.start())

const api = express()

api.post('/toggle/:id', (req, res) => {
  const isRunning = cluster.toggleNode(parseInt(req.params.id, 10))
  res.send({ isRunning })
})

api.get('/state', async (_, res) => {
  const { token, cancelAfter } = cancellationToken.createSource()
  const dispose = cancelAfter(timespan.fromSeconds(1))

  try {
    const state = await cluster.createClient().getState(token)
    res.send({ state })
  } finally {
    dispose()
  }
})

api.post('/add/:n', async (req, res) => {
  const { token, cancelAfter } = cancellationToken.createSource()
  const dispose = cancelAfter(timespan.fromSeconds(1))

  try {
    const state = await cluster.createClient().executeCommand(parseInt(req.params.n, 10), token)
    res.send({ state })
  } finally {
    dispose()
  }
})

host.use('/api', api)

try {
  let isDisposed = false

  const terminate = () => {
    if (isDisposed) {
      return
    }

    log.info('shutting down...')

    isDisposed = true
    combineDisposables(...disposables)()
  }

  process.once('SIGUSR2', terminate)
  process.once('SIGTERM', terminate)
  process.once('SIGINT', terminate)
} catch (error) {
  const isProduction = CONFIG.environment === 'production'

  let messages: readonly string[] = [isProduction ? 'an unknown error occured' : JSON.stringify(error)]
  let stackTrace: string | undefined

  if (error instanceof Error) {
    messages = [error.message]
    stackTrace = isProduction ? undefined : error.stack
  } else if (isFailureWithMessages(error)) {
    messages = error.messages
  } else if (isFailureWithPayload(error)) {
    messages = [JSON.stringify(error.payload)]
  }

  if (isFailure(error)) {
    stackTrace = isProduction ? undefined : error.$stackTrace
  }

  log.error(`an error occured during initialization\n${messages}\n${stackTrace}`)
}

export const server = host.listen(CONFIG.port, CONFIG.hostnameToBind, () => {
  log.info(`app is running at http://${CONFIG.hostnameToBind}:${CONFIG.port} in ${CONFIG.environment} mode`)
  log.info('press CTRL-C to stop')

  disposables.push(() => server.close())
})
