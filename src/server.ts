import bodyParser from 'body-parser'
import compression from 'compression'
import errorHandler from 'errorhandler'
import express from 'express'
import lusca from 'lusca'
import { CONFIG } from './infrastructure/config.js'
import { log } from './infrastructure/logging.js'
import { combineDisposables, Dispose } from './util/disposable.js'
import { isFailure, isFailureWithMessages, isFailureWithPayload } from './util/result.js'

const host = express()

host.use(compression())
host.use(bodyParser.json())
host.use(bodyParser.urlencoded({ extended: true }))
host.use(lusca.xframe('SAMEORIGIN'))
host.use(lusca.xssProtection(true))
host.use(errorHandler())

const disposables: Dispose[] = []

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
