import winston from 'winston'

const { createLogger, format, transports } = winston

const logger = createLogger({
  level: 'info',
})

if (process.env.NODE_ENV !== 'production') {
  logger.add(
    new transports.Console({
      format: format.combine(
        format.colorize(),
        format.timestamp(),
        format.ms(),
        format.printf((info) => {
          let prefix = `${info.timestamp}|${info.level.padEnd(15, ' ')}`

          if (info.name) {
            prefix += `|${info.name} ${info.ms}`
          }

          return `${prefix}|${info.message}`
        }),
      ),
    }),
  )
}

export const log = {
  debug: (msg: string) => logger.debug(msg),
  info: (msg: string) => logger.info(msg),
  error: (msg: string) => logger.error(msg),

  named: (name: string) => {
    const child = logger.child({ name })
    return {
      debug: (msg: string) => child.debug(msg),
      info: (msg: string) => child.info(msg),
      error: (msg: string) => child.error(msg),
    }
  },
}

export type RootLogger = typeof log
