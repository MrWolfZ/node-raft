import type { CancellationToken } from './cancellation-token.js'
import { timespan, Timespan } from './timespan.js'

export function wait(duration: Timespan): (token: CancellationToken) => Promise<void>
export function wait(duration: Timespan, token: CancellationToken): Promise<void>
export function wait(duration: Timespan, token?: CancellationToken) {
  if (!token) {
    return (t: CancellationToken) => wait(duration, t)
  }

  if (timespan.totalMilliseconds(duration) === 0) {
    return Promise.resolve()
  }

  return new Promise<void>((r) => {
    const timer = setTimeout(resolve, timespan.totalMilliseconds(duration))

    const unregister = token.register(() => clearTimeout(timer))

    function resolve() {
      unregister()
      r()
    }
  })
}
