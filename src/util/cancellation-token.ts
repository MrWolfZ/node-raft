import type { Dispose } from './disposable.js'
import { timespan, Timespan } from './timespan.js'

export const CANCEL_PROMISE = Symbol()

export interface CancellationToken {
  readonly register: (fn: () => void) => Dispose
  readonly isCancelled: boolean
}

export type CancellationTokenSource = ReturnType<typeof createSource>

export function createSource(...parentTokens: CancellationToken[]) {
  let isCancelled = false
  const callbacks = new Set<() => void>()

  const token: CancellationToken = {
    get isCancelled() {
      return isCancelled
    },

    register: (fn) => {
      callbacks.add(fn)
      return () => callbacks.delete(fn)
    },
  }

  function cancel() {
    isCancelled = true

    for (const callback of callbacks) {
      callback()
    }

    callbacks.clear()
  }

  for (const parentToken of parentTokens) {
    callbacks.add(parentToken.register(cancel))
  }

  function cancelAfter(duration: Timespan) {
    const timer = setTimeout(cancel, timespan.totalMilliseconds(duration))
    const dispose = () => clearTimeout(timer)
    callbacks.add(dispose)

    return dispose
  }

  return {
    token,
    cancel,
    cancelAfter,
  }
}

export function combine(...tokens: CancellationToken[]) {
  return createSource(...tokens).token
}

export const never: CancellationToken = {
  isCancelled: false,
  register: () => () => void 0,
}

export const cancellationToken = {
  createSource,
  combine,
  never,
}
