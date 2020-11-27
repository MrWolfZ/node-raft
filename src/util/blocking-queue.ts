import { cancellationToken, CancellationToken } from './cancellation-token.js'
import type { Dispose } from './disposable.js'

export interface BlockingQueue<T> {
  enqueue: (item: T, token?: CancellationToken) => void
  dequeue: (token: CancellationToken) => Promise<T>
}

export function create<T>(): BlockingQueue<T> {
  type Entry = { item: T; cleanup: Dispose }
  const entries: Entry[] = []

  let resolve: ((item: T) => void) | undefined

  return {
    enqueue: (item, token) => {
      if (resolve) {
        const r = resolve
        resolve = undefined
        r(item)
        return
      }

      const entry: Entry = { item, cleanup: () => void 0 }

      const unregister = (token || cancellationToken.never).register(() => {
        const idx = entries.indexOf(entry)

        if (idx >= 0) {
          entries.splice(idx, 1)
        }
      })

      entry.cleanup = unregister

      entries.push(entry)
    },

    dequeue: (token) => {
      if (entries.length > 0) {
        const { item, cleanup } = entries.shift() as Entry
        cleanup()
        return Promise.resolve(item)
      }

      if (resolve) {
        return Promise.reject('cannot dequeue multiple times in parallel')
      }

      return new Promise((r) => {
        const unregister = token.register(() => {
          resolve = undefined
        })

        resolve = (item) => {
          unregister()
          r(item)
        }
      })
    },
  }
}

export const blockingQueue = {
  create,
}
