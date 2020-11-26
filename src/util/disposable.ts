export type Dispose = () => void

export function combineDisposables(...disposables: Dispose[]): Dispose {
  return () => {
    for (const disposable of disposables) {
      disposable()
    }
  }
}
