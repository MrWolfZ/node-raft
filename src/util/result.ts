/* eslint-disable @typescript-eslint/ban-types */

import { hasProperty } from './has-property.js'

const SUCCESS = Symbol('success')
const FAILURE = Symbol('failure')

interface SuccessBase {
  readonly [SUCCESS]: true
}

interface WithPayload<T> {
  readonly payload: T
}

type SuccessPayload<TSuccess> = TSuccess extends void | undefined
  ? {}
  : TSuccess extends AtomicObject | readonly unknown[]
  ? WithPayload<TSuccess>
  : TSuccess

interface FailureBase {
  readonly [FAILURE]: true
  readonly $stackTrace: string | undefined
}

type FailurePayload<TFailure> = TFailure extends void | undefined
  ? {}
  : TFailure extends AtomicObject | readonly unknown[]
  ? WithPayload<TFailure>
  : TFailure

export type Success<TSuccess = void> = SuccessBase & SuccessPayload<TSuccess>

export type Failure<TFailure = void> = FailureBase & FailurePayload<TFailure>

export type Result<TSuccess = void, TFailure = void> = Success<TSuccess> | Failure<TFailure>

export function success(): Success
export function success<TSuccess>(value: TSuccess): Success<TSuccess>
export function success<TSuccess>(value?: TSuccess): Success<TSuccess> {
  if (value === undefined) {
    return { [SUCCESS]: true } as Success<TSuccess>
  }

  if (isAtomicObject(value) || Array.isArray(value)) {
    return ({ [SUCCESS]: true, payload: value } as unknown) as Success<TSuccess>
  }

  return ({ [SUCCESS]: true, ...value } as unknown) as Success<TSuccess>
}

export function failure(): Failure
export function failure<TFailure>(value: TFailure): Failure<TFailure>
export function failure<TFailure>(value?: TFailure): Failure<TFailure> {
  let stackTrace: string | undefined

  if (Error.captureStackTrace) {
    const stackHolder = { stack: '' }
    Error.captureStackTrace(stackHolder, failure)
    stackTrace = stackHolder.stack
  } else {
    stackTrace = new Error().stack
  }

  const [, , ...stackTraceLines] = stackTrace?.split(/\r?\n/) || []

  stackTrace = ['failure', ...stackTraceLines].join('\n')

  const result: FailureBase = {
    [FAILURE]: true,
    $stackTrace: stackTrace,
  }

  if (value === undefined) {
    return result as Failure<TFailure>
  }

  if (isAtomicObject(value) || Array.isArray(value)) {
    return ({ ...result, payload: value } as unknown) as Failure<TFailure>
  }

  return ({ ...result, ...value } as unknown) as Failure<TFailure>
}

interface WithMessages {
  readonly messages: readonly string[]
}

export function failureMessage(message: string) {
  return failure<WithMessages>({ messages: [message] })
}

export function failureMessages(messages: string[]) {
  return failure<WithMessages>({ messages })
}

export function matchResult<TResult, TSuccess = void, TFailure = void>(
  result: Result<TSuccess, TFailure>,
  onSuccess: (value: TSuccess) => TResult,
  onFailure: (value: TFailure) => TResult,
): TResult {
  return isSuccess(result)
    ? onSuccess(hasPayload<TSuccess>(result) ? result.payload : (result as TSuccess))
    : onFailure(hasPayload<TFailure>(result) ? result.payload : (result as TFailure))
}

export function isResult<TSuccess = void, TFailure = void>(object: unknown): object is Result<TSuccess, TFailure> {
  return isSuccess(object) || isFailure(object)
}

export function isSuccess<TSuccess>(obj: unknown): obj is Success<TSuccess> {
  return !!obj && hasProperty(obj as Success, SUCCESS)
}

export function isFailure<TFailure>(obj: unknown): obj is Failure<TFailure> {
  return !!obj && hasProperty(obj as Failure, FAILURE)
}

export function isFailureWithPayload(obj: unknown): obj is Failure<WithPayload<unknown>> {
  return (
    isFailure(obj) &&
    hasProperty(obj as Failure<WithPayload<unknown>>, 'payload') &&
    Array.isArray((obj as Failure<WithPayload<unknown>>).payload)
  )
}

export function isFailureWithMessages(obj: unknown): obj is Failure<WithMessages> {
  return (
    isFailure(obj) &&
    hasProperty(obj as Failure<WithMessages>, 'messages') &&
    Array.isArray((obj as Failure<WithMessages>).messages)
  )
}

export function unwrap<TSuccess>(result: TSuccess | Result<TSuccess, unknown>): TSuccess {
  if (!isResult<TSuccess, unknown>(result)) {
    return result
  }

  if (isSuccess(result)) {
    return result as TSuccess
  }

  throw failureMessage(`cannot unwrap a failure! payload: ${result}`)
}

export function combineResults<TSuccess = void>(
  ...results: (Result<TSuccess, string> | Result<TSuccess, WithMessages>)[]
): Result<TSuccess[], WithMessages> {
  if (results.every(isSuccess)) {
    return success(results.map(unwrap))
  }

  const messages = results
    .filter(isFailure)
    .map((f) => (hasPayload(f) ? f.payload : (f as WithMessages).messages))
    .map((v) => (Array.isArray(v) ? v : [v]))
    .reduce((a, b) => [...a, ...b], [])

  return failure<WithMessages>({ messages })
}

function hasPayload<T>(result: unknown): result is WithPayload<T> {
  return !!result && hasProperty(result as WithPayload<T>, 'payload')
}

function isAtomicObject(value: unknown): value is AtomicObject {
  const isUndefined = value === undefined
  const isNull = value === null
  const isPrimitive = ['string', 'boolean', 'number'].includes(typeof value)
  const isRegExp = value instanceof RegExp
  const isDate = value instanceof Date
  const isPromise = typeof (value as Promise<unknown>).then === 'function'
  return isUndefined || isNull || isPrimitive || isRegExp || isDate || isPromise
}

declare global {
  interface ErrorConstructor {
    // eslint-disable-next-line @typescript-eslint/ban-types
    captureStackTrace(targetObject: Object, constructorOpt?: Function): void
  }
}
