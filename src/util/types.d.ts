/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/ban-types */

// export something to make this file a module
export class _PlatformTypesModule {}

declare global {
  type NonFunctionPropertyNames<T> = { [K in keyof T]: T[K] extends Function ? never : K }[keyof T]
  type NonFunctionProperties<T> = Pick<T, NonFunctionPropertyNames<T>> extends T
    ? T
    : Pick<T, NonFunctionPropertyNames<T>>
  type NonUndefined<T> = Exclude<T, undefined>
  type Exact<A, B = {}> = A & Record<keyof Omit<B, keyof A>, never>
  type ExactParam<T, TParam> = TParam extends AtomicObject ? TParam : TParam & Exact<T, TParam>
  type VoidIfUnknown<T> = T extends any ? T : unknown extends T ? void : T

  type AtomicObject = Promise<unknown> | Date | RegExp | boolean | number | string | null | undefined | void

  type PromiseOrT<T> = T | Promise<T>
  type UnwrapPromise<T> = T extends Promise<infer U> ? U : T
}
