import { hasProperty } from './has-property.js'
import { ObjectSwitchMap, switchObject } from './switch.js'

export interface TaggedUnion<T extends string = string> {
  $type: T
}

export type TaggedUnionFactoryFn<TTaggedUnion extends TaggedUnion> = Exclude<keyof TTaggedUnion, '$type'> extends never
  ? () => TTaggedUnion
  : Exclude<keyof TTaggedUnion, '$type' | OptionalPropertyNames<TTaggedUnion>> extends never
  ? (payload?: TaggedUnionPayload<TTaggedUnion>) => TTaggedUnion
  : (payload: TaggedUnionPayload<TTaggedUnion>) => TTaggedUnion

export type TaggedUnionPayload<TTaggedUnion> = Omit<TTaggedUnion, '$type'>

export type TaggedUnionFactory<
  TTaggedUnion extends TaggedUnion,
  TFactoryFn = TaggedUnionFactoryFn<TTaggedUnion>
> = TFactoryFn & {
  is: (value: unknown) => value is TTaggedUnion
}

export function create<TTaggedUnion extends TaggedUnion, TFactoryFn = TaggedUnionFactoryFn<TTaggedUnion>>(
  $type: TTaggedUnion['$type'],
): TaggedUnionFactory<TTaggedUnion, TFactoryFn> {
  const fn = (((payload?: TaggedUnionPayload<TTaggedUnion>) =>
    ({ $type, ...payload } as TTaggedUnion)) as unknown) as TFactoryFn

  const factory = fn as TaggedUnionFactory<TTaggedUnion, TFactoryFn>

  factory.is = (value: unknown): value is TTaggedUnion =>
    hasProperty(value as TaggedUnion, '$type') && (value as TaggedUnion).$type === $type

  return factory
}

export function switch$<
  TTaggedUnion extends TaggedUnion,
  TSwitchMap extends ObjectSwitchMap<TTaggedUnion, '$type', TTaggedUnion['$type']>
>(value: TTaggedUnion, switchMap: TSwitchMap) {
  return switchObject(value, '$type', switchMap)
}

export const taggedUnion = {
  create,
  switch: switch$,
}
