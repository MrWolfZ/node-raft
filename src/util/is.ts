import { hasProperty } from './has-property.js'

export function is<T, TKey extends keyof T>(
  instance: unknown,
  propertyName: TKey,
  propertyValue?: T[TKey],
): instance is T {
  return hasProperty(instance as T, propertyName) && (!propertyValue || (instance as T)[propertyName] === propertyValue)
}
