export function hasProperty<T>(instance: T, propertyName: keyof T) {
  return Object.prototype.hasOwnProperty.call(instance, propertyName as keyof T)
}
