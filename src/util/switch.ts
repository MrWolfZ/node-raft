export type Switchable = string | number | symbol

export type SwitchableOrNever<T> = T extends Switchable ? T : never

export type ValueOfType<TSwitch, TKeyProp extends keyof TSwitch, TCase extends Switchable> = TSwitch extends {
  [key in TKeyProp]: TCase
}
  ? TSwitch
  : never

export type SwitchMap<TSwitch extends Switchable> = {
  [case$ in TSwitch]: (value: case$) => unknown
}

export type ObjectSwitchMap<TSwitch, TKeyProp extends keyof TSwitch, TKey extends Switchable> = {
  [case$ in TKey]: (value: ValueOfType<TSwitch, TKeyProp, case$>) => unknown
}

export function switchObject<
  TSwitch,
  TKeyProp extends keyof TSwitch,
  TSwitchMap extends ObjectSwitchMap<TSwitch, TKeyProp, SwitchableOrNever<TSwitch[TKeyProp]>>
>(
  value: TSwitch,
  keyProp: TKeyProp,
  switchMap: TSwitchMap,
): ReturnType<TSwitchMap[SwitchableOrNever<TSwitch[TKeyProp]>]> {
  const fn = switchMap[value[keyProp] as SwitchableOrNever<TSwitch[TKeyProp]>]

  if (!fn) {
    throw new Error(`did not find switch function for key '${value[keyProp]}'`)
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return fn(value as any) as ReturnType<TSwitchMap[SwitchableOrNever<TSwitch[TKeyProp]>]>
}

export function switch$<TSwitch extends Switchable, TSwitchMap extends SwitchMap<TSwitch>>(
  value: TSwitch,
  switchMap: TSwitchMap,
): ReturnType<TSwitchMap[TSwitch]> {
  const res = switchMap[value]

  if (!res) {
    throw new Error(`did not find switch function for key '${value}'`)
  }

  return res(value as never) as ReturnType<TSwitchMap[TSwitch]>
}
