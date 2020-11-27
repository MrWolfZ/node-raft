export interface Timespan {
  days?: number
  hours?: number
  minutes?: number
  seconds?: number
  milliseconds?: number
}

export const timespan = {
  totalMilliseconds: ({ days, hours, minutes, seconds, milliseconds }: Timespan) => {
    const totalHours = (days || 0) * 24 + (hours || 0)
    const totalMinutes = totalHours * 60 + (minutes || 0)
    const totalSeconds = totalMinutes * 60 + (seconds || 0)
    return totalSeconds * 1000 + (milliseconds || 0)
  },

  fromDays: (days: number): Timespan => {
    return timespan.fromHours(days * 24)
  },

  fromHours: (hours: number): Timespan => {
    return timespan.fromMinutes(hours * 60)
  },

  fromMinutes: (minutes: number): Timespan => {
    return timespan.fromSeconds(minutes * 60)
  },

  fromSeconds: (seconds: number): Timespan => {
    return timespan.fromMilliseconds(seconds * 1000)
  },

  fromMilliseconds: (totalMilliseconds: number): Timespan => {
    totalMilliseconds = Math.round(totalMilliseconds)

    const milliseconds = totalMilliseconds % 1000
    const totalSeconds = (totalMilliseconds - milliseconds) / 1000
    const seconds = totalSeconds % 60
    const totalMinutes = (totalSeconds - seconds) / 60
    const minutes = totalMinutes % 60
    const totalHours = (totalMinutes - minutes) / 60
    const hours = totalHours % 24
    const days = (totalHours - hours) / 24

    return {
      days,
      hours,
      minutes,
      seconds,
      milliseconds,
    }
  },

  format: ({ days, hours, minutes, seconds, milliseconds }: Timespan) => {
    let result = ''

    if (days || 0 > 0) {
      result += `${days}d `
    }

    if (hours || 0 > 0) {
      result += `${hours}h `
    }

    if (minutes || 0 > 0) {
      result += `${minutes}m `
    }

    if (seconds || 0 > 0) {
      result += `${seconds}s `
    }

    if (milliseconds || 0 > 0) {
      result += `${milliseconds}ms`
    }

    return (result || '0ms').trim()
  },
}
