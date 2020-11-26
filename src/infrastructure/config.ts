import dotenv from 'dotenv'
import fs from 'fs'
import path from 'path'

export interface AppConfig {
  readonly environment: 'development' | 'production'
  readonly hostnameToBind: string
  readonly port: number
}

const defaultConfig: AppConfig = {
  environment: 'development',
  hostnameToBind: '0.0.0.0',
  port: 3000,
}

export const CONFIG: AppConfig = {
  ...defaultConfig,
  ...parseEnv(),
}

export function parseEnv() {
  let rootDir = process.cwd()

  while (!fs.existsSync(path.join(rootDir, '.editorconfig'))) {
    rootDir = path.dirname(rootDir)
  }

  const envPath = path.join(rootDir, '.env')

  if (fs.existsSync(envPath)) {
    const result = dotenv.config({ path: envPath })

    if (result.parsed) {
      return result.parsed
    }

    if (result.error) {
      throw result.error
    }
  }

  return {}
}
