export default {
  preset: 'ts-jest',
  globals: {
    'ts-jest': {
      tsconfig: './tsconfig.test.json',
    },
  },
  roots: ['<rootDir>/src'],
  transform: {
    '^.+\\.tsx?$': './jest.transform.cjs',
  },
  testMatch: ['**/*.test.ts'],
  reporters: [
    '@jest/reporters/build/SummaryReporter.js',
    ['jest-silent-reporter', { useDots: true, showWarnings: true }],
  ],
  setupFilesAfterEnv: ['./jest.setup.js'],
}
