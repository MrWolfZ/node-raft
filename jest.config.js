module.exports = {
  globals: {
    'ts-jest': {
      babelConfig: true,
    },
  },
  roots: ['<rootDir>/src'],
  moduleFileExtensions: ['js', 'jsx', 'json', 'ts', 'tsx'],
  transform: {
    '^.+\\.tsx?$': 'ts-jest',
    '.+\\.(css|styl|less|sass|scss|svg|png|jpg|ttf|woff|woff2)$': 'jest-transform-stub',
    '^.+\\.jsx?$': 'babel-jest',
  },
  testMatch: ['src/**/*.test.ts'],
  reporters: [
    '@jest/reporters/build/summary_reporter.js',
    ['jest-silent-reporter', { useDots: true, showWarnings: true }],
  ],
}
