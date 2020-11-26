module.exports = {
  root: true,
  parser: '@typescript-eslint/parser',
  plugins: ['@typescript-eslint'],
  extends: [
    'eslint:recommended',
    'plugin:@typescript-eslint/eslint-recommended',
    'plugin:@typescript-eslint/recommended',
  ],
  ignorePatterns: ['bin/', 'node_modules/', '*.js', '*.generated.ts'],
  parserOptions: {
    project: require('path').join(__dirname, 'tsconfig.base.json'),
  },
  rules: {
    indent: ['error', 2, { SwitchCase: 1 }],
    quotes: [
      'error',
      'single',
      {
        avoidEscape: false,
        allowTemplateLiterals: true,
      },
    ],
    semi: ['error', 'never'],
    curly: 'error',
    'comma-dangle': ['error', 'always-multiline'],
    'no-multiple-empty-lines': ['error', { max: 1 }],
    'no-restricted-globals': [
      'warn',
      {
        name: 'event',
        message: 'Use local parameter instead.',
      },
      {
        name: 'name',
        message: 'Use local parameter instead.',
      },
      {
        name: 'fdescribe',
        message: 'Do not commit fdescribe. Use describe instead.',
      },
      {
        name: 'xdescribe',
        message: 'Do not commit xdescribe. Use describe instead.',
      },
      {
        name: 'fit',
        message: 'Do not commit fit. Use it instead.',
      },
      {
        name: 'xit',
        message: 'Do not commit xit. Use it instead.',
      },
    ],
    'require-await': 'error',
    'padding-line-between-statements': [
      'error',
      {
        blankLine: 'always',
        prev: 'block-like',
        next: '*',
      },
    ],
    'space-infix-ops': [
      'error',
      {
        int32Hint: true,
      },
    ],
    'object-curly-spacing': ['error', 'always'],
    'key-spacing': 'error',
    'max-len': [
      'error',
      {
        code: 120,
        ignoreStrings: true,
        ignoreTemplateLiterals: true,
        ignoreTrailingComments: true,
        ignoreUrls: true,
      },
    ],
    '@typescript-eslint/explicit-function-return-type': ['off'],
    '@typescript-eslint/explicit-module-boundary-types': ['off'],
    '@typescript-eslint/member-delimiter-style': [
      'error',
      {
        multiline: {
          delimiter: 'none',
        },
        singleline: {
          delimiter: 'semi',
          requireLast: false,
        },
      },
    ],
    '@typescript-eslint/no-use-before-define': [
      'error',
      {
        functions: false,
      },
    ],
    '@typescript-eslint/no-floating-promises': 'error',
  },
}
