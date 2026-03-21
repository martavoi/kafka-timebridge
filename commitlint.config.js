module.exports = {
  extends: ['@commitlint/config-conventional'],
  rules: {
    // Default 100 is tight for legitimate links in human commits too (release notes, PR URLs).
    'body-max-line-length': [2, 'always', 200],
  },
};
