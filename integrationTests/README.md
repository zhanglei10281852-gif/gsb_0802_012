# Integration Tests

This directory contains integration tests for GraphQL.js across different environments and bundlers, testing basic GraphQL.JS functionality, as well as development mode and production mode behavior.

Tests are run via the main integration test suite in `resources/integration-test.ts`.

## Test Structure

### Basic GraphQL.JS Functionality Tests

Each subdirectory represents a different environment/bundler:

- `node` - tests for supported Node.js versions
- `ts` - tests for supported Typescript versions
- `webpack` - tests for Webpack

### Verifying Conditional Exports

The `conditions` subdirectory contains tests that verify the conditional exports of GraphQL.js. These tests ensure that the correct files are imported based on the environment being used.

### Verifying Development Mode Tests

Each subdirectory represents a different platform/bundler demonstrating enabling development mode by enabling the `development` condition or by calling `enableDevMode()`.

### Verifying Production Mode Tests

Each subdirectory represents a different environment/bundler demonstrating production mode when development mode is not enabled.

### Verifying Cancellation and Diagnostics Lifecycle Tests

The `cancellation-node` subdirectory runs the pre-release cancellation regression against the built package. It drives a cancellable incremental query and a cancellable subscription through the package's ESM entry points (`graphql` and `graphql/execution`), subscribes to the `graphql:execute`, `graphql:resolve`, and `graphql:subscribe` diagnostics channels, and asserts that normal completion, client cancellation via `AbortSignal`, and resolver failure each publish their channel lifecycle events exactly once and leave no active iterators behind.

The matching unit-level suites live in `src/execution/__tests__/cancellation-test.ts` (execution semantics: execute, subscribe, and incremental delivery) and `src/__tests__/cancellation-test.ts` (public package surface plus diagnostics channel lifecycle). Both layers are self-contained and order independent, so repeated runs are stable.

## Running the Checks

- `npm run testonly` - runs all unit-level suites, including both cancellation test files.
- `npm run check:ts` - type-checks the sources, tests, and test fixtures.
- `npm run build:npm` - rebuilds `npmDist` into a clean output directory.
- `npm run check:integrations` - builds `npmDist`, packs it, installs it into every integration project (including `cancellation-node`), and runs each project's test script.
