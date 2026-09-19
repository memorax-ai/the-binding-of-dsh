import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { copyFile, cp, mkdir, mkdtemp, readFile, rm } from 'node:fs/promises'
import test from 'node:test'
import { runInNewContext } from 'node:vm'
import { Context } from '@deepseek-ai/cordis'
import TypertRegistry from '@deepseek-ai/dsh-typert-registry'
import {
  GatewayDispatchError,
  HostRemoteService,
  NodePeerClient,
  apply,
  createGatewayDispatcher,
  inject,
  installClientGateway,
  name,
} from '../lib/index.js'

test('exports the Host plugin entrypoint', () => {
  assert.equal(name, 'the-binding-of-dsh')
  assert.deepEqual(inject, ['typert'])
  assert.equal(typeof apply, 'function')
  assert.equal(typeof createGatewayDispatcher, 'function')
  assert.equal(typeof installClientGateway, 'function')
  assert.equal(typeof HostRemoteService, 'function')
  assert.equal(typeof NodePeerClient, 'function')
  assert.equal(typeof GatewayDispatchError, 'function')
})

test('Host entrypoint installs the peer-bound Remote service', async () => {
  const root = new Context()
  const services = root.plugin({
    name: 'host-entrypoint-test-services',
    apply(ctx) {
      ctx.provide('harmony', {})
      new TypertRegistry(ctx)
    },
  })
  await services
  const fiber = root.plugin({ name, inject, apply })
  await fiber
  await new Promise(resolve => setImmediate(resolve))
  assert.ok(fiber.ctx.get('remote') instanceof HostRemoteService)
  await fiber.dispose()
  await services.dispose()
})

test('Host entrypoint stays inactive without Harmony', async () => {
  const root = new Context()
  const typert = root.plugin({
    name: 'host-entrypoint-test-typert',
    apply(ctx) {
      new TypertRegistry(ctx)
    },
  })
  await typert
  const fiber = root.plugin({ name, inject, apply })
  await fiber
  assert.equal(fiber.ctx.get('remote'), undefined)
  await fiber.dispose()
  await typert.dispose()
})

test('builds a DSH browser module', async () => {
  const source = await readFile(new URL('../lib/client.js', import.meta.url), 'utf8')
  assert.match(source, /window\.__ModuleLoader__\.load\(/)
  assert.match(source, /id: "the-binding-of-dsh"/)
})

test('Client entrypoint installs the Gateway and exposes its Connection binding', async () => {
  const source = await readFile(new URL('../lib/client.js', import.meta.url), 'utf8')
  let definition
  runInNewContext(source, {
    AbortController,
    window: {
      __ModuleLoader__: {
        load(value) {
          definition = value
        },
      },
    },
  })
  assert.equal(definition.id, 'the-binding-of-dsh')
  const exports = definition.factory(() => assert.fail('browser module has no external dependency'))
  const state = { registration: undefined, dispose: undefined }
  exports.apply({
    get(name) {
      assert.equal(name, 'connection')
      return {
        rpc: {
          intercept(channel, matches, handler) {
            state.registration = { channel, matches, handler }
            return () => {}
          },
        },
      }
    },
    on() {
      return () => {}
    },
    effect(factory) {
      state.dispose = factory()
    },
  })
  assert.equal(state.registration.channel, '/api')
  assert.equal(typeof state.registration.matches, 'function')
  assert.equal(typeof state.registration.handler, 'function')
  assert.equal(typeof state.dispose, 'function')
  assert.equal(typeof exports.createClientConnectionBinding, 'function')
})

test('Client entrypoint stays inactive without the Harmony Connection patch', async () => {
  const source = await readFile(new URL('../lib/client.js', import.meta.url), 'utf8')
  let definition
  runInNewContext(source, {
    AbortController,
    window: {
      __ModuleLoader__: {
        load(value) {
          definition = value
        },
      },
    },
  })
  const exports = definition.factory(() => assert.fail('browser module has no external dependency'))
  assert.doesNotThrow(() => exports.apply({
    get: () => ({ rpc: {} }),
    effect: () => assert.fail('inactive client must not install an effect'),
  }))
})

test('ships built files without an install-time build script', async () => {
  const manifest = JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8'))
  assert.equal(manifest.scripts.prepare, undefined)
  assert.equal(manifest.scripts.prepack, 'npm run build')
  assert.equal(typeof manifest.exports['./browser-peer'].default, 'string')
  assert.equal(manifest.exports['./host/connection'].default, './lib/host/connection.js')
  assert.equal(manifest.exports['./host/gateway'].default, './lib/host/gateway.js')
  assert.deepEqual(manifest.dsh.harmony.patches, ['./patches/connection.patch.cjs'])
  assert.deepEqual(manifest.peerDependenciesMeta, {
    '@deepseek-ai/cordis': { optional: true },
    '@deepseek-ai/dsh-api-gateway': { optional: true },
    '@deepseek-ai/dsh-client-connection': { optional: true },
    '@deepseek-ai/dsh-host-apiproxy': { optional: true },
    '@deepseek-ai/dsh-typert-protocol': { optional: true },
    '@deepseek-ai/dsh-typert-registry': { optional: true },
  })
  assert.equal(manifest.peerDependencies['dsh-harmony'], '>=0.8.6 <0.9.0')
  assert.equal(manifest.peerDependenciesMeta['dsh-harmony'], undefined)
})

test('accepts supported DSH prereleases as peers', async () => {
  const manifest = JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8'))
  const semver = createRequire(import.meta.url)('semver')
  const peers = Object.entries(manifest.peerDependencies)
    .filter(([name]) => name.startsWith('@deepseek-ai/dsh-'))

  for (const [name, range] of peers) {
    assert.equal(semver.satisfies('0.1.0-rc.7', range), false, name)
    assert.equal(semver.satisfies('0.1.0-rc.8', range), true, name)
    assert.equal(semver.satisfies('0.1.0-rc.9', range), true, name)
    assert.equal(semver.satisfies('0.1.1-rc.2', range), true, name)
    assert.equal(semver.satisfies('1.0.0', range), true, name)
  }
})

test('ships CommonJS patches loadable from node_modules', async (context) => {
  const root = await mkdtemp(join(tmpdir(), 'the-binding-of-dsh-package-'))
  context.after(() => rm(root, { recursive: true, force: true }))
  const packageDir = join(root, 'node_modules', 'the-binding-of-dsh')
  const manifest = JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8'))
  await mkdir(packageDir, { recursive: true })
  await copyFile(new URL('../package.json', import.meta.url), join(packageDir, 'package.json'))
  await cp(new URL('../patches', import.meta.url), join(packageDir, 'patches'), { recursive: true })

  for (const patch of manifest.dsh.harmony.patches) {
    assert.match(patch, /\.cjs$/)
    const destination = join(packageDir, patch)
    await mkdir(dirname(destination), { recursive: true })
    await copyFile(new URL(`..${patch.slice(1)}`, import.meta.url), destination)
    assert.ok(createRequire(join(root, 'consumer.cjs'))(destination))
  }
})
