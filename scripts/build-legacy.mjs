import { build } from 'tsdown'
import { readFile, writeFile } from 'node:fs/promises'

await build({
  config: false,
  entry: { 'legacy-events': 'scripts/legacy-events.mjs' },
  format: 'esm', platform: 'browser', target: 'es2023',
  outDir: 'lib/shared', clean: false, dts: false,
  deps: { alwaysBundle: [/.*/], onlyBundle: ['zod', '@deepseek-ai/dsh-host-apiproxy'] },
})

const notices = await Promise.all(['zod', '@deepseek-ai/dsh-host-apiproxy'].map(async name => {
  const license = await readFile(new URL(`../node_modules/${name}/LICENSE`, import.meta.url), 'utf8')
  return `${name}\n${license}`
}))
await writeFile(new URL('../lib/shared/legacy-events.LICENSE.txt', import.meta.url), notices.join('\n\n'))
