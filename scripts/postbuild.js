#!/usr/bin/env -S node

import { writeFile } from 'node:fs/promises'
import { readFileSync } from 'node:fs'

const packageJson = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf-8'))

const name = packageJson.name
const version = packageJson.version

// To address https://github.com/platformatic/kafka/issues/91
await writeFile(
  new URL('../dist/version.js', import.meta.url),
  `export const name = "${name}";\nexport const version = "${version}";\n`,
  'utf-8'
)
