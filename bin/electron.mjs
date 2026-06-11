#!/usr/bin/env node

/**
 * Demo entry point for the Electron adapter build.
 *
 * Usage:
 *   pnpm build                       # produce dist/electron.mjs
 *   node bin/electron.mjs INPUT OUTPUT
 *
 * Reads the input file via the `node:fs` bridge (i.e. directly on disk
 * since this is running in Node, not a renderer), runs the same
 * convertGsplat pipeline that an Electron renderer would, and writes the
 * output. Useful as a smoke test that the Electron build still works
 * outside an Electron host.
 */
import { promises as fsp } from 'node:fs';
import { dirname } from 'node:path';
import { convertGsplat, WebPCodec } from '../dist/electron.mjs';

const [, , input, output] = process.argv;
if (!input || !output) {
    console.error('Usage: node bin/electron.mjs <input> <output>');
    process.exit(1);
}

// In Node the relative path resolves correctly because __dirname-based
// resolve works without an Electron host.
WebPCodec.wasmUrl = new URL('../lib/webp.wasm', import.meta.url).href;

const result = await convertGsplat({
    input,
    output,
    options: { iterations: 10, lodSelect: [], cpu: true },
    bridge: {
        read:  (p) => fsp.readFile(p),
        write: async (p, d, opts) => {
            if (!opts?.append) {
                await fsp.mkdir(dirname(p), { recursive: true });
            }
            await fsp.writeFile(p, d, { flag: opts?.append ? 'a' : 'w' });
        },
        mkdir: (p) => fsp.mkdir(p, { recursive: true })
    }
});

if (!result.isOk) {
    console.error('Conversion failed:', result.error);
    process.exit(1);
}

console.log(`OK: ${result.rowCount} gaussians in ${(result.durationMs / 1000).toFixed(1)}s`);
