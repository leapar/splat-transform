/**
 * Smoke test for the Electron build.
 *
 * Run with: `pnpm pretest && node --test test/electron.test.mjs`
 * (pretest runs `pnpm build` which produces dist/electron.mjs)
 *
 * Verifies that:
 *   1. dist/electron.mjs exists after the build.
 *   2. All public Electron-specific exports are present.
 *   3. The library re-exports (curated subset) are reachable.
 *   4. `isElectron` correctly reports Node's process.versions.electron.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';

const ELECTRON_DIST = new URL('../dist/electron.mjs', import.meta.url);

test('electron build exists', () => {
    assert.ok(existsSync(ELECTRON_DIST), 'dist/electron.mjs should be produced by `pnpm build`');
});

test('electron module exports', async () => {
    const m = await import(ELECTRON_DIST.href);

    // Electron-specific surface
    assert.equal(typeof m.convertGsplat, 'function', 'convertGsplat must be exported');
    assert.equal(typeof m.setupElectronLogger, 'function', 'setupElectronLogger must be exported');
    assert.equal(typeof m.IpcReadFileSystem, 'function', 'IpcReadFileSystem must be exported');
    assert.equal(typeof m.IpcWriteFileSystem, 'function', 'IpcWriteFileSystem must be exported');
    assert.equal(typeof m.createIpcFileSystems, 'function', 'createIpcFileSystems must be exported');
    assert.equal(typeof m.ElectronLoggerRenderer, 'function', 'ElectronLoggerRenderer must be exported');
    assert.equal(typeof m.isElectron, 'function', 'isElectron must be exported');
    assert.equal(typeof m.isRenderer, 'function', 'isRenderer must be exported');

    // Curated lib re-exports
    assert.equal(typeof m.logger, 'object', 'logger must be re-exported');
    assert.equal(typeof m.WebPCodec, 'function', 'WebPCodec must be re-exported');
    assert.equal(typeof m.getInputFormat, 'function', 'getInputFormat must be re-exported');
    assert.equal(typeof m.getOutputFormat, 'function', 'getOutputFormat must be re-exported');
    assert.equal(typeof m.readFile, 'function', 'readFile must be re-exported');
    assert.equal(typeof m.writeFile, 'function', 'writeFile must be re-exported');
    assert.equal(typeof m.combine, 'function', 'combine must be re-exported');
    assert.equal(typeof m.processDataTable, 'function', 'processDataTable must be re-exported');
});

test('isElectron detects Node', async () => {
    // We're running under Node, not Electron.
    assert.equal(typeof process, 'object');
    const m = await import(ELECTRON_DIST.href);
    // In a plain Node test process this should be false.
    if (!process.versions.electron) {
        assert.equal(m.isElectron(), false, 'isElectron() should be false in plain Node');
    } else {
        assert.equal(m.isElectron(), true, 'isElectron() should be true when process.versions.electron is set');
    }
});

test('IpcWriteFileSystem creates a writer', async () => {
    const m = await import(ELECTRON_DIST.href);
    const fs = new m.IpcWriteFileSystem({
        read:  async () => new Uint8Array(),
        write: async () => { /* mock */ },
        mkdir: async () => { /* mock */ }
    });
    const writer = fs.createWriter('test.bin');
    assert.equal(typeof writer.write, 'function');
    assert.equal(typeof writer.close, 'function');
    assert.equal(typeof writer.bytesWritten, 'number');
});
