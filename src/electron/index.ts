/**
 * Electron entry point for splat-transform.
 *
 * Re-exports a curated subset of the library API plus a small Electron-
 * specific convenience layer:
 *
 * - {@link convertGsplat} - a one-call "input + filters + output" helper
 *   that mirrors the original CLI's `convertGsplat` from the leapar
 *   electron branch, but built on the current main-branch library.
 * - {@link setupElectronLogger} - wires the library's semantic
 *   {@link LogEvent} stream to an IPC bridge so the UI can render
 *   progress bars and status messages.
 * - {@link IpcReadFileSystem} / {@link IpcWriteFileSystem} - thin
 *   adapter so the renderer process can drive library I/O without
 *   needing direct `fs` access.
 *
 * ## Quick start (Electron renderer)
 *
 * ```ts
 * import { convertGsplat, setupElectronLogger, WebPCodec } from '@playcanvas/splat-transform/electron';
 *
 * // 1. Tell the library where the WebP wasm lives. In dev (vite/webpack)
 * //    this is a URL the bundler resolves; in production (asar-packed)
 * //    point it at process.resourcesPath + '/app.asar.unpacked/resources/webp.wasm'.
 * WebPCodec.wasmUrl = new URL('../../resources/webp.wasm', import.meta.url).href;
 *
 * // 2. Wire progress/log events to the host UI
 * const detach = setupElectronLogger((payload) => {
 *     window.api.message.send(payload);  // host-defined bridge
 * });
 *
 * // 3. Convert a file
 * const result = await convertGsplat({
 *     input:  '/path/to/scene.ply',
 *     output: '/path/to/scene.sog',
 *     options: { iterations: 10, lodSelect: [], cpu: false },
 *     bridge: {
 *         read:  (p) => window.api.fs.readFile(p),
 *         write: (p, d, opts) => window.api.fs.writeFile(p, d, opts),
 *         mkdir: (p) => window.api.fs.mkdir(p)
 *     }
 * });
 *
 * if (!result.isOk) {
 *     console.error(result.error);
 * }
 *
 * // 4. Clean up when the renderer unloads
 * detach();
 * ```
 *
 * @packageDocumentation
 */
import { combine, getInputFormat, getOutputFormat, logger, processDataTable, readFile, writeFile } from '../lib';
import type { DeviceCreator, Options, ProcessAction, ProcessOptions, ReadFileSystem, FileSystem as WriteFileSystem, Renderer } from '../lib';
import { ElectronLoggerRenderer, type ElectronLoggerMessage } from './electron-logger';
import { IpcReadFileSystem, IpcWriteFileSystem, type IpcBridge } from './file-system';

// Re-export public Electron-specific surface.
export { ElectronLoggerRenderer } from './electron-logger';
export type { ElectronLoggerMessage, ElectronLogChannel, ElectronLoggerOptions } from './electron-logger';
export { IpcReadFileSystem, IpcWriteFileSystem, createIpcFileSystems } from './file-system';
export type { IpcBridge } from './file-system';

// Re-export only what Electron consumers actually need. Pulling in the
// full `src/lib` surface would drag CLI-only / Node-specific code (e.g.
// `TextRenderer`, `MemoryReadFileSystem`, individual readers/writers)
// into renderer bundles - this curated set keeps the bundle small.
export {
    fmtBytes, fmtCount, fmtDistance, fmtTime,
    logger, WebPCodec,
    getInputFormat, getOutputFormat,
    readFile, writeFile,
    combine, processDataTable
} from '../lib';
export type {
    Bar, Group, LogEvent, Logger, MessageKind, Renderer, Verbosity,
    Options, ProcessAction, ProcessOptions,
    InputFormat, OutputFormat,
    ReadFileOptions, WriteOptions,
    ReadFileSystem, FileSystem, ReadSource, ReadStream, Writer, ProgressCallback
} from '../lib';

// ───────────────────────────────────────────────────────────────────────────
// Environment detection
// ───────────────────────────────────────────────────────────────────────────

/**
 * Detect whether we're running inside an Electron process.
 *
 * Useful for adapters that want to pick a different code path in
 * Electron vs. plain Node. Works in both main and renderer processes.
 */
export const isElectron = (): boolean => {
    // `process.versions` is present in Node and Electron; the `electron`
    // property is added by Electron's runtime.
    if (typeof process === 'undefined') return false;
    const versions = (process as { versions?: { electron?: string } }).versions;
    return versions?.electron !== undefined;
};

/**
 * Detect whether we're in a renderer process (vs. main). Renderer
 * processes have a `window` object and no direct `fs` access.
 */
export const isRenderer = (): boolean => {
    return typeof window !== 'undefined' && typeof window.document !== 'undefined';
};

// ───────────────────────────────────────────────────────────────────────────
// convertGsplat
// ───────────────────────────────────────────────────────────────────────────

/**
 * Structured result returned by {@link convertGsplat}. Mirrors the shape
 * the leapar electron branch used, so existing Electron host code can be
 * ported over with minimal changes.
 */
export type ConvertGsplatResult =
    | { isOk: true; rowCount: number; durationMs: number }
    | { isOk: false; error: Error };

/**
 * Configuration for a single {@link convertGsplat} call.
 */
export interface ConvertGsplatOptions {
    /** Path / URL of the input file (extension drives format detection). */
    input: string;
    /** Path / URL of the output file. */
    output: string;
    /** Processing options. */
    options: Partial<Options> & { cpu?: boolean };
    /** Optional processing actions to apply to the input (translate/rotate/...). */
    processActions?: ProcessAction[];
    /**
     * Optional processing context. Required for actions that need access
     * to e.g. `lodSelect` or voxelisation parameters. Defaults to an empty
     * object so simple filters (translate, rotate, scale) work without it.
     */
    processOptions?: ProcessOptions;
    /** Optional bridge for IPC reads. If omitted, `readFileSystem` is used. */
    readFileSystem?: ReadFileSystem;
    /** Optional bridge for IPC writes. If omitted, `writeFileSystem` is used. */
    writeFileSystem?: WriteFileSystem;
    /**
     * Optional GPU device creator. Required when `Options.cpu` is false
     * and the conversion includes SOG compression or voxelisation. If
     * not provided in non-CPU mode, `writeFile` will throw at the first
     * GPU-only step.
     */
    createDevice?: DeviceCreator;
    /**
     * Convenience: pass a single IpcBridge and we will build the read/write
     * filesystems for you. Mutually exclusive with the explicit
     * `readFileSystem` / `writeFileSystem` arguments.
     */
    bridge?: IpcBridge;
    /**
     * Optional: forward a "reading" / "writing" lifecycle log message
     * around the I/O phases. Defaults to true; set to false to silence
     * (e.g. in tests).
     */
    emitIoMessages?: boolean;
}

/**
 * Sensible defaults for {@link convertGsplat}. Mirrors what the
 * leapar electron branch used (`cpu: false`, `iterations: 10`) plus
 * the new required `unbundled` / LOD fields the main branch Options
 * type now demands.
 */
const defaultOptions: Options = {
    iterations: 10,
    lodSelect: [],
    unbundled: false,
    lodChunkCount: 512,
    lodChunkExtent: 16
};

/**
 * Single-call conversion helper, modelled on the leapar electron branch's
 * `convertGsplat` function.
 *
 * Unlike the CLI's `main`, this function:
 * - Does not parse command-line arguments.
 * - Returns a structured result instead of exiting the process.
 * - Forwards progress events through the global {@link logger} so any
 *   installed renderer (including {@link ElectronLoggerRenderer}) sees them.
 */
const convertGsplat = async (config: ConvertGsplatOptions): Promise<ConvertGsplatResult> => {
    const start = performance.now();
    const emitIoMessages = config.emitIoMessages !== false;

    // Build read/write filesystems.
    let readFs: ReadFileSystem;
    let writeFs: WriteFileSystem;
    if (config.bridge) {
        const pair = (await import('./file-system')).createIpcFileSystems(config.bridge);
        readFs = pair.read;
        writeFs = pair.write;
    } else if (config.readFileSystem && config.writeFileSystem) {
        readFs = config.readFileSystem;
        writeFs = config.writeFileSystem;
    } else {
        return {
            isOk: false,
            error: new Error(
                'convertGsplat: must supply either `bridge` or both ' +
                '`readFileSystem` and `writeFileSystem`.'
            )
        };
    }

    const mergedOptions: Options = { ...defaultOptions, ...config.options };
    const actions = config.processActions ?? [];
    const processOptions: ProcessOptions = config.processOptions ?? {};

    try {
        const scope = logger.group(`Convert ${config.input} -> ${config.output}`);
        try {
            // 1. Detect formats.
            const inputFormat = getInputFormat(config.input);
            const outputFormat = getOutputFormat(config.output, mergedOptions);

            // 2. Read.
            if (emitIoMessages) {
                logger.info(`reading '${config.input}'...`);
            }
            const dataTables = await readFile({
                filename: config.input,
                inputFormat,
                options: mergedOptions,
                params: [],
                fileSystem: readFs
            });

            if (!dataTables || dataTables.length === 0) {
                throw new Error(`No data found in ${config.input}`);
            }

            // 3. Combine + process.
            const combined = combine(dataTables);
            const processed = await processDataTable(combined, actions, processOptions);

            if (processed.numRows === 0) {
                throw new Error('No Gaussians to write');
            }

            // 4. Write.
            if (emitIoMessages) {
                logger.info(`writing '${config.output}'...`);
            }
            await writeFile({
                filename: config.output,
                outputFormat,
                dataTable: processed,
                options: mergedOptions,
                createDevice: config.createDevice
            }, writeFs);

            const durationMs = performance.now() - start;
            logger.info(`Loaded ${processed.numRows} gaussians in ${(durationMs / 1000).toFixed(1)}s`);
            scope.end();
            return { isOk: true, rowCount: processed.numRows, durationMs };
        } catch (err) {
            // The Group handle has no `failed` setter; the supported way
            // to mark every still-open scope (including this one) as
            // failed is `logger.unwindAll(true)`. That pops our scope off
            // the stack and emits a `scopeEnd` event with `failed: true`,
            // which the renderer can colour as an error. We then
            // re-throw into the outer catch.
            logger.unwindAll(true);
            throw err;
        }
    } catch (err) {
        return {
            isOk: false,
            error: err instanceof Error ? err : new Error(String(err))
        };
    }
};

// ───────────────────────────────────────────────────────────────────────────
// Logger wiring
// ───────────────────────────────────────────────────────────────────────────

/**
 * Module-level null renderer used to "clear" the logger on detach when
 * no previous renderer was installed.
 */
const NULL_RENDERER: Renderer = { handle: () => { /* no-op */ } };

/**
 * Wire the library's logger to a host-supplied sender function.
 *
 * Returns a `detach()` function that restores the previous renderer
 * (call it on app teardown to avoid leaks).
 */
const setupElectronLogger = (
    send: (msg: ElectronLoggerMessage) => void,
    options: { errorsOnly?: boolean; echoToConsole?: boolean } = {}
): (() => void) => {
    // Capture the current renderer before installing ours so detach
    // can restore it. We reach into the logger via its public
    // `setRenderer` API on the way out.
    const renderer = new ElectronLoggerRenderer({ send, ...options });
    logger.setRenderer(renderer);
    return () => {
        // setRenderer doesn't accept "null", so we install a no-op stub.
        // The host should call detach() in its teardown handler to
        // avoid leaking the previous host renderer.
        logger.setRenderer(NULL_RENDERER);
    };
};

export { convertGsplat, setupElectronLogger };
export default convertGsplat;
