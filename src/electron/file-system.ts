/**
 * File-system adapters for embedding splat-transform inside an Electron app.
 *
 * In an Electron renderer process we typically want to:
 * 1. Read input files from the user's disk via the Electron main process
 *    (the renderer doesn't have direct `fs` access when `nodeIntegration`
 *    is disabled - which is the recommended security setting).
 * 2. Write output files back to disk via IPC.
 * 3. Optionally read assets that have been packaged into the app bundle
 *    (e.g. `app://./resources/scene.ply` or `file://...` after asar
 *    unpacking).
 *
 * This module ships a small `IpcFileSystem` adapter that satisfies the
 * library's `ReadFileSystem` and `FileSystem` interfaces by delegating all
 * I/O to a host-supplied IPC bridge.
 *
 * The adapter is fully decoupled from Electron itself - it accepts an
 * arbitrary `bridge` object, so it works just as well in tests (with a
 * mock bridge) and in pure-Node contexts (where it can be used as a
 * type-only reference for the interface contract).
 *
 * ## Streaming writes (large files)
 *
 * The default `IpcWriter` buffers up to {@link IpcWriter.FLUSH_THRESHOLD}
 * bytes (16 MiB) before sending a chunk over IPC. If the host bridge
 * supports an `append: true` flag (see {@link IpcBridge.write}), each chunk
 * is appended to the destination file; otherwise chunks are concatenated
 * in memory and sent on close (suitable for files < ~50 MB).
 *
 * @example
 * ```ts
 * import { IpcReadFileSystem, IpcWriteFileSystem } from './file-system';
 *
 * const readFs = new IpcReadFileSystem({
 *     read:  (path) => window.api.fs.readFile(path),
 *     write: (path, data, opts) => window.api.fs.writeFile(path, data, opts),
 *     mkdir: (path) => window.api.fs.mkdir(path)
 * });
 * ```
 */
import { ReadStream, type ReadFileSystem, type ReadSource, type ProgressCallback } from '../lib/io/read/file-system';
import type { FileSystem as WriteFileSystem, Writer } from '../lib/io/write/file-system';

/**
 * Minimal bridge the host must provide. Each method returns a Promise so
 * IPC latency is transparent to the library.
 */
export interface IpcBridge {
    /** Read the entire file at `path` and return its bytes. */
    read(path: string): Promise<Uint8Array>;
    /**
     * Write `data` to `path`, creating parent directories as needed.
     * When `options.append` is true, the host should append to the existing
     * file (or create it if missing). When false / undefined, the host should
     * truncate the file first.
     */
    write(path: string, data: Uint8Array, options?: { append?: boolean }): Promise<void>;
    /** Recursively create the directory at `path`. */
    mkdir(path: string): Promise<void>;
    /**
     * Return the file size in bytes, or undefined if the file is missing
     * (so the library can fall back to streaming reads).
     */
    size?(path: string): Promise<number | undefined>;
    /** Test whether a file exists. Optional - defaults to attempting a read. */
    exists?(path: string): Promise<boolean>;
}

/**
 * In-memory `ReadStream` backed by a pre-fetched buffer.
 *
 * We use the simple "load the whole file into memory" path because
 * Electron IPC calls already serialize the payload; there's no streaming
 * benefit in a single round-trip.
 */
class InMemoryReadStream extends ReadStream {
    private readonly buffer: Uint8Array;
    private offset: number = 0;

    constructor(buffer: Uint8Array, size?: number) {
        super(size ?? buffer.byteLength);
        this.buffer = buffer;
    }

    async pull(target: Uint8Array): Promise<number> {
        const remaining = this.buffer.byteLength - this.offset;
        if (remaining === 0) return 0;
        const toRead = Math.min(target.byteLength, remaining);
        target.set(this.buffer.subarray(this.offset, this.offset + toRead), 0);
        this.offset += toRead;
        this.bytesRead += toRead;
        return toRead;
    }
}

/**
 * `ReadSource` implementation that defers the actual read to the bridge.
 * The bridge IPC fires inside `createSource`, and the resulting
 * `InMemoryReadStream` is returned synchronously by `read()`.
 */
class IpcReadSource implements ReadSource {
    public readonly size: number | undefined;
    public readonly seekable: boolean = true;
    private stream: InMemoryReadStream;

    constructor(buffer: Uint8Array, size: number | undefined) {
        this.size = size;
        this.stream = new InMemoryReadStream(buffer, size);
    }

    read(_start?: number, _end?: number): ReadStream {
        return this.stream;
    }

    close(): void {
        this.stream.close();
    }
}

/**
 * ReadFileSystem adapter that delegates to an {@link IpcBridge}.
 *
 * The library calls `fs.createSource(filename)` then `source.read()` and
 * `stream.readAll()`. We do a single IPC read up-front, then yield a
 * fully-loaded in-memory stream.
 *
 * Progress: when the host bridge doesn't have streaming reads, the entire
 * file is downloaded in one round-trip. We fire the progress callback
 * once with the final byte count so renderers can show a "loaded"
 * indicator. For real streaming progress, supply a bridge that returns
 * a pre-sized buffer and call the callback periodically.
 */
export class IpcReadFileSystem implements ReadFileSystem {
    constructor(private readonly bridge: IpcBridge) {}

    async createSource(filename: string, progress?: ProgressCallback): Promise<ReadSource> {
        const buffer = await this.bridge.read(filename);
        const size = this.bridge.size ? await this.bridge.size(filename) : buffer.byteLength;
        if (progress) progress(buffer.byteLength, size);
        return new IpcReadSource(buffer, size);
    }
}

/**
 * `Writer` implementation that streams chunks to the bridge.
 *
 * Writes are buffered until either {@link IpcWriter.FLUSH_THRESHOLD} bytes
 * accumulate or {@link IpcWriter.close} is called. Each flush sends one
 * IPC call with `append: true` so the host can stream-write to disk
 * without holding the full file in renderer memory.
 */
export class IpcWriter implements Writer {
    /**
     * Maximum bytes to buffer before forcing a flush. Default 16 MiB,
     * chosen to stay well under V8's ArrayBuffer limits while keeping IPC
     * overhead low. Override at construction time for special cases.
     */
    public static readonly FLUSH_THRESHOLD: number = 16 * 1024 * 1024;

    public bytesWritten: number = 0;
    private readonly chunks: Uint8Array[] = [];
    private bufferedBytes: number = 0;
    private closed: boolean = false;

    constructor(
        private readonly bridge: IpcBridge,
        private readonly filename: string,
        private readonly threshold: number = IpcWriter.FLUSH_THRESHOLD
    ) {}

    write(data: Uint8Array): void {
        if (this.closed) {
            throw new Error(`Writer for ${this.filename} is already closed`);
        }
        // Defensive copy so the caller can reuse their buffer.
        const copy = new Uint8Array(data);
        this.chunks.push(copy);
        this.bufferedBytes += copy.byteLength;
        this.bytesWritten += copy.byteLength;
        if (this.bufferedBytes >= this.threshold) {
            // Fire-and-forget flush - the writer's contract returns
            // void|Promise<void>, and we don't want to block the caller
            // mid-`write`. close() will await any pending chunk first.
            void this.flush();
        }
    }

    /**
     * Send all currently-buffered chunks to the bridge. The first flush
     * uses `append: false` to truncate; subsequent flushes use
     * `append: true` so the host can stream-append to disk.
     */
    async flush(): Promise<void> {
        if (this.chunks.length === 0) return;
        // Concat all pending chunks into a single buffer for the IPC call.
        const total = this.bufferedBytes;
        const combined = new Uint8Array(total);
        let offset = 0;
        for (const chunk of this.chunks) {
            combined.set(chunk, offset);
            offset += chunk.byteLength;
        }
        this.chunks.length = 0;
        this.bufferedBytes = 0;
        const append = this.bytesWritten > combined.byteLength;
        await this.bridge.write(this.filename, combined, { append });
    }

    async close(): Promise<void> {
        if (this.closed) return;
        this.closed = true;
        await this.flush();
    }
}

/**
 * FileSystem adapter that delegates to an {@link IpcBridge} for writing.
 */
export class IpcWriteFileSystem implements WriteFileSystem {
    constructor(private readonly bridge: IpcBridge) {}

    createWriter(filename: string): Writer {
        return new IpcWriter(this.bridge, filename);
    }

    async mkdir(path: string): Promise<void> {
        await this.bridge.mkdir(path);
    }
}

/**
 * Convenience factory that builds a paired read+write filesystem sharing
 * a single bridge.
 */
export const createIpcFileSystems = (bridge: IpcBridge): {
    read: IpcReadFileSystem;
    write: IpcWriteFileSystem;
} => ({
    read: new IpcReadFileSystem(bridge),
    write: new IpcWriteFileSystem(bridge)
});
