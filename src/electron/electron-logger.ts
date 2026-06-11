/**
 * Electron renderer for the splat-transform logger.
 *
 * Translates semantic {@link LogEvent}s into IPC messages that the Electron
 * main process / preload script can forward to the UI.
 *
 * In Node (non-Electron) environments this renderer falls back to
 * `console.log` / `console.error`, so importing this module from a CLI
 * context does not throw.
 *
 * ## Usage
 *
 * ### In Electron renderer
 * ```ts
 * import { logger } from '../lib';
 * import { ElectronLoggerRenderer } from './electron-logger';
 *
 * logger.setRenderer(new ElectronLoggerRenderer({
 *     send: (channel, payload) => (window as any).api.message.send(channel, payload)
 * }));
 * ```
 *
 * ### In Node / CLI
 * ```ts
 * import { logger } from '../lib';
 * import { ElectronLoggerRenderer } from './electron-logger';
 *
 * logger.setRenderer(new ElectronLoggerRenderer()); // falls back to console.*
 * ```
 */
import type { LogEvent, MessageKind, Renderer, Verbosity } from '../lib/utils/logger';

export type ElectronLogChannel = 'progress' | 'message' | 'log' | 'output';

export interface ElectronLoggerMessage {
    /** Channel name; consumers dispatch on this. */
    channel: ElectronLogChannel;
    /** Renderer-supplied verbosity tag (mainly for filtering on the host). */
    verbosity?: Verbosity;
    /** Optional structured payload. */
    data?: unknown;
    /** Pre-formatted single-line text (convenience for hosts that only want strings). */
    text?: string;
}

export interface ElectronLoggerOptions {
    /**
     * Sender function invoked for every emitted event. Defaults to a
     * `console.*` fallback so the class is safe to instantiate in Node.
     */
    send?: (msg: ElectronLoggerMessage) => void;
    /**
     * When true, only `error` / `warn` messages are forwarded; the rest
     * are dropped. Useful for hot loops or "quiet" UI modes.
     */
    errorsOnly?: boolean;
    /**
     * When true, also echo events to the local console (handy during dev).
     */
    echoToConsole?: boolean;
}

const KIND_TO_LEVEL: Record<MessageKind, 'log' | 'warn' | 'error'> = {
    info: 'log',
    debug: 'log',
    warn: 'warn',
    error: 'error'
};

const formatScope = (event: LogEvent): string | undefined => {
    if (event.kind === 'scopeStart' || event.kind === 'scopeEnd') {
        if (event.index !== undefined && event.total !== undefined) {
            return `[${event.index + 1}/${event.total}] ${event.name}`;
        }
        return event.name;
    }
    if (event.kind === 'barStart' || event.kind === 'barTick' || event.kind === 'barEnd') {
        return event.name;
    }
    return undefined;
};

const formatBar = (event: Extract<LogEvent, { kind: 'barTick' }>): string => {
    const pct = event.total > 0 ? Math.floor((event.current / event.total) * 100) : 0;
    return `${event.name}: ${event.current}/${event.total} (${pct}%)`;
};

/**
 * Default sender: writes to the host console.
 *
 * In an Electron renderer process the host (preload script) should provide
 * a real `send` function that forwards events over IPC.
 */
const consoleSender = (msg: ElectronLoggerMessage) => {
    const text = msg.text ?? '';
    // Choose the most appropriate console method based on channel/level.
    if (msg.channel === 'message' && msg.data && typeof msg.data === 'object') {
        const level = (msg.data as { level?: string }).level;
        if (level === 'error') {
            // eslint-disable-next-line no-console
            console.error(text);
            return;
        }
        if (level === 'warn') {
            // eslint-disable-next-line no-console
            console.warn(text);
            return;
        }
    }
    if (msg.channel === 'output') {
        // `output` is the user's pipeable stdout; print verbatim.
        // eslint-disable-next-line no-console
        console.log(text);
        return;
    }
    // progress/log/message - all go to console.log by default.
    // eslint-disable-next-line no-console
    console.log(text);
};

export class ElectronLoggerRenderer implements Renderer {
    private readonly send: (msg: ElectronLoggerMessage) => void;
    private readonly errorsOnly: boolean;
    private readonly echoToConsole: boolean;

    constructor(options: ElectronLoggerOptions = {}) {
        this.send = options.send ?? consoleSender;
        this.errorsOnly = options.errorsOnly ?? false;
        this.echoToConsole = options.echoToConsole ?? false;
    }

    handle(event: LogEvent): void {
        switch (event.kind) {
            case 'message': {
                if (this.errorsOnly && event.level !== 'error' && event.level !== 'warn') {
                    return;
                }
                const consoleFn = KIND_TO_LEVEL[event.level] ?? 'log';
                const text = event.text;
                this.send({
                    channel: 'message',
                    data: { level: event.level, text, depth: event.depth },
                    text
                });
                if (this.echoToConsole) {
                    // eslint-disable-next-line no-console
                    (console as any)[consoleFn](text);
                }
                return;
            }

            case 'output': {
                if (this.errorsOnly) return;
                this.send({ channel: 'output', text: event.text });
                if (this.echoToConsole) {
                    // eslint-disable-next-line no-console
                    console.log(event.text);
                }
                return;
            }

            case 'scopeStart': {
                if (this.errorsOnly) return;
                const text = formatScope(event);
                this.send({ channel: 'log', data: { phase: 'start', ...event }, text });
                return;
            }

            case 'scopeEnd': {
                const text = formatScope(event);
                const data: Record<string, unknown> = { phase: 'end', ...event };
                if (event.failed) {
                    data.level = 'error';
                }
                this.send({ channel: 'log', data, text });
                return;
            }

            case 'barStart': {
                if (this.errorsOnly) return;
                this.send({
                    channel: 'progress',
                    data: { phase: 'start', name: event.name, total: event.total, depth: event.depth },
                    text: `${event.name} (0/${event.total})`
                });
                return;
            }

            case 'barTick': {
                if (this.errorsOnly) return;
                this.send({
                    channel: 'progress',
                    data: { phase: 'tick', name: event.name, current: event.current, total: event.total, depth: event.depth },
                    text: formatBar(event)
                });
                return;
            }

            case 'barEnd': {
                const data: Record<string, unknown> = { phase: 'end', ...event };
                if (event.failed) data.level = 'error';
                this.send({
                    channel: 'progress',
                    data,
                    text: `${event.name}: ${event.current}/${event.total}`
                });
                return;
            }
        }
    }
}

export default ElectronLoggerRenderer;
