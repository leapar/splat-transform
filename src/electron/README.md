# Electron 集成指南

`@playcanvas/splat-transform` 自 v2.5+ 起原生支持 Electron 渲染进程嵌入，无需 fork 任何源码。

## 核心思路

main 分支相比 leapar 老版 electron 分支已经做了**重大重构**：

| 老版（leapar） | 新版（main 分支） |
|----------------|------------------|
| `console.log` 散落在各文件 | 统一 `logger` 抽象 + `Renderer` 接口 |
| `src/index.ts` 混杂 CLI + 库代码 | 拆出 `src/lib/`（库）+ `src/cli/`（CLI） |
| `node:fs` 直接耦合 | `ReadFileSystem` / `FileSystem` 抽象，支持 URL/Memory/Zip |
| `webp.wasm` 路径硬编码 `process.env.NODE_ENV` | `WebPCodec.wasmUrl` 静态属性（运行时注入） |
| `kmeans(points, k, iter, device?)` | `Options.cpu` + 自动 `DeviceCreator` 注入 |

这些抽象让 Electron 集成变得**极其简洁**——基本就是组装现有 API，不需要改一行业务代码。

## 安装

```bash
pnpm add @playcanvas/splat-transform
# 或从本仓库构建后引用 dist/electron.mjs
```

## 集成步骤

### 1. 主进程：暴露 IPC 桥接

`electron/main.ts`:

```ts
import { app, ipcMain } from 'electron';
import { promises as fsp } from 'node:fs';
import { dirname, join } from 'node:path';

ipcMain.handle('fs:read',  async (_e, p) => fsp.readFile(p));
ipcMain.handle('fs:write', async (_e, p, d, opts) => {
    // append=false 时先截断
    if (!opts?.append) {
        await fsp.mkdir(dirname(p), { recursive: true });
    }
    return fsp.writeFile(p, d, { flag: opts?.append ? 'a' : 'w' });
});
ipcMain.handle('fs:mkdir', async (_e, p) => fsp.mkdir(p, { recursive: true }));
ipcMain.handle('fs:size',  async (_e, p) => fsp.stat(p).then(s => s.size).catch(() => undefined));

// lib:message 接收来自渲染进程的事件转发（progress / message / log / output）
ipcMain.on('lib:message', (_e, payload) => {
    // 派发到主窗口的 webContents，或主进程控制台
    // console.log('[lib]', payload);
});
```

### 2. preload：暴露 API 给渲染进程

`electron/preload.ts`:

```ts
import { contextBridge, ipcRenderer } from 'electron';

contextBridge.exposeInMainWorld('api', {
    fs: {
        read:    (p: string) => ipcRenderer.invoke('fs:read', p),
        write:   (p: string, d: Uint8Array, opts?: { append?: boolean }) =>
                    ipcRenderer.invoke('fs:write', p, d, opts),
        mkdir:   (p: string) => ipcRenderer.invoke('fs:mkdir', p),
        size:    (p: string) => ipcRenderer.invoke('fs:size', p)
    },
    message: {
        // 渲染进程把库事件转发回主进程 / UI
        send: (payload: unknown) => ipcRenderer.send('lib:message', payload)
    }
});
```

### 3. 渲染进程：调用库

`renderer/scene.ts`:

```ts
import { convertGsplat, setupElectronLogger, WebPCodec } from '@playcanvas/splat-transform/electron';

// 1. WebP wasm URL（Electron 打包后路径与开发不同）
WebPCodec.wasmUrl = new URL('../../resources/webp.wasm', import.meta.url).href;

// 2. 进度/日志推送
const detach = setupElectronLogger((msg) => {
    (window as any).api.message.send(msg);
    // 你的 UI 派发示例：
    if (msg.channel === 'progress' && typeof msg.data === 'object' && msg.data) {
        const { phase, current, total, name } = msg.data as { phase: string; current?: number; total?: number; name?: string };
        if (phase === 'tick' && current !== undefined && total !== undefined) {
            ui.setProgress(name ?? '', current / total);
        }
    } else if (msg.channel === 'message' && typeof msg.data === 'object' && msg.data) {
        const { level, text } = msg.data as { level: string; text: string };
        ui.log(level, text);
    }
});

// 3. 转换
const result = await convertGsplat({
    input:  '/Users/me/scene.ply',
    output: '/Users/me/scene.sog',
    options: { iterations: 10, lodSelect: [], cpu: false },
    bridge: {
        read:    (p) => window.api.fs.read(p),
        write:   (p, d, opts) => window.api.fs.write(p, d, opts),
        mkdir:   (p) => window.api.fs.mkdir(p),
        size:    (p) => window.api.fs.size(p)
    }
});

if (result.isOk) {
    ui.notify(`Converted ${result.rowCount} gaussians in ${(result.durationMs / 1000).toFixed(1)}s`);
} else {
    ui.error(result.error.message);
}

// 4. 卸载时清理
window.addEventListener('beforeunload', detach);
```

## API 概览

### `convertGsplat(config)`

单次转换函数，返回 `{ isOk, rowCount?, durationMs?, error? }`。

参数：

| 字段 | 类型 | 说明 |
|------|------|------|
| `input` | `string` | 输入文件路径 |
| `output` | `string` | 输出文件路径 |
| `options` | `Partial<Options>` | 见 `Options` 类型（含 `cpu`） |
| `processActions` | `ProcessAction[]?` | translate/rotate/scale/filter 等 |
| `processOptions` | `ProcessOptions?` | 处理上下文，filterByValue 等需要 |
| `bridge` | `IpcBridge?` | 一体化 IPC 桥接（推荐） |
| `readFileSystem` | `ReadFileSystem?` | 单独传读文件系统（与 `bridge` 互斥） |
| `writeFileSystem` | `FileSystem?` | 单独传写文件系统（与 `bridge` 互斥） |
| `createDevice` | `DeviceCreator?` | GPU 设备创建器（CPU 模式不需要） |
| `emitIoMessages` | `boolean?` | 是否在读写阶段发 `logger.info`（默认 true） |

返回值（`ConvertGsplatResult`）：

```ts
type ConvertGsplatResult =
  | { isOk: true;  rowCount: number; durationMs: number }
  | { isOk: false; error: Error };
```

### `setupElectronLogger(send, options?)`

把库内部的 `LogEvent` 流转换成 IPC 消息，返回 `detach()` 函数。

- `send(msg)`：每条事件回调一次
- `options.errorsOnly`（默认 false）：只转发 `error` / `warn`
- `options.echoToConsole`（默认 false）：同时在本地 `console.*` 输出（开发时方便）

`msg` 形状：

```ts
interface ElectronLoggerMessage {
    channel: 'progress' | 'message' | 'log' | 'output';
    verbosity?: 'quiet' | 'normal' | 'verbose';
    data?: unknown;        // 结构化 payload（phase / level / counters...）
    text?: string;         // 预格式化单行文本，方便直接显示
}
```

### `IpcReadFileSystem` / `IpcWriteFileSystem`

底层适配器，如果你需要直接传 `ReadFileSystem` / `FileSystem` 给 `readFile` / `writeFile`，可以用：

```ts
import { IpcReadFileSystem, IpcWriteFileSystem, createIpcFileSystems } from '@playcanvas/splat-transform/electron';

const { read, write } = createIpcFileSystems({
    read:    (p) => window.api.fs.read(p),
    write:   (p, d, opts) => window.api.fs.write(p, d, opts),
    mkdir:   (p) => window.api.fs.mkdir(p)
});

// 直接用：
const data = await read.createSource('scene.ply');
// 或与库函数组合：
// import { readFile } from '@playcanvas/splat-transform/electron';
// const tables = await readFile({ filename: 'scene.ply', ..., fileSystem: read });
```

### `ElectronLoggerRenderer`

更细粒度的控制——可以直接实例化并 `logger.setRenderer(...)`：

```ts
import { logger, ElectronLoggerRenderer } from '@playcanvas/splat-transform/electron';

logger.setRenderer(new ElectronLoggerRenderer({
    send: (msg) => window.api.message.send(msg),
    errorsOnly: false,
    echoToConsole: true  // 开发时方便调试
}));
```

### `isElectron()` / `isRenderer()`

```ts
import { isElectron, isRenderer } from '@playcanvas/splat-transform/electron';

if (isElectron()) {
    // we're inside an Electron process
}
if (isRenderer()) {
    // we're in a renderer process (no direct fs access)
}
```

## WebP wasm 在 Electron 打包后的路径处理

`WebPCodec.wasmUrl` 是个静态字符串，库在初始化时会 fetch 它。在 Electron 里需要根据环境注入：

```ts
import { WebPCodec } from '@playcanvas/splat-transform/electron';
import { isElectron } from '@playcanvas/splat-transform/electron';

if (isElectron() && !process.env.NODE_ENV?.includes('dev')) {
    // 生产环境（asar 打包后）wasm 在 app.asar.unpacked/resources/
    // 假设 main 进程暴露了 resourcesPath：
    const resourcesPath = (window as any).api.resourcesPath();
    WebPCodec.wasmUrl = `file://${resourcesPath}/app.asar.unpacked/resources/webp.wasm`;
} else {
    // 开发环境：vite / webpack / 静态服务器都能解析的相对 URL
    WebPCodec.wasmUrl = new URL('../../resources/webp.wasm', import.meta.url).href;
}
```

`electron-builder` 配置（`package.json`）：

```json
{
  "build": {
    "asarUnpack": [
      "resources/webp.wasm"
    ]
  }
}
```

## 流式写大文件

`IpcWriter` 默认每 16 MiB 触发一次 IPC `write` 调用，参数 `{ append: true }`。
如果你的 Electron 主进程 IPC 实现支持 `flag: 'a'`（Node `fs.writeFile` 标准做法），
**几百 MB 的 SOG 文件** 也能稳定转换，渲染进程内存峰值 < 16 MiB。

如果不想用流式写（开发/调试场景），把 `IpcWriter.FLUSH_THRESHOLD` 调到 `Infinity`：

```ts
import { IpcWriter, IpcWriteFileSystem } from '@playcanvas/splat-transform/electron';

class DebugWriteFs extends IpcWriteFileSystem {
    createWriter(filename: string) {
        return new IpcWriter(this['bridge' as keyof this] as any, filename, Infinity);
    }
}
```

## 与 leapar 老版 electron 分支的对比

| 维度 | leapar 老版 | 新版（main 分支直接用） |
|------|-------------|----------------------|
| 入口文件 | `src/index-electron.ts`（手写 263 行） | `src/electron/index.ts`（基于现成库 230 行） |
| 日志通道 | 散落替换 12 处 `console.log` | 1 个 `Renderer` 即可拦截全部事件 |
| 文件路径 | `process.env.NODE_ENV` 三元判断 | 运行时设置 `WebPCodec.wasmUrl` |
| 跨平台 fs | 直接 `node:fs/promises` | 注入 `ReadFileSystem` 抽象 |
| GPU/CPU | `kmeans(points, k, iter, device?)` 改签名 | `Options.cpu` + 自动 `DeviceCreator` |
| 代码侵入度 | 改 7 个核心文件 | **0 处**核心文件改动 |
| 升级到新版 | 需要重新 apply 所有 patch | 跟随 main 分支 merge 即可 |
| 大文件写 | 一次性 concat 在内存 | 流式 append（16 MiB 阈值） |
| 错误处理 | `error: any` | `error: Error`（类型安全） |
| 进度回调 | 散落 `console.log` | `ProgressCallback` → `message` 事件 |
| WebP wasm | 独立的 `webp.electron.mjs` | 用 `WebPCodec.wasmUrl` 一行设置 |

## 升级到 main 新版本时

```bash
git checkout electron-v2  # 或你的 electron 集成分支
git fetch upstream
git merge upstream/main
# 通常没有冲突，因为 main 分支的所有改动都集中在 src/lib/ + src/cli/
# 而你的修改在 src/electron/ 全新目录
```

验证清单：
- [ ] `src/electron/` 目录未被删除（4 个文件都在）
- [ ] `src/electron/index.ts` 中的 `convertGsplat` 仍能调用
- [ ] `rollup.config.mjs` 包含 `electron` 构建配置（`entryFileNames: 'electron.mjs'`）
- [ ] `package.json` `exports` 包含 `"./electron"` 子路径
- [ ] `WebPCodec.wasmUrl` 在 renderer 中被正确设置
- [ ] `setupElectronLogger` 能收到 progress / message 事件
- [ ] `npx tsc --noEmit` 在 `src/electron/` 下零错误
- [ ] `pnpm build` 产物含 `dist/electron.mjs` + `dist/electron.d.ts`
