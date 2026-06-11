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

## 集成步骤

### 1. 安装

```bash
pnpm add @playcanvas/splat-transform
# 或构建后引用 dist/electron.mjs
```

### 2. 主进程：暴露 IPC 桥接

```ts
// electron/main.ts
import { ipcMain } from 'electron';
import { promises as fsp } from 'node:fs';
import { dirname } from 'node:path';

ipcMain.handle('fs:read',  async (_e, p) => fsp.readFile(p));
ipcMain.handle('fs:write', async (_e, p, d) => {
    await fsp.mkdir(dirname(p), { recursive: true });
    return fsp.writeFile(p, Buffer.from(d));
});
ipcMain.handle('fs:mkdir', async (_e, p) => fsp.mkdir(p, { recursive: true }));
ipcMain.handle('fs:size',  async (_e, p) => fsp.stat(p).then(s => s.size).catch(() => undefined));
```

### 3. preload：暴露 API 给渲染进程

```ts
// electron/preload.ts
import { contextBridge, ipcRenderer } from 'electron';

contextBridge.exposeInMainWorld('api', {
    fs: {
        read:  (p: string) => ipcRenderer.invoke('fs:read', p),
        write: (p: string, d: Uint8Array) => ipcRenderer.invoke('fs:write', p, d),
        mkdir: (p: string) => ipcRenderer.invoke('fs:mkdir', p),
        size:  (p: string) => ipcRenderer.invoke('fs:size', p)
    },
    message: {
        // 库会把进度/日志通过这个 channel 推回来
        send: (payload: unknown) => ipcRenderer.send('lib:message', payload)
    }
});
```

### 4. 渲染进程：调用库

```ts
// renderer/scene.ts
import { convertGsplat, setupElectronLogger, WebPCodec } from '@playcanvas/splat-transform/electron';

// 1. WebP wasm URL（Electron 打包后路径与开发不同）
WebPCodec.wasmUrl = new URL('../../resources/webp.wasm', import.meta.url).href;

// 2. 进度/日志推送
const detach = setupElectronLogger((msg) => {
    (window as any).api.message.send(msg);
    if (msg.channel === 'progress' && msg.text) {
        ui.setProgress(msg.text);
    } else if (msg.channel === 'message' && msg.data && typeof msg.data === 'object') {
        const { level, text } = msg.data as { level: string; text: string };
        ui.log(level, text);
    }
});

// 3. 转换
const result = await convertGsplat({
    input:  '/Users/me/scene.ply',
    output: '/Users/me/scene.sog',
    options: { iterations: 10, lodSelect: [], cpu: false },
    read:  (p) => (window as any).api.fs.read(p),
    write: (p, d) => (window as any).api.fs.write(p, d),
    mkdir: (p) => (window as any).api.fs.mkdir(p),
    size:  (p) => (window as any).api.fs.size(p)
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
| `options` | `Partial<Options>` | 见 `Options` 类型 |
| `processActions` | `ProcessAction[]?` | translate/rotate/scale/filter 等 |
| `read` | `(path) => Promise<Uint8Array>` | IPC 读回调 |
| `write` | `(path, data) => Promise<void>` | IPC 写回调 |
| `mkdir` | `(path) => Promise<void>` | IPC 创建目录回调 |
| `size` | `(path) => Promise<number \| undefined>?` | 可选：文件大小探测 |
| `createDevice` | `DeviceCreator?` | GPU 设备创建器（CPU 模式不需要） |

### `setupElectronLogger(send, options?)`

把库内部的 `LogEvent` 流转换成 IPC 消息，返回 `detach()` 函数。

### `IpcReadFileSystem` / `IpcWriteFileSystem`

底层适配器，如果你需要直接传 `ReadFileSystem` / `FileSystem` 给 `readFile` / `writeFile`，可以用：

```ts
import { IpcReadFileSystem, IpcWriteFileSystem } from '@playcanvas/splat-transform/electron';

const readFs = new IpcReadFileSystem({
    read:  (p) => (window as any).api.fs.read(p),
    write: () => Promise.reject(new Error('read-only')),
    mkdir: () => Promise.reject(new Error('read-only'))
});
```

### `ElectronLoggerRenderer`

更细粒度的控制——可以直接实例化并 `logger.setRenderer(...)`：

```ts
import { logger, ElectronLoggerRenderer } from '@playcanvas/splat-transform/electron';

logger.setRenderer(new ElectronLoggerRenderer({
    send: (msg) => (window as any).api.message.send(msg),
    errorsOnly: false,
    echoToConsole: true  // 开发时方便调试
}));
```

## 与 leapar 老版 electron 分支的对比

| 维度 | leapar 老版 | 新版（main 分支直接用） |
|------|-------------|----------------------|
| 入口文件 | `src/index-electron.ts`（手写） | `src/electron/index.ts`（基于现成库） |
| 日志通道 | 散落替换 12 处 `console.log` | 1 个 `Renderer` 即可拦截全部事件 |
| 文件路径 | `process.env.NODE_ENV` 三元判断 | 运行时设置 `WebPCodec.wasmUrl` |
| 跨平台 fs | 直接 `node:fs/promises` | 注入 `ReadFileSystem` 抽象 |
| GPU/CPU | `kmeans(points, k, iter, device?)` 改签名 | `Options.cpu` + 自动 `DeviceCreator` |
| 代码侵入度 | 改 7 个核心文件 | **0 处**核心文件改动 |
| 升级到新版 | 需要重新 apply 所有 patch | 跟随 main 分支 merge 即可 |

## 升级到 main 新版本时

```bash
git checkout electron-v2  # 或你的 electron 集成分支
git fetch upstream
git merge upstream/main
# 通常没有冲突，因为 main 分支的所有改动都集中在 src/lib/ + src/cli/
# 而你的修改在 src/electron/ 全新目录
```

验证清单：
- [ ] `src/electron/` 目录未被删除
- [ ] `src/electron/index.ts` 中的 `convertGsplat` 仍能调用
- [ ] `rollup.config.mjs` 包含 electron 构建配置
- [ ] `WebPCodec.wasmUrl` 在 renderer 中被正确设置
- [ ] `setupElectronLogger` 能收到 progress/message 事件
