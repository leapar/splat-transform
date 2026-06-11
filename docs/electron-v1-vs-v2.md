# Electron 分支对比：v1（leapar 老版） vs v2（main 升级版）

> 本文按**函数 / 能力**维度对比两个方案的实现差异。配套文档：
> - [leapar-changes.md](./leapar-changes.md) — leapar 老分支的 11 个 commit 史
> - [electron-upgrade.md](./electron-upgrade.md) — 升级方法论

## 总览

| 维度 | v1（leapar `electron` 分支） | v2（`electron-v2` 分支） |
|------|--------------------------|----------------------|
| 入口文件数 | 1 个新增 `src/index-electron.ts`（263 行） | 3 个新增 `src/electron/*.ts`（771 行） |
| 业务代码改动文件数 | **7 个**核心文件 | **0 个** |
| 业务代码改动总行数 | 100+ 行散落 | 0 |
| 新增专用文件 | 2 个（`index-electron.ts` + `webp.electron.mjs` 468 行） | 6 个（`index.ts`/`file-system.ts`/`electron-logger.ts`/`README.md` + `bin/electron.mjs` + `test/electron.test.mjs`） |
| 构建产物 | 1 个 `gsplat-electron.js`（esm） | 1 个 `dist/electron.mjs`（esm，含 .d.ts） |
| 包发布 | 单一 .mjs | 4 个产物 + `exports["./electron"]` 子路径 |
| 可执行 demo | 无 | `bin/electron.mjs` |
| 单元测试 | 无 | `test/electron.test.mjs` |
| 流式写大文件 | ❌ 一次性 concat | ✅ 16 MiB 阈值 `append` |
| 错误类型 | `error: any` | `error: Error`（类型安全） |
| 进程上下文 | 无 | `processOptions` 第三参数 |

---

## 1. `convertGsplat` —— 主入口

### v1（leapar 老版）

**位置**：`src/index-electron.ts:203-261`

```typescript
const convertGsplat = async(filename: string, outputFilename: string) => {
    try {
        // 1. 读
        const inputFile = await (async () => {
            const file = await readFile(resolve(filename), []);
            if (file.elements.length !== 1 || file.elements[0].name !== 'vertex') {
                throw new Error(`Unsupported data in file '${filename}'`);
            }
            const element = file.elements[0];
            const { dataTable } = element;
            if (dataTable.numRows === 0 || !isGSDataTable(dataTable)) {
                throw new Error(`Unsupported data in file '${filename}'`);
            }
            element.dataTable = processDataTable(dataTable, []);
            return file;
        })();

        // 2. combine + process
        const dataTable = processDataTable(
            combine([inputFile].map(file => file.elements[0].dataTable)),
            []
        );

        if (dataTable.numRows === 0) {
            throw new Error('No splats to write');
        }

        (window as any).api.message.log(`Loaded ${dataTable.numRows} gaussians`);

        // 3. 写（硬编码 cpu: false / iterations: 10）
        const options: Options = { overwrite: true, help: false, version: false, cpu: false, iterations: 10 };
        await writeFile(resolve(outputFilename), dataTable, options);

        return { isOk: true };
    } catch (err) {
        console.error(err);
        return { isOk: false, error: err };   // error: any，无类型保证
    }
};
```

**问题**：
- 硬编码 `cpu: false` / `iterations: 10`，调用方无法覆盖
- `processDataTable(dataTable, [])` 没传 `processOptions`
- `error: any` 类型不安全
- `console.error(err)` 而不是通过 IPC
- 自己手写 `readFile` / `writeFile` / `combine` / `isGSDataTable` 全套（直接复制 `src/index.ts` 的 CLI 版逻辑）

### v2（main 升级版）

**位置**：`src/electron/index.ts:151-238`

```typescript
const convertGsplat = async (config: ConvertGsplatOptions): Promise<ConvertGsplatResult> => {
    const start = performance.now();
    const emitIoMessages = config.emitIoMessages !== false;

    // 1. 组装 ReadFileSystem / FileSystem（来自 IpcBridge）
    let readFs: ReadFileSystem;
    let writeFs: WriteFileSystem;
    if (config.bridge) { ... }
    else if (config.readFileSystem && config.writeFileSystem) { ... }
    else return { isOk: false, error: new Error('must supply either `bridge` or both ...') };

    const mergedOptions: Options = { ...defaultOptions, ...config.options };  // 允许覆盖
    const actions = config.processActions ?? [];
    const processOptions: ProcessOptions = config.processOptions ?? {};       // processOptions 第三参数

    try {
        const scope = logger.group(`Convert ${config.input} -> ${config.output}`);
        try {
            const inputFormat = getInputFormat(config.input);
            const outputFormat = getOutputFormat(config.output, mergedOptions);

            if (emitIoMessages) logger.info(`reading '${config.input}'...`);
            const dataTables = await readFile({ ... fileSystem: readFs });  // 用 main 库的 readFile

            if (!dataTables || dataTables.length === 0) throw new Error(`No data found in ${config.input}`);

            const combined = combine(dataTables);
            const processed = await processDataTable(combined, actions, processOptions);

            if (processed.numRows === 0) throw new Error('No Gaussians to write');

            if (emitIoMessages) logger.info(`writing '${config.output}'...`);
            await writeFile({ ... createDevice: config.createDevice }, writeFs);  // 用 main 库的 writeFile

            const durationMs = performance.now() - start;
            logger.info(`Loaded ${processed.numRows} gaussians in ${(durationMs / 1000).toFixed(1)}s`);
            scope.end();
            return { isOk: true, rowCount: processed.numRows, durationMs };
        } catch (err) {
            logger.unwindAll(true);
            throw err;
        }
    } catch (err) {
        return { isOk: false, error: err instanceof Error ? err : new Error(String(err)) };  // error: Error
    }
};
```

**差异**：
- ✅ 配置驱动：Options / processActions / processOptions / createDevice / emitIoMessages 都可外部覆盖
- ✅ 复用 main 分支的 `readFile` / `writeFile` / `combine` / `getInputFormat` / `getOutputFormat`
- ✅ 通过 `logger` 转发事件 → `ElectronLoggerRenderer` 一处拦截
- ✅ `error: Error` 类型安全
- ✅ 支持 `bridge` 简化调用，也支持分离的 `readFileSystem`/`writeFileSystem`
- ✅ 返回值带 `durationMs`（v1 没有）
- ✅ `processDataTable(combined, actions, processOptions)` 传第三个参数

---

## 2. `readFile` —— 读取输入

### v1（`src/index-electron.ts:29-66`）

```typescript
const readFile = async (filename: string, params: Param[]) => {
    // ... 根据后缀手动分派到 readKsplat / readSplat / readSog / readPly / readSpz
    // 硬编码 node:fs 的 open()
    const inputFile = await open(filename, 'r');
    if (lowerFilename.endsWith('.ksplat')) { ... }
    // ...
};
```

**问题**：
- ❌ 直接 `node:fs.open` —— 在 Electron renderer 进程里跑不了
- ❌ 文件格式分发硬编码（加新格式要改这里）
- ❌ 与 `src/index.ts` 的 CLI 版 `readFile` 重复实现

### v2（`src/lib/read.ts` 直接复用，v2 不实现）

新版**不实现 `readFile`**，而是直接调用 main 分支的：

```typescript
import { readFile, getInputFormat } from '../lib';

const dataTables = await readFile({
    filename: config.input,
    inputFormat: getInputFormat(config.input),
    options: mergedOptions,
    params: [],
    fileSystem: readFs   // ← 来自 IpcBridge 适配器
});
```

**差异**：
- ✅ 零代码复用 main 分支的格式分发
- ✅ `fileSystem` 参数传入 `IpcReadFileSystem`，I/O 走 IPC
- ✅ 加新格式只改 main 分支

---

## 3. `writeFile` —— 写输出

### v1（`src/index-electron.ts:88-132`）

```typescript
const writeFile = async (filename: string, dataTable: DataTable, options: Options) => {
    const outputFormat = getOutputFormat(filename);
    (window as any).api.message.log(`writing '${filename}'...`);

    // 直接 node:fs 操作
    const tmpFilename = `.${basename(filename)}.${process.pid}.${Date.now()}.${randomBytes(6).toString('hex')}.tmp`;
    const tmpPathname = join(dirname(filename), tmpFilename);
    const outputFile = await open(tmpPathname, 'wx');

    try {
        switch (outputFormat) {
            case 'sog':   await writeSog(outputFile, dataTable, filename, options.iterations, options.cpu ? 'cpu' : 'gpu'); break;
            case 'lod':   await writeLod(outputFile, dataTable, filename, options.iterations, options.cpu ? 'cpu' : 'gpu'); break;
            // ...
        }
        await outputFile.sync();
    } finally {
        await outputFile.close().catch(() => {});
    }
    await rename(tmpPathname, filename);   // 原子 rename
};
```

**问题**：
- ❌ 直接 `node:fs.open` —— renderer 跑不了
- ❌ 自己手写 tmpfile + rename 原子写入（v2 由 main 库处理）
- ❌ 硬编码 `options.cpu ? 'cpu' : 'gpu'`（v2 暴露 `Options.cpu`）

### v2（`src/lib/write.ts` 直接复用）

```typescript
import { writeFile, getOutputFormat } from '../lib';

await writeFile({
    filename: config.output,
    outputFormat: getOutputFormat(config.output, mergedOptions),
    dataTable: processed,
    options: mergedOptions,   // mergedOptions.cpu 控制 cpu/gpu
    createDevice: config.createDevice
}, writeFs);   // ← 来自 IpcBridge 适配器
```

**差异**：
- ✅ 复用 main 分支的 tmpfile + rename 原子写入
- ✅ `Options.cpu` + `createDevice` 取代硬编码的 `'cpu' | 'gpu'`
- ✅ `fileSystem` 参数走 IPC

---

## 4. `combine` —— 合并多表

### v1（`src/index-electron.ts:136-188`，53 行）

手写 `combine`，逻辑跟 `src/index.ts` 的 CLI 版完全一致。

### v2（`src/lib/data-table.ts` 直接复用）

```typescript
import { combine } from '../lib';
const combined = combine(dataTables);
```

**差异**：
- ✅ 直接 import，零代码重复
- ✅ 加新合并规则只改 main 分支

---

## 5. `isGSDataTable` —— 输入验证

### v1（`src/index-electron.ts:190-201`）

手写 12 行的列名白名单检查。

### v2

**完全省略**。新版 `readFile` / `combine` / `processDataTable` 流水线里已经做了列名校验，调用方无需自己写。

---

## 6. `WebPCodec.create` —— WebP wasm 路径解析

### v1（`src/utils/webp-codec.ts:7-25`）

```typescript
static async create() {
    const instance = new WebPCodec();
    instance.Module = await createModule({
        locateFile: (path: string) => {
            if (path.endsWith('.wasm')) {
                let wasmPath = "";
                if (process.env.NODE_ENV === "development") {
                    wasmPath = "file://" + resolve(__dirname, `../../resources/${path}`);
                } else {
                    wasmPath = "file://" + resolve(join((process as any).resourcesPath, "app.asar.unpacked", "resources", path));
                }
                return wasmPath;
            }
            return path;
        }
    });
    return instance;
}
```

**问题**：
- ❌ 改 main 分支的业务文件
- ❌ 编译时 hardcode `process.env.NODE_ENV`（build-time 替换）
- ❌ 改 import 为 `webp.electron.mjs`（专用 468 行 Emscripten 输出）
- ❌ 不同环境要重新 build

### v2

main 分支的 `WebPCodec.wasmUrl` 已经是公开静态属性。v2 端**不改任何业务代码**：

```typescript
// renderer 进程注入
import { WebPCodec } from '@playcanvas/splat-transform/electron';

if (isElectron() && !process.env.NODE_ENV?.includes('dev')) {
    WebPCodec.wasmUrl = `file://${resourcesPath}/app.asar.unpacked/resources/webp.wasm`;
} else {
    WebPCodec.wasmUrl = new URL('../../resources/webp.wasm', import.meta.url).href;
}
```

**差异**：
- ✅ main 分支业务代码零改动
- ✅ 运行时根据 `isElectron()` 切换
- ✅ 不需要 `webp.electron.mjs` 专用副本

---

## 7. `gpu-device.createDevice` —— GPU 设备创建

### v1（`src/gpu/gpu-device.ts:108-150`）

**修改了 3 处**：
- 第 28 行：注释 `//import { create, globals } from 'webgpu';`
- 第 31 行：注释 `//Object.assign(globalThis, globals);`
- 第 64 行：注释 `//initializeGlobals();`
- 第 113 行：注释 `//window.navigator.gpu = create([]);`
- 第 130 行：GPU info 日志改为 `api.message.log`

```typescript
const createDevice = async () => {
    globalThis.Worker = Worker;
    // window.navigator.gpu = create([]);   // 注释掉

    const canvas = document.createElement('canvas');
    // ...
    const graphicsDevice = new WebgpuGraphicsDevice(canvas, { ... });
    await graphicsDevice.createDevice();

    const info = (graphicsDevice as any).gpuAdapter.info || {};
    (window as any).api.message.log(`Created gpu device="${info.device || '-'}"...`);  // ← leapar 改

    // ...
};
```

### v2

**完全不动 `src/gpu/gpu-device.ts`**。新版让 host 端在 `createDevice` 字段里注入：

```typescript
await convertGsplat({
    input, output, options, bridge,
    createDevice: () => /* host 在 renderer 创建 GPU device */ Promise.resolve(device)
});
```

**差异**：
- ✅ main 分支业务代码零改动
- ✅ GPU device 创建逻辑移到 host（renderer 进程里更自然）

---

## 8. `kmeans` —— 聚类（GPU/CPU 分支）

### v1（`src/utils/k-means.ts:137-201`）

```typescript
const kmeans = async (points: DataTable, k: number, iterations: number, device?: GpuDevice) => {
    // ...
    const gpuClustering = device && new GpuClustering(device, points.numColumns, k);
    // ...
    while (!converged) {
        if (gpuClustering) {
            await gpuClustering.execute(points, centroids, labels);
        } else {
            clusterKdTreeCpu(points, centroids, labels);
        }
        // ...
    }
    (window as any).api.message.log(`Running k-means clustering: ...`);  // ← leapar 加
    // ...
    (window as any).api.message.log(' done 🎉');  // ← leapar 加
};
```

**修改了 2 处**：
- 162 行 / 198 行：把 `stdout.write('#')` 替换为 `api.message.log`
- 137 行签名：`kmeans(points, k, iterations, device?)` 接受 GpuDevice

### v2

**完全不动 `src/utils/k-means.ts`**。新版通过 main 库 `Options.cpu` 控制，host 端无需关心。

```typescript
// v2 用户
await convertGsplat({
    options: { cpu: true, ... },   // true → main 库走 CPU kd-tree 路径
    bridge
});
```

**差异**：
- ✅ main 分支业务代码零改动
- ✅ `device?` 参数在 main 分支里就有，v2 不需要新增

---

## 9. `writeSog` —— SOG 写入（GPU/CPU 分支）

### v1（`src/writers/write-sog.ts:110`）

```typescript
const writeSog = async (fileHandle, dataTable, outputFilename, shIterations = 10, shMethod: 'cpu' | 'gpu', indices = ...) => {
    // ...
    if (shMethod === 'gpu' && !gpuDevice) {
        gpuDevice = await createDevice();
    }
    // ...
};
```

**修改了**：函数签名加 `shMethod: 'cpu' | 'gpu'` 参数。

### v2

**完全不动 `src/writers/write-sog.ts`**。v2 通过 main 库 `Options.cpu` + `createDevice` 注入。

---

## 10. `rollup.config.mjs` —— 构建配置

### v1（11 行 → 43 行）

```javascript
const umdapplication = {
    input: 'src/index-electron.ts',
    output: { dir: 'dist', format: 'esm', sourcemap: true, name: 'SPLAT', entryFileNames: 'gsplat-electron.js' },
    external: ['webgpu'],
    plugins: [typescript({ tsconfig: './tsconfig.json' }), resolve(), json()],
    cache: false
};
export default [application, umdapplication];
```

### v2（基于 main 分支的 4 个构建配置）

```javascript
const electron = {
    input: 'src/electron/index.ts',
    output: { dir: 'dist', format: 'esm', sourcemap: true, entryFileNames: 'electron.mjs' },
    external: ['playcanvas', 'webgpu'],
    plugins: [versionReplace(), typescript({ tsconfig: './tsconfig.json', declaration: true, declarationDir: 'dist' }), resolve(), json()],
    cache: false
};
export default [esm, cjs, cli, electron];
```

**差异**：
- ✅ 4 个产物（esm + cjs + cli + electron），覆盖全部使用场景
- ✅ 产物名 `electron.mjs`（更清晰）
- ✅ 带类型声明 `dist/electron.d.ts`
- ✅ `versionReplace` 注入版本号
- ✅ external `playcanvas`（peerDep，避免打包进产物）

---

## 11. 日志转发

### v1：12 处散落替换

```bash
$ grep -rn "(window as any).api.message.log" src/
src/gpu/gpu-device.ts:130
src/index-electron.ts:33, 92, 237        # 3 处
src/index.ts:51, 110, 556, 568          # 4 处（CLI 版）
src/utils/k-means.ts:162, 198           # 2 处
src/writers/write-lod.ts:265
src/writers/write-sog.ts:126
# 共 12 处
```

**问题**：
- ❌ 12 处散落，要靠人工记得替换
- ❌ 漏一个就少一条日志
- ❌ 改业务代码（CLI 版 `index.ts` 也被改，破坏了 CLI 工具的纯 stdout 输出）

### v2：1 个 Renderer 拦截

```typescript
// 单一拦截点
export class ElectronLoggerRenderer implements Renderer {
    handle(event: LogEvent): void {
        switch (event.kind) {
            case 'message':   this.send({ channel: 'message', data: { level, text }, text }); break;
            case 'output':    this.send({ channel: 'output', text }); break;
            case 'scopeStart':this.send({ channel: 'log', data: { phase: 'start', ...event }, text }); break;
            case 'scopeEnd':  this.send({ channel: 'log', data: { phase: 'end', ...event }, text }); break;
            case 'barStart':  this.send({ channel: 'progress', data: { phase: 'start', ... }, text }); break;
            case 'barTick':   this.send({ channel: 'progress', data: { phase: 'tick', current, total }, text }); break;
            case 'barEnd':    this.send({ channel: 'progress', data: { phase: 'end', ... }, text }); break;
        }
    }
}
```

```typescript
// 调用方
const detach = setupElectronLogger((msg) => window.api.message.send(msg));
```

**差异**：
- ✅ 1 个类拦截所有 7 种 `LogEvent` 种类
- ✅ 业务代码零改动（CLI 版 `index.ts` 完全保持纯 stdout）
- ✅ 新增 `LogEvent` 类型自动被拦截

---

## 12. 文件系统抽象

### v1

- ❌ `src/index-electron.ts` 直接 `import { open, rename } from 'node:fs/promises'`
- ❌ 渲染进程跑不了（`node:fs` 在 sandbox renderer 里不可用）
- ❌ 假如用 `nodeIntegration: true`（不安全）才能跑

### v2

- ✅ `IpcReadFileSystem` / `IpcWriteFileSystem` 适配器（`src/electron/file-system.ts`）
- ✅ `IpcBridge` 接口由 host 端实现 → 走 IPC
- ✅ `IpcWriter` 支持流式写（16 MiB 阈值 + `append: true`）
- ✅ 适合 `contextIsolation: true` + `sandbox: true` 安全配置

---

## 13. 类型安全

### v1

- `Options` 类型（v1 自己的）：`{ overwrite, help, version, cpu, iterations, viewerSettingsPath? }` —— 跟 main 分支的 `Options` 不兼容
- `convertGsplat` 返回 `{ isOk: true } | { isOk: false, error: any }` —— `error: any`
- `convertGsplat(filename, outputFilename)` 只接受 2 个 string

### v2

- `Options` 直接用 main 分支的（`{ iterations, lodSelect, unbundled, lodChunkCount, lodChunkExtent, ... }`）
- `ConvertGsplatResult` 强类型：`{ isOk: true, rowCount, durationMs } | { isOk: false, error: Error }`
- `convertGsplat({ input, output, options, processActions, processOptions, bridge, createDevice, ... })` —— 完整配置对象
- 精选的 `lib` re-export 提供完整类型

---

## 14. 包发布

### v1

- 单一 `dist/gsplat-electron.js`
- 没有 `package.json` 子路径
- 只能 `import 'splat-transform/dist/gsplat-electron.js'`（丑陋）

### v2

- `dist/electron.mjs` + `dist/electron.d.ts` + `dist/electron.d.cts`
- `package.json` 含 `"./electron"` subpath：
  ```json
  "exports": {
    "./electron": {
      "import": { "types": "./dist/electron.d.ts", "default": "./dist/electron.mjs" }
    }
  }
  ```
- 用户 `import { convertGsplat } from '@playcanvas/splat-transform/electron'` 干净
- `bin` 多一个 `splat-transform-electron` 可执行命令

---

## 15. 测试 / 调试

### v1

- ❌ 没有专门测试
- ❌ 没有 demo 命令
- ❌ 升级时只能手动对比 patch

### v2

- ✅ `test/electron.test.mjs` 用 `node --test` 跑导出完整性
- ✅ `bin/electron.mjs` Node 端 smoke test（不依赖 Electron 进程模型）
- ✅ `docs/electron-v1-vs-v2.md`（本文）+ `docs/electron-upgrade.md` 双文档

---

## 总结

v1 走的是"打补丁"路线 —— 把库本身改成 Electron 专用。v2 走的是"在抽象层之上组合"路线 —— 库保持通用，Electron 适配作为外部薄包装。

**关键差异**：
1. 业务代码侵入：v1 改 7 个核心文件，v2 改 0 个
2. 升级成本：v1 需要 reapply 12 处 patch，v2 跟随 main merge 零冲突
3. 大文件写：v1 一次性内存，v2 流式 append
4. 类型安全：v1 `error: any`，v2 `error: Error`
5. 可测试性：v1 只能在 Electron 里跑，v2 Node 端就能 smoke test

如果坚持要 v1 风格（直接改业务代码、用 `webp.electron.mjs` 专用副本、硬编码 NODE_ENV 分支），仍可保留 `electron` 分支作为兼容方案。v2 不替代 v1，而是为新项目提供更干净的选择。
