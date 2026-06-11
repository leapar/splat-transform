# Electron 集成升级方案（main → electron-v2）

> 配套文档：`docs/leapar-changes.md`（记录 leapar 老分支的 11 个提交与设计动机）。
> 本文记录**从 main 分支升级支持 Electron 的新方案**——不复制 leapar 老分支的 patch，而是利用 main 分支自带的抽象。

---

## 1. 背景

leapar 老 `electron` 分支（`v0.13.1`，11 个直接修改业务文件的提交）走的是「打补丁」路线：

- `src/index.ts` 被反复修改（`main` 删掉再恢复，`deal` → `convertGsplat` 反复重命名，4 处 `console.log` 替换）
- `src/index-electron.ts` 整个新增（263 行 Electron 专用入口）
- `src/utils/k-means.ts`、`src/writers/write-sog.ts` 直接改签名以支持 GPU
- `src/gpu/gpu-device.ts` 注释掉 `initializeGlobals()`
- `lib/webp.electron.mjs` 新增并改 import
- `rollup.config.mjs` 新增 `umdapplication` 构建配置

main 分支（`v2.5+`）重构后暴露了一组抽象，**让 Electron 集成几乎零业务代码改动**：

| main 分支抽象 | 对应老分支打补丁的地方 | 新版处理 |
|--------------|----------------------|---------|
| `logger` + `Renderer` 接口 | 12 处 `(window as any).api.message.log` 手动替换 | 写一个 `ElectronLoggerRenderer` 拦截 `LogEvent` 流即可 |
| `ReadFileSystem` / `FileSystem` | `src/index-electron.ts` 手写 `readFile` / `writeFile` | 写一个 `IpcReadFileSystem` / `IpcWriteFileSystem` 适配器 |
| `WebPCodec.wasmUrl` 静态属性 | webp-codec.ts 里的 `process.env.NODE_ENV` / `IS_ELECTRON` 分支 | 运行时一行赋值即可 |
| `Options.cpu` + `DeviceCreator` 注入 | `k-means.ts` `device?` 参数、`write-sog.ts` `shMethod` 参数 | 不用动 |
| `src/cli/` vs `src/lib/` 拆分 | `main` 函数删/恢复反复折腾 | 直接从 `src/lib` 复用 `combine / readFile / writeFile` |

---

## 2. 新版方案的设计原则

1. **零业务代码改动** —— `src/lib/`、`src/cli/` 下面任何文件都不应被修改。所有 Electron 适配都在新增目录 `src/electron/` 下。
2. **基于抽象编程** —— 任何对 main 分支代码的依赖都通过 `ReadFileSystem` / `FileSystem` / `Renderer` / `Options.cpu` 等公开抽象进行。
3. **结构化返回值** —— `convertGsplat` 沿用 leapar 老分支的 `{ isOk, rowCount, durationMs, error? }` 形状，方便已有 Electron 主进程代码零改动迁移。
4. **IPC 桥接可插拔** —— Electron host 提供一个 `IpcBridge`（4 个 `Promise` 方法），Electron 适配器只负责把这个桥接到 `ReadFileSystem` / `FileSystem`，便于在测试中替换为 mock。
5. **构建独立** —— `rollup.config.mjs` 追加第 4 个构建配置 `electron`（esm 格式，输出 `dist/electron.mjs`），不与 `cli` / `lib` 的 esm/cjs 配置耦合。

---

## 3. 文件清单（electron-v2 相对 main 的所有变更）

| 路径 | 状态 | 作用 |
|------|------|------|
| `src/electron/electron-logger.ts` | 新增 | `ElectronLoggerRenderer`，把 `LogEvent` 翻译成 IPC 消息 |
| `src/electron/file-system.ts` | 新增 | `IpcReadFileSystem` / `IpcWriteFileSystem` / `IpcBridge` 适配器（含流式写） |
| `src/electron/index.ts` | 新增 | `convertGsplat` + `setupElectronLogger` + `isElectron` + 精选 lib re-export |
| `src/electron/README.md` | 新增 | 集成指南 + 快速开始 + API 概览 + `electron-builder` 配置 |
| `rollup.config.mjs` | 修改 | 追加 `electron` 构建配置 |
| `package.json` | 修改 | `exports` 增加 `"./electron"` 子路径；`build` 拷贝 `electron.d.cts`；`bin` 增加 `splat-transform-electron` |
| `bin/electron.mjs` | 新增 | 演示入口（运行 `node bin/electron.mjs INPUT OUTPUT`） |
| `test/electron.test.mjs` | 新增 | Electron 构建的烟雾测试（导出完整性） |
| `docs/electron-upgrade.md` | 新增 | 本文档 |
| `docs/leapar-changes.md` | 保留 | 历史决策记录（不动） |

---

## 4. 升级到 main 新版本时的操作流程

### 4.1 流程

```bash
# 1. 备份
git checkout electron-v2
git checkout -b electron-v2-backup-$(date +%Y%m%d)

# 2. 同步 upstream
git remote add upstream https://github.com/playcanvas/splat-transform.git
git fetch upstream
git merge upstream/main
```

### 4.2 冲突处理原则

**新方案下几乎不会有冲突**，因为 main 分支的演进主要在：

- `src/lib/**` 的内部实现
- `src/cli/**` 的命令行参数
- `Options` / `LogEvent` / `Renderer` / `ReadFileSystem` 等**公开抽象**的字段

而 electron-v2 的所有改动都在**新增**的 `src/electron/` 目录 + `rollup.config.mjs` 末尾追加的配置块。**唯一需要关注**的潜在冲突点：

| 冲突点 | 出现条件 | 处理方式 |
|--------|----------|---------|
| `rollup.config.mjs` | upstream 在末尾追加新的构建配置 | 把 `electron` 配置块追加到新数组末尾，不要插入到中间 |
| `package.json` | upstream 改 `exports` / `bin` 段 | 把新增的 `"./electron"` 子路径和 `splat-transform-electron` bin 追加进去；`build` script 增加 `electron.d.cts` 拷贝 |
| `Options` 新增必填字段 | upstream 加新必填字段 | 同步更新 `src/electron/index.ts` 的 `defaultOptions`，保持其它业务逻辑零改动 |
| `ReadStream` / `Writer` / `ReadSource` 接口签名 | upstream 调整 `ReadStream` 抽象方法 | 同步更新 `src/electron/file-system.ts` 里的 `InMemoryReadStream` / `IpcWriter` |
| `LogEvent` 增删 kind | upstream 改 logger 事件种类 | 同步更新 `src/electron/electron-logger.ts` 的 `switch (event.kind)` |
| `WebPCodec.wasmUrl` 改名/废弃 | upstream 改 wasm 注入方式 | 同步更新 `src/electron/README.md` 的快速开始示例，并在 `electron-logger.ts` 旁边加 deprecation 警告 |
| `ProcessOptions` 字段增删 | upstream 改 process.ts 的类型 | 同步更新 `src/electron/index.ts` 里 `processOptions` 默认值 |

### 4.3 验证清单

合并完成后逐项确认：

- [ ] `src/lib/` 下面没有 electron-v2 的修改痕迹（`git diff upstream/main..HEAD -- src/lib/` 为空）
- [ ] `src/cli/` 下面没有 electron-v2 的修改痕迹
- [ ] `src/electron/` 4 个文件齐全（`index.ts` / `file-system.ts` / `electron-logger.ts` / `README.md`）
- [ ] `rollup.config.mjs` 的 export 数组里**最后**一项是 `electron` 配置块
- [ ] `npx tsc --noEmit` 在 `src/electron/` 下零错误
- [ ] `pnpm build` 成功，产物含 `dist/electron.mjs` + `dist/electron.d.ts`
- [ ] `node bin/cli.mjs --help` 仍可用（CLI 没被破坏）
- [ ] 文档 `docs/leapar-changes.md` 与 `docs/electron-upgrade.md` 都在仓库里

### 4.4 自动化校验脚本

把上面的"验证清单"写成 `scripts/verify-electron-upgrade.mjs`：

```js
// 伪代码：检查 electron-v2 升级是否完整
const fs = require('node:fs');
const path = require('node:path');

const checks = [
    ['src/electron/index.ts',          fs.existsSync],
    ['src/electron/file-system.ts',    fs.existsSync],
    ['src/electron/electron-logger.ts',fs.existsSync],
    ['src/electron/README.md',         fs.existsSync],
    ['bin/electron.mjs',               fs.existsSync],
    ['test/electron.test.mjs',         fs.existsSync],
    ['docs/electron-upgrade.md',       fs.existsSync],
    ['docs/leapar-changes.md',         fs.existsSync],
    ['rollup.config.mjs', f => {
        const txt = fs.readFileSync('rollup.config.mjs', 'utf8');
        return /const\s+electron\s*=/.test(txt) && /entryFileNames:\s*'electron\.mjs'/.test(txt);
    }],
    ['package.json', f => {
        const pkg = JSON.parse(fs.readFileSync('package.json', 'utf8'));
        return !!pkg.exports?.['./electron']
            && !!pkg.bin?.['splat-transform-electron']
            && /electron\.d\.cts/.test(JSON.stringify(pkg.scripts));
    }]
];

let ok = true;
for (const [p, fn] of checks) {
    if (!fn(p)) { console.error('❌', p); ok = false; }
    else console.log('✅', p);
}
process.exit(ok ? 0 : 1);
```

实际脚本以 `test/electron.test.mjs` 为准（用 `node --test` 跑）。

---

## 5. 与 leapar 老分支方案的关键差异

| 维度 | leapar 老分支（11 个提交） | electron-v2 新分支 |
|------|--------------------------|---------------------|
| 入口 | 新建 `src/index-electron.ts`（263 行手写） | 在 `src/electron/index.ts`（220 行）组装 main 分支的 `combine` / `readFile` / `writeFile` |
| 日志 | 12 处散落替换 `console.log` → `api.message.log` | 1 个 `ElectronLoggerRenderer` 拦截 `LogEvent` 流 |
| WASM 路径 | `src/utils/webp-codec.ts` 改 import + `NODE_ENV` 分支 | 运行时 `WebPCodec.wasmUrl = new URL(...)` |
| 跨平台 fs | `node:fs/promises` 直接 import | 注入 `IpcReadFileSystem` / `IpcWriteFileSystem` |
| GPU/CPU | `kmeans(points, k, iter, device?)` 改签名 | `Options.cpu` + 注入 `createDevice` |
| 业务代码侵入 | 7 个核心文件被改 | **0 个** |
| 升级到 upstream 新版 | 需 reapply 所有 patch | 跟随 main 分支 merge 即可，理论零冲突 |
| `webp.electron.mjs` | 新增（468 行 Emscripten 输出） | 不需要（main 分支已用 `WebPCodec.wasmUrl` 解耦） |

---

## 6. 何时回退到 leapar 老方案

electron-v2 方案**不是**对老方案的全面替代。下列情况需要保留 leapar 老 `electron` 分支：

1. **需要 Node.js 端直接调用** —— 如果 Electron 主进程想用 `node:fs/promises` 直接读盘（`nodeIntegration: true` 的旧式 Electron 应用），leapar 老分支的 `src/index-electron.ts` 更直接。
2. **需要旧式 umd 产物** —— 某些打包工具链（`require('gsplat-electron')` 风格）需要 umd 而非 esm；leapar 老分支的 `umdapplication` 配置可以输出。
3. **WebP WASM 资源与 Emscripten 输出耦合** —— 一些 Electron 应用希望 wasm 文件**嵌入**到 `resources/` 目录（不走 `WebPCodec.wasmUrl` 动态注入），leapar 老分支的 `process.resourcesPath + app.asar.unpacked` 路径处理更直接。

> 总结：electron-v2 是**干净、可维护、易升级**的方案；leapar 老分支是**直接、可控、贴近 Electron 底层**的方案。两者并存，按场景选用。

---

## 7. 给未来 AI 助手的沟通话术

如果你让 AI 助手把 electron-v2 升级到 upstream 新版本，建议这样说：

> 请按照 `docs/electron-upgrade.md` 第 4 节的"升级流程"和"冲突处理原则"，把 upstream main 的新功能 merge 到当前 electron-v2 分支。注意：
> 1. `src/electron/` 下面所有文件是 electron-v2 新增的，upstream 没有，**不要删除**。
> 2. `rollup.config.mjs` 末尾的 `electron` 配置块是 electron-v2 新增的，**保留**为数组最后一项。
> 3. `src/lib/` 和 `src/cli/` 不应被 electron-v2 的代码改动。
> 4. 合并后跑 `docs/electron-upgrade.md` 第 4.3 节的验证清单。
> 5. 冲突时优先接受 upstream 的 `src/lib/**` 改动，electron-v2 侧的 `src/electron/**` 不动。
