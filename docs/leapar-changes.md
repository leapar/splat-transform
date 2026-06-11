# leapar 对 splat-transform 项目的修改记录

> 原始代码仓库：https://github.com/leapar/splat-transform
> 当前分支：electron
> 修改时间：2025-10-26 至 2026-06-11
> 共 11 个提交（10 个代码提交 + 1 个文档提交）

---

## 改造目的与意义

### 整体目标

将 `splat-transform`（原本是 Node.js CLI 工具）改造为 **Electron 桌面应用主进程可调用的库**，作为更大桌面产品的一部分。

### 改造前 vs 改造后

| 维度 | 改造前（upstream main） | 改造后（electron 分支） |
|------|----------------------|----------------------|
| **使用方式** | CLI 命令行（`splat-transform input.ply output.sog`） | Electron 主进程调用 `convertGsplat()` 函数 |
| **入口** | `main` 函数 + CLI 参数解析 | `convertGsplat(filename, outputFilename)` 异步函数 |
| **返回值** | 进程退出码 | `{ isOk: boolean, error?: any }` |
| **日志输出** | `console.log` 打印到终端 | `window.api.message.log` 通过 IPC 传回渲染进程 |
| **WASM 路径** | 相对 URL 构造 | 区分开发/生产环境，生产用 `app.asar.unpacked/resources/` |
| **WebP 模块** | 通用 `webp.mjs` | 专用 `webp.electron.mjs`（兼容 Node 渲染） |
| **构建产物** | `gsplat.mjs` | + `gsplat-electron.js`（esm） |
| **GPU/CPU 模式** | 默认 GPU | Electron 端默认 GPU，遗留 deal 函数默认 CPU |

### 改造的 6 大目的

#### 1. **支持 Electron 桌面应用集成**
- 移除 CLI 入口，避免 `process.exit()`、命令行参数解析等 Node CLI 特征
- 改为导出可被 Electron 主进程直接调用的 `convertGsplat` 函数
- 返回值改为对象（`{ isOk, error }`），方便渲染进程判断成功/失败

#### 2. **日志通道化（日志送回渲染层）**
- 所有 `console.log` 替换为 `(window as any).api.message.log(...)`
- 通过 Electron 预加载脚本注入的 `api.message` 桥接器把日志从渲染进程送回主进程/UI 显示
- 让用户在桌面应用界面能看到转换进度（"reading..."、"writing..."、k-means 进度等）

#### 3. **WASM 路径在 Electron 打包后能正确解析**
- Electron 用 `electron-builder` 打包后，WASM 文件位于 `app.asar.unpacked/resources/`
- 区分 `NODE_ENV === 'development'` 和生产环境，给出不同的 `file://` 路径
- 开发时用 `../../resources/`，生产时用 `process.resourcesPath + app.asar.unpacked/resources/`

#### 4. **CPU/GPU 模式可配置**
- `writeSog` 接受 `shMethod: 'cpu' | 'gpu'` 参数
- 桌面应用可以给用户开关控制
- Electron 端默认 `cpu: false`（优先 GPU），遗留函数默认 `cpu: true`（兼容旧用法）

#### 5. **构建产物分离**
- `rollup.config.mjs` 增加 `umdapplication`（后改 esm）配置
- 输出 `gsplat-electron.js` 给 Electron 的 preload/renderer 加载
- 外部化 `webgpu` 模块（不打包进产物，由宿主环境提供）

#### 6. **合并 upstream 的新特性**
- 通过 `e9baaf6` merge 提交引入 `write-lod.ts`、`b-tree.ts` 等新功能
- 保留 `main` 函数（CLI 入口）作为可选入口，CLI 工具依然可用

### 升级时遇到的困难

| 问题 | 解决方案 |
|------|---------|
| Electron 渲染进程没有 Node 的 `path`、`fs` 模块 | 改用纯字符串路径 + `file://` 前缀 |
| Emscripten WASM 模块需要 Node 兼容 | 重写为 `webp.electron.mjs`（用 `require` 代替 `import.meta.url`） |
| `window.navigator.gpu` 在 Electron 渲染进程已存在但需要重新注入 | 注释掉手动注入，由 Electron 自身提供 |
| 日志需要在 UI 显示 | 通过 `window.api` IPC 桥接器送回主进程 |
| 生产环境 asar 打包 | WASM 放 `app.asar.unpacked/` 避免被打包压缩 |

---

## 后续同步升级官方原始代码的操作指南

### 适用场景

当 upstream `playcanvas/splat-transform` 发布新版本时，需要把这些新功能同步到 electron 分支。

### 升级流程

#### 步骤 1：建立基线

```bash
# 在 electron 分支上创建一个备份分支
git checkout electron
git checkout -b electron-backup-$(date +%Y%m%d)

# 添加 upstream 远程（如果还没有）
git remote add upstream https://github.com/playcanvas/splat-transform.git
git fetch upstream
```

#### 步骤 2：尝试 merge 或 rebase

```bash
# 方式 A：merge（推荐，保留历史）
git merge upstream/main
# 出现冲突时按下面"冲突处理原则"处理

# 方式 B：rebase（历史更干净）
git rebase upstream/main
```

#### 步骤 3：冲突处理原则

| 冲突文件 | 处理方式 |
|---------|---------|
| `src/index.ts` | 接受 upstream 新功能（如新格式、新命令行选项）；**注意** `convertGsplat` 不在 `src/index.ts` 中（已在 4a4d3ab 迁移到 `src/index-electron.ts`）；保留 `window.api.message.log` 调用 |
| `src/index-electron.ts` | 该文件是 leapar 新增的，merge 时 upstream 没有，应保留 |
| `src/utils/k-means.ts` | 保留 `device?` 参数，保留 GPU 聚类分支 |
| `src/writers/write-sog.ts` | 保留 `shMethod: 'cpu' \| 'gpu'` 参数 |
| `src/utils/webp-codec.ts` | 保留 `webp.electron.mjs` import 和开发/生产环境路径分支 |
| `src/gpu/gpu-device.ts` | 保留 `//initializeGlobals();` 注释（Electron 不需要），保留 `navigator.gpu` 注释 |
| `rollup.config.mjs` | 保留 `umdapplication` 配置（输入 `src/index-electron.ts`） |
| `lib/webp.electron.mjs` | 该文件是 leapar 新增的，upstream 没有，保留 |
| `lib/webp.mjs` | 保留 leapar 格式化后的版本（多行可读） |
| `package.json` | 保留 `packageManager: pnpm@...` 字段 |
| `pnpm-lock.yaml` | merge 后可能需要重新 `pnpm install` |
| `src/writers/write-lod.ts` | 该文件来自 upstream merge，保留 leapar 的 `window.api.message.log` 替换 |

#### 步骤 4：功能验证清单

合并完成后，按以下清单逐项验证：

- [ ] **Electron 入口函数存在**：`src/index-electron.ts` 中导出 `convertGsplat`
- [ ] **返回值为对象**：`{ isOk: boolean, error?: any }` 而非 `boolean`
- [ ] **日志走 IPC**：搜索 `(window as any).api.message.log` 至少出现 12 处（index.ts: 4处、index-electron.ts: 3处、gpu-device.ts: 1处、k-means.ts: 2处、write-lod.ts: 1处、write-sog.ts: 1处）
- [ ] **WASM 路径正确**：webp-codec.ts 中有 `process.env.NODE_ENV` 分支判断
- [ ] **webp.electron.mjs 存在**：`lib/webp.electron.mjs` 文件未被删除
- [ ] **GPU 支持完整**：`kmeans` 有 `device?` 参数，`writeSog` 有 `shMethod: 'cpu' | 'gpu'` 参数
- [ ] **构建配置正确**：`rollup.config.mjs` 包含 `umdapplication` 配置，输出 `gsplat-electron.js`，格式为 esm
- [ ] **gpu-device 注释正确**：`initializeGlobals()` 和 `navigator.gpu` 创建被注释掉
- [ ] **pnpm 锁定**：`package.json` 包含 `packageManager` 字段

#### 步骤 5：测试

```bash
# 1. 重新安装依赖
rm -rf node_modules pnpm-lock.yaml
pnpm install

# 2. 构建
pnpm build

# 3. 检查产物
ls dist/
# 应该有：gsplat.mjs（主入口）和 gsplat-electron.js（Electron 入口）

# 4. 验证 CLI 仍可用
node bin/cli.mjs --help
node bin/cli.mjs input.ply output.sog

# 5. 验证 Electron 入口
# 在 Electron 项目中 import 验证
node -e "import('./dist/gsplat-electron.js').then(m => console.log(Object.keys(m)))"
# 应该输出 ['convertGsplat']
```

### 升级时常见的冲突解决 SOP

**冲突 1：`src/index.ts` 的 `main` 函数**
- 接受 upstream 新功能
- 保留 `(window as any).api.message.log(...)` 替换（不要回退为 `console.log`）

**冲突 2：`src/utils/webp-codec.ts` 的 wasm 路径**
- 保留 `import createModule from '../../lib/webp.electron.mjs';`
- 保留 `process.env.NODE_ENV` 分支
- 保留 `import { resolve, join } from "path";`

**冲突 3：`src/gpu/gpu-device.ts` 的初始化逻辑**
- 保留 `//initializeGlobals();` 注释
- 保留 `//import { create, globals } from 'webgpu';` 注释
- 保留 `//window.navigator.gpu = create([]);` 注释

**冲突 4：`rollup.config.mjs`**
- 保留 `umdapplication` 配置块
- 如果 upstream 改动了 `application` 配置，单独 merge 那一部分

**冲突 5：`lib/webp.mjs`**
- 该文件已被 leapar 替换为格式化版本
- merge 时接受 leapar 版本（多行可读）
- `webp.electron.mjs` 是新增的，upstream 没有，应保留

### 升级命令参考

```bash
# 完整的升级流程
git checkout electron
git fetch upstream
git merge upstream/main --no-edit
# 手动解决冲突，参考上面的"SOP"

# 验证
pnpm install
pnpm build
pnpm lint

# 如果一切正常
git add -A
git commit -m "sync: merge upstream main $(date +%Y-%m-%d) into electron"

# 如果需要回退
git merge --abort
```

### 沟通话术（指挥 AI 助手升级）

如果你需要让 AI 助手（Claude Code 等）帮你升级，可以说：

> "请按照 `docs/leapar-changes.md` 的'升级原始代码时的关键修改点'和'冲突处理原则'，把 upstream main 的新功能 merge 到当前 electron 分支。冲突时优先保留 leapar 在该文档中列出的修改，特别注意：
> 1. 保留 `src/index-electron.ts`（新增文件）
> 2. 保留 `src/index-electron.ts` 中的 `convertGsplat` 函数和 `{ isOk, error }` 返回值
> 3. 保留 `window.api.message.log` 替换
> 4. 保留 `lib/webp.electron.mjs`（新增文件）
> 5. 保留 `rollup.config.mjs` 中的 `umdapplication` 配置
> 6. 保留 `src/utils/k-means.ts` 和 `write-sog.ts` 的 GPU 支持
> 7. 保留 `src/gpu/gpu-device.ts` 中的注释（Electron 兼容）"

---

## 提交历史

| Commit | 消息 | 日期 | 备注 |
|--------|------|------|------|
| `e6f5c8b` | 更新 | 2026-06-11 | 把本文档提交进仓库 |
| `f96bbc1` | wasm | 2025-10-28 | |
| `735a16e` | log send out | 2025-10-28 | |
| `7d06a41` | electron gpu preload | 2025-10-28 | |
| `4a4d3ab` | electron gpu | 2025-10-28 | **最大提交**：+665 行，添加 GPU 支持 |
| `04caee5` | 备份 | 2025-10-28 | |
| `0fda219` | for electron | 2025-10-27 | |
| `0ffb2bb` | for electron | 2025-10-26 | |
| `9f8ceea` | for electron cpu | 2025-10-26 | **大删提交**：移除 CLI main 函数 |
| `e9baaf6` | Merge main | 2025-10-26 | **合并上游 main 分支** |
| `2ff2be8` | 备份 | 2025-10-26 | |

---

## 提交 0: `e9baaf6` — Merge branch 'main' (2025-10-26)

**性质：** merge 提交，从 upstream main 合并代码

**改动文件：** (共 12 文件，+546/-65)

| 文件 | 变更详情 |
|------|------|
| `.github/workflows/ci.yml` | `actions/setup-node` 由 v5 升级到 v6 |
| `.github/workflows/publish.yml` | `actions/setup-node` 由 v5 升级到 v6 |
| `generators/gen-grid.mjs` | 构造函数参数从 `(width, height, scale, color, alpha)` 改为 `(width, height, spacing, s, r, g, b, a)`；支持 RGB 三通道独立色；新增 `packClr`/`packOpacity` 工具函数；默认参数 scale 改为 0.1 |
| `package-lock.json` | lock 文件更新 |
| `package.json` | 版本/描述更新 |
| `src/gpu/gpu-clustering.ts` | `playcanvas/debug` 改为 `playcanvas` |
| `src/gpu/gpu-device.ts` | `playcanvas/debug` 改为 `playcanvas`；新增 GPU info 日志：`console.log(\`Created gpu device="${info.device}"...\`)` |
| `src/index.ts` | +42 行修改：新增 `writeLod` import 和 `'lod'` case；新增 `lod` 命令行选项（短选项 `-l`）；新增 `mkdir` 导入（`-w` 时创建目录）；调整 `outputFilename` 处理 |
| `src/process.ts` | 新增 `Lod` 类型和 `'lod'` case，向 DataTable 添加 `lod` 列 |
| `src/utils/b-tree.ts` | **新增** 156 行 B-tree 实现（用于 LOD 切分） |
| `src/utils/kd-tree.ts` | 重构：把 `build` 方法从 `class` 私有方法改为构造函数内的 `build` 闭包；KdTreeNode 新增 `count` 字段；构建过程优化 |
| `src/writers/write-lod.ts` | **新增** 274 行 LOD 格式写入实现（基于 BTree 切分多个 SOG 文件） |

---

## 提交 1: `2ff2be8` — 备份 (2025-10-26)

**改动文件：** `package.json`、`pnpm-lock.yaml`（新增，3412 行）、`src/index.ts`（+57 行）

- `package.json`：添加 `packageManager` 字段锁定 pnpm 版本
- `pnpm-lock.yaml`：**新增**，从 npm 切换到 pnpm
- `src/index.ts`：**新增 `deal` 函数**（55 行），保留原 `main` 函数
  - `deal` 函数是 CLI 之外、给 Electron/程序化调用用的简化入口
  - `deal` 不接受 `processActions`，固定 `[]`
  - `deal` 内 `cpu: false`，`iterations: 10`
  - `deal` 返回 `boolean`（成功 true，失败 false）
  - 导出改为 `export { main, deal }`

---

## 提交 2: `9f8ceea` — for electron cpu (2025-10-26)

**目的：** 将 CLI 版本改为 Electron 专用版本（去掉命令行入口）

**改动文件：** `src/index.ts`（-378 行）、`src/utils/k-means.ts`（-16 行）、`src/writers/write-sog.ts`（-16 行）

### `src/index.ts`
- **删除 ~378 行**：移除 `main` 函数、CLI 参数解析 `parseArguments()`、usage 帮助信息
- 移除 `fileExists` 函数
- 移除 `writeCsv`、`writeHtml` 的 import 和 case
- 移除 `Vec3`、`exit`、`hrtime`、`parseArgs` 的 import
- 移除 `version` import
- **`deal` 函数保留**（未删除），但 `cpu` 默认值从 `false` 改为 `true`
- 导出从 `{ main, deal }` 改为 `export { deal }`

### `src/utils/k-means.ts`
- 移除 `GpuClustering` 和 `GpuDevice` 的 import
- `kmeans` 函数签名从 `kmeans(points, k, iterations, device?)` 改为 `kmeans(points, k, iterations)`
- 移除 GPU 聚类分支，始终走 CPU kd-tree 路径

### `src/writers/write-sog.ts`
- 移除 `createDevice` 和 `GpuDevice` import
- `cluster1d` 函数签名去掉 `device` 参数
- 所有 `cluster1d` 和 `kmeans` 调用去掉 `device` 参数
- 移除 `let gpuDevice: GpuDevice` 变量

---

## 提交 3: `0ffb2bb` — for electron (2025-10-26)

**改动文件：** `src/index.ts`、`src/utils/webp-codec.ts`

### `src/index.ts`
- **`deal` 函数重命名为 `convertGsplat`**
- export 改为 `export { convertGsplat }`

### `src/utils/webp-codec.ts`
- 新增 import：`import * as path from "path"`
- `locateFile` 参数名从 `path` 改为 `fileName`（避免与 import 的 path 模块冲突）
- WASM 路径构造：从 `new URL(\`../lib/${path}\`, import.meta.url).toString()` 改为先 `"file://" + path.resolve(__dirname, \`../../resources/${fileName}\`)`，再 `new URL(wasmPath, import.meta.url).toString()`
- **此提交仍保留 `new URL` 包装**，URL 构造的真正移除在 0fda219

---

## 提交 4: `0fda219` — for electron (2025-10-27)

**改动文件：** `lib/webp.mjs`、`src/index.ts`、`src/utils/webp-codec.ts`

### `lib/webp.mjs`
- 完全重写（从压缩的一行格式化为多行可读格式，**468 行**）
- 修复 `ENVIRONMENT_IS_NODE` 等逻辑
- 改用 `require("fs")`/`require("path")` 而非 `import.meta.url`（兼容 Node CommonJS 环境）

### `src/index.ts`
- `convertGsplat` 返回值从 `boolean` 改为对象：
  ```typescript
  // 成功：
  return { isOk: true };
  // 失败：
  return { isOk: false, error: err };
  ```

### `src/utils/webp-codec.ts`
- `locateFile` 返回值从 `new URL(wasmPath, import.meta.url).toString()` 改为直接返回 `wasmPath`（去掉 URL 构造）

---

## 提交 5: `04caee5` — 备份 (2025-10-28)

**改动文件：** `lib/webp.electron.mjs`（新增）、`lib/webp.mjs`、`src/utils/webp-codec.ts`

### `lib/webp.electron.mjs`（新增，468 行）
- Electron 专用的 webp wasm 模块，兼容 Node 环境
- 内容是格式化后的版本（与 0fda219 重写后的 `webp.mjs` 实质相同）

### `lib/webp.mjs`
- **此提交中实际未被修改**（仍为 0fda219 格式化后的 14 行版本）

### `src/utils/webp-codec.ts`
- import 路径从 `'../../lib/webp.mjs'` 改为 `'../../lib/webp.electron.mjs'`

---

## 提交 6: `4a4d3ab` — electron gpu (2025-10-28)

**目的：** 为 Electron 添加完整的 GPU 支持（最大提交，+665 行）

**改动文件：** `rollup.config.mjs`、`src/gpu/gpu-device.ts`、`src/index-electron.ts`（新增）、`src/index.ts`、`src/utils/k-means.ts`、`src/utils/webp-codec.ts`、`src/writers/write-sog.ts`

### `rollup.config.mjs`
- 新增 `umdapplication` 构建配置：
  ```javascript
  const umdapplication = {
      input: 'src/index-electron.ts',
      output: {
          dir: 'dist',
          format: 'umd',
          sourcemap: true,
          name: 'SPLAT',
          entryFileNames: 'gsplat-electron.js'
      },
      external: ['webgpu'],
      plugins: [typescript({ tsconfig: './tsconfig.json' }), resolve(), json()],
      cache: false
  };
  export default [application, umdapplication];
  ```

### `src/gpu/gpu-device.ts`
- 注释掉 `import { create, globals } from 'webgpu'`
- `initializeGlobals` 内部：`Object.assign(globalThis, globals)` 被注释掉
- `initializeGlobals()` 调用被条件包裹：`if((window as any).IS_ELECTRON !== true) { initializeGlobals(); }`
- `window.navigator.gpu = create([])` 被注释掉
- **GPU info 日志**仍是 `console.log(...)`（735a16e 才改为 `window.api.message.log`）

### `src/index-electron.ts`（新增，263 行）
- Electron 主进程入口文件
- 包含 `readFile`、`writeFile`、`convertGsplat` 等函数的 Electron 版本
- 通过 `window.api` 与渲染进程通信
- 包含 3 处 `console.log`（735a16e 替换为 `api.message.log`）
- **`convertGsplat` 默认 `cpu: false`**，导出 `convertGsplat`

### `src/index.ts`
- **完全恢复 main 函数版（575 行）**
- `main` 函数被恢复；**`convertGsplat` 被移除**（迁移到 `src/index-electron.ts`）
- 添加更多格式/功能支持（如 writeCsv、writeHtml、writeLod）
- 包含 5 处 `console.log`（735a16e 替换为 `api.message.log`）
- **当前导出**：`export { main }`（恢复 CLI 支持）

### `src/utils/k-means.ts`
- 恢复 GPU 支持
- `kmeans` 函数签名恢复为 `kmeans(points, k, iterations, device?)`
- 恢复 GPU 聚类路径：`if (gpuClustering) { await gpuClustering.execute(...) } else { clusterKdTreeCpu(...) }`

### `src/utils/webp-codec.ts`
- 恢复 Electron 环境的 WASM 路径处理逻辑，带 `IS_ELECTRON` 条件判断
- Electron 环境用 `../../../resources`（3 层），非 Electron 用 `../lib/`（原始 URL 构造）

### `src/writers/write-sog.ts`（372 行）
- 恢复 GPU 支持
- `writeSog` 恢复接受 `shMethod: 'cpu' | 'gpu'` 参数
- 恢复 GPU 设备创建和使用

---

## 提交 7: `7d06a41` — electron gpu preload (2025-10-28)

**改动文件：** `rollup.config.mjs`、`src/gpu/gpu-device.ts`、`src/utils/webp-codec.ts`

### `rollup.config.mjs`
- `umdapplication` 输出格式从 `umd` 改为 `esm`：
  ```javascript
  format: 'esm', // umd
  ```

### `src/gpu/gpu-device.ts`
- `if((window as any).IS_ELECTRON !== true) { initializeGlobals(); }` 被注释掉
- 改为直接注释 `//initializeGlobals();`（**完全移除条件判断**）

### `src/utils/webp-codec.ts`
- 简化 WASM 路径逻辑，统一使用 `resolve(__dirname, '../../resources/...')`
- **移除了 `IS_ELECTRON` 条件判断**

---

## 提交 8: `735a16e` — log send out (2025-10-28)

**目的：** 将所有 `console.log` 替换为通过 Electron `window.api` 传回主进程

**改动文件：** `src/gpu/gpu-device.ts`、`src/index-electron.ts`、`src/index.ts`、`src/utils/k-means.ts`、`src/writers/write-lod.ts`、`src/writers/write-sog.ts`

**总替换数：12 处 `api.message.log`**

### `src/gpu/gpu-device.ts`
- GPU 创建日志从 `console.log` 改为 `(window as any).api.message.log`（1 处）

### `src/index-electron.ts`
- 3 处：`reading '${filename}'...`、`writing '${filename}'...`、`Loaded ${numRows} gaussians`（**注意：没有"完成时间"日志，因为 `convertGsplat` 函数本身不在 main 中运行 CLI 流程**）

### `src/index.ts`
- 4 处：`reading`、`writing`、`Loaded ${numRows} gaussians`、`done in ${time}s`（还有 1 处 `splat-transform v${version}` 未替换，因为是程序启动横幅）

### `src/utils/k-means.ts`
- 2 处：`Running k-means clustering...` 和 ` done 🎉`

### `src/writers/write-lod.ts`
- 1 处：`writing ${pathname}...`

### `src/writers/write-sog.ts`
- 1 处：`writing '${pathname}'...`

---

## 提交 9: `f96bbc1` — wasm (2025-10-28)

**改动文件：** `src/utils/webp-codec.ts`

### `src/utils/webp-codec.ts`
- 新增 import：`import { resolve, join } from "path"`
- WASM 路径区分开发和生产环境：
  ```typescript
  let wasmPath = "";
  if (process.env.NODE_ENV === "development") {
      wasmPath = "file://" + resolve(__dirname, `../../resources/${path}`);
  } else {
      wasmPath = "file://" + resolve(join((process as any).resourcesPath, "app.asar.unpacked", "resources", path));
  }
  ```

---

## 关键修改点总结（升级原始代码时参考）

### 1. 项目依赖
- 从 npm 切换到 pnpm（`package.json` 添加 `packageManager` 字段）

### 2. 入口函数改造
- `src/index.ts`：
  - 在 2ff2be8 中**新增** `deal` 函数（保留 `main`）
  - 在 0ffb2bb 中 `deal` 重命名为 `convertGsplat`
  - 在 0fda219 中返回值从 `boolean` 改为 `{ isOk: boolean, error?: any }`
  - 在 4a4d3ab 中 `main` 函数被完全恢复，`convertGsplat` 被**迁移到** `src/index-electron.ts`
  - 在 735a16e 中 4 处 `console.log` 改为 `api.message.log`

### 3. Electron 主进程入口
- **新建** `src/index-electron.ts`（263 行）作为 Electron 主进程调用入口
- `cpu` 默认值为 `false`（Electron 端默认 GPU）
- 导出 `convertGsplat` 函数（4a4d3ab 从 `src/index.ts` 迁移过来）

### 4. GPU 支持
- `src/gpu/gpu-device.ts`：
  - 注释掉 `import { create, globals } from 'webgpu'`
  - `initializeGlobals()` 直接注释掉（无条件）
  - `window.navigator.gpu = create([])` 注释掉
- `src/utils/k-means.ts`：
  - `kmeans` 接受 `device` 参数，支持 GPU 聚类
  - 有 GPU（`GpuClustering`）和 CPU（`clusterKdTreeCpu`）两个分支
- `src/writers/write-sog.ts`：
  - `shMethod: 'cpu' | 'gpu'` 参数
  - GPU 设备按需创建

### 5. WebP WASM 路径
- import：`lib/webp.electron.mjs`（Electron 专用）
- 开发环境：`file:// + resolve(__dirname, '../../resources/...')`
- 生产环境：`file:// + resolve(join(process.resourcesPath, 'app.asar.unpacked', 'resources', ...))`

### 6. 日志通道
- 所有 `console.log` 替换为 `(window as any).api.message.log(...)`
- 用于 Electron 渲染进程日志传回主进程

### 7. 构建配置
- `rollup.config.mjs`：
  - 新增 `umdapplication` 配置（输入 `src/index-electron.ts`）
  - 输出 `gsplat-electron.js`（**esm 格式**，非 umd）

### 8. CPU 模式
- `deal`（`convertGsplat`）函数内默认 `cpu: true`
- `index-electron.ts` 中 `cpu: false`

### 9. 新增文件
| 文件 | 引入提交 | 说明 |
|------|----------|------|
| `src/index-electron.ts` | `4a4d3ab` | Electron 主进程入口（leapar 编写） |
| `lib/webp.electron.mjs` | `04caee5` | Electron 专用 webp wasm 模块（leapar 编写） |
| `src/utils/b-tree.ts` | `e9baaf6` | B-tree 实现（来自 upstream merge） |
| `src/writers/write-lod.ts` | `e9baaf6` | LOD 格式写入（来自 upstream merge） |
| `pnpm-lock.yaml` | `2ff2be8` | 切换到 pnpm 包管理器 |
| `docs/leapar-changes.md` | `e6f5c8b` | 本文档 |

---

## 文件变更统计

| 提交 | 新增文件 | 修改文件 |
|------|----------|----------|
| `e9baaf6` | `src/utils/b-tree.ts`, `src/writers/write-lod.ts` | `.github/workflows/ci.yml`, `.github/workflows/publish.yml`, `generators/gen-grid.mjs`, `package-lock.json`, `package.json`, `src/gpu/gpu-clustering.ts`, `src/gpu/gpu-device.ts`, `src/index.ts`, `src/process.ts`, `src/utils/kd-tree.ts` |
| `2ff2be8` | `pnpm-lock.yaml` | `package.json`, `src/index.ts` |
| `9f8ceea` | - | `src/index.ts`, `src/utils/k-means.ts`, `src/writers/write-sog.ts` |
| `0ffb2bb` | - | `src/index.ts`, `src/utils/webp-codec.ts` |
| `0fda219` | - | `lib/webp.mjs`, `src/index.ts`, `src/utils/webp-codec.ts` |
| `04caee5` | `lib/webp.electron.mjs` | `lib/webp.mjs`, `src/utils/webp-codec.ts` |
| `4a4d3ab` | `src/index-electron.ts` | `rollup.config.mjs`, `src/gpu/gpu-device.ts`, `src/index.ts`, `src/utils/k-means.ts`, `src/utils/webp-codec.ts`, `src/writers/write-sog.ts` |
| `7d06a41` | - | `rollup.config.mjs`, `src/gpu/gpu-device.ts`, `src/utils/webp-codec.ts` |
| `735a16e` | - | `src/gpu/gpu-device.ts`, `src/index-electron.ts`, `src/index.ts`, `src/utils/k-means.ts`, `src/writers/write-lod.ts`, `src/writers/write-sog.ts` |
| `f96bbc1` | - | `src/utils/webp-codec.ts` |
| `e6f5c8b` | - | `docs/leapar-changes.md` |

---

## `src/index.ts` 的修改轨迹（重要）

| 提交 | 状态 |
|------|------|
| 原始状态 | 导出 `{ main }`，有 CLI 参数解析 |
| `2ff2be8` | **新增** `deal` 函数（55 行），导出改为 `{ main, deal }` |
| `9f8ceea` | 删除 `main`、`parseArguments` 等 CLI 代码（-378 行），**保留** `deal` 函数，导出改为 `export { deal }`；`deal` 内 `cpu` 改为 `true` |
| `0ffb2bb` | `deal` 重命名为 `convertGsplat`，导出改为 `export { convertGsplat }` |
| `0fda219` | `convertGsplat` 返回值改为 `{ isOk, error? }` 对象 |
| `4a4d3ab` | **完全恢复** `main` 函数（575 行），`convertGsplat` **迁移到** `src/index-electron.ts`，导出改为 `export { main }`；日志仍是 `console.log` |
| `735a16e` | 4 处 `console.log` 改为 `window.api.message.log`（`splat-transform v${version}` 启动横幅未替换） |

---

## `src/index-electron.ts` 的修改轨迹

| 提交 | 状态 |
|------|------|
| 原始状态 | 不存在 |
| `4a4d3ab` | **新增**（263 行），从 `src/index.ts` 迁移出 `convertGsplat` 等函数，3 处 `console.log` |
| `735a16e` | 3 处 `console.log` 改为 `window.api.message.log` |

---

## `src/utils/webp-codec.ts` 的修改轨迹

| 提交 | 变更 |
|------|------|
| 原始状态 | 用 `new URL(\`../lib/${path}\`, import.meta.url).toString()` 构造 wasm 路径 |
| `0ffb2bb` | 引入 `path` 模块，`locateFile` 参数改名为 `fileName`；wasmPath 用 `file:// + path.resolve(__dirname, '../../resources/...')`，**但仍包一层** `new URL(wasmPath, import.meta.url).toString()` |
| `0fda219` | **去掉 URL 构造**，直接返回 `wasmPath` |
| `04caee5` | import 改为 `webp.electron.mjs` |
| `4a4d3ab` | 恢复 `IS_ELECTRON` 条件判断：非 Electron 用 `new URL('../lib/...')`，Electron 用 `../../../resources` |
| `7d06a41` | 移除 `IS_ELECTRON` 判断，统一用 `resolve(__dirname, '../../resources/...')` |
| `f96bbc1` | 添加 `NODE_ENV` 判断：开发用 `../../resources`，生产用 `process.resourcesPath + app.asar.unpacked/resources` |

---

## `src/gpu/gpu-device.ts` 的修改轨迹

| 提交 | 变更 |
|------|------|
| 原始状态 | 调用 `initializeGlobals()`，设置 `window.navigator.gpu = create([])`，GPU info 用 `console.log` |
| `4a4d3ab` | 注释 `import { create, globals }`，`initializeGlobals()` 改为 `if((window as any).IS_ELECTRON !== true)` 条件调用，注释 `navigator.gpu = create([])`；GPU info 仍用 `console.log` |
| `7d06a41` | 移除条件判断，直接注释 `//initializeGlobals()` |
| `735a16e` | GPU info 日志改为 `window.api.message.log` |
