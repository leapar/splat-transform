import json from '@rollup/plugin-json';
import resolve from '@rollup/plugin-node-resolve';
import typescript from '@rollup/plugin-typescript';

const application = {
    input: 'src/index.ts',
    output: {
        dir: 'dist',
        format: 'esm',
        sourcemap: true,
        entryFileNames: '[name].mjs'
    },
    external: ['webgpu'],
    plugins: [
        typescript({ tsconfig: './tsconfig.json' }),
        resolve(),
        json()
    ],
    cache: false
};

const umdapplication = {
    input: 'src/index-electron.ts',
    output: {
        dir: 'dist',
        format: 'esm',//umd
        sourcemap: true,
        name: 'SPLAT',
        entryFileNames: 'gsplat-electron.js'
    },
    external: ['webgpu'],
    plugins: [
        typescript({ tsconfig: './tsconfig.json' }),
        resolve(),
        json()
    ],
    cache: false
};

export default [
    application,
    umdapplication
];
