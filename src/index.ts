import { randomBytes } from 'crypto';
import { lstat, mkdir, open, rename } from 'node:fs/promises';
import { basename, dirname, join, resolve } from 'node:path';
import { Column, DataTable, TypedArray } from './data-table';
import { ProcessAction, processDataTable } from './process';
import { isCompressedPly, decompressPly } from './readers/decompress-ply';
import { readKsplat } from './readers/read-ksplat';
import { readMjs, Param } from './readers/read-mjs';
import { readPly } from './readers/read-ply';
import { readSog } from './readers/read-sog';
import { readSplat } from './readers/read-splat';
import { readSpz } from './readers/read-spz';
import { writeCompressedPly } from './writers/write-compressed-ply';
import { writeLod } from './writers/write-lod';
import { writePly } from './writers/write-ply';
import { writeSog } from './writers/write-sog';

type Options = {
    overwrite: boolean,
    help: boolean,
    version: boolean,
    cpu: boolean,
    iterations: number,
    viewerSettingsPath?: string
};



const readFile = async (filename: string, params: Param[]) => {
    const lowerFilename = filename.toLowerCase();
    let fileData;

    console.log(`reading '${filename}'...`);

    if (lowerFilename.endsWith('.mjs')) {
        fileData = await readMjs(filename, params);
    } else {
        const inputFile = await open(filename, 'r');

        if (lowerFilename.endsWith('.ksplat')) {
            fileData = await readKsplat(inputFile);
        } else if (lowerFilename.endsWith('.splat')) {
            fileData = await readSplat(inputFile);
        } else if (lowerFilename.endsWith('.sog') || lowerFilename.endsWith('meta.json')) {
            fileData = await readSog(inputFile, filename);
        } else if (lowerFilename.endsWith('.ply')) {
            const ply = await readPly(inputFile);
            if (isCompressedPly(ply)) {
                fileData = {
                    comments: ply.comments,
                    elements: [{ name: 'vertex', dataTable: decompressPly(ply) }]
                };
            } else {
                fileData = ply;
            }
        } else if (lowerFilename.endsWith('.spz')) {
            fileData = await readSpz(inputFile);
        } else {
            await inputFile.close();
            throw new Error(`Unsupported input file type: ${filename}`);
        }

        await inputFile.close();
    }
    return fileData;
};

const getOutputFormat = (filename: string) => {
    const lowerFilename = filename.toLowerCase();

    if (lowerFilename.endsWith('.csv')) {
        return 'csv';
    } else if (lowerFilename.endsWith('lod-meta.json')) {
        return 'lod';
    } else if (lowerFilename.endsWith('.sog') || lowerFilename.endsWith('meta.json')) {
        return 'sog';
    } else if (lowerFilename.endsWith('.compressed.ply')) {
        return 'compressed-ply';
    } else if (lowerFilename.endsWith('.ply')) {
        return 'ply';
    } else if (lowerFilename.endsWith('.html')) {
        return 'html';
    }

    throw new Error(`Unsupported output file type: ${filename}`);
};

const writeFile = async (filename: string, dataTable: DataTable, options: Options) => {
    // get the output format, throws on failure
    const outputFormat = getOutputFormat(filename);

    console.log(`writing '${filename}'...`);

    // write to a temporary file and rename on success
    const tmpFilename = `.${basename(filename)}.${process.pid}.${Date.now()}.${randomBytes(6).toString('hex')}.tmp`;
    const tmpPathname = join(dirname(filename), tmpFilename);

    // open the tmp output file
    const outputFile = await open(tmpPathname, 'wx');

    try {
        // write the file data
        switch (outputFormat) {
            case 'sog':
                await writeSog(outputFile, dataTable, filename, options.iterations, options.cpu ? 'cpu' : 'gpu');
                break;
            case 'lod':
                await writeLod(outputFile, dataTable, filename, options.iterations, options.cpu ? 'cpu' : 'gpu');
                break;
            case 'compressed-ply':
                await writeCompressedPly(outputFile, dataTable);
                break;
            case 'ply':
                await writePly(outputFile, {
                    comments: [],
                    elements: [{
                        name: 'vertex',
                        dataTable: dataTable
                    }]
                });
                break;
        }

        // flush to disk
        await outputFile.sync();
    } finally {
        await outputFile.close().catch(() => { /* ignore */ });
    }

    // atomically rename to target filename
    await rename(tmpPathname, filename);
};

// combine multiple tables into one
// columns with matching name and type are combined
const combine = (dataTables: DataTable[]) => {
    if (dataTables.length === 1) {
        // nothing to combine
        return dataTables[0];
    }

    const findMatchingColumn = (columns: Column[], column: Column) => {
        for (let i = 0; i < columns.length; ++i) {
            if (columns[i].name === column.name &&
                columns[i].dataType === column.dataType) {
                return columns[i];
            }
        }
        return null;
    };

    // make unique list of columns where name and type much match
    const columns = dataTables[0].columns.slice();
    for (let i = 1; i < dataTables.length; ++i) {
        const dataTable = dataTables[i];
        for (let j = 0; j < dataTable.columns.length; ++j) {
            if (!findMatchingColumn(columns, dataTable.columns[j])) {
                columns.push(dataTable.columns[j]);
            }
        }
    }

    // count total number of rows
    const totalRows = dataTables.reduce((sum, dataTable) => sum + dataTable.numRows, 0);

    // construct output dataTable
    const resultColumns = columns.map((column) => {
        const constructor = column.data.constructor as new (length: number) => TypedArray;
        return new Column(column.name, new constructor(totalRows));
    });
    const result = new DataTable(resultColumns);

    // copy data
    let rowOffset = 0;
    for (let i = 0; i < dataTables.length; ++i) {
        const dataTable = dataTables[i];

        for (let j = 0; j < dataTable.columns.length; ++j) {
            const column = dataTable.columns[j];
            const targetColumn = findMatchingColumn(result.columns, column);
            targetColumn.data.set(column.data, rowOffset);
        }

        rowOffset += dataTable.numRows;
    }

    return result;
};

const isGSDataTable = (dataTable: DataTable) => {
    if (![
        'x', 'y', 'z',
        'rot_0', 'rot_1', 'rot_2', 'rot_3',
        'scale_0', 'scale_1', 'scale_2',
        'f_dc_0', 'f_dc_1', 'f_dc_2',
        'opacity'
    ].every(c => dataTable.hasColumn(c))) {
        return false;
    }
    return true;
};

const convertGsplat = async(filename: string, outputFilename: string)=>{
    try {
        // read, filter, process input files
        const inputFile = await (async () => {
            // read input
            const file = await readFile(resolve(filename), []);

            // filter out non-gs data
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

        // combine inputs into a single output dataTable
        const dataTable = processDataTable(
            combine([inputFile].map(file => file.elements[0].dataTable)),
            []
        );

        if (dataTable.numRows === 0) {
            throw new Error('No splats to write');
        }

        console.log(`Loaded ${dataTable.numRows} gaussians`);

        const options: Options = {
            overwrite: true,
            help: false,
            version: false,
            cpu: true,
            iterations: 10
        };

        // write file
        await writeFile(resolve(outputFilename), dataTable, options);

        return {
            isOk:true
        };
    } catch (err) {
        // handle errors
        console.error(err);
        return {
            isOk: false,
            error: err
        };
    }
}

export { convertGsplat };
