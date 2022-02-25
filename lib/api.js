import { createWriteStream, createReadStream } from 'fs';
import { readFile } from 'fs/promises';
import { NFLoad } from './load.js';
import { NFExtract } from './extract.js';

export class NFEi {
    /**
     * @method exportBySchemeFile
     * @description Экспорт данных по файлу схемы в указанный файл
     * @param context {ExecContext|RequestContext} сессионные данные пользователя
     * @param schemeFile {string} абсолютный путь к файлу схемы экспорта
     * @param outfile {string} абсолютный путь к файлу, куда нужно выгрузить данные
     * @param filter {Object} фильтры применяемые к основной сущности выгрузки
     * @returns {Promise<*>}
     */
    static async exportBySchemeFile(context, schemeFile, outfile, filter) {
        const schema = JSON.parse(
            await readFile(schemeFile)
        );
        return this.exportByScheme(context, schema, outfile, filter);
    }

    /**
     * @method exportByScheme
     * @description Экспорт данных по схеме в указанный файл
     * @param context {ExecContext|RequestContext} сессионные данные пользователя
     * @param schema {Object} схема экспорта
     * @param outfile {string} абсолютный путь к файлу, куда нужно выгрузить данные
     * @param filter {Object} фильтры применяемые к основной сущности выгрузки
     * @returns {Promise<*>}
     */
    static async exportByScheme(context, schema, outfile, filter) {
        const extractor = new NFExtract(schema);
        extractor.setContext(context);
        const loader = new NFLoad(schema);
        loader.setContext(context);
        await loader.prepare();
        const output = createWriteStream(outfile);
        extractor.pipe(loader).pipe(output);
        return extractor.export(filter);
    }

    /**
     * @method importBySchemeFile
     * @description Импорт данных по файлу схемы из указанного файла с данными
     * @param context {ExecContext|RequestContext} сессионные данные пользователя
     * @param schemeFile {string} абсолютный путь к файлу схемы импорта
     * @param infile {string} абсолютный путь к файлу, откуда нужно загрузить данные
     * @param filter {Object} фильтры применяемые к основной сущности выгрузки
     * @returns {Promise<*>}
     */
    static async importBySchemeFile(context, schemeFile, infile, filter) {
        const schema = JSON.parse(
            await readFile(schemeFile)
        );
        return this.importByScheme(context, schema, infile, filter);
    }

    /**
     * @method importByScheme
     * @description Импорт данных по схеме из указанного файла с данными
     * @param context {ExecContext|RequestContext} сессионные данные пользователя
     * @param schema {Object} схема импорта
     * @param infile {string} абсолютный путь к файлу, откуда нужно загрузить данные
     * @param filter {Object} фильтры применяемые к основной сущности выгрузки
     * @returns {Promise<*>}
     */
    static async importByScheme(context, schema, infile, filter) {
        return new Promise(async (res, rej) => {
            const instream = createReadStream(infile);
            const extractor = new NFExtract(schema, instream);
            extractor.on('error', (e) => { rej(e); });
            await extractor.setContext(context);
            const loader = new NFLoad(schema);
            loader.on('finish', () => { res({ extracted: extractor.counter, processed: extractor.filtered }); });
            loader.on('error', (e) => { rej(e); });
            loader.setContext(context);
            await loader.prepare();
            extractor.pipe(loader);
            await extractor.export(filter);
        });
    }
}
