import { Readable } from 'stream';
import { dbapi } from '@nfjs/back';
import JSONParser from './json_parser.js';
import XMLParser from './xml_parser.js';

/**
 * Извлекает данные из источника данных
 */
export class NFExtract extends Readable {
    /**
     * @param {Object} schema - схема выгрузки
     * @param {ReadStream} source - читающий поток из источника данных
     */
    constructor(schema, source) {
        super({ objectMode: true });
        /**
         * Читающий поток данных из источника
         */
        this.source = source;
        /**
         * Схема извлечения
         * @type {Object}
         */
        this.schema = schema.schema;
        /**
         * Счетчик обработанных записей
         * @type {number}
         */
        this.counter = 0;
        /**
         * Счетчик прошедших через фильтр записей
         * @type {number}
         */
        this.filtered = 0;
    }

    /**
     * Установка контекста выполнения
     * @param {ExecContext|RequestContext} context
     */
    setContext(context) {
        /**
         * Контекст выполнения
         */
        this.context = context;
    }

    /**
     * Установка соединения с базой данных
     * @return {Promise<Connect>}
     */
    async getConnectDb() {
        if (!this.connectDb) {
            /**
             * Соединение с базой данных через провайдер
              * @type {Connect}
             */
            this.connectDb = await dbapi.getConnect(this.context);
        }
        return this.connectDb;
    }

    /**
     * Основной метод старта выгрузки
     * @param {Object} filter - параметры фильтрации обрабатываемых данных
     * @return {Promise<void>}
     */
    async export(filter = {}) {
        await this.export_rows(this.schema.main, this.schema.extract, filter, false);
    }

    _read() {

    }

    /**
     * Рекурсивная выгрузка данных элемента схемы выгрузки. Для выгрузки зависимых элементов вызывается на каждый выгруженную запись основного элемента.
     * @param {Object} element - описание выгружаемого элемента из схемы
     * @param {Object} extracts - описатели способа выгрузки элементов схемы
     * @param {Object} filter - параметры фильтрации обрабатываемых данных
     * @param {boolean} [detailmode] - признак, что обрабатываются данные зависимых элементов схемы для конкретного узла
     * @return {Promise<null|*[]>}
     */
    async export_rows(element, extracts, filter, detailmode) {
        const info = extracts[element];
        let res;
        const filter_data = {};
        // заменяем алиасы фильтров на реальные колонки если нужно
        if (filter) {
            Object.keys(filter).forEach((f) => {
                if (info.filter && info.filter[f]) {
                    filter_data[info.filter[f]] = filter[f];
                } else {
                    filter_data[f] = filter[f];
                }
            });
        }
        switch (info.type) {
            case 'db': {
                await this.getConnectDb();
                try {
                    let sql = `select * from ${info.table}`;
                    const where = [];
                    // готовим фильтры для запроса
                    // пример: {поле:значение} или {поле:[значение1,значение2]}
                    if (filter) {
                        Object.keys(filter_data).forEach((fr) => {
                            const predicate = `${fr} = ${(typeof filter_data[fr] === 'string' ? `:${fr}` : `any(:${fr})`)}`;
                            where.push(predicate);
                        });
                    }
                    if (info.hierarchykey) {
                        where.push(`${info.hierarchykey} is null`);
                        sql = `
                            with recursive r as (
                                select
                                    t.*,
                                    1 as level
                                from ${info.table} t
                                where ${where.join(' and ')}
                                union all
                                select
                                    t.*,
                                    r.level + 1
                                from ${info.table} t
                                join r on r.${info.pk} = t.${info.hierarchykey}
                            )
                            select * from r
                            order by level
                        `
                    } else {
                        if (where.length > 0) {
                            sql += ` where ${where.join(' and ')}`;
                        }
                        if (info.sort) {
                            if (typeof info.sort === 'string') {
                                sql += ` order by ${info.sort}`;
                            } else {
                                sql += ` order by ${info.sort.join(',')}`;
                            }
                        }
                    }
                    res = await this.connectDb.query(sql, filter_data);
                    const out_data = [];
                    // eslint-disable-next-line no-restricted-syntax
                    for (const i of res.data) {
                        let obj = {};
                        info.fields.forEach((f) => {
                            if (typeof f === 'string') {
                                obj[f] = i[f];
                            } else {
                                const field = Object.keys(f)[0];
                                obj[field] = i[field];
                            }
                        });

                        // проверяем на детейлы
                        if (info.details) {
                            let details = [];
                            // eslint-disable-next-line no-restricted-syntax
                            for (const d of info.details) {
                                const detail = extracts[d];
                                // eslint-disable-next-line no-await-in-loop
                                const rows = await this.export_rows(d, extracts,
                                    { [detail.parentkey]: obj[info.pk] }, true);
                                details = details.concat(rows);
                            }
                            if (details.length > 0) obj['#details'] = details;
                        }
                        if (info.references) {
                            let references = [];
                            for (const item of info.references) {
                                const reference = extracts[item.table];
                                if (obj[item.field] === null) { continue; }

                                const rows = await this.export_rows(item.table, extracts, { [reference.pk]: obj[item.field] }, true);
                                if (! rows) { continue; }
                                
                                references = references.concat(rows);
                            }
                            if (references.length > 0) obj['#references'] = references;
                        }
                        obj = info.output === 'named' ? { [element]: obj } : obj;
                        if (detailmode) out_data.push(obj);
                        else this.push(obj);
                    }

                    if (!detailmode) this.push(null);
                    return out_data;
                } catch(e) {
                    console.log(`@nf/ei | Ошибка при извлечении данных: ${e.message}`);
                } finally {
                    if (this.connectDb && !detailmode) await this.connectDb.release();
                }
                break;
            }
            case 'json': {
                const parser = new JSONParser({ valueScoped: info?.valueScoped });
                Object.keys(extracts).forEach((i) => {
                    const path = extracts[i].path ?? `!.*.${i}`;
                    parser.onPath(path, this.processObject);
                });
                parser.on('data', (d) => {
                    if (d == null) this.push(null);
                    this.counter++;
                    if ( // пропускаем объект только если поля в нём удовлетворяют фильтрам
                        // расшифровка: если нет таких фильтров для которых значение не подошло
                        !Object.keys(filter_data).some((i) => {
                            // фильтр применяется только если соответствующее поле есть в объекте
                            if (d[i]) {
                                if (Array.isArray(filter_data[i])) {
                                    return !filter_data[i].includes(d[i]);
                                }
                                return filter_data[i] != d[i];
                            }
                        })
                    ) {
                        this.filtered++;
                        this.push(d);
                    }
                });
                parser.on('end', () => this.push(null));
                this.source.pipe(parser);
                return null;
            }
            case 'xml': {
                const parser = new XMLParser();
                Object.keys(extracts).forEach((i) => {
                    const path = extracts[i].path || `!.*.${i}`;
                    parser.onPath(path, this.processObject);
                });
                parser.on('data', (d) => {
                    if (d == null) this.push(null);
                    this.counter++;
                    if ( // пропускаем объект только если поля в нём удовлетворяют фильтрам
                        // расшифровка: если нет таких фильтров для которых значение не подошло
                        !Object.keys(filter_data).some((i) => {
                        // фильтр применяется только если соответствующее поле есть в объекте
                            if (d[i]) {
                                if (Array.isArray(filter_data[i])) {
                                    return !filter_data[i].includes(d[i]);
                                }
                                return filter_data[i] != d[i];
                            }
                        })
                    ) {
                        this.filtered++;
                        this.push(d);
                    }
                });
                parser.on('end', () => this.push(null));
                this.source.pipe(parser);
                return null;
            }
            default: {
                throw new Error(`Нереализованный тип extract: ${info.type}`);
            }
        }
    }

    processObject(x) {
    }
}
