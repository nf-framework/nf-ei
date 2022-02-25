import { Transform } from 'stream';
import pgCopyStreams from 'pg-copy-streams';
import { dbapi } from '@nfjs/back';

/**
 * Загружает данные в источник данных
 */
export class NFLoad extends Transform {
    /**
     * @param {Object} schema - Схема загрузки
     * @param {Object} streamOptions - Дополнительные опции для преобразующего потока
     */
    constructor(schema, streamOptions) {
        const opt = { writableObjectMode: true, ...streamOptions };
        super(opt);
        /**
         * Схема загрузки
         */
        this.schema = schema.schema;
        switch (this.schema.load.type) {
            case 'jsonString':
            case 'execSqlArray':
                this.push('[\n');
                break;
            default:
                break;
        }
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
        if (!this.dbconnect) {
            /**
             * Соединение с базой данных через провайдер
             * @type {Connect}
             */
            this.dbconnect = await dbapi.getConnect(this.context);
        }
        return this.dbconnect;
    }

    /**
     * Подготовка перед запуском
     */
    async prepare() {
        switch (this.schema.load.type) {
            case 'dbcopyload':
                await this.getConnectDb();
                const [sch, tbl] = this.schema.load.loadtable.split('.');
                await this.dbconnect.begin();
                await this.dbconnect._connect.query('select nfc.f_db8bulk_ins_before($1,$2)', [sch, tbl]);
                this.dbcopystream = await this.dbconnect._connect.query(pgCopyStreams.from(`COPY ${this.schema.load.loadtable} FROM STDIN`));
                this.dbcopystream.on('error', (e) => {
                    this.destroy(e);
                });
                break;
            case 'db':
                await this.getConnectDb();
                await this.dbconnect.begin();
                break;
            default:
                break;
        }
    }

    /**
     * Обработка записи данных при типе db и execSqlArray
     * @param {'db'|'execSqlArray'} type - тип загрузки из схемы
     * @param {Object} data - обрабатываемая запись
     * @param {*} pkey - значение для ключа, ссылающегося на родительскую (уже обработанную) запись
     * @param {[*]} container - временный буфер для накопления (применяется при type = 'execSqlArray'
     * @return {Promise<void>}
     */
    async db(type, data, pkey, container) {
        const unit = (this.schema.load.unitField[0] === '$') ? Object.keys(data)[+this.schema.load.unitField.substring(1)] : data[this.schema.load.unitField];
        const info = this.schema.load.units[unit];
        if (info && info.type === 'db') {
            const key = (this.schema.load.unitData.substring(0, 2) === ':$') ?
                Object.keys(data)[+this.schema.load.unitField.substring(2)] :
                this.schema.load.unitData;
            const sdata = data[key];
            const params = {};
            const dfields = info.fields.slice();
            if (pkey !== undefined) {
                sdata[info.parentkey] = pkey;
                dfields.push(info.parentkey);
            }
            dfields.forEach((f) => {
                params[f] = sdata[f];
            });
            if (pkey !== undefined) params[info.parentkey] = pkey;
            // TODO реализовать возврат суррогатного ключа для дальнейшей вставки детейлов корректной
            const sql = `insert into ${info.tablename} 
                (${dfields.map((f) => `${f}`).join(', ')}) 
                values (${dfields.map((f) => `:${f}`).join(', ')})
                on conflict (${info.uk.map((f) => `${f}`).join(', ')}) 
                do update set ${dfields.map((f) => `${f} = :${f}`).join(', ')};`;
            if (type === 'db') await this.dbconnect.query(sql, params);
            if (type === 'execSqlArray') container.push({ sql, params });
            if (sdata['#details']) {
                for (const ddata of sdata['#details']) {
                    // eslint-disable-next-line no-await-in-loop
                    await this.db(type, ddata, sdata[info.pk], container);
                }
            }
        }
    }

    /**
     * Обработка одной цельной записи данных
     * @param chunk
     * @param encoding
     * @param callback
     * @return {Promise<void>}
     * @private
     */
    async _transform(chunk, encoding, callback) {
        try {
            let data = chunk;
            if (data instanceof Object) {
                const {type: loadType} = this.schema.load;
                switch (loadType) {
                    case 'jsonString': // простая выгрузка в строку json
                        data = (this.previosIsObject ? ',\n' : '') + JSON.stringify(data, null, '\t');
                        this.previosIsObject = true;
                        break;
                    case 'db': // загрузка в БД
                        await this.db(loadType, data);
                        data = '';
                        break;
                    case 'execSqlArray':
                        const arr = [];
                        await this.db(loadType, data, undefined, arr);
                        data = {arr};
                        break;
                    case 'dbcopyload':
                        const str = `${this.schema.load.fields.map((f) => data[f] || '\\N').join('\t')}\n`;
                        this.dbcopystream.write(Buffer.from(str));
                        data = '';
                        break;
                    case 'console':
                        data = JSON.stringify(data, null, '\t');
                        console.log(data);
                        break;
                    default:
                        break;
                }
            } else {
                this.previosIsObject = false;
            }
            callback(null, data);
        } catch(e) {
            if (this.dbconnect && this.dbconnect.release) await this.dbconnect.release();
            callback(e);
        }
    }

    /**
     * Действия при завершении загрузки
     * @param {Function} callback
     * @return {Promise<void>}
     * @private
     */
    async _final(callback) {
        switch (this.schema.load.type) {
            case 'jsonString':
            case 'execSqlArray':
                this.push('\n]');
                break;
            case 'dbcopyload':
                await this.dbcopystream.end(null);
                await this.dbconnect._connect.query('select nfc.f_db8bulk_ins_after()');
                await this.dbconnect._connect.query('COMMIT');
                await this.dbconnect.release();
                break;
            case 'db':
                await this.dbconnect.commit();
                await this.dbconnect.release();
                break;
            default:
                break;
        }
        callback();
    }
}
