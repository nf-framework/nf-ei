import assert from 'assert';
import { Readable } from 'stream';
import path from 'path';
import { config } from '@nfjs/core';
import { init } from '@nfjs/db-postgres/nf-module.js';
import NFload from '../load.js';

const cfg = config?.['@nfjs/ei'] ?? config?.test;
describe('@nfjs/ei/lib/load', () => {
    describe('NFload', () => {
        it('jsonString', async () => {
            // Arrange
            const schema = {
                load: {
                    type: 'jsonString'
                }
            };
            const res = [];
            // Act
            const loader = new NFload({ schema });
            const ws = new Readable({ objectMode: true });
            ws._read = () => {};
            ws.push({ g: 0 });
            ws.push({ qq: 'qq' });
            ws.push(null);
            ws.pipe(loader);
            loader.on('data', (data) => {
                res.push(data.toString());
            });
            // Assert
            loader.on('end', () => {
                assert.strictEqual(res.length, 4);
                assert.deepStrictEqual(JSON.parse(res[1]), { g: 0 });
            });
        });
        it('execSqlArray', async () => {
            // Arrange
            const schema = {
                main: 'nfc.datatypes',
                load: {
                    type: 'execSqlArray',
                    unitField: '$0',
                    unitData: ':$0',
                    units: {
                        'nfc.datatypes': {
                            type: 'db',
                            tablename: 'nfc.datatypes',
                            pk: 'id',
                            uk: ['id'],
                            fields: [
                                'id',
                                'code',
                                'caption'
                            ]
                        }
                    }
                }
            };
            const res = [];
            // Act
            const loader = new NFload({ schema }, { objectMode: true });
            const ws = new Readable({ objectMode: true });
            ws._read = () => {};
            ws.push({ 'nfc.datatypes': { id: '3', code: 'bool', caption: 'Логический' } });
            ws.push({ 'nfc.datatypes': { id: '3', code: 'bool', caption: 'Логический' } });
            ws.push(null);
            ws.pipe(loader);
            loader.on('data', (data) => {
                res.push(data);
            });
            // Assert
            loader.on('end', () => {
                assert.strictEqual(res.length, 4);
                assert.strictEqual(
                    (res[1].arr[0].sql).replace(/[ |\n]*/gm,''),
                    "insert into nfc.datatypes (id, code, caption) values (:id, :code, :caption) on conflict (id) do update set id = :id, code = :code, caption = :caption;".replace(/[ |\n]*/gm,'')
                );
            });
        });
        it('db', async () => {
            // Arrange
            await init();
            const schema = {
                main: 'dbloadtest',
                load: {
                    type: 'db',
                    unitField: '$0',
                    unitData: ':$0',
                    units: {
                        'dbloadtest': {
                            type: 'db',
                            tablename: 'dbloadtest',
                            pk: 'id',
                            uk: ['id'],
                            fields: [
                                'id',
                                'code'
                            ]
                        }
                    }
                }
            };
            const res = [];
            // Act
            const loader = new NFload({ schema }, { objectMode: true });
            loader.setContext(cfg);
            await loader.getConnectDb();
            await loader.dbconnect.query('create temporary table dbloadtest(id int2, code text, primary key (id));');
            await loader.prepare();
            const ws = new Readable({ objectMode: true });
            ws._read = () => {};
            ws.push({ 'dbloadtest': { id: '3', code: 'bool' } });
            ws.push({ 'dbloadtest': { id: '2', code: 'text' } });
            ws.push(null);
            ws.pipe(loader);
            loader.on('data', async () => {
                const r = await loader.dbconnect.query('select * from dbloadtest;');
                res.push(...r.data);
            });
            await new Promise((resolve, reject) => {
                loader.on('error', (e) => {
                    reject(e);
                });
                // Assert
                loader.on('end',() => {
                    assert.strictEqual(res.length, 3);
                    assert.strictEqual(res[0].id,3);
                    resolve();
                });
            })
        });
        it('dbcopyload', async () => {
            // Arrange
            await init();
            const schema = {
                main: 'dbloadtest',
                load: {
                    type: 'dbcopyload',
                    loadtable: 'pg_temp.dbloadtest',
                    fields: ['id','code']
                }
            };
            const res = [];
            // Act
            const loader = new NFload({ schema }, { objectMode: true });
            loader.setContext(cfg);
            await loader.getConnectDb();
            await loader.dbconnect.query('create temporary table dbloadtest(id int2, code text, primary key (id));');
            await loader.prepare();
            const ws = new Readable({ objectMode: true });
            ws._read = () => {};
            ws.push({ id: '3', code: 'bool' });
            ws.push({ id: '2', code: 'text' });
            ws.push(null);
            ws.pipe(loader);
            loader.on('data', async () => {
                const r = await loader.dbconnect.query('select * from dbloadtest;');
                res.push(...r.data);
            });
            await new Promise((resolve, reject) => {
                loader.on('error', (e) => {
                    reject(e);
                });
                // Assert
                loader.on('end',async () => {
                    assert.strictEqual(res.length, 4); //on data сработало, когда уже обе строки были в таблице
                    assert.strictEqual(res[0].id,3);
                    resolve();
                });
            })
        });
    });
});
