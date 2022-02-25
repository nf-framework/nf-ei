import assert from 'assert';
import { createReadStream } from 'fs';
import path from 'path';
import { config } from '@nfjs/core';
import { init } from '@nfjs/db-postgres/nf-module.js';
import NFExtract from '../extract.js';

const __dirname = path.join(path.dirname(decodeURI(new URL(import.meta.url).pathname))).replace(/^\\([A-Z]:\\)/, '$1');
const cfg = config?.['@nfjs/ei'] ?? config?.test;
describe('@nfjs/ei/lib/extract', () => {
    describe('NFExtract', () => {
        it('db', async () => {
            // Arrange
            await init();
            const schema = {
                main: 'nfc.datatypes',
                extract: {
                    'nfc.datatypes': {
                        type: 'db',
                        table: 'nfc.datatypes',
                        pk: 'id',
                        output: 'named',
                        fields: ['id', 'code', 'caption'],
                        filter: { id: 'id' }
                    }
                }
            };
            const res = [];
            // Act
            const extractor = new NFExtract({ schema });
            extractor.setContext(cfg);
            extractor.on('data', (data) => {
                res.push(data);
            });
            await extractor.export({ id: [3] });
            // Assert
            extractor.on('end', () => {
                assert.strictEqual(res.length, 1);
                assert.strictEqual(res[0]['nfc.datatypes'].code, 'bool');
            });
        });
        it('json !.var1.*', async () => {
            // Arrange
            const schema = {
                main: 'x',
                extract: {
                    x: {
                        type: 'json',
                        path: '!.var1.*',
                        filter: {
                            f14: 'f1'
                        }
                    }
                }
            };
            const res = [];
            // Act
            const istream = createReadStream(path.join(__dirname, 'extract.test.json'));
            const extractor = new NFExtract({ schema }, istream);
            extractor.on('data', (data) => {
                res.push(data);
            });
            await extractor.export({ f14: 'v2' });
            // Assert
            extractor.on('end', () => {
                assert.strictEqual(res.length, 1);
                assert.strictEqual(res[0].f4[1].s, 'str');
            });
        });
        it('json !.var2.*.el', async () => {
            // Arrange
            const schema = {
                main: 'x',
                extract: {
                    x: {
                        type: 'json',
                        path: '!.var2.*.el',
                        filter: {
                            f14: 'f1'
                        }
                    }
                }
            };
            const res = [];
            // Act
            const istream = createReadStream(path.join(__dirname, 'extract.test.json'));
            const extractor = new NFExtract({ schema }, istream);
            extractor.on('data', (data) => {
                res.push(data);
            });
            await extractor.export({ f14: 'v2' });
            // Assert
            extractor.on('end', () => {
                assert.strictEqual(res.length, 1);
                assert.strictEqual(res[0].f4[1].s, 'str');
            });
        });
        it('json !.var2.*.el scoped', async () => {
            // Arrange
            const schema = {
                main: 'x',
                extract: {
                    x: {
                        type: 'json',
                        path: '!.var2.*.el',
                        valueScoped: true
                    }
                }
            };
            const res = [];
            // Act
            const istream = createReadStream(path.join(__dirname, 'extract.test.json'));
            const extractor = new NFExtract({ schema }, istream);
            extractor.on('data', (data) => {
                res.push(data);
            });
            await extractor.export();
            // Assert
            extractor.on('end', () => {
                assert.strictEqual(res.length, 2);
                assert.strictEqual(res[1].el.f4[1].s, 'str');
            });
        });
        it('xml', async () => {
            // Arrange
            const schema = {
                main: 'x',
                extract: {
                    x: {
                        type: 'xml',
                        path: '!.rt.el',
                        filter: {
                            f1: 'f1'
                        }
                    }
                }
            };
            const res = [];
            // Act
            const istream = createReadStream(path.join(__dirname, 'extract.test.xml'));
            const extractor = new NFExtract({ schema }, istream);
            extractor.on('data', (data) => { res.push(data); });
            await extractor.export({ f1: 'v2' });
            // Assert
            extractor.on('end', () => {
                assert.strictEqual(res.length, 1);
                assert.strictEqual(res[0].f2, 'vv2');
            });
        });
    });
});
