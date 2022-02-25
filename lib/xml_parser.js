import sax from 'sax';
import { Transform } from 'stream';

class XMLParser extends Transform {
    constructor() {
        super({ readableObjectMode: true });
        this.paths = [];
        this.stack = [{ '!': {} }];
        this.current_path = ['!'];
        this.parser = sax.createStream(true, { trim: true });

        this.parser.ontext = (v) => {
            // got some value.  v is the value. can be string, double, bool, or null.
            this.addValue(v);
        };
        this.parser.onopentag = (node) => {
            // opened an object. key is the first key.
            this.current_path.push(node.name);
            this.stack.push({
                [node.name]: { ...node.attributes }
            });
        };
        this.parser.onclosetag = (name) => {
            // closed an object.
            this.addValue(this.stack.pop());
            this.current_path.pop();
        };
    }

    addValue(v) {
        const cpath = this.current_path[this.current_path.length - 1];
        let flag = false;
        // проверяем нужно ли эмитить объект
        const path = this.current_path.join('.');
        this.paths.filter((p) => path.match(p.re))
            .forEach((i) => {
                flag = true;
                this.push(v[cpath]);
            });

        // TODO: проверить понадобиться ли объект в будущем, если нет то выкидываем
        // пока выкидываем если объект передан дальше на обработку
        if (!flag) {
            // объект не был передан в поток, сохраняем
            const ppath = this.current_path[this.current_path.length - 2];
            const citem = this.stack[this.stack.length - 1];
            if (citem[ppath][cpath]) {
                // объект уже есть у родителя, значит это массив
                if (Array.isArray(citem[ppath][cpath])) {
                    citem[ppath][cpath].push(v[cpath]);
                } else {
                    // еще не массив, нужно сконвертировать
                    citem[ppath][cpath] = [citem[ppath][cpath], v[cpath]];
                }
            } else {
                citem[ppath][cpath] = v[cpath];
            }
        }
    }

    onPath(path, callback) {
        this.paths.push({ path, callback, re: new RegExp(`${path.replace(/\*/, '.\\w+').replace(/\./g, '\.')}$`) });
    }

    _transform(chunk, encoding, callback) {
        this.parser.write(chunk);
        callback();
    }

    _final(callback) {
        this.parser.end();
        this.push(null);
        callback();
    }
}

export default XMLParser;
