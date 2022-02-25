import clarinet from 'clarinet';
import { Transform } from 'stream';

class JSONParser extends Transform {
    constructor(options = {}) {
        super({ readableObjectMode: true });
        this.valueScoped = options?.valueScoped ?? false;
        this.paths = [];
        this.stack = [{}];
        this.current_path = ['!'];
        this.parser = clarinet.createStream();

        this.parser.onvalue = (v) => {
            // got some value.  v is the value. can be string, double, bool, or null.
            this.addValue(v);
        };
        this.parser.onopenobject = (key) => {
            // opened an object. key is the first key.
            this.current_path.push(key || '#');
            this.stack.push({});
        };
        this.parser.onkey = (key) => {
            // got a subsequent key in an object.
            this.current_path[this.current_path.length - 1] = key;
        };
        this.parser.oncloseobject = () => {
            // closed an object.
            this.current_path.pop();
            this.addValue(this.stack.pop());
        };
        this.parser.onopenarray = () => {
            // opened an array.
            this.current_path.push('*');
            this.stack.push([]);
        };
        this.parser.onclosearray = () => {
            // closed an array.
            const item = this.current_path.pop();
            if (item !== '*') {
                throw Error('Invalid JSON. array end');
            }
            this.addValue(this.stack.pop());
        };
    }

    addValue(v) {
        const cpath = this.current_path[this.current_path.length - 1];
        const citem = this.stack[this.stack.length - 1];
        if (cpath === '*') {
            citem.push(v);
        } else {
            citem[cpath] = v;
        }

        // проверяем нужно ли эмитить объект
        const path = this.current_path.join('.');
        this.paths.filter((p) => path.match(p.re))
            .forEach((i) => {
                this.push(this.valueScoped ? citem : v);
            });
        // TODO: проверить понадобиться ли объект в будущем, если нет то выкидываем
    }

    onPath(path, callback) {
        this.paths.push({ path, callback, re: new RegExp(`${path.replace('*', '\\*')}$`) });
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

export default JSONParser;
