# @nfjs/ei

**ОГЛАВЛЕНИЕ**
- [Назначенме](#Назначение)
- [Принцип работы](#Принцип-работы)
  - [extract](#extract)
  - [load](#load)  
  - [Основные методы](#Основные-методы)

## Назначение
Инструмент служит для настраиваемого извлечения данных из одного источника данных и загрузки в другой.
Источником на данный момент могут быть: `db`(база данных postgresql), файлы формата `xml` и `json`.
Приемником же база данных postgresql (в двух вариантах: через отдельные операции `insert on conflict do update` и потоковую), `json` файл, консоль приложения.

## Принцип работы
### extract
Класс **extract** представляет собой реализацию читающего потока nodejs, работающего в режиме передачи частей данных в виде объектов.
Для типов загрузки `xml` и `json` на вход требуется подать обычный читающий поток из, например, файла. В случае с источником `db` настраивается по факту запрос в бузу данных.
На выходе работы класса получается поток из отфильтрованных объектов. 

* `xml` - выбирает все элементы по пути в xml, свойствами выходного объекта становятся на данный момент аттрибуты элемента. Примечание: не работает обработка, если есть хоть один тег с текстом в файле.
    * `path` - путь до необходимого повторяющегося элемента в виде строки с разделителем `.`, начинающейся с `!`(корень). Например: `!.elem1.elem2`
    * `filter` - маппинг свойств объекта со значениями фильтров при вызове метода извлечения(передается через параметр) с именами свойств перебираемых элементов.
 Например, "filter1": "elemprop1" означает, что будет сравниваться значение свойства "elemprop1" перебираемых элементов со значением свойства "filter1" объекта переданного при выполнении.
 Сама фильтрация на данный момент — простое сравнение на нестрогое равенство или вхождение в список, если переданное в фильтре значение есть массив. Элемент считается прошедшим фильтрацию если выполнились все настроенные фильтры. 
* `json`
    * `path` - путь от корня объекта примерного вида `!.prop1.*.prop2`. Где `!` корень, `*` означает перебор массива.
    * `filter` - как у `xml`
    * `valueScoped` - признак, что на выход пойдет не найденные по пути элемент, а содержащий его объект целиком. Используется только для совместимости.
* `db` - возвращает данные таблицы и связанных с ней запросом в базу данных через провайдер данных `default`
    * `table` - имя таблицы вместе со схемой
    * `pk` - имя поля, являющегося первичным ключом таблицы
    * `output` - при значении **named** каждая запись таблицы будет в виде объекта с одним свойством, имя которого - имя таблицы, а содержимое - поля записи. В противном случае каждая запись таблицы будет просто объектом из выгружаемых полей.
    * `fields` - перечень выгружаемых полей
    * `filter` - как и у остальных типов. Для зависимых таблиц к указанным для неё фильтрам добавляется условие по связи. 
    * `details` - перечень зависимых таблиц(точнее имён схем выгрузок), записи которых будут выгружены вместе с записью основной таблицы.
    * `parentkey` - имя поля, которое ссылается на основную таблицу. По нему будут отобраны записи таблицы связанные с записью из основной.
    * `hierarchykey` - имя поля иерархии.
    * `references` - перечень полей и таблиц (имён схем выгрузок), на которые они ссылаются.
    * `sort` - массив имен полей, по которым сортируются записи в выгрузке по возрастанию.

### load
Класс **load** это реализация трансформирующего потока nodejs, в который направляется поток объектов. Доступны следующие типы со своими настройками
* `jsonString` - перевод объектов в json строки для дальнейшей укладки в файл через соединение с пишущим потоком путем pipe. Весь вывод обрамляется знаками массива `[` и `]`.
* `console` - вывод объектов в консоль приложения через JSON.stringify
* `dbcopyload` - потоковая запись данных в таблицу базы данных с помощью команды **copy**. Происходит добавление всех записей без каких-либо проверок на дубли и т.п.
  * `loadtable` - имя таблицы вместе со схемой
  * `fields` - имена загружаемых полей
* `db` - загрузка в базу данных с помощью формирования по настройкам **sql** вида `insert on conflict do update` для каждой загружаемой записи и их выполнения. Каждая таблица в схеме должна иметь уникальный ключ, на который завязывается `on conflict`. Примечание: если есть зависимые таблицы в схеме загрузки, то все таблицы не могут иметь суррогатный(на ходу генерящийся) первичный ключи, а только явно заданные и передаваемые в данных значения.
  * `unitField` - имя свойства в обрабатываемом объекте, в котором указано имя таблицы(точнее описателя таблицы) в перечне участвующих в загрузке(`units`). Если задавать в виде **$0**, то это указание не на имя свойства, а его номер в объекте, получаемых через Object.keys()
  * `unitData` - имя свойства в обрабатываемом объекте, в котором будут находиться непосредственно данные записи. Если задавать в виде **:$0**, то это указание не на имя свойства, а его номер в объекте, получаемых через Object.keys().
  * `units` - перечень (в виде объектов в свойстве с именем таблицы) всех таблиц, подлежащих обработке со следующими настройками
    * `tablename` - имя таблицы вместе со схемой
    * `pk` - имя поля, являющегося первичным ключом таблицы
    * `uk` - перечень полей, составляющих уникальный ключ, по которому будет происходить поиск обрабатываемой записи в существующих данных таблиц в базе данных.
    * `parentkey` - имя поля, которое ссылается на основную таблицу. Нужно когда в данных зависимой таблицы не передается его значение и тогда при рекурсивном вызове обработки записей зависимой таблицы значение будет взято из данных записи основной таблицы.
    * `hierarchykey` - имя поля иерархии.
    * `transform` - преобразования данных, json.stringify для объектов
    * `fields` - имена загружаемых полей
    * `details` - перечень зависимых таблиц(точнее имён схем выгрузок), записи которых идут вместе с данными каждой записи основной в свойстве **#details**.
    * `references` - перечень полей и таблиц (имён схем выгрузок), на которые они ссылаются.
* `execSqlArray` - на выходе выдает объекты с массивами операторов **sql**, построенных аналогично способу `db` с параметрами, без выполнения их подключенной базе данных. Работает только когда экземпляр класс создается с параметром `objectMode` равным true.

### Основные методы
Основные методы инструмента `exportByScheme` и `importByScheme` работают на основе схем, которые включают в себя такие настройки
* `extract` - настройка для экземпляра класса **extract**, участвующего в предстоящей обработке.
* `load` - для **load**.
* `main` - определяет **основной** обрабатываемый элемент из перечня обрабатываемых.

Метод`exportByScheme` использует экземпляр класса **extract** на данный момент для экспорта данных из источника бд (так как нет на входе читающих потоков для типов json или xml),
и через экземпляр класса **load** с возможными только типом `jsonString` производится запись в файл. Т.е. выгрузка записей таблицы в json файл.

Метод `importByScheme` используя также по одному экземпляру обоих классов и открывая читающий поток из указанного файла предназначен для загрузки записей в таблицы бд из файлов типа json и xml.

Другие манипуляции с данными можно строить используя нужные комбинации классов **extract** и **load** самостоятельно.


