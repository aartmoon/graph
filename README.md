# External-Memory PageRank на Java 21

## Статус реализации

Репозиторий содержит Java 21 / Gradle проект для semi-external memory PageRank:

- создан CLI с валидацией аргументов;
- реализован preprocessing pipeline: потоковое чтение CSV, in-memory vertex indexing, подсчет `outDegree`, запись modulo destination-partitioned binary edge-файлов;
- создаются `out_degree.bin`, `vertex_ids.bin`, `current_rank.bin`, `next_rank.bin`;
- реализован итерационный `PageRankEngine` поверх подготовленных файлов;
- edge partitions обрабатываются параллельно через `ExecutorService`;
- `danglingMass`, base term, L1 diff и остановка по `epsilon` / `maxIterations` реализованы;
- создан writer итогового CSV;
- добавлены JUnit-тесты для CLI parser, preprocessing и PageRankEngine.

### Что хранится в памяти сейчас

Текущая реализация является semi-external memory подходом: главный большой объект `E` ребер остается на диске, а CSV и binary partitions читаются потоково. Adjacency list и полный edge list в памяти не создаются.

В памяти в текущей версии находятся:

- in-memory mapping `original vertex id -> internal dense id`;
- обратный список `internal dense id -> original vertex id`;
- массив `outDegree[int]` на `N` вершин;
- массив `currentRank[double]` на `N` вершин;
- массив `nextRank[double]` на `N` вершин;
- небольшие IO-буферы для чтения CSV и записи бинарных файлов.

Это честное ограничение текущей версии. Если `V` тоже не помещается в RAM, mapping, `outDegree`, `currentRank` и `nextRank` нужно заменить на external sort / external join, memory-mapped или chunked rank store.

### Сборка

```bash
gradle test
gradle run --args="--input data/edges.csv --output output/pagerank.csv --workdir work --partitions 64 --threads 8 --damping 0.85 --max-iterations 30 --epsilon 1e-8 --id-mode contiguous --sort-by-rank false"
```

### Запуск тестов

```bash
gradle test
```

Тестовый набор покрывает:

- cycle graph `1 -> 2 -> 3 -> 1`, где ранги должны быть равны;
- dangling node graph `1 -> 2`, где сумма PageRank сохраняется около `1.0`;
- граф из задания `1 -> 2`, `2 -> 3`, `3 -> 1`, `4 -> 1`;
- hyper-node smoke test `1 -> 2..10000`, который проверяет, что код проходит без построения adjacency list.

### Пример запуска с 128 MB heap

```bash
gradle installDist
java -Xmx128m \
  -cp build/install/largegraph-pagerank/lib/largegraph-pagerank-0.1.0.jar \
  org.example.largegraph.Main \
  --input data/edges.csv \
  --output output/pagerank.csv \
  --workdir work \
  --partitions 64 \
  --threads 8 \
  --damping 0.85 \
  --max-iterations 30 \
  --epsilon 1e-8 \
  --id-mode contiguous \
  --sort-by-rank false
```

### Docker

```bash
docker build -t largegraph-pagerank .
docker run --rm \
  -v "$PWD/data:/app/data" \
  -v "$PWD/output:/app/output" \
  -v "$PWD/work:/app/work" \
  largegraph-pagerank \
  --input data/edges.csv \
  --output output/pagerank.csv \
  --workdir work \
  --partitions 64 \
  --threads 8 \
  --damping 0.85 \
  --max-iterations 30 \
  --epsilon 1e-8 \
  --id-mode contiguous \
  --sort-by-rank false
```

## How To Reproduce Results

1. Проверьте Java:

```bash
java -version
```

Ожидается Java 21.

2. Запустите тесты:

```bash
gradle test
```

3. Запустите пример:

```bash
gradle run --args="--input data/edges.csv --output output/pagerank.csv --workdir work --partitions 64 --threads 8 --damping 0.85 --max-iterations 30 --epsilon 1e-8 --id-mode contiguous --sort-by-rank false"
```

4. Посмотрите результат:

```bash
cat output/pagerank.csv
```

5. Для сортировки по рангу:

```bash
gradle run --args="--input data/edges.csv --output output/pagerank-by-rank.csv --workdir work --partitions 64 --threads 8 --damping 0.85 --max-iterations 30 --epsilon 1e-8 --id-mode contiguous --sort-by-rank true"
```

## RAM And Disk Layout

В текущей версии в RAM находятся:

- `HashMap<Integer, Integer>` для mapping `original vertex id -> internal dense id`;
- `ArrayList<Integer>` для восстановления `internal dense id -> original vertex id`;
- `int[] outDegree`;
- `double[] currentRank`;
- `double[] nextRank`;
- небольшие `BufferedReader`, `BufferedOutputStream`, `DataInputStream`, `DataOutputStream` буферы.

На диске находятся:

- исходный CSV `data/edges.csv`;
- edge partitions `work/partitions/part-xxxxx.bin`;
- `work/out_degree.bin`;
- `work/vertex_ids.bin`;
- `work/current_rank.bin`;
- `work/next_rank.bin`;
- итоговый CSV `output/pagerank.csv`.

Почему нет `O(E)` RAM:

- входной CSV читается потоково;
- preprocessing не сохраняет список ребер в коллекции;
- каждое ребро сразу записывается в binary partition;
- PageRankEngine читает partition-файлы потоково на каждой итерации;
- adjacency list для source-вершин не строится.

Текущая память зависит в основном от `V`, а не от `E`:

```text
O(V) for mapping + outDegree + currentRank + nextRank
O(1) / O(partitions) for IO buffers
O(E) on disk for binary edge partitions
```

## Multithreading

PageRankEngine использует `ExecutorService` с числом потоков из `--threads`.

На каждой итерации:

- dangling mass считается параллельно по диапазонам vertex id;
- edge partitions обрабатываются параллельно;
- каждая partition-задача потоково читает свой файл `part-xxxxx.bin`;
- общий `nextRank[]` обновляется без locks, потому что partition `p` содержит только destination-вершины, для которых `floorMod(toInternalId, partitions) == p`.

Это означает, что два worker-потока не пишут вклад в одну и ту же destination-вершину.

## Hyper-Nodes

Гипер-узел со степенью 50 000 не приводит к выделению списка соседей в памяти.

Во время preprocessing его ребра записываются как отдельные бинарные записи:

```text
int fromInternalId
int toInternalId
```

Во время PageRank каждая запись читается последовательно, вклад считается сразу:

```text
damping * currentRank[from] / outDegree[from]
```

После этого ребро больше не удерживается в памяти.

## Why Results Are Deterministic

Результаты воспроизводимы при одинаковом входном CSV, параметрах запуска и версии JVM:

- mapping vertex id создается в порядке первого появления id во входном CSV;
- partition id вычисляется чистой функцией `floorMod(toInternalId, partitions)`;
- каждое ребро записывается в partition-файл в порядке чтения CSV;
- каждая destination-вершина обновляется ровно одной partition-задачей;
- нет `AtomicDouble`, locks или конкурентного сложения в одну ячейку `nextRank`;
- итоговый CSV сортируется детерминированно: по `vertex ASC` или по `rank DESC`, затем `vertex ASC` для равных rank.

Обычная floating-point погрешность `double` остается, но порядок сложения вкладов для каждой destination-вершины стабилен.

## Limitations

Текущая версия является semi-external memory реализацией:

- ребра `E` находятся на диске и читаются потоково;
- массивы и mapping по вершинам `V` пока находятся в RAM.

Ограничения:

- `original id -> internal id` mapping строится in-memory;
- `outDegree`, `currentRank`, `nextRank` хранятся как heap-массивы;
- `--id-mode contiguous` отклоняет отрицательные vertex id;
- дубликаты ребер не удаляются и считаются повторными переходами;
- metadata-файл пока не пишется;
- partitioning через modulo прост и детерминирован, но может быть хуже для локальности доступа к rank-массивам, чем range partitioning или sorted shards.

Что улучшить для графа, где `V` тоже не помещается в память:

- заменить mapping на external sort / external join;
- хранить rank и outDegree через memory-mapped files;
- обрабатывать rank chunks вместо heap-массивов;
- сортировать partition-файлы по `fromInternalId` для лучшей локальности;
- добавить metadata/checksum для промежуточных файлов.

## Цель проекта

Проект реализует аналитическую метрику **PageRank** для большого ориентированного невзвешенного графа, который не помещается в RAM одного узла.

Исходные условия:

- входной граф задается CSV-файлом `from,to`;
- идентификаторы вершин: `int32`;
- граф ориентированный и невзвешенный;
- один узел, Ubuntu 22.04 x64, SSD;
- ограничение памяти: например, 128 MB RAM при размере графа около 1 GB;
- возможны гипер-узлы со степенью около 50 000 при средней степени 50;
- язык реализации: Java 21;
- нельзя хранить весь список ребер или adjacency list в памяти;
- требуется многопоточность;
- результат должен быть воспроизводимым и проверяемым.

Основной выбранный подход: **destination-partitioned external-memory PageRank**.

---

## Что такое PageRank

### Что измеряет

PageRank измеряет важность вершины в ориентированном графе. Интуитивно вершина важна, если на нее ссылаются другие важные вершины.

В контексте веб-графа вершины являются страницами, а ребра — ссылками. В более общем виде PageRank применим к графам зависимостей, цитирований, рекомендаций, транзакций, социальных связей и графам знаний.

### Где применяется

PageRank используется для:

- ранжирования веб-страниц;
- оценки авторитетности документов, статей или пользователей;
- поиска важных узлов в графах ссылок, цитирований и зависимостей;
- построения признаков для ML-моделей на графах;
- анализа графов знаний и рекомендательных систем.

### Формула

Для вершины `v` на итерации `k + 1`:

```text
PR_next(v) = (1 - d) / N
             + d * danglingMass / N
             + d * sum(PR(u) / outDegree(u)), где u -> v
```

Где:

- `N` — количество вершин;
- `d` — damping factor;
- `PR(u)` — PageRank вершины `u` на текущей итерации;
- `outDegree(u)` — исходящая степень вершины `u`;
- `danglingMass` — суммарный PageRank вершин без исходящих ребер;
- сумма берется по всем входящим соседям `u`, у которых есть ребро `u -> v`.

Начальное значение:

```text
PR_0(v) = 1 / N
```

### Damping factor

`damping factor` обычно равен `0.85`.

Он моделирует вероятность того, что случайный пользователь перейдет по ссылке. С вероятностью `1 - d` пользователь переходит в случайную вершину графа. Это делает алгоритм устойчивым, помогает сходимости и позволяет обрабатывать графы с изолированными компонентами.

### Dangling nodes

`Dangling nodes` — вершины без исходящих ребер, то есть `outDegree(v) = 0`.

Если их не учитывать, часть PageRank-массы будет теряться на каждой итерации. Поэтому суммарный ранг всех dangling-вершин распределяется равномерно между всеми вершинами:

```text
d * danglingMass / N
```

### Критерии остановки

Итерации останавливаются при выполнении одного из условий:

- достигнуто максимальное число итераций, например `maxIterations = 30`;
- L1-разница между текущим и следующим rank-вектором меньше порога:

```text
sum(abs(PR_next(v) - PR_current(v))) < epsilon
```

Рекомендуемый практический критерий:

```text
epsilon = 1e-8
```

Для больших графов также полезно логировать:

- номер итерации;
- L1 delta;
- сумму всех PageRank-значений;
- dangling mass;
- время чтения партиций;
- время записи rank-файлов.

---

## Варианты хранения и обработки графа

### 1. Edge streaming

В этом варианте все ребра читаются потоково из одного edge-файла на каждой итерации.

Идея:

- preprocessing считает `outDegree`;
- на каждой итерации потоково читается файл ребер `from,to`;
- для каждого ребра вычисляется вклад `rank[from] / outDegree[from]`;
- вклад добавляется в `nextRank[to]`.

Плюсы:

- простая модель;
- не нужно хранить adjacency list;
- последовательное чтение хорошо подходит для SSD.

Минусы:

- при многопоточности сложно безопасно обновлять общий `nextRank[to]`;
- нужны атомики, локальные буферы или reduce-фаза;
- случайные записи в `nextRank` ухудшают производительность;
- гипер-узлы не ломают память, но могут создавать перекос нагрузки.

### 2. Destination partitioning

Ребра заранее разбиваются на партиции по вершине назначения `to`.

Идея:

- `partitionId = floorMod(denseTo, partitionCount)`;
- каждая партиция содержит только ребра, ведущие в закрепленный за ней modulo-bucket destination-вершин;
- на итерации каждый worker обрабатывает одну или несколько destination-партиций;
- worker пишет только в destination-вершины своей партиции.

Плюсы:

- весь список ребер не хранится в памяти;
- ребра читаются потоково из бинарных партиций;
- нет больших adjacency list даже для гипер-узлов;
- разные партиции можно обрабатывать параллельно;
- запись в `nextRank` не имеет data race, потому что партиции владеют непересекающимися множествами destination-вершин.

Минусы:

- нужна preprocessing-фаза;
- требуется хранить бинарные edge-партиции на диске;
- при сильной неравномерности входящих степеней возможен перекос времени обработки партиций.

### 3. GraphChi-like shard processing

GraphChi-подход делит граф на shard-файлы, каждый из которых содержит ребра для диапазона destination-вершин, обычно отсортированные по source или destination. Алгоритм использует модель sliding shards: для текущего диапазона вершин читается нужная часть входящих и исходящих данных.

Плюсы:

- хорошо подходит для external-memory graph processing;
- можно строить более универсальный движок для разных графовых алгоритмов;
- можно оптимизировать последовательное чтение и локальность доступа.

Минусы:

- сложнее реализация;
- требуется больше метаданных, сортировок и индексов;
- для тестового задания PageRank можно реализовать проще через destination partitioning.

---

## Выбранный вариант

Основной вариант: **destination-partitioned external-memory PageRank**.

Граф хранится на диске в виде набора бинарных edge-партиций. В текущей реализации каждая партиция содержит ребра, у которых destination-вершина попадает в определенный modulo-bucket:

```text
partition = floorMod(toInternalId, partitionCount)
```

На каждой итерации worker читает edge-партицию последовательно, вычисляет вклады от source-вершин и аккумулирует значения только для destination-вершин своей партиции.

---

## Почему этот вариант подходит

### Граф не держится в памяти

В памяти не хранится полный список ребер и не строится adjacency list. Ребра находятся на диске в бинарных партициях и читаются потоково.

В памяти на итерации находятся:

- текущий rank-вектор;
- outDegree-массив;
- локальный буфер или набор блочных записей `nextRank` для destination-вершин партиции;
- небольшие IO-буферы.

### Ребра читаются потоково

Каждая edge-партиция читается последовательным scan-ом. Это хорошо подходит для SSD и снижает стоимость случайного доступа.

### Гипер-узлы не создают больших adjacency lists

Гипер-узел со степенью 50 000 представлен как 50 000 отдельных edge-записей в партициях. Для него не создается большой объект adjacency list в heap.

При обработке ребра достаточно знать:

```text
rank[from]
outDegree[from]
to
```

### Партиции можно обрабатывать параллельно

Destination-партиции независимы на фазе вычисления `nextRank`, потому что каждая партиция отвечает за свое непересекающееся множество destination-вершин.

Например:

```text
worker-1 -> partition 0 -> denseTo % K == 0
worker-2 -> partition 1 -> denseTo % K == 1
worker-3 -> partition 2 -> denseTo % K == 2
```

### Запись в nextRank без data race

Ключевое свойство destination partitioning: все ребра, ведущие в вершину `v`, лежат в одной destination-партиции.

Следовательно:

- только один worker пишет вклад в `nextRank[v]`;
- не нужны `AtomicDouble`, locks или synchronized-блоки для обновления rank;
- итоговая reduce-фаза не нужна для объединения вкладов по одной вершине.

Это упрощает реализацию и делает результат детерминированнее.

---

## Архитектура проекта Java 21

Предлагаемая структура пакетов:

```text
src/main/java/com/example/pagerank/
  App.java
  cli/
  config/
  graph/
  io/
  preprocess/
  rank/
  partition/
  util/
```

### `com.example.pagerank.App`

Точка входа приложения.

Ответственность:

- парсит CLI-аргументы;
- загружает конфигурацию;
- запускает preprocessing, если нужно;
- запускает PageRank-итерации;
- экспортирует результат.

### `cli`

Классы:

- `CliArguments`;
- `CliParser`;
- `Command`.

Ответственность:

- разбор параметров командной строки;
- команды вида `preprocess`, `run`, `export`, `all`;
- валидация обязательных путей и числовых параметров.

Пример параметров:

```text
--input graph.csv
--work-dir work
--output pagerank.csv
--partitions 64
--threads 8
--damping 0.85
--epsilon 1e-8
--max-iterations 30
--memory-budget-mb 128
```

### `config`

Классы:

- `PageRankConfig`;
- `StorageConfig`;
- `PreprocessingConfig`.

Ответственность:

- хранение параметров запуска;
- расчет производных параметров, например размера партиции;
- проверка ограничений памяти.

### `graph`

Классы:

- `Edge`;
- `VertexId`;
- `DenseIdMapping`;
- `GraphMetadata`.

Ответственность:

- доменные сущности графа;
- mapping исходных `int32` vertex id в плотные `denseId`;
- хранение `vertexCount`, `edgeCount`, `partitionCount`;
- хранение min/max id и статистики степеней.

### `io`

Классы:

- `CsvEdgeReader`;
- `BinaryEdgeWriter`;
- `BinaryEdgeReader`;
- `RankFileReader`;
- `RankFileWriter`;
- `OutDegreeStore`;
- `MetadataStore`;
- `ResultCsvWriter`.

Ответственность:

- потоковое чтение CSV;
- запись и чтение бинарных edge-партиций;
- чтение и запись rank-файлов;
- хранение outDegree;
- сериализация metadata;
- экспорт итогового CSV.

### `preprocess`

Классы:

- `GraphPreprocessor`;
- `VertexIndexer`;
- `OutDegreeBuilder`;
- `PartitionWriter`;
- `PreprocessingStats`.

Ответственность:

- построение mapping vertex id -> dense id;
- подсчет outDegree;
- разбиение ребер по destination-партициям;
- запись бинарных partition-файлов;
- сбор статистики и проверок.

### `partition`

Классы:

- `PartitionDescriptor`;
- `PartitionPlanner`;
- `PartitionRange`;
- `PartitionAssignment`.

Ответственность:

- расчет destination-диапазонов;
- сопоставление `denseTo` с `partitionId`;
- описание файлов партиций;
- распределение партиций между worker-потоками.

### `rank`

Классы:

- `PageRankRunner`;
- `PageRankIteration`;
- `PartitionRankTask`;
- `DanglingMassCalculator`;
- `ConvergenceChecker`;
- `RankVector`;
- `RankStats`;

Ответственность:

- управление итерациями PageRank;
- запуск parallel partition tasks;
- расчет dangling mass;
- расчет базового teleportation-члена;
- проверка сходимости;
- swap `currentRank` и `nextRank`;
- сбор статистики итераций.

### `util`

Классы:

- `MemoryBudget`;
- `LongDoubleAccumulator`;
- `Stopwatch`;
- `FileUtils`;
- `ChecksumUtils`.

Ответственность:

- контроль memory budget;
- измерение времени;
- безопасная работа с файлами;
- checksum для проверки целостности промежуточных файлов.

---

## Формат файлов

### Входной CSV

Файл:

```text
graph.csv
```

Формат:

```text
from,to
1,2
1,3
2,3
3,1
```

Правила:

- первая строка может быть header-ом `from,to`;
- `from` и `to` — `int32`;
- в режиме `--id-mode contiguous` отрицательные vertex id отклоняются;
- пробелы вокруг значений допускаются;
- пустые строки игнорируются;
- некорректные строки считаются ошибкой preprocessing;
- дубликаты ребер по умолчанию сохраняются и влияют на вес переходов как повторные ссылки;
- self-loop по умолчанию допускается.

### Бинарные партиции ребер

Каталог:

```text
work/partitions/
```

Файлы:

```text
part-00000.bin
part-00001.bin
...
part-00063.bin
```

Запись ребра:

```text
int32 denseFrom
int32 denseTo
```

Размер записи: `8 bytes`.

Все `denseTo` внутри файла удовлетворяют условию:

```text
floorMod(denseTo, partitionCount) == partitionId
```

Файл читается последовательным scan-ом. Порядок ребер внутри партиции не важен для корректности PageRank.

### Metadata

Файл metadata пока не создается текущей реализацией. На следующем этапе его стоит добавить как:

```text
work/metadata.json
```

Содержит:

- `vertexCount`;
- `edgeCount`;
- `partitionCount`;
- `dampingFactor`;
- формулу партиционирования `floorMod(toInternalId, partitionCount)`;
- checksum входных и промежуточных файлов;
- статистику степеней.

### Vertex id file

Файл:

```text
work/vertex_ids.bin
```

Формат:

```text
int32 originalVertexIdByDenseId[0]
int32 originalVertexIdByDenseId[1]
...
int32 originalVertexIdByDenseId[N - 1]
```

Используется для:

- экспорта результата обратно в исходные vertex id.

В первой версии mapping `original id -> dense id` строится в памяти, а `vertex_ids.bin` сохраняет обратное отображение.

### OutDegree-файл

Файл:

```text
work/out_degree.bin
```

Формат:

```text
int32 outDegree[0]
int32 outDegree[1]
...
int32 outDegree[N - 1]
```

Размер:

```text
4 * N bytes
```

`outDegree[denseId]` нужен для вычисления вклада:

```text
rank[denseFrom] / outDegree[denseFrom]
```

### Rank-файлы

Файлы:

```text
work/current_rank.bin
work/next_rank.bin
```

Формат:

```text
float64 rank[0]
float64 rank[1]
...
float64 rank[N - 1]
```

Размер одного rank-файла:

```text
8 * N bytes
```

Для очень больших `N` rank-вектор можно читать через memory-mapped file или блочное чтение. В базовом варианте допускается хранение `currentRank` в памяти, если `8 * N` укладывается в memory budget. Если не укладывается, используется блочный или memory-mapped режим.

### Выходной CSV

Файл:

```text
pagerank.csv
```

Формат:

```text
vertex,rank
1,0.123456789
2,0.234567891
3,0.641975320
```

Правила:

- `vertex` — исходный `int32` id;
- `rank` — `double`;
- сумма PageRank по всем вершинам должна быть близка к `1.0`;
- по умолчанию строки сортируются по `vertex` ASC;
- если `--sort-by-rank true`, строки сортируются по `rank` DESC.

---

## Preprocessing

Preprocessing превращает исходный CSV в набор файлов, удобных для external-memory PageRank.

### Шаг 1. Чтение CSV и построение множества вершин

Потоково читается входной CSV.

Для каждого ребра `from,to`:

- валидируются оба id;
- `from` добавляется в множество вершин;
- `to` добавляется в множество вершин;
- увеличивается счетчик ребер.

Результат:

- список уникальных vertex id;
- `vertexCount`;
- `edgeCount`.

### Шаг 2. Построение dense-id mapping

Каждому исходному `int32` id назначается плотный id:

```text
originalVertexId -> denseId in [0, N)
```

Плотные id нужны для компактных массивов:

- `rank[denseId]`;
- `outDegree[denseId]`;
- partition buckets.

В первой версии mapping может строиться в памяти через hash map. Для графов, где число вершин само не помещается в memory budget, это нужно заменить на external sort.

### Шаг 3. Подсчет outDegree

CSV читается повторно.

Для каждого ребра `from,to`:

- `denseFrom = mapping[from]`;
- `outDegree[denseFrom]++`.

Результат записывается в:

```text
work/out_degree.bin
```

### Шаг 4. Разбиение ребер по destination-партициям

CSV читается еще раз.

Для каждого ребра:

```text
denseFrom = mapping[from]
denseTo = mapping[to]
partitionId = floorMod(denseTo, partitionCount)
```

Ребро записывается в бинарный файл:

```text
work/partitions/part-{partitionId}.bin
```

Чтобы не держать ребра в памяти, запись выполняется через `BufferedOutputStream` / `DataOutputStream`. Текущая версия создает все partition-файлы заранее, включая пустые.

### Шаг 5. Инициализация rank-файлов

Создаются rank-файлы:

```text
current_rank.bin
next_rank.bin
```

Начальное значение для каждой вершины:

```text
1.0 / vertexCount
```

### Шаг 6. Запись metadata

Сохраняется:

- количество вершин;
- количество ребер;
- количество партиций;
- формула partition bucket;
- статистика степеней;
- настройки запуска;
- checksum промежуточных файлов.

---

## Итерации PageRank

Каждая итерация состоит из нескольких фаз.

### Фаза 1. Расчет dangling mass

Сканируется `outDegree`.

Для всех вершин `v`, где:

```text
outDegree[v] == 0
```

суммируется:

```text
danglingMass += currentRank[v]
```

Эту фазу можно распараллелить по диапазонам вершин.

### Фаза 2. Расчет базового значения

Для каждой вершины базовая часть PageRank одинакова:

```text
base = (1 - damping) / N + damping * danglingMass / N
```

### Фаза 3. Обработка destination-партиций

Каждая задача `PartitionRankTask` получает:

- descriptor партиции;
- множество destination-вершин, принадлежащих partition bucket;
- доступ к `currentRank`;
- доступ к `outDegree`;
- `base`;
- путь к edge-файлу партиции;
- путь или буфер для записи соответствующего диапазона `nextRank`.

Алгоритм для партиции:

1. Создать локальный accumulator для destination-вершин этой партиции.
2. Заполнить `localNext` значением `base`.
3. Последовательно прочитать все ребра `denseFrom,denseTo` из файла партиции.
4. Если `outDegree[denseFrom] > 0`, добавить вклад:

```text
localIndex = denseTo - partitionStart
localNext[localIndex] += damping * currentRank[denseFrom] / outDegree[denseFrom]
```

5. Записать значения в соответствующие позиции `next_rank.bin`.
6. Посчитать локальную L1 delta относительно `currentRank` для этого диапазона.
7. Вернуть `RankStats` в главный поток.

### Фаза 4. Сбор статистики

Главный поток собирает результаты всех `PartitionRankTask`:

- суммарная L1 delta;
- сумма `nextRank`;
- время обработки;
- количество прочитанных ребер;
- ошибки IO, если были.

### Фаза 5. Проверка сходимости

Если:

```text
L1 delta < epsilon
```

алгоритм завершает работу.

Иначе:

- `next_rank.bin` становится `current_rank.bin`;
- начинается следующая итерация.

Физически swap можно сделать переименованием файлов или swap-ом ссылок на файлы.

---

## Многопоточность

Многопоточность строится через `ExecutorService`.

### Пул потоков

Создается фиксированный пул:

```text
threads = min(configuredThreads, availableProcessors)
```

Каждая задача обрабатывает одну destination-партицию или группу маленьких партиций.

### Parallel partition tasks

На каждой итерации:

- главный поток создает список `PartitionRankTask`;
- задачи отправляются в `ExecutorService`;
- результаты собираются через `Future` или `CompletionService`;
- ошибки из worker-потоков пробрасываются в главный поток.

### Почему нет data race

Destination-партиции не пересекаются по `denseTo`.

Это значит:

- `PartitionRankTask` для partition `i` пишет только диапазон `nextRank[start_i, end_i)`;
- другой task не пишет в этот диапазон;
- `currentRank` и `outDegree` только читаются;
- shared mutable state отсутствует на hot path.

Для статистики используются локальные счетчики, которые возвращаются как immutable result object.

### Балансировка нагрузки

Если некоторые партиции оказываются тяжелее из-за большого числа входящих ребер, планировщик может:

- делать больше партиций, чем потоков;
- назначать задачи динамически через очередь ExecutorService;
- хранить в metadata размер каждой партиции и сначала запускать тяжелые партиции;
- в будущем делить слишком большие партиции на sub-partitions.

---

## Сложность

Обозначения:

- `N` — количество вершин;
- `M` — количество ребер;
- `K` — количество destination-партиций;
- `I` — количество итераций до сходимости.

### Preprocessing

Время:

```text
O(M + N)
```

На практике CSV читается несколько раз:

1. построение множества вершин;
2. подсчет outDegree;
3. запись edge-партиций.

Итого:

```text
O(3M + N) = O(M + N)
```

Если mapping строится через external sort, preprocessing становится:

```text
O(M log M)
```

или ближе к:

```text
O(sort(M))
```

в модели external-memory сортировки.

### Одна итерация

Время:

```text
O(M + N)
```

Состав:

- `O(N)` на dangling mass;
- `O(M)` на чтение всех edge-партиций;
- `O(N)` на запись `nextRank` и расчет delta.

При `T` потоках идеальная CPU-часть приближается к:

```text
O((M + N) / T)
```

Но фактическая скорость ограничена:

- пропускной способностью SSD;
- стоимостью чтения rank/outDegree;
- перекосом размеров партиций;
- overhead работы с файлами.

### Total

Общая сложность:

```text
O(M + N) + I * O(M + N)
```

То есть:

```text
O(I * (M + N))
```

### Disk IO

Preprocessing:

- чтение CSV несколько раз: около `3 * inputSize`;
- запись edge-партиций: около `8 * M bytes`;
- запись outDegree: `4 * N bytes`;
- запись mapping и metadata.

Одна итерация:

- чтение всех edge-партиций: `8 * M bytes`;
- чтение `currentRank`: до `8 * N bytes`, в зависимости от режима доступа;
- чтение `outDegree`: до `4 * N bytes`;
- запись `nextRank`: `8 * N bytes`;
- возможно чтение `currentRank` для delta: `8 * N bytes`.

### Memory usage

Базовая модель памяти:

```text
currentRank: 8 * N bytes, если хранится в heap
outDegree:   4 * N bytes, если хранится в heap
partition accumulator: depends on partition bucket size and implementation
IO buffers:  O(K или activeFiles)
metadata:    O(K)
mapping:     зависит от реализации
```

При memory budget 128 MB нужно выбирать:

- количество партиций;
- размер `localNext`;
- режим хранения rank/outDegree.

Если `currentRank` и `outDegree` не помещаются в heap, они должны читаться через:

- memory-mapped files;
- direct buffers;
- блочный file access;
- off-heap структуры.

---

## Почему результатам можно доверять

### Математическая корректность

Реализуется стандартная power iteration формула PageRank:

```text
PR_next(v) = (1 - d) / N
             + d * danglingMass / N
             + d * sum(PR(u) / outDegree(u))
```

Destination partitioning меняет только физический порядок обработки ребер, но не меняет математическую формулу.

### Сохранение rank-массы

На каждой итерации проверяется:

```text
sum(PR_next) ~= 1.0
```

Допустимое отклонение связано с floating-point арифметикой. Если сумма существенно отличается от `1.0`, итерация считается подозрительной и логируется как ошибка качества.

### Учет dangling nodes

PageRank-масса dangling-вершин не теряется, а равномерно распределяется по всем вершинам на каждой итерации.

### Детерминированность

Каждая destination-вершина обрабатывается ровно одной партицией. Поэтому нет nondeterministic race при обновлении `nextRank[v]`.

Порядок сложения вкладов внутри одной партиции фиксирован порядком ребер в файле. Это делает результаты воспроизводимыми при одинаковых входных файлах, настройках и версии JVM.

### Инварианты и проверки

Во время preprocessing проверяется:

- количество прочитанных ребер;
- количество записанных ребер по всем партициям;
- соответствие `sum(partitionEdges) == edgeCount`;
- корректность диапазонов destination-партиций;
- отсутствие dense id вне `[0, N)`.

Во время итераций проверяется:

- `sum(PR_next)` близка к `1.0`;
- `L1 delta` не является `NaN` или `Infinity`;
- все rank-значения неотрицательны;
- количество прочитанных edge-записей равно ожидаемому.

### Тестирование

Для доверия к реализации нужны следующие уровни тестов:

- unit-тесты для CSV parser, mapping, partition planner, binary IO;
- unit-тесты PageRank на маленьких графах с известным результатом;
- сравнение с in-memory reference implementation на графах малого размера;
- property-based проверки инвариантов: сумма рангов, неотрицательность, стабильность после сходимости;
- integration-тест с графом, который превышает заданный memory budget, но обрабатывается через partition files;
- тесты на dangling nodes, self-loops, disconnected components и гипер-узлы.

---

## Ограничения текущего дизайна

### In-memory mapping vertex id -> dense id

В первой версии допускается хранить mapping исходных vertex id в dense id в памяти.

Это ограничение может стать проблемой, если количество уникальных вершин очень велико. Даже если edge list не помещается в память, mapping тоже может быть слишком большим для 128 MB.

Улучшение:

- построить mapping через external sort;
- отсортировать все vertex id из колонок `from` и `to`;
- удалить дубликаты потоково;
- назначить dense id последовательным scan-ом;
- преобразовывать CSV в dense edges через external join.

### Rank/outDegree могут не помещаться в heap

Для больших `N` массивы:

```text
double[] rank
int[] outDegree
```

могут превысить memory budget.

Улучшение:

- использовать `MappedByteBuffer`;
- использовать direct/off-heap buffers;
- читать rank/outDegree блоками;
- кешировать hot ranges;
- делать source-aware layout для снижения случайного доступа к `currentRank`.

### Возможен перекос destination-партиций

Если много ребер ведет в небольшой диапазон destination-вершин, одна партиция может стать намного тяжелее остальных.

Улучшение:

- увеличивать количество партиций;
- строить партиции по количеству входящих ребер, а не по фиксированному диапазону dense-id;
- делить тяжелые партиции на sub-partitions;
- планировать задачи по размеру partition-файла.

### Несортированные партиции

В базовой версии порядок ребер внутри партиции не оптимизирован.

Улучшение:

- сортировать ребра внутри партиции по `denseFrom`;
- улучшить локальность чтения `currentRank` и `outDegree`;
- группировать одинаковые `denseFrom`, чтобы один раз читать `rank[from] / outDegree[from]`.

### Дубликаты ребер

По умолчанию дубликаты сохраняются. Это математически означает, что повторная ссылка увеличивает вклад source-вершины в destination-вершину.

Если бизнес-смысл требует считать граф простым, нужна отдельная dedup-фаза. Для большого графа ее лучше делать external sort-ом по `(from, to)`.

### Floating-point погрешность

PageRank использует `double`, поэтому возможны небольшие различия из-за порядка сложения.

В выбранной архитектуре порядок сложения внутри партиции стабилен, а разные потоки не складывают значения в одну и ту же destination-вершину. Это снижает недетерминизм, но не отменяет обычную floating-point погрешность.

---

## План репозитория

На следующих этапах репозиторий можно оформить так:

```text
.
├── README.md
├── pom.xml
├── src/
│   ├── main/java/com/example/pagerank/
│   └── test/java/com/example/pagerank/
├── examples/
│   └── small-graph.csv
├── docs/
│   ├── file-formats.md
│   └── algorithm.md
└── scripts/
    └── generate-synthetic-graph.sh
```

Текущий документ описывает архитектурный дизайн. Код реализации намеренно не добавлен на этом этапе.
