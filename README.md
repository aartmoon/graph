# External-Memory PageRank для больших графов

Проект считает PageRank для ориентированного невзвешенного графа из CSV:

```csv
from,to
1,2
2,3
3,1
```

Целевой сценарий: один узел с SSD и небольшим Java heap, например `-Xmx128m`, когда ребра и массивы вершин нельзя держать в памяти.

## Главное

- Java 21 + Gradle;
- arbitrary non-negative `int32` original ids;
- external dense reindexing в диапазон `[0..V)`;
- PageRank считает по dense ids, output пишет original ids;
- source-partitioned edges;
- X-Stream-style scatter/gather;
- disk-backed `DiskDoubleArray` и `DiskIntArray`;
- preprocessing без random disk increment на каждое исходное ребро;
- message manifest вместо проверки всех `workers * partitions` файлов;
- балансировка scatter/gather через очередь задач с тяжелыми partitions первыми;
- потоковый output и `--top-k` через heap размера K.

Реализация остается специализированной под PageRank, а не превращается в общий graph framework.

## PageRank

Формула не изменялась:

```text
PR_next[v] =
  (1 - damping) / V
  + damping * danglingMass / V
  + damping * sum(PR_current[u] / outDegree[u])
```

Сумма берется по ребрам `u -> v`. Dangling vertices (`outDegree == 0`) на каждой итерации равномерно отдают массу всем вершинам.

Дефолты:

```text
damping = 0.85
maxIterations = 200
epsilon = 1e-8
```

## External Reindexing

Входные ids могут быть sparse:

```csv
from,to
100,1000000
```

Такой граф имеет `vertexCount = 2`, а не `1000001`. Вершина `0` не появляется, если ее нет во входе.

Preprocessing:

1. CSV читается потоково.
2. Пишутся `raw_edges.bin` и `vertex_ids_unsorted.bin`.
3. `vertex_ids_unsorted.bin` сортируется внешней сортировкой по int chunks.
4. Во время merge выполняется deduplicate.
5. Пишутся:
   - `vertices.bin`: `int originalIdByDenseId[denseId]`;
   - `mapping.bin`: пары `int originalId, int denseId`, отсортированные по `originalId`.
6. Endpoint references сортируются по `originalId`.
7. Sort-merge join с `mapping.bin` переписывает endpoints в dense ids.
8. Endpoint assignments сортируются по `(edgeId, side)`.
9. Dense edges пишутся сразу в `edges_by_source/src-part-xxxxx.bin`.

В heap не создается `HashMap<originalId, denseId>` для всех вершин.

## Формат CSV

Входной файл читается как CSV, где первые две колонки являются `from` и `to`.
Дополнительные колонки разрешены и игнорируются:

```csv
from,to,weight,comment
1,2,10,abc
2,3,20,def
```

Правила:

- header с первыми двумя колонками `from,to` разрешен;
- пустые строки пропускаются;
- пробелы вокруг ids обрезаются;
- если в строке меньше двух колонок, preprocessing завершится понятной ошибкой;
- если первые две колонки не являются `int32`, preprocessing завершится понятной ошибкой;
- отрицательные ids отклоняются.

## Storage Layout

Рабочая директория:

```text
work/
  meta.properties
  raw_edges.bin
  vertex_ids_unsorted.bin
  vertices.bin
  mapping.bin
  endpoint_refs.bin
  endpoint_refs.sorted.bin
  endpoint_assignments.bin
  endpoint_assignments.sorted.bin

  sort/
    ...

  vertex/
    out_degree.bin
    rank_current.bin
    rank_next.bin

  edges_by_source/
    source-partitions.tsv
    src-part-00000.bin
    src-part-00001.bin
    ...

  messages/
    iter-000001/
      worker-00000/
        manifest.txt
        msg-part-00000.bin
        msg-part-00007.bin
```

Форматы:

```text
raw_edges.bin:              long edgeId, int fromOriginal, int toOriginal
vertex_ids_unsorted.bin:    int originalId
vertices.bin:               int originalId
mapping.bin:                int originalId, int denseId
endpoint_refs.bin:          int originalId, long edgeId, byte side
endpoint_assignments.bin:   long edgeId, byte side, int denseId
src-part-xxxxx.bin:         int denseFrom, int denseTo
message file:               int denseTo, double contribution
rank files:                 double rank[denseId]
out_degree.bin:             int outDegree[denseId]
```

## Out-Degree

Out-degree больше не считается через random `incrementInt(from)` при чтении CSV.

После dense rewrite ребра уже лежат по source partitions. Для каждой partition:

```text
sourceStart = partition * chunkSize
sourceLength = min(chunkSize, vertexCount - sourceStart)
```

Загружается только `int[] outDegreeChunk`, source partition читается потоково, затем chunk записывается обратно в `out_degree.bin`.

## Итерация PageRank

Фаза 0, dangling mass:

- `rank_current.bin` и `out_degree.bin` читаются chunk-ами;
- полный `rank[]` или `outDegree[]` в heap не загружается.

Фаза 1, scatter:

- source partitions сортируются по размеру файла по убыванию;
- partitions greedily раскладываются в `threads` worker buckets, чтобы тяжелые partitions не закреплялись циклически за одним worker;
- один worker bucket пишет в одну worker directory;
- task читает только `rankChunk` и `outDegreeChunk` своего source chunk;
- messages пишутся в worker directory.

Фаза 2, gather:

- каждый worker пишет `manifest.txt` со списком только реально созданных destination partitions;
- gather строит index из manifest-файлов;
- `rank_next.bin` сначала sequentially заполняется base value для всех partitions;
- task создается только для destination partitions, реально присутствующих в manifest index;
- touched destination partitions сортируются по суммарным message bytes по убыванию;
- task открывает свой `DiskDoubleArray` для `rank_next.bin` и выделяет только `nextChunk` для своего destination chunk;
- `Files.exists` для всех `workers * partitions` не выполняется.

Фаза 3, convergence:

- `rank_current.bin` и `rank_next.bin` читаются chunk-ами;
- считается `l1Diff` и `rankSum`;
- если `abs(rankSum - 1.0) > 1e-6`, пишется warning;
- rank files меняются местами через rename;
- messages удаляются после итерации, если `--keep-messages false`.

## Output

По умолчанию output потоковый:

```csv
vertex,rank
100,0.3508771929824562
1000000,0.6491228070175439
```

Порядок строк соответствует dense id order, то есть ascending original id для текущего reindexing. Печатаются original ids из `vertices.bin`.

`--top-k K` держит в памяти только min-heap размера K и выводит:

```text
rank DESC, originalId ASC
```

## Memory Model

Heap:

```text
O(chunkSize * threads + IO buffers + topK + small metadata)
```

Практически heap ближе к:

```text
O(chunkSize * activeTasks)
```

но с заметным constant factor: одновременно существуют Java arrays (`double[]`, `int[]`), heap `ByteBuffer` для binary IO, chunks внешней сортировки и небольшая metadata. Для `-Xmx128m` разумно начинать с:

```text
--chunk-size 10000..100000
```

Для большего heap `chunk-size` можно увеличивать. При явно рискованной комбинации `chunk-size`, `threads` и `Runtime.maxMemory()` приложение пишет conservative warning, но не останавливает запуск.

External sort chunks имеют internal caps отдельно от PageRank `chunk-size`:

```text
int sort chunk <= 500000 ids
endpoint record sort chunk <= 250000 records
```

Runtime warning остается advisory и отдельно показывает `pagerankChunkEstimate`, `intSortChunkEstimate`, `recordSortChunkEstimate` и `maxHeap`; это не полноценный memory planner.

В heap не хранятся:

- все edges;
- adjacency list;
- `HashMap` для всех вершин;
- `double[] rank` для всех V;
- `double[] nextRank` для всех V;
- `int[] outDegree` для всех V.

Disk:

```text
persistent: O(E + V)
preprocessing temporary files: O(E + V)
messages per iteration: O(E)
```

Preprocessing может требовать несколько размеров edge storage во временном диске:

```text
raw_edges.bin              O(E)
vertex_ids_unsorted.bin    O(E)
endpoint_refs.bin          O(E)
endpoint_refs.sorted.bin   O(E)
endpoint_assignments.bin   O(E)
endpoint_assignments.sorted.bin O(E)
sort runs                  O(E + V)
edges_by_source            O(E)
```

Message IO остается `O(E)` на итерацию. Manifest уменьшает metadata overhead и лишние проверки файлов, но не меняет асимптотику. Message record больше edge record:

```text
edge record:    int,int        = 8 bytes
message record: int,double     = 12 bytes
```

## Гипер-узлы

Гипер-узел не разворачивается в adjacency list. Его ребра остаются последовательными records в source partition. Scatter читает partition потоково, а scheduling запускает более тяжелые partitions раньше.

Если один source chunk содержит огромный гипер-узел, эта partition все равно может быть тяжелой. Текущая реализация балансирует partitions, но не делит один source partition на subranges.

## Детерминизм

- dense ids назначаются в порядке ascending original id;
- source partitions определяются как `denseFrom / chunkSize`;
- destination partitions определяются как `denseTo / chunkSize`;
- manifests пишутся sorted by partition id;
- gather читает worker directories и manifest entries в стабильном порядке;
- output deterministic для одинакового input/config.

Обычные ограничения floating-point arithmetic остаются.

## CLI

Сборка и тесты:

```bash
./gradlew test
./gradlew jar
```

Docker build использует Gradle wrapper из репозитория:

```bash
docker build -t largegraph-pagerank .
```

Запуск:

```bash
java -Xmx128m -jar build/libs/largegraph-pagerank-0.1.0.jar \
  --input data/edges.csv \
  --output output/pagerank.csv \
  --workdir work \
  --chunk-size 100000 \
  --threads 8 \
  --max-iterations 200
```

Полный пример:

```bash
java -Xmx128m -jar build/libs/largegraph-pagerank-0.1.0.jar \
  --input data/edges.csv \
  --output output/pagerank.csv \
  --workdir work \
  --chunk-size 1000000 \
  --threads 8 \
  --damping 0.85 \
  --max-iterations 200 \
  --epsilon 1e-8 \
  --id-mode contiguous \
  --top-k 0 \
  --keep-messages false
```

`--id-mode contiguous` сохранен как CLI-совместимое имя, но preprocessing теперь делает external dense reindexing observed ids.

## Synthetic Graph

```bash
./gradlew jar

java -cp build/libs/largegraph-pagerank-0.1.0.jar \
  org.example.largegraph.tools.SyntheticGraphGenerator \
  --vertices 100000 \
  --edges 1000000 \
  --output data/synthetic.csv \
  --seed 42 \
  --hyper-node true
```

```bash
java -Xmx128m -jar build/libs/largegraph-pagerank-0.1.0.jar \
  --input data/synthetic.csv \
  --output output/synthetic-pagerank.csv \
  --workdir work-synthetic \
  --chunk-size 100000 \
  --threads 8 \
  --max-iterations 200
```

## Ограничения

- external sort implementation простой: chunk sort + k-way merge;
- hot-path sort для endpoint refs/assignments использует primitive arrays, generic record sorter оставлен как fallback;
- preprocessing делает несколько disk passes;
- messages занимают `O(E)` временного места на каждой итерации: на каждое ребро пишется record `int denseTo,double contribution` при наличии исходящей степени;
- число unique observed vertices должно помещаться в `int denseId`;
- `partitionCount = ceil(V / chunkSize)` должен помещаться в `int`;
- preprocessing может требовать несколько размеров исходного edge storage во временном диске;
- compression/checkpoint/resume пока нет;
- `DiskDoubleArray` и `DiskIntArray` используют synchronized positional writes внутри одного handle; parallel gather открывает independent handles для непересекающихся destination ranges;
- один сверхтяжелый source partition пока не дробится внутри partition.
