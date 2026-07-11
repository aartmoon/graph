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
- arbitrary `int32` original ids;
- external dense reindexing в диапазон `[0..V)`;
- PageRank считает по dense ids, output пишет original ids;
- source-partitioned edges;
- X-Stream-style scatter/gather;
- disk-backed `DiskDoubleArray` и `DiskIntArray`;
- preprocessing без random disk increment на каждое исходное ребро;
- scatter передает список созданных message-файлов напрямую в gather;
- message buckets покрывают непрерывные диапазоны destination partitions;
- балансировка scatter/gather через очередь задач с тяжелыми slices/buckets первыми;
- атомарная запись полного потокового CSV output.

Реализация остается специализированной под PageRank, а не превращается в общий graph framework.

## Рассмотренные варианты

1. In-memory CSR / adjacency list. Самый быстрый вариант для умеренных графов, но отклонен: хранит adjacency и rank/outDegree массивы в RAM и плохо подходит под условие, где граф не помещается в heap.
2. GraphChi-style sliding shards. Хороший external-memory вариант с сильной locality, но сложнее в реализации: нужно строить shards, аккуратно вести updates и поддерживать more involved scheduler.
3. X-Stream-style scatter/gather + source partitions. Выбранный вариант: простой, streaming-friendly, устойчив к sparse ids и гипер-узлам, хорошо ложится на PageRank и держит heap bounded by chunk sizes и числом активных tasks.

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
2. Пишется `endpoint_refs.bin`: `originalId, edgeId, side`.
3. Endpoint references сортируются по `originalId`.
4. Один последовательный проход назначает dense ids, пишет `vertices.bin` и `endpoint_assignments.bin`.
5. Endpoint assignments сортируются по `(edgeId, side)`.
6. Dense edges пишутся во временный stream.
7. Dense edges сортируются по `(denseFrom, denseTo)` и deduplicate-ятся.
8. Unique dense edges одним проходом пишутся в `edges_by_source/src-part-xxxxx.bin` и `out_degree.bin`.
9. После каждого успешного этапа уже ненужные preprocessing intermediates удаляются.
10. `rank_current.bin` и `rank_next.bin` создаются после построения graph storage.

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
- отрицательные `int32` ids разрешены и получают обычные dense ids.

## Storage Layout

Рабочая директория:

```text
work/
  vertices.bin

  vertex/
    out_degree.bin
    rank_current.bin
    rank_next.bin

  edges_by_source/
    src-part-00000.bin
    src-part-00001.bin
    ...

  messages/
    iter-000001/
      worker-00000/
        msg-bucket-00000.bin
        msg-bucket-00007.bin
```

Форматы:

```text
vertices.bin:               int originalId
src-part-xxxxx.bin:         int denseFrom, int denseTo
message bucket file:        int denseTo, double contribution
rank files:                 double rank[denseId]
out_degree.bin:             int outDegree[denseId]
```

Во время preprocessing также временно создаются `endpoint_refs*.bin`,
`endpoint_assignments*.bin`, `dense_edges*.bin` и `sort/`. Они удаляются
поэтапно сразу после того, как следующий этап успешно создал свой output.

## Out-Degree

После dense edge sort ребра идут по source order. Для каждой partition:

```text
sourceStart = partition * chunkSize
sourceLength = min(chunkSize, vertexCount - sourceStart)
```

Загружается только `int[] outDegreeChunk`; при записи source partition одновременно считается out-degree и затем chunk записывается в `out_degree.bin`.

## Итерация PageRank

Фаза 0, dangling mass:

- `rank_current.bin` и `out_degree.bin` читаются chunk-ами;
- chunks обрабатываются параллельно worker ranges и затем reduce-ятся в общий dangling mass;
- полный `rank[]` или `outDegree[]` в heap не загружается.

Фаза 1, scatter:

- source partitions сортируются по размеру файла по убыванию;
- размеры source partitions передаются из preprocessing result без transient metadata-файла;
- большие source partitions режутся на bounded byte-aligned edge slices;
- slices greedily раскладываются в `threads` worker buckets, чтобы тяжелая partition не закреплялась целиком за одним worker;
- один worker bucket пишет в одну worker directory;
- task читает только `rankChunk` и `outDegreeChunk` своего source chunk;
- messages пишутся в worker directory в bounded destination buckets (`<= 8192` buckets на worker), а не в отдельный файл на каждую destination partition;
- bucket соответствует непрерывному диапазону destination partitions, поэтому gather читает и пишет rank chunks локальнее.

Фаза 2, gather:

- scatter task возвращает список реально созданных message bucket files;
- gather строит bucket index из этого списка без manifest-файлов;
- task создается для каждого диапазона message bucket layout, включая buckets без messages, чтобы каждый destination chunk был перезаписан на итерации;
- buckets сортируются по суммарным message bytes по убыванию;
- task открывает свой `DiskDoubleArray` для `rank_next.bin` и держит один массив значений для диапазона bucket;
- chunk впервые инициализируется base value внутри gather task, отдельного полного прохода заполнения `rank_next.bin` нет;
- запись rank chunk идет через фиксированный небольшой IO buffer, а не через `ByteBuffer` размером со весь bucket;
- task читает соответствующие chunks из `rank_current.bin` и сразу считает локальные `l1Diff` и `rankSum`;
- если `abs(rankSum - 1.0) > 1e-6` или обнаружен non-finite/negative rank, итерация завершается ошибкой;
- rank files логически меняются местами через swap `Path` references;
- messages всегда удаляются в `finally` после итерации;
- `Files.exists` для всех `workers * partitions` не выполняется.

## Output

По умолчанию output потоковый:

```csv
vertex,rank
100,0.3508771929824562
1000000,0.6491228070175439
```

Порядок строк соответствует dense id order, то есть ascending original id для текущего reindexing. Печатаются original ids из `vertices.bin`.

## Memory Model

Heap:

```text
O(chunkSize * activeTasks + IO buffers + small metadata)
```

Практически heap ближе к:

```text
O(chunkSize * activeTasks)
```

но с заметным constant factor: одновременно существуют Java arrays (`double[]`, `int[]`), heap `ByteBuffer` для binary IO, chunks внешней сортировки и небольшая metadata. Для `-Xmx128m` разумно начинать с:

```text
--chunk-size 10000..100000
```

Для большего heap `chunk-size` можно увеличивать. Конфигурации, которые требуют impossible Java buffer sizes, отклоняются на этапе parsing. Preprocessing отдельно ограничивает размер sort chunks по heap budget. После preprocessing PageRank планирует параллелизм по heap budget: если один task не помещается, запуск отклоняется; если все запрошенные threads не помещаются, runtime уменьшает параллелизм PageRank-фаз.

External sort chunks имеют internal caps отдельно от PageRank `chunk-size`:

```text
endpoint record sort chunk <= 250000 records
```

Endpoint refs, endpoint assignments и dense edges сортируются primitive sorters. Dense edges dedup-ятся во время merge.

Точный PageRank parallelism выбирается после того, как известны `vertexCount` и bucket layout.

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
endpoint_refs.bin          O(E)
endpoint_refs.sorted.bin   O(E)
endpoint_assignments.bin   O(E)
endpoint_assignments.sorted.bin O(E)
dense_edges.bin            O(E)
dense_edges.sorted.bin     O(E)
sort runs                  O(E + V)
edges_by_source            O(E)
```

Эти preprocessing temporary files не держатся до конца preprocessing, если следующий этап уже успешно завершился.

Message IO остается `O(E)` на итерацию. Message buckets уменьшают file-count и metadata overhead, но не меняют асимптотику. Message record больше edge record:

```text
edge record:    int,int        = 8 bytes
message record: int,double     = 12 bytes
```

## Гипер-узлы

Гипер-узел не разворачивается в adjacency list. Его ребра остаются последовательными records в source partition. Scatter читает partition потоково, а scheduling запускает более тяжелые partitions раньше.

Если один source chunk содержит огромный гипер-узел, scatter режет файл source partition на byte-aligned edge slices и может распределить slices между worker buckets. Размер slice задается `--scatter-slice-mb` с default `16`. Внутри worker slices группируются по source partition, поэтому файл partition открывается один раз на группу и читается позиционно; adjacency list по-прежнему не создается.

## Детерминизм

- dense ids назначаются в порядке ascending original id;
- source partitions определяются как `denseFrom / chunkSize`;
- destination partitions определяются как `denseTo / chunkSize`;
- duplicate input edges схлопываются после dense rewrite, граф считается unweighted simple directed graph;
- scatter work и message bucket files агрегируются в стабильном порядке;
- output deterministic для одинакового input/config.

Обычные ограничения floating-point arithmetic остаются.

## CLI

Сборка и тесты:

```bash
./gradlew test
./gradlew jar
```

Первый запуск Gradle wrapper может требовать интернет, потому что Gradle distribution скачивается с `services.gradle.org`. Docker build также использует Gradle wrapper и требует интернет для Gradle/JUnit dependencies:

```bash
docker build -t largegraph-pagerank .
```

Fallback без Gradle/JUnit, если нужно только собрать и запустить приложение:

```bash
mkdir -p out
javac --release 21 -d out $(find src/main/java -name '*.java')

java -Xmx128m -cp out org.example.largegraph.Main \
  --input data/edges.csv \
  --output output/pagerank.csv \
  --workdir work \
  --chunk-size 100000 \
  --threads 8
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

## Benchmark Evidence

Пример smoke benchmark на synthetic graph с фиксированным seed:

| Dataset | V | input E | stored E | heap | chunk-size | threads | iterations | time | workdir | output | rankSum |
| --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| synthetic-hyper seed=42 | 50,000 | 300,000 | 299,986 | `-Xmx128m` | 10,000 | 4 | 3 | 1.03s | 3.5 MiB | 1.3 MiB | 1.000000000000001 |

`usedHeap` в логах — это Java heap, а не полный RSS процесса. Wall-clock time зависит от SSD/OS cache.

Полный пример:

```bash
java -Xmx128m -jar build/libs/largegraph-pagerank-0.1.0.jar \
  --input data/edges.csv \
  --output output/pagerank.csv \
  --workdir work \
  --chunk-size 100000 \
  --threads 8 \
  --damping 0.85 \
  --max-iterations 200 \
  --epsilon 1e-8 \
  --scatter-slice-mb 16
```

External dense reindexing observed ids используется всегда.

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
- hot-path sort для endpoint refs/assignments/dense edges использует primitive arrays;
- sort run metadata хранится как список `Path` для текущего уровня merge; runs сливаются сбалансированными pass-ами с fan-in 128;
- preprocessing делает несколько disk passes;
- preprocessing intermediate files удаляются поэтапно после успешного создания следующего файла;
- messages занимают `O(E)` временного места на каждой итерации: на каждое ребро пишется record `int denseTo,double contribution` при наличии исходящей степени;
- число unique observed vertices должно помещаться в `int denseId`;
- `partitionCount = ceil(V / chunkSize)` должен помещаться в `int`;
- preprocessing во время работы может требовать несколько размеров исходного edge storage во временном диске;
- compression/checkpoint/resume пока нет;
- recursive cleanup использует streaming `walkFileTree`, без materialization всех paths в heap;
- binary readers валидируют кратность file size record size и не проглатывают partial records;
- `DiskDoubleArray` и `DiskIntArray` используют synchronized positional writes внутри одного handle; parallel gather открывает independent handles для непересекающихся destination ranges;
- очень маленький граф или один небольшой source slice не сможет загрузить все CPU независимо от `--threads`.
