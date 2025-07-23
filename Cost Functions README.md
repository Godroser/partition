# Table 2: Cost Function of Operators

The execution overhead of operators on different store is calculated using different Factor parameters.

| Operator        | Cost Function |
|-----------------|---------------|
| **TableScan**   | `rows * log2(rowSize) * scanFactor + c * log2(rowSize) * scanFactor` |
| **IndexScan**   | `rows * log2(rowSize) * scanFactor` |
| **Selection**   | `row * cpufactor * numFuncs` |
| **Update & Insert** | `rows * rowSize * netFactor` |
| **Sort**        | `rows * log2(rows) * len(sort-items) * cpu-factor + memQuota * memFactor + rows * rowSize * diskFactor` |
| **HashAgg**     | `rows * len(aggFuncs) * cpuFactor + rows * numFuncs * cpuFactor + (buildRows * nKeys * cpuFactor + buildRows * buildRowSize * memFactor + buildRows * cpuFactor + probeRows * nKeys * cpuFactor + probeRows * cpuFactor) / concurrency` |
| **SortMergeJoin** | `leftRows * leftRowSize * cpuFactor + rightRows * rightRowSize * cpuFactor + leftrows*numFuncs*cpuFactor + rightrows*numFuncs*cpuFactor` |
| **HashJoin**    | `buildRows * buildFilters * cpuFactor + buildRows * nKeys * cpuFactor + buildRows * buildRowSize * memFactor + buildRows * cpuFactor + (probeRows * probeFilters * cpuFactor + probeRows * nKeys * cpuFactor + probeRows * probeRowSize * memFactor + probeRows * cpuFactor) / concurrency` |

## Symbol Explanation

| Symbol          | Description                                                                 |
|-----------------|-----------------------------------------------------------------------------|
| `rows`          | Number of rows processed by the operator                                    |
| `rowSize`       | Average size (in bytes) of a single row                                     |
| `c`             | Constant representing fixed overhead in TableScan                           |
| `scanFactor`    | Hardware-specific coefficient for scanning operations                      |
| `cpuFactor`     | Hardware-specific coefficient for CPU computation                          |
| `memFactor`     | Hardware-specific coefficient for memory access                            |
| `diskFactor`    | Hardware-specific coefficient for disk I/O                                 |
| `netFactor`     | Hardware-specific coefficient for network transmission                     |
| `numFuncs`      | Number of predicate functions in Selection                                  |
| `len(sort-items)` | Number of columns in ORDER BY clause                                     |
| `memQuota`      | Memory quota allocated for the operation                                   |
| `aggFuncs`      | Number of aggregation functions in HashAgg                                 |
| `nKeys`         | Number of join/group keys                                                  |
| `buildRows`     | Number of rows in the build phase (HashJoin/HashAgg)                       |
| `probeRows`     | Number of rows in the probe phase (HashJoin/HashAgg)                       |
| `concurrency`   | Parallelism degree for distributed execution                              |
| `leftRows`      | Number of rows from the left input relation (SortMergeJoin)                |
| `rightRows`     | Number of rows from the right input relation (SortMergeJoin)               |

### Notes:

1. All `*Factor` parameters represent hardware-calibrated coefficients
2. Logarithmic terms (log2) account for binary search/tree traversal overhead
3. Division by `concurrency` indicates parallelizable cost components

