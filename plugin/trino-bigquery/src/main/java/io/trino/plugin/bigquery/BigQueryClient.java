/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition.Type;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.http.BaseHttpServiceException;
import com.google.common.base.CharMatcher;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.StreamSupport;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_OBJECT_NOT_FOUND;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;

/**
 * Every schema/dataset or table being passed into BigQueryClient is assumed to be normalised and we try to find it's remote name (via cache or lookup).
 * Every schema/dataset or table being returned from BigQueryClient is normalised to lowercase and the callers shouldn't care since Trino doesn't care.
 * BigQueryClient wraps all BigQuery API calls and hence it will make adjustments where needed.
 */
class BigQueryClient
{
    private final BigQuery bigQuery;
    private final Optional<String> viewMaterializationProject;
    private final Optional<String> viewMaterializationDataset;
    private final ConcurrentMap<TableId, TableId> tableIds = new ConcurrentHashMap<>();
    private final ConcurrentMap<DatasetId, DatasetId> datasetIds = new ConcurrentHashMap<>();
    private final boolean caseInsensitiveNameMatching;
    private final Cache<String, String> remoteDatasetNames;
    private final Cache<String, Map<String, String>> remoteTableNames;

    BigQueryClient(BigQuery bigQuery, BigQueryConfig config)
    {
        this.bigQuery = bigQuery;
        this.viewMaterializationProject = config.getViewMaterializationProject();
        this.viewMaterializationDataset = config.getViewMaterializationDataset();
        this.caseInsensitiveNameMatching = config.isCaseInsensitiveNameMatching();

        requireNonNull(config.getCaseInsensitiveNameMatchingCacheTtl(), "caseInsensitiveNameMatchingCacheTtl is null");
        CacheBuilder<Object, Object> remoteNamesCacheBuilder = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getCaseInsensitiveNameMatchingCacheTtl().toMillis(), MILLISECONDS);

        this.remoteDatasetNames = remoteNamesCacheBuilder.build();
        this.remoteTableNames = remoteNamesCacheBuilder.build();
    }

    private List<Type> getTableTypes()
    {
        return ImmutableList.of(Type.TABLE, Type.VIEW);
    }

    String toRemoteDatasetName(String project, String dataset)
    {
        requireNonNull(project, "project is null");
        requireNonNull(dataset, "dataset is null");
        verify(CharMatcher.forPredicate(Character::isUpperCase).matchesNoneOf(dataset), "Expected dataset name from internal metadata to be lowercase: %s", dataset);
        if (!caseInsensitiveNameMatching) {
            return dataset;
        }

        try {
            String remoteDatasetName = remoteDatasetNames.getIfPresent(dataset);
            if (remoteDatasetName == null) {
                // this might be a new dataset, force reload
                remoteDatasetNames.putAll(listDatasetsByLowerCase(project));
            }
            remoteDatasetName = remoteDatasetNames.getIfPresent(dataset);
            if (remoteDatasetName != null) {
                return remoteDatasetName;
            }
        }
        catch (RuntimeException e) {
            throw new TrinoException(BIGQUERY_OBJECT_NOT_FOUND, "Failed to find remote dataset name: " + firstNonNull(e.getMessage(), e), e);
        }

        return dataset;
    }

    private Map<String, String> listDatasetsByLowerCase(String project)
    {
        Iterable<Dataset> datasets = bigQuery.listDatasets(project).iterateAll();
        return StreamSupport.stream(datasets.spliterator(), false)
                // will throw on collision to avoid ambiguity
                .collect(toImmutableMap(dataset -> dataset.getDatasetId().getDataset().toLowerCase(ENGLISH), dataset -> dataset.getDatasetId().getDataset()));
    }

    String toRemoteTableName(String remoteDataset, String table)
    {
        requireNonNull(remoteDataset, "remoteDataset is null");
        requireNonNull(table, "table is null");
        verify(CharMatcher.forPredicate(Character::isUpperCase).matchesNoneOf(table), "Expected table name from internal metadata to be lowercase: %s", table);
        if (!caseInsensitiveNameMatching) {
            return table;
        }

        try {
            Map<String, String> mapping = remoteTableNames.getIfPresent(remoteDataset);
            if (mapping != null && !mapping.containsKey(table)) {
                // this might be a new table, force reload
                mapping = null;
            }
            if (mapping == null) {
                mapping = listTablesByLowerCase(remoteDataset);
                remoteTableNames.put(remoteDataset, mapping);
            }
            String remoteTableName = mapping.get(table);
            if (remoteTableName != null) {
                return remoteTableName;
            }
        }
        catch (RuntimeException e) {
            throw new TrinoException(BIGQUERY_OBJECT_NOT_FOUND, "Failed to find remote table name: " + firstNonNull(e.getMessage(), e), e);
        }

        return table;
    }

    private Map<String, String> listTablesByLowerCase(String remoteDataset)
    {
        Iterable<Table> tables = bigQuery.listTables(remoteDataset).iterateAll();
        return StreamSupport.stream(tables.spliterator(), false)
                .filter(table -> getTableTypes().contains(table.getDefinition().getType()))
                // will throw on collision to avoid ambiguity
                .collect(toImmutableMap(table -> table.getTableId().getTable().toLowerCase(ENGLISH), table -> table.getTableId().getTable()));
    }

    String getProjectId()
    {
        return bigQuery.getOptions().getProjectId();
    }

    List<String> listDatasets(String project)
    {
        return listDatasetsByLowerCase(project).keySet().stream()
                .collect(toImmutableList());
    }

    List<String> listTables(String project, String dataset)
    {
        DatasetId remoteDatasetId = DatasetId.of(project, toRemoteDatasetName(project, dataset));
        Iterable<Table> allTables = bigQuery.listTables(remoteDatasetId).iterateAll();
        return StreamSupport.stream(allTables.spliterator(), false)
                .map(table -> table.getTableId().getTable())
                .collect(toImmutableList());
    }

    TableInfo getTable(String project, String dataset, String table)
    {
        return bigQuery.getTable(TableId.of(project, dataset, table));
    }

    DatasetId toDatasetId(TableId tableId)
    {
        return DatasetId.of(tableId.getProject(), tableId.getDataset());
    }

    TableId createDestinationTable(TableId tableId)
    {
        String project = viewMaterializationProject.orElse(tableId.getProject());
        String dataset = viewMaterializationDataset.orElse(tableId.getDataset());
        DatasetId datasetId = mapIfNeeded(project, dataset);
        String name = format("_pbc_%s", randomUUID().toString().toLowerCase(ENGLISH).replace("-", ""));
        return TableId.of(datasetId.getProject(), datasetId.getDataset(), name);
    }

    private DatasetId mapIfNeeded(String project, String dataset)
    {
        DatasetId datasetId = DatasetId.of(project, dataset);
        return datasetIds.getOrDefault(datasetId, datasetId);
    }

    Table update(TableInfo table)
    {
        return bigQuery.update(table);
    }

    Job create(JobInfo jobInfo)
    {
        return bigQuery.create(jobInfo);
    }

    TableResult query(String sql)
    {
        try {
            return bigQuery.query(QueryJobConfiguration.of(sql));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BigQueryException(BaseHttpServiceException.UNKNOWN_CODE, format("Failed to run the query [%s]", sql), e);
        }
    }

    String selectSql(TableId table, List<String> requiredColumns)
    {
        String columns = requiredColumns.isEmpty() ? "*" :
                requiredColumns.stream().map(column -> format("`%s`", column)).collect(joining(","));

        return selectSql(table, columns);
    }

    // assuming the SELECT part is properly formatted, can be used to call functions such as COUNT and SUM
    String selectSql(TableId table, String formattedColumns)
    {
        String tableName = fullTableName(table);
        return format("SELECT %s FROM `%s`", formattedColumns, tableName);
    }

    private String fullTableName(TableId tableId)
    {
        tableId = tableIds.getOrDefault(tableId, tableId);
        return format("%s.%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
    }
}
