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
import com.google.cloud.bigquery.Schema;
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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.TableNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
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
 * BigQueryClient wraps all BigQuery API calls.
 *
 * Private methods expect the remote dataset/table names as input.
 * Public/package-private methods expect the input is lowercase and will try to lookup remote dataset/table names.
 * Public/package-private methods return lowercase output since ConnectorMetadata doesn't care about the case.
 */
class BigQueryClient
{
    private final BigQuery bigQuery;
    private final Optional<String> viewMaterializationProject;
    private final Optional<String> viewMaterializationDataset;
    private final boolean caseInsensitiveNameMatching;
    // lowercase dataset name mapped to remote dataset name
    private final Cache<String, String> remoteDatasetNames;
    // remote dataset name mapped to map of lowercase table name to remote table name
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

    private String toRemoteDatasetName(String projectId, String dataset)
    {
        requireNonNull(projectId, "projectId is null");
        requireNonNull(dataset, "dataset is null");
        verify(CharMatcher.forPredicate(Character::isUpperCase).matchesNoneOf(dataset), "Expected dataset name from internal metadata to be lowercase: %s", dataset);
        if (!caseInsensitiveNameMatching) {
            return dataset;
        }

        try {
            String remoteDatasetName = remoteDatasetNames.getIfPresent(dataset);
            if (remoteDatasetName == null) {
                // this might be a new dataset, force reload
                remoteDatasetNames.putAll(listDatasetsByLowerCase(projectId));
            }
            remoteDatasetName = remoteDatasetNames.getIfPresent(dataset);
            if (remoteDatasetName != null) {
                return remoteDatasetName;
            }
            // TODO: should we fail if remoteDatasetName == null since this means that the dataset doesn't exist on the remote?
        }
        catch (RuntimeException e) {
            throw new TrinoException(BIGQUERY_OBJECT_NOT_FOUND, "Failed to find remote dataset name: " + firstNonNull(e.getMessage(), e), e);
        }

        return dataset;
    }

    List<String> listDatasets(String projectId)
    {
        requireNonNull(projectId, "projectId is null");
        return listDatasetsByLowerCase(projectId).keySet().stream()
                .collect(toImmutableList());
    }

    private Map<String, String> listDatasetsByLowerCase(String projectId)
    {
        requireNonNull(projectId, "projectId is null");
        Iterable<Dataset> datasets = bigQuery.listDatasets(projectId).iterateAll();
        return StreamSupport.stream(datasets.spliterator(), false)
                // will throw on collision to avoid ambiguity
                .collect(toImmutableMap(dataset -> dataset.getDatasetId().getDataset().toLowerCase(ENGLISH), dataset -> dataset.getDatasetId().getDataset()));
    }

    private String toRemoteTableName(String projectId, String dataset, String table)
    {
        requireNonNull(projectId, "projectId is null");
        requireNonNull(dataset, "dataset is null");
        requireNonNull(table, "table is null");
        verify(CharMatcher.forPredicate(Character::isUpperCase).matchesNoneOf(table), "Expected table name from internal metadata to be lowercase: %s", table);
        if (!caseInsensitiveNameMatching) {
            return table;
        }

        try {
            String remoteDataset = toRemoteDatasetName(projectId, dataset);

            Map<String, String> mapping = remoteTableNames.getIfPresent(remoteDataset);
            if (mapping != null && !mapping.containsKey(table)) {
                // this might be a new table, force reload
                mapping = null;
            }
            if (mapping == null) {
                mapping = listTablesByLowerCase(projectId, remoteDataset);
                remoteTableNames.put(remoteDataset, mapping);
            }
            String remoteTableName = mapping.get(table);
            if (remoteTableName != null) {
                return remoteTableName;
            }
            // TODO: should we fail if remoteTableName == null because this means that the table doesn't exist on the remote?
        }
        catch (RuntimeException e) {
            throw new TrinoException(BIGQUERY_OBJECT_NOT_FOUND, "Failed to find remote table name: " + firstNonNull(e.getMessage(), e), e);
        }

        return table;
    }

    List<String> listTables(String projectId, String dataset)
    {
        requireNonNull(projectId, "projectId is null");
        requireNonNull(dataset, "dataset is null");
        return listTablesByLowerCase(projectId, toRemoteDatasetName(projectId, dataset)).keySet().stream()
                .collect(toImmutableList());
    }

    private Map<String, String> listTablesByLowerCase(String projectId, String remoteDataset)
    {
        requireNonNull(projectId, "projectId is null");
        requireNonNull(remoteDataset, "remoteDataset is null");
        Iterable<Table> tables = bigQuery.listTables(DatasetId.of(projectId, remoteDataset)).iterateAll();
        return StreamSupport.stream(tables.spliterator(), false)
                .filter(table -> getTableTypes().contains(table.getDefinition().getType()))
                // will throw on collision to avoid ambiguity
                .collect(toImmutableMap(table -> table.getTableId().getTable().toLowerCase(ENGLISH), table -> table.getTableId().getTable()));
    }

    Optional<BigQueryTableHandle> getTableHandle(String projectId, String dataset, String table)
    {
        requireNonNull(projectId, "projectId is null");
        requireNonNull(dataset, "dataset is null");
        requireNonNull(table, "table is null");

        return Optional.ofNullable(bigQuery.getTable(TableId.of(projectId, toRemoteDatasetName(projectId, dataset), toRemoteTableName(projectId, dataset, table))))
                .map(BigQueryTableHandle::from);
    }

    List<BigQueryColumnHandle> getColumns(BigQueryTableHandle tableHandle)
    {
        String remoteDatasetName = toRemoteDatasetName(tableHandle.getProjectId(), tableHandle.getSchemaName());
        String remoteTableName = toRemoteTableName(tableHandle.getProjectId(), tableHandle.getSchemaName(), tableHandle.getTableName());
        TableInfo tableInfo = bigQuery.getTable(TableId.of(tableHandle.getProjectId(), remoteDatasetName, remoteTableName));
        if (tableInfo == null) {
            throw new TableNotFoundException(
                    tableHandle.getSchemaTableName(),
                    format("Table '%s' not found", tableHandle.getSchemaTableName()));
        }

        Schema schema = tableInfo
                .getDefinition()
                .getSchema();
        if (schema == null) {
            return ImmutableList.of();
        }

        return schema.getFields()
                .stream()
                .map(Conversions::toColumnHandle)
                .collect(toImmutableList());
    }

    // Methods that take input as BigQuery API models

    Optional<TableInfo> getTable(TableId tableId)
    {
        requireNonNull(tableId, "tableId is null");
        String remoteDataset = toRemoteDatasetName(tableId.getProject(), tableId.getDataset());
        String remoteTable = toRemoteTableName(tableId.getProject(), tableId.getDataset(), tableId.getTable());
        return Optional.ofNullable(bigQuery.getTable(TableId.of(tableId.getProject(), remoteDataset, remoteTable)));
    }

    TableId createDestinationTableId(TableId tableId)
    {
        requireNonNull(tableId, "tableId is null");
        String destinationProjectId = viewMaterializationProject.orElse(tableId.getProject());
        String destinationDataset = viewMaterializationDataset.orElse(toRemoteDatasetName(tableId.getProject(), tableId.getDataset()));
        String name = format("_pbc_%s", randomUUID().toString().toLowerCase(ENGLISH).replace("-", ""));
        return TableId.of(destinationProjectId, destinationDataset, name);
    }

    Table update(TableInfo table)
    {
        // TODO: why is this needed? Only used by ReadSessionCreator.
        // TODO: ReadSessionCreator calls this to mark an existing table with an expiration, let's rename this and
        //  change impl to expire the provided table name by fetching and then creating a builder and setting expiry on the builder
        // TODO: we want to avoid inputs as BigQuery API models
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

    String selectSql(TableId tableId, List<String> requiredColumns)
    {
        requireNonNull(tableId, "tableId is null");

        String columns = requiredColumns.isEmpty() ? "*" :
                // TODO: add constant/method for quoting column/object names
                requiredColumns.stream().map(column -> format("`%s`", column)).collect(joining(","));

        return selectSql(tableId, columns);
    }

    // assuming the SELECT part is properly formatted, can be used to call functions such as COUNT and SUM
    String selectSql(TableId tableId, String formattedColumns)
    {
        requireNonNull(tableId, "tableId is null");

        String remoteDatasetName = toRemoteDatasetName(tableId.getProject(), tableId.getDataset());
        String remoteTableName = toRemoteTableName(tableId.getProject(), tableId.getDataset(), tableId.getTable());
        String fullyQualifiedTableName = fullyQualifiedTableName(tableId.getProject(), remoteDatasetName, remoteTableName);
        return format("SELECT %s FROM `%s`", formattedColumns, fullyQualifiedTableName);
    }

    private String fullyQualifiedTableName(String projectId, String dataset, String table)
    {
        // TODO: add proper quoting to handle special characters and case
        return format("%s.%s.%s", projectId, dataset, table);
    }

    String getProjectId()
    {
        return requireNonNull(bigQuery.getOptions().getProjectId(), "projectId is null");
    }
}
