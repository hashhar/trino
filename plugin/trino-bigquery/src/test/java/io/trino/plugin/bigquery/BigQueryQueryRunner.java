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

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.sql.SqlExecutor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public final class BigQueryQueryRunner
{
    private static final String TPCH_SCHEMA = "tpch";

    private BigQueryQueryRunner() {}

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties, Map<String, String> connectorProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession())
                    .setExtraProperties(extraProperties)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new BigQueryPlugin());
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("bigquery.views-enabled", "true");
            queryRunner.createCatalog(
                    "bigquery",
                    "bigquery",
                    connectorProperties);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("bigquery")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static class BigQuerySqlExecutor
            implements SqlExecutor
    {
        private final BigQuery bigQuery;

        public BigQuerySqlExecutor()
        {
            this.bigQuery = createBigQueryClient();
        }

        @Override
        public void execute(String sql)
        {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql)
                    .setUseLegacySql(false)
                    .build();

            JobId jobId = JobId.of();
            Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

            try {
                queryJob = queryJob.waitFor();

                if (queryJob == null) {
                    throw new RuntimeException(format("Job with uuid %s does not longer exists", jobId.getJob()));
                }

                if (queryJob.getStatus().getError() != null) {
                    throw new RuntimeException(format("Query '%s' failed: %s", sql, queryJob.getStatus().getError()));
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        private static BigQuery createBigQueryClient()
        {
            try {
                InputStream jsonKey = new ByteArrayInputStream(Base64.getDecoder().decode(System.getProperty("bigquery.credentials-key")));
                return BigQueryOptions.newBuilder()
                        .setCredentials(ServiceAccountCredentials.fromStream(jsonKey))
                        .build()
                        .getService();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = createQueryRunner(ImmutableMap.of("http-server.http.port", "8080"), ImmutableMap.of());
        Thread.sleep(10);
        Logger log = Logger.get(BigQueryQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
