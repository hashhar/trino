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

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestDistributedQueries;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestBigQueryDistributedQueries
        extends AbstractTestDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BigQueryQueryRunner.createQueryRunner(ImmutableMap.of());
    }

    @Override
    protected boolean supportsDelete()
    {
        return false;
    }

    @Override
    protected boolean supportsArrays()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnTable()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnColumn()
    {
        return false;
    }

    @Override
    protected boolean supportsViews()
    {
        // TODO: remove override once view creation is supported
        return false;
    }

    @Override
    public void testCreateTable()
    {
        assertThatThrownBy(super::testCreateTable)
                .hasMessageContaining("This connector does not support creating tables");
        throw new SkipException("CREATE TABLE unsupported");
    }

    @Override
    public void testCreateTableAsSelect()
    {
        assertThatThrownBy(super::testCreateTableAsSelect)
                .hasStackTraceContaining("This connector does not support creating tables with data");
        throw new SkipException("CTAS unsupported");
    }

    @Override
    public void testRenameTable()
    {
        assertThatThrownBy(super::testRenameTable)
                .hasMessageContaining("This connector does not support creating tables with data");
        throw new SkipException("CTAS unsupported");
    }

    @Override
    public void testCommentTable()
    {
        assertThatThrownBy(super::testCommentColumn)
                .hasMessageContaining("This connector does not support creating tables");
        throw new SkipException("CREATE TABLE unsupported");
    }

    @Override
    public void testCommentColumn()
    {
        assertThatThrownBy(super::testCommentColumn)
                .hasMessageContaining("This connector does not support creating tables");
        throw new SkipException("CREATE TABLE unsupported");
    }

    @Override
    public void testRenameColumn()
    {
        assertThatThrownBy(super::testRenameColumn)
                .hasMessageContaining("This connector does not support creating tables with data");
        throw new SkipException("CTAS unsupported");
    }

    @Override
    public void testDropColumn()
    {
        assertThatThrownBy(super::testDropColumn)
                .hasMessageContaining("This connector does not support creating tables with data");
        throw new SkipException("CTAS unsupported");
    }

    @Override
    public void testAddColumn()
    {
        assertThatThrownBy(super::testAddColumn)
                .hasMessageContaining("This connector does not support creating tables with data");
        throw new SkipException("CTAS unsupported");
    }

    @Override
    public void testInsert()
    {
        assertThatThrownBy(super::testInsert)
                .hasMessageContaining("This connector does not support creating tables with data");
        throw new SkipException("CTAS unsupported");
    }

    @Override
    public void testInsertUnicode()
    {
        assertThatThrownBy(super::testInsertUnicode)
                .hasMessageContaining("This connector does not support creating tables");
        throw new SkipException("CREATE TABLE unsupported");
    }

    @Override
    public void testInsertArray()
    {
        assertThatThrownBy(super::testInsertArray)
                .hasMessageContaining("This connector does not support creating tables");
        throw new SkipException("CREATE TABLE unsupported");
    }

    @Override
    public void testDelete()
    {
        assertThatThrownBy(super::testDelete)
                .hasMessageContaining("This connector does not support creating tables with data");
        throw new SkipException("CTAS unsupported");
    }

    @Override
    public void testQueryLoggingCount()
    {
        assertThatThrownBy(super::testQueryLoggingCount)
                .hasMessageContaining("This connector does not support creating tables with data");
        throw new SkipException("CTAS unsupported");
    }

    @Override
    public void testSymbolAliasing()
    {
        assertThatThrownBy(super::testSymbolAliasing)
                .hasMessageContaining("This connector does not support creating tables with data");
        throw new SkipException("CTAS unsupported");
    }

    @Override
    @Test
    public void testWrittenStats()
    {
        assertThatThrownBy(super::testWrittenStats)
                .hasMessageContaining("This connector does not support creating tables with data");
        throw new SkipException("CTAS unsupported");
    }

    @Override
    public void testCreateSchema()
    {
        assertThatThrownBy(super::testCreateSchema)
                .hasMessageContaining("This connector does not support creating schemas");
        throw new SkipException("CREATE SCHEMA unsupported");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("BigQuery does not support column default values");
    }

    @Override
    public void testColumnName(String columnName)
    {
        assertThatThrownBy(() -> super.testColumnName(columnName))
                .hasMessageContaining("This connector does not support creating tables");
        throw new SkipException("CREATE TABLE unsupported");
    }

    @Override
    public void testDataMappingSmokeTest(DataMappingTestSetup dataMappingTestSetup)
    {
        assertThatThrownBy(() -> super.testDataMappingSmokeTest(dataMappingTestSetup))
                .hasMessageContaining("This connector does not support creating tables with data");
        throw new SkipException("CTAS unsupported");
    }

    @Override
    public void testShowColumns()
    {
        // Overridden because we map BigQuery INTEGER to BIGINT
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedUnparametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                // shippriority shows up as BIGINT because INTEGER is mapped to BIGINT
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar", "", "")
                .build();

        assertEquals(actual, expectedUnparametrizedVarchar);
    }

    @Override
    public void testScalarSubquery()
    {
        // TODO: fails because the query
        //  SELECT * FROM lineitem WHERE orderkey = (SELECT max(orderkey) FROM orders)
        //  returns an empty result set
        throw new SkipException("scalar subqueries not fully supported");
    }

    @Override
    public void testCorrelatedJoin()
    {
        // TODO: fails because the query
        //  SELECT * FROM region, LATERAL (SELECT * FROM nation WHERE nation.regionkey = region.regionkey)
        //  returns an empty result set
        throw new SkipException("correlated joins not fully supported");
    }

    // BigQuery specific tests should normally go in TestBigQueryIntegrationSmokeTest
}
