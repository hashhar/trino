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
package io.trino.plugin.catalog.db;

import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorName;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class DbStore
        implements CatalogStore
{
    private static final CatalogStore.StoredCatalog STORED_CATALOG = new CatalogStore.StoredCatalog()
    {
        @Override
        public CatalogName name()
        {
            return new CatalogName("ashhar");
        }

        @Override
        public CatalogProperties loadProperties()
        {
            return new CatalogProperties(CatalogHandle.createRootCatalogHandle(new CatalogName("ashhar"), new CatalogHandle.CatalogVersion("foo")), new ConnectorName("ashhar"), Map.of());
        }
    };

    @Override
    public Collection<StoredCatalog> getCatalogs()
    {
        return List.of(STORED_CATALOG);
    }

    @Override
    public CatalogProperties createCatalogProperties(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addOrReplaceCatalog(CatalogProperties catalogProperties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeCatalog(CatalogName catalogName)
    {
        throw new UnsupportedOperationException();
    }
}
