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

import io.trino.spi.Plugin;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.catalog.CatalogStoreFactory;

import java.util.Collections;
import java.util.Map;

public class CatalogPlugin
        implements Plugin
{
    @Override
    public Iterable<CatalogStoreFactory> getCatalogStoreFactories()
    {
        return Collections.singleton(new DbCatalogStoreFactory());
    }

    private static class DbCatalogStoreFactory
            implements CatalogStoreFactory
    {
        @Override
        public String getName()
        {
            return "ashhar";
        }

        @Override
        public CatalogStore create(Map<String, String> config)
        {
            return new DbStore();
        }
    }
}
