/*
 * Copyright (C) Red Gate Software Ltd 2010-2022
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.flywaydb.community.database.databricks;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.jdbc.Result;
import org.flywaydb.core.internal.jdbc.Results;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.SQLException;
import java.util.List;

public class DatabricksConnection extends Connection<DatabricksDatabase> {

    private static final Log LOG = LogFactory.getLog(DatabricksConnection.class);
    public static String DEFAULT_SCHEMA = "default";
    public static String DEFAULT_CATALOG = "hive_metastore";
    protected DatabricksConnection(DatabricksDatabase database, java.sql.Connection connection) {
        super(database, connection);
        String catalogNameFromConfig = database.getConfiguration().getDefaultCatalog();
        String currentCatalogName = (catalogNameFromConfig != null) ? catalogNameFromConfig : DEFAULT_CATALOG;
        if (DEFAULT_CATALOG.equals(currentCatalogName)) {
            LOG.info("The defaultCatalog was not specified in the config, so calling back to the default catalog, " + DEFAULT_CATALOG);
        }
        setCatalog(currentCatalogName);
    }

    /**
     * Until the Databricks driver supports setting the catalog as part of the URL,
     * let's manually set it here on the connection. This is not ideal, but it works well enough, as long as you only
     * work with one catalog for your environment.
     * @param catalog
     */
    protected void setCatalog(String catalog) {
        LOG.info("=====Setting the current catalog to " + catalog);
        try {
            //jdbcTemplate.getConnection().setCatalog(catalogName);
            String sql = "USE CATALOG " + database.doQuote(catalog) + ";";
            jdbcTemplate.execute(sql);
            sql = "select current_catalog();";
            Results results = jdbcTemplate.executeStatement(sql);
            for (Result result : results.getResults()) {
                for (List<String> row : result.getData()) {
                    String catalogName = row.get(0);
                    LOG.debug(String.format("The catalog is set to: %s", catalogName));
                }
            }
        } catch (SQLException e) {
            LOG.error("Something went wrong when trying to set the Databricks catalog to " + catalog +
                    ", so using the default catalog " + DEFAULT_CATALOG + " instead.");
        }
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        String currentSchema = jdbcTemplate.queryForString("SELECT current_database();");
        return (currentSchema != null) ? currentSchema : DEFAULT_SCHEMA;
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema) throws SQLException {
        String sql = "USE SCHEMA " + database.doQuote(schema) + ";";
        jdbcTemplate.execute(sql);
    }

    @Override
    public Schema doGetCurrentSchema() throws SQLException {
        String currentSchema = getCurrentSchemaNameOrSearchPath();
        if (!StringUtils.hasText(currentSchema)) {
            throw new FlywayException("Unable to determine current schema as currentSchema is empty.");
        }
        return getSchema(currentSchema);
    }

    @Override
    public Schema getSchema(String name) {
        return new DatabricksSchema(jdbcTemplate, database, name);
    }
}
