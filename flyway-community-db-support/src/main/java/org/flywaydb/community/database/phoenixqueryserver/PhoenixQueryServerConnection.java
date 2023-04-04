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

package org.flywaydb.community.database.phoenixqueryserver;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.SQLException;

public class PhoenixQueryServerConnection extends Connection<PhoenixQueryServerDatabase> {
    private static final Log LOG = LogFactory.getLog(PhoenixQueryServerConnection.class);

    public static String DEFAULT_SCHEMA = "DEFAULT";
    protected PhoenixQueryServerConnection(PhoenixQueryServerDatabase database, java.sql.Connection connection) {
        super(database, connection);
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
       return DEFAULT_SCHEMA;
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema) throws SQLException {
        LOG.info("Phoenix does not support setting the schema. Default schema NOT changed to " + schema);
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
        return new PhoenixQueryServerSchema(jdbcTemplate, database, name);
    }
}