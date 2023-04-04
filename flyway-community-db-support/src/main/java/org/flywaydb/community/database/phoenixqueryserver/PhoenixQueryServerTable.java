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
import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;

public class PhoenixQueryServerTable extends Table<PhoenixQueryServerDatabase, PhoenixQueryServerSchema> {
    private static final Log LOG = LogFactory.getLog(PhoenixQueryServerTable.class);

    /**
     * @param jdbcTemplate The JDBC template for communicating with the DB.
     * @param database     The database-specific support.
     * @param schema       The schema this table lives in.
     * @param name         The name of the table.
     */
    public PhoenixQueryServerTable(JdbcTemplate jdbcTemplate, PhoenixQueryServerDatabase database, PhoenixQueryServerSchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP TABLE IF EXISTS " + database.quote(schema.getName(), name) + ";");
    }

    @Override
    protected boolean doExists() throws SQLException {
        return exists(null, schema, name);
    }

    @Override
    protected void doLock() throws SQLException {
        LOG.debug("Unable to lock " + this + " as Phoenix does not support locking. No concurrent migration supported.");
    }

    @Override
    protected void doUnlock() throws SQLException {
        LOG.debug("Unable to unlock " + this + " as Phoenix does not support locking. No concurrent migration supported.");
    }

}