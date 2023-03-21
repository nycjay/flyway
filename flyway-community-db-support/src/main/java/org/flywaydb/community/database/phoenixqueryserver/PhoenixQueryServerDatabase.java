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

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;

public class PhoenixQueryServerDatabase extends Database<PhoenixQueryServerConnection> {

    public PhoenixQueryServerDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    protected String doGetCatalog() throws SQLException {
        return super.doGetCatalog();
    }

    @Override
    protected PhoenixQueryServerConnection doGetConnection(Connection connection) {
        return new PhoenixQueryServerConnection(this, connection);
    }

    @Override
    protected String doGetCurrentUser() throws SQLException {
        String userName = null;
        try {
            userName = getMainConnection().getJdbcTemplate().getConnection().getMetaData().getUserName();
        } catch (SQLException e) {
            //A username is not required for PQS
        }
        return userName;
    }

    @Override
    public void ensureSupported() {
        // Always the latest Phoenix Query Server version.
    }

    @Override
    public boolean supportsDdlTransactions() {
        return false;
    }

    @Override
    public String getBooleanTrue() {
        return "TRUE";
    }

    @Override
    public String getBooleanFalse() {
        return "FALSE";
    }

    @Override
    public boolean catalogIsSchema() {
        return false;
    }

    @Override
    public boolean supportsMultiStatementTransactions() {
        return false;
    }

    @Override
    public boolean useSingleConnection() {
        return true;
    }

    @Override
    public String doQuote(String identifier) {
        return getOpenQuote() + StringUtils.replaceAll(identifier, getCloseQuote(), getEscapedQuote()) + getCloseQuote();
    }

    @Override
    protected String getOpenQuote() {
        return "\"";
    }

    @Override
    protected String getCloseQuote() {
        return "\"";
    }

    @Override
    public String getEscapedQuote() {
        return "\"\"";
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline) {
        String sql = "CREATE TABLE IF NOT EXISTS " + table + " (\n" +
                "    \"installed_rank\" INTEGER NOT NULL PRIMARY KEY,\n" +
                "    \"version\" VARCHAR(50),\n" +
                "    \"description\" VARCHAR(200),\n" +
                "    \"type\" VARCHAR(20),\n" +
                "    \"script\" VARCHAR(1000),\n" +
                "    \"checksum\" INTEGER,\n" +
                "    \"installed_by\" VARCHAR(100),\n" +
                "    \"installed_on\" TIMESTAMP,\n" +
                "    \"execution_time\" INTEGER,\n" +
                "    \"success\" BOOLEAN\n" +
                ");\n" +
                (baseline ? getBaselineStatement(table) + ";\n" : "");
        return sql;
    }

    @Override
    public String getInsertStatement(Table table) {
        // Explicitly set installed_on to CURRENT_TIMESTAMP().
        return "UPSERT INTO " + table
                + " (" + quote("installed_rank")
                + ", " + quote("version")
                + ", " + quote("description")
                + ", " + quote("type")
                + ", " + quote("script")
                + ", " + quote("checksum")
                + ", " + quote("installed_by")
                + ", " + quote("installed_on")
                + ", " + quote("execution_time")
                + ", " + quote("success")
                + ")"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIME(), ?, ?)";
    }
}