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

import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DatabricksSchema extends Schema<DatabricksDatabase, DatabricksTable> {

    /**
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param database     The database-specific support.
     * @param name         The name of the schema.
     */
    public DatabricksSchema(JdbcTemplate jdbcTemplate, DatabricksDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate
                .queryForInt("select count(table_name) from " + database.getCatalog() +".information_schema.tables where table_schema = ?;", name) > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return jdbcTemplate
                .queryForInt("select count(table_name) from " + database.getCatalog() +".information_schema.tables where table_schema = ?;", name) == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        jdbcTemplate.execute("create schema if not exists " + database.quote(name) + ";");
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("drop schema if exists " + database.quote(name) + " cascade;");
    }

    @Override
    protected void doClean() throws SQLException {
        for (String statement : generateDropStatements("MANAGED", "TABLE")) {
            jdbcTemplate.execute(statement);
        }
        for (String statement : generateDropStatements("VIEW", "VIEW")) {
            jdbcTemplate.execute(statement);
        }
        for (String statement : generateDropStatementsForRoutines("FUNCTION")) {
            jdbcTemplate.execute(statement);
        }
    }

    private List<String> generateDropStatements(String type, String objType) throws SQLException {
        List<String> names =
                jdbcTemplate.queryForStringList(
                        // Search for all views
                        "select table_name from " + database.getCatalog() +"information_schema.tables where table_schema = ? WHERE table_type = ?;",
                        name,
                        type
                );
        List<String> statements = new ArrayList<>();
        for (String domainName : names) {
            statements.add("drop " + objType + " if exists " + database.quote(name, domainName) + ";");
        }
        return statements;
    }

    private List<String> generateDropStatementsForRoutines(String objType) throws SQLException {
        List<String> objNames =
                jdbcTemplate.queryForStringList(
                        // Search for all functions
                        "select routine_name from information_schema.routines where routine_schema = ? WHERE routine_type = ?;",
                        objType
                );
        List<String> statements = new ArrayList<>();
        for (String objName : objNames) {
            statements.add("drop " + objType + " if exists " + database.quote(name, objName) + ";");
        }
        return statements;
    }

    @Override
    protected DatabricksTable[] doAllTables() throws SQLException {
        List<String> tableNames = jdbcTemplate.queryForStringList(
                "select table_name from " + database.getCatalog() +".information_schema.tables where table_schema = ? and table_type = 'MANAGED';",
                name
        );
        DatabricksTable[] tables = new DatabricksTable[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new DatabricksTable(jdbcTemplate, database, this, tableNames.get(i));
        }
        return tables;
    }

    @Override
    public Table getTable(String tableName) {
        return new DatabricksTable(jdbcTemplate, database, this, tableName);
    }

}