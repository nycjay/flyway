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
package org.flywaydb.community.database.MemSQL;

import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.database.base.BaseDatabaseType;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.JdbcUtils;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;

import java.sql.Connection;
import java.sql.Types;

public class MemSQLDatabaseType extends BaseDatabaseType {

    private static final Log LOG = LogFactory.getLog(MemSQLDatabaseType.class);
    @Override
    public String getName() {
        return "memsql";
    }
    @Override
    public int getPriority() {
        // MemSQL needs to be checked in advance of MySql
        return 1;
    }
    @Override
    public int getNullType() {
        return Types.VARCHAR;
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        return url.startsWith("jdbc:singlestore:") || url.startsWith("jdbc:p6spy:singlestore:") || url.startsWith("jdbc:mysql:");
    }

    @Override
    public String getDriverClass(String url, ClassLoader classLoader) {
        if (url.startsWith("jdbc:p6spy:singlestore:")) {
            return "com.p6spy.engine.spy.P6SpyDriver";
        } else if(url.startsWith("jdbc:mysql:")){
            return "org.mariadb.jdbc.Driver";
        }
        return "com.singlestore.jdbc.Driver";
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(String databaseProductName, String databaseProductVersion, Connection connection) {
        return ((databaseProductName.contains("MySQL") && JdbcUtils.getCatalog(connection).contains("memsql")) || databaseProductName.contains("SingleStore"));
    }

    @Override
    public Database createDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        return new MemSQLDatabase(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    public Parser createParser(Configuration configuration, ResourceProvider resourceProvider, ParsingContext parsingContext) {
        return new MemSQLParser(configuration, parsingContext);
    }

    @Override
    public String instantiateClassExtendedErrorMessage() {
        return "Failure probably due to inability to load dependencies. Please ensure you have downloaded 'https://mvnrepository.com/artifact/com.singlestore/singlestore-jdbc-client' and extracted to 'flyway/drivers' folder";
    }
}