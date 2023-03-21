package org.flywaydb.community.database.phoenixqueryserver;

import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.BaseDatabaseType;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;

import java.sql.Connection;
import java.sql.Types;

public class PhoenixQueryServerDatabaseType extends BaseDatabaseType {
    private static final String PHOENIX_QUERY_SERVER_THIN_CLIENT_JDBC_DRIVER = "org.apache.phoenix.queryserver.client.Driver";
    @Override
    public String getName() {
        return "phoenix";
    }

    @Override
    public int getNullType() {
        return Types.VARCHAR;
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        return url.startsWith("jdbc:phoenix:thin:");
    }

    @Override
    public String getDriverClass(String url, ClassLoader classLoader) {
        return PHOENIX_QUERY_SERVER_THIN_CLIENT_JDBC_DRIVER;
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(String databaseProductName, String databaseProductVersion, Connection connection) {
        if (databaseProductName.toLowerCase().contains("phoenix".toLowerCase())) {
            return true;
        }
        return false;
    }

    @Override
    public Database createDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        return new PhoenixQueryServerDatabase(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    public Parser createParser(Configuration configuration, ResourceProvider resourceProvider, ParsingContext parsingContext) {
        return new PhoenixQueryServerParser(configuration, parsingContext);
    }

    @Override
    public boolean supportsEscapeProcessing() {
        return false;
    }

    @Override
    public boolean supportsResultsExtraction() {
        return false;
    }

}