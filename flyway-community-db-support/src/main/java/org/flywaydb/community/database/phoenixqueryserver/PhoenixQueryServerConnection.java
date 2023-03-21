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