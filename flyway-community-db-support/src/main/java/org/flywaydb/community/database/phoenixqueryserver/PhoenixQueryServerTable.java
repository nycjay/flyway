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