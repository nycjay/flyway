package org.flywaydb.community.database.phoenixqueryserver;

import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.jdbc.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PhoenixQueryServerSchema extends Schema<PhoenixQueryServerDatabase, PhoenixQueryServerTable>{

    private enum ObjectTypes {
        TABLE,
        VIEW,
        INDEX,
        SEQUENCE,
        FUNCTION
    }
    private static final Log LOG = LogFactory.getLog(PhoenixQueryServerSchema.class);

    /**
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param database     The database-specific support.
     * @param name         The name of the schema.
     */
    public PhoenixQueryServerSchema(JdbcTemplate jdbcTemplate, PhoenixQueryServerDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        // Find a matching schema
        ResultSet rs = jdbcTemplate.getConnection().getMetaData().getSchemas();
        while(rs.next()) {
            String schemaName = rs.getString("TABLE_SCHEM");
            if(schemaName == null) {
                if(name == null) {
                    return true;
                }
            }
            else {
                if(name != null && schemaName.equals(name)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return allTables().length == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        LOG.info("Phoenix does not support creating schemas. Schema not created: " + name);
    }

    @Override
    protected void doDrop() throws SQLException {
        LOG.info("Phoenix does not support dropping schemas directly. Running clean of objects instead");
        doClean();
    }

    @Override
    protected void doClean() throws SQLException {
        // Clean sequences
        for (String statement : generateDropStatementsUsingQuery(ObjectTypes.SEQUENCE)) {
            jdbcTemplate.execute(statement);
        }
        // Clean functions
        for (String statement : generateDropStatementsUsingQuery(ObjectTypes.FUNCTION)) {
            jdbcTemplate.execute(statement);
        }

        // Clean views
        List<String> viewNames = listObjectsOfType(ObjectTypes.VIEW);
        for (String statement : generateDropStatementsForUsingMetaData(ObjectTypes.VIEW)) {
            jdbcTemplate.execute(statement);
        }

        // Clean indexes
        for (String statement : generateDropStatementsForUsingMetaData(ObjectTypes.INDEX)) {
            jdbcTemplate.execute(statement);
        }
        // Clean tables
        for (String statement : generateDropStatementsForUsingMetaData(ObjectTypes.TABLE)) {
            jdbcTemplate.execute(statement);
        }

    }

    private List<String> generateDropStatementsUsingQuery(ObjectTypes objType) throws SQLException {
        String listQuery = "";
        switch (objType) {
            case SEQUENCE:
                listQuery =  "select sequence_name from system.\"SEQUENCE\";";
                break;
            case FUNCTION:
                listQuery = "select function_name from system.\"FUNCTION\";";
                break;
        }
        List<String> objNames = jdbcTemplate.queryForStringList(listQuery);
        List<String> statements = new ArrayList<>();
        for (String objName : objNames) {
            statements.add("drop " + objType.name() + " if exists " + database.quote(name, objName) + ";");
        }
        return statements;
    }

    private List<String> generateDropStatementsForUsingMetaData(ObjectTypes objType) throws SQLException {
        // A null schema name actually does a cross-schema search in Phoenix, change to 0-length
        String finalName = (name == null ? "" : name);

        List<String> statements = new ArrayList<>();
        switch (objType) {
            case VIEW:
                ResultSet rs = jdbcTemplate.getConnection().getMetaData().getTables(null, finalName, null, new String[]{"VIEW"});
                while(rs.next()) {
                    String viewName = rs.getString("TABLE_NAME");
                    if(viewName != null) {
                        statements.add("drop " + objType.name() + " if exists " + database.quote(name, viewName) + ";");
                    }
                }
         //       rs.close();
                break;
            case INDEX:
                break;
        }
        return statements;
    }

    private List<String> generateDropStatementsForSequences(String objType) throws SQLException {
        // A null schema name actually does a cross-schema search in Phoenix, change to 0-length
        //String finalName = (name == null ? "" : name);

        List<String> objNames = jdbcTemplate.queryForStringList(
                // Search for all sequences
                "select sequence_name from system.\"SEQUENCE\";");
        List<String> statements = new ArrayList<>();
        for (String objName : objNames) {
            statements.add("drop " + objType + " if exists " + database.quote(name, objName) + ";");
        }
        return statements;
    }


    @Override
    protected PhoenixQueryServerTable[] doAllTables() throws SQLException {
        List<String> tableNames = listObjectsOfType(ObjectTypes.TABLE);
        PhoenixQueryServerTable[] tables = new PhoenixQueryServerTable[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new PhoenixQueryServerTable(jdbcTemplate, database, this, tableNames.get(i));
        }
        return tables;
    }

    @Override
    public Table getTable(String tableName) {
        return new PhoenixQueryServerTable(jdbcTemplate, database, this, tableName);
    }

    protected List<String> listObjectsOfType(ObjectTypes type) throws SQLException {
        List<String> retVal = new ArrayList<String>();

        // A null schema name actually does a cross-schema search in Phoenix, change to 0-length
        String finalName = (name == null ? "" : name);

        // Available through metadata interface
        if (type.equals(ObjectTypes.VIEW)) {
            ResultSet rs = jdbcTemplate.getConnection().getMetaData().getTables(null, finalName, null, new String[]{"VIEW"});
            while(rs.next()) {
                String viewName = rs.getString("TABLE_NAME");
                if(viewName != null) {
                    retVal.add(viewName);
                }

            }
        //    rs.close();
        }
        else if (type.equals(ObjectTypes.TABLE)) {
            ResultSet rs = jdbcTemplate.getConnection().getMetaData().getTables(null, finalName, null, new String[] {"TABLE"} );
            while(rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                Set<String> tables = new HashSet<String>();
                if(tableName != null) {
                    tables.add(tableName);
                }
                retVal.addAll(tables);
            }
       //     rs.close();
        }
        // Sequences aren't available through the DatabaseMetaData interface
        else if (type.equals(ObjectTypes.SEQUENCE)) {
            if(name == null) {
                String query = "SELECT SEQUENCE_NAME FROM SYSTEM.\"SEQUENCE\" WHERE SEQUENCE_SCHEMA IS NULL";
                return jdbcTemplate.queryForStringList(query);
            }
            else {
                String query = "SELECT SEQUENCE_NAME FROM SYSTEM.\"SEQUENCE\" WHERE SEQUENCE_SCHEMA = ?";
                return jdbcTemplate.queryForStringList(query, name);
            }
        }
        // Neither are indices, unless we know the table ahead of time
        else if (type.equals(ObjectTypes.INDEX)) {
            String query = "SELECT TABLE_NAME, DATA_TABLE_NAME FROM SYSTEM.CATALOG WHERE TABLE_SCHEM";

            if(name == null) {
                query = query + " IS NULL";
            }
            else {
                query = query + " = ?";
            }
            query = query + " AND TABLE_TYPE = 'i'";

            String finalQuery = query.replaceFirst("\\?", "'" + name + "'");
            // Return the index and table as a comma separated string
            retVal = jdbcTemplate.query(finalQuery, new RowMapper<String>() {
                @Override
                public String mapRow(ResultSet rs) throws SQLException {
                    return rs.getString("TABLE_NAME") + "," + rs.getString("DATA_TABLE_NAME");
                }
            });
        }
        return retVal;
    }
}
