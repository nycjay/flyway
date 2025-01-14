---
menu: how
subtitle: How Flyway works
redirect_from: /getStarted/how/
---
<div id="how">
    <h1>How Flyway works</h1>

    <p>The easiest scenario is when you point <strong>Flyway</strong> to an <strong>empty database</strong>. </p>

    <p align="center"><img src="assets/EmptyDb.png" style="max-width: 100%"/></p>

    <p>It will try to
        locate its <strong>schema history table</strong>. As the database is empty, Flyway won't find it and will
        <strong>create</strong> it instead.<br/> <br/> You now have a database with a single empty table called <i>flyway_schema_history</i>
        by default: </p>

    <p align="center"><img src="assets/EmptySchemaVersion.png" style="max-width: 100%"/></p>

    <p>This table will be used to track the state of the database. </p>

    <p>Immediately afterwards Flyway will begin <strong>scanning</strong> the filesystem or the classpath of the
        application for <strong>migrations</strong>.
        They can be written in either Sql or Java. </p>

    <p>The migrations are then <strong>sorted</strong> based on their <strong>version number</strong> and applied in
        order: </p>

    <p align="center"><img src="assets/Migration-1-2.png" style="max-width: 100%"/></p>

    <p>As each
        migration gets applied, the schema history table is updated accordingly: </p>

    <h5>flyway_schema_history</h5>
    <table class="table table-bordered table-striped table-condensed" style="font-size: 70%">
        <tr>
            <th>installed_rank</th>
            <th>version</th>
            <th>description</th>
            <th>type</th>
            <th>script</th>
            <th>checksum</th>
            <th>installed_by</th>
            <th>installed_on</th>
            <th>execution_time</th>
            <th>success</th>
        </tr>
        <tr>
            <td>1</td>
            <td>1</td>
            <td>Initial Setup</td>
            <td>SQL</td>
            <td>V1__Initial_Setup.sql</td>
            <td>1996767037</td>
            <td>axel</td>
            <td>2016-02-04 22:23:00.0</td>
            <td>546</td>
            <td>true</td>
        </tr>
        <tr>
            <td>2</td>
            <td>2</td>
            <td>First Changes</td>
            <td>SQL</td>
            <td>V2__First_Changes.sql</td>
            <td>1279644856</td>
            <td>axel</td>
            <td>2016-02-06 09:18:00.0</td>
            <td>127</td>
            <td>true</td>
        </tr>
    </table>

    <p>With the metadata and the initial state in place, we can now talk about <strong>migrating to newer
        versions</strong>.
    </p>

    <p>Flyway will once again scan the filesystem or the classpath of the application for migrations. The migrations are
        checked against
        the schema history table. If their version number is lower or equal to the one of the version marked as current, they
        are ignored.<br/> <br/> The remaining migrations are the <strong>pending migrations</strong>: available,
        but not applied. </p>

    <p align="center"><img src="assets/PendingMigration.png" style="max-width: 100%"/></p>

    <p>They are then <strong>sorted by version number</strong> and <strong>executed in order</strong>: </p>

    <p align="center"><img src="assets/Migration21.png" style="max-width: 100%"/></p>

    <p>The <strong>schema history table</strong> is <strong>updated</strong> accordingly: </p>

    <h5>flyway_schema_history</h5>
    <table class="table table-bordered table-striped table-condensed" style="font-size: 70%">
        <tr>
            <th>installed_rank</th>
            <th>version</th>
            <th>description</th>
            <th>type</th>
            <th>script</th>
            <th>checksum</th>
            <th>installed_by</th>
            <th>installed_on</th>
            <th>execution_time</th>
            <th>success</th>
        </tr>
        <tr>
            <td>1</td>
            <td>1</td>
            <td>Initial Setup</td>
            <td>SQL</td>
            <td>V1__Initial_Setup.sql</td>
            <td>1996767037</td>
            <td>axel</td>
            <td><nobr>2016-02-04 22:23:00.0</nobr></td>
            <td>546</td>
            <td>true</td>
        </tr>
        <tr>
            <td>2</td>
            <td>2</td>
            <td>First Changes</td>
            <td>SQL</td>
            <td>V2__First_Changes.sql</td>
            <td>1279644856</td>
            <td>axel</td>
            <td>2016-02-06 09:18:00.0</td>
            <td>127</td>
            <td>true</td>
        </tr>
        <tr>
            <td>3</td>
            <td>2.1</td>
            <td>Refactoring</td>
            <td>JDBC</td>
            <td>V2_1__Refactoring</td>
            <td></td>
            <td>axel</td>
            <td>2016-02-10 17:45:05.4</td>
            <td>251</td>
            <td>true</td>
        </tr>
    </table>

    <p><strong>And that's it!</strong> Every time the need to evolve the database arises, whether structure (DDL)
        or reference data (DML),
        simply create a new migration with a version number higher than the current one. The next time Flyway starts, it
        will
        find it and upgrade the database accordingly.</p>
</div>
