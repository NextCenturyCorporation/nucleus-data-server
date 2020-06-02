package com.ncc.neon.adapters.sql;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.models.queries.ImportQuery;
import com.ncc.neon.models.queries.MutateQuery;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.results.ActionResult;
import com.ncc.neon.models.results.FieldType;
import com.ncc.neon.models.results.FieldTypePair;
import com.ncc.neon.models.results.TableWithFields;
import com.ncc.neon.models.results.TabularQueryResult;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.data.r2dbc.core.DatabaseClient;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SqlAdapter extends QueryAdapter {
    ConnectionPool pool;
    SqlType type;

    class SingleStringSubscriber implements Subscriber<CharSequence> {
        private Subscription subscription;
        public String data;
        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            this.subscription.request(1);
        }
        @Override
        public void onNext(CharSequence next) {
            this.data = next.toString();
            this.subscription.cancel();
        }
        @Override
        public void onError(Throwable t) {
            t.printStackTrace();
        }
        @Override
        public void onComplete() {
            // Do nothing.
        }
    }

    public SqlAdapter(SqlType type, String host, String usernameFromConfig, String passwordFromConfig) {
        super(type.prettyName, host, usernameFromConfig, passwordFromConfig);
        this.type = type;

        // Expect host to be "host", "username@host", or "username:password@host" (ending with optional ":port")
        String[] hostAndAuthData = host.split("@");
        String[] userAndPassData = hostAndAuthData.length > 1 ? hostAndAuthData[0].split(":") : new String[] {};
        String username = usernameFromConfig != null ? usernameFromConfig : (userAndPassData.length > 0 ?
            userAndPassData[0] : null);
        String password = passwordFromConfig != null ? passwordFromConfig : (userAndPassData.length > 1 ?
            userAndPassData[1] : null);

        String hostAndPort = hostAndAuthData[hostAndAuthData.length > 1 ? 1 : 0];
        String auth = ((username != null) ? (username + (password != null ? (":" + password) : "") + "@") : "");

        ConnectionFactory connectionFactory = ConnectionFactories.get(this.type.driverName + "://" + auth +
            hostAndPort);
        ConnectionPoolConfiguration config = ConnectionPoolConfiguration.builder(connectionFactory)
            .maxIdleTime(Duration.ofSeconds(2)).initialSize(2).maxSize(20).build();
        this.pool = new ConnectionPool(config);
    }

    @Override
    public Mono<TabularQueryResult> execute(Query query) {
        verifyQueryTablesExist(query);

        String sqlQueryString = SqlQueryConverter.convertQuery(query, this.type);
        logQuery(query, sqlQueryString);

        if (sqlQueryString == null) {
            return Mono.just(null);
        }

        return runSqlQueryAndReturnMaps(sqlQueryString).collectList().map(list -> new TabularQueryResult(list));
    }

    private Flux<Map<String, Object>> runSqlQueryAndReturnMaps(String sqlQueryString) {
        // Create a new DatabaseClient for every query so the connection is released back to the pool upon completion.
        DatabaseClient database = DatabaseClient.create(this.pool);
        return database.execute(sqlQueryString).fetch().all();
    }

    private Flux<String> runSqlQueryAndReturnStrings(String sqlQueryString, String columnName) {
        return runSqlQueryAndReturnMaps(sqlQueryString).map(data -> data.get(columnName).toString());
    }

    @Override
    public Flux<String> showDatabases() {
        String sqlQueryString = this.type == SqlType.POSTGRESQL ?
            "SELECT schema_name FROM information_schema.schemata" : "SHOW DATABASES";
        String columnName = this.type == SqlType.POSTGRESQL ? "schema_name" : "Database";
        return runSqlQueryAndReturnStrings(sqlQueryString, columnName);
    }

    @Override
    public Flux<String> showTables(String databaseName) {
        String sqlQueryString = this.type == SqlType.POSTGRESQL ?
            ("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + databaseName + "'") :
            ("SHOW TABLES FROM " + databaseName);
        String columnName = this.type == SqlType.POSTGRESQL ? "table_name" : "Tables_in_" + databaseName;
        return runSqlQueryAndReturnStrings(sqlQueryString, columnName);
    }

    @Override
    public Flux<String> getFieldNames(String databaseName, String tableName) {
        String sqlQueryString = this.type == SqlType.POSTGRESQL ?
            ("SELECT column_name FROM information_schema.columns WHERE table_schema = '" + databaseName +
                "' AND table_name = '" + tableName + "'") :
            ("SHOW COLUMNS FROM " + databaseName + "." + tableName);
        String columnName = this.type == SqlType.POSTGRESQL ? "column_name" : "Field";
        return runSqlQueryAndReturnStrings(sqlQueryString, columnName);
    }

    @Override
    public Flux<TableWithFields> getTableAndFieldNames(String databaseName) {
        // TODO Deprecated
        return Flux.just();
    }

    @Override
    public Flux<FieldTypePair> getFieldTypes(String databaseName, String tableName) {
        String sqlQueryString = type == SqlType.POSTGRESQL ?
            ("SELECT * FROM information_schema.columns WHERE table_schema = '" + databaseName +
                "' AND table_name = '" + tableName + "'") :
            ("SHOW COLUMNS FROM " + databaseName + "." + tableName);
        String fieldColumn = this.type == SqlType.POSTGRESQL ? "column_name" : "Field";
        String typeColumn = this.type == SqlType.POSTGRESQL ? "data_type" : "Type";

        return runSqlQueryAndReturnMaps(sqlQueryString).map(data -> {
            String fieldType = data.get(typeColumn).toString();
            if (this.type == SqlType.MYSQL) {
                Clob clob = (Clob) data.get(typeColumn);
                SingleStringSubscriber subscriber = new SingleStringSubscriber();
                clob.stream().subscribe(subscriber);
                // Yes, this does look dangerous, but the Clob is actually a SingletonLob that returns immediately, so
                // it should be fine.  (We can't cast it to a SingletonLob because the class is hidden.)
                fieldType = subscriber.data;
            }
            return new FieldTypePair(data.get(fieldColumn).toString(), retrieveFieldType(fieldType));
        });
    }

    @Override
    public Mono<ActionResult> importData(ImportQuery importQuery) {
        // TODO THOR-1500 THOR-1501
        return Mono.just(new ActionResult("Import not yet supported for SQL"));
    }

    @Override
    public Mono<ActionResult> mutateData(MutateQuery mutateQuery) {
        DatabaseClient database = DatabaseClient.create(this.pool);
        return database.execute(SqlQueryConverter.convertMutationQuery(mutateQuery)).fetch().rowsUpdated()
            .map(rowCount -> new ActionResult(rowCount + " rows updated in " + mutateQuery.getDatabaseName() + "." +
                mutateQuery.getTableName() + " with " + mutateQuery.getIdFieldName() + " = " +
                mutateQuery.getDataId(), new ArrayList<String>()));
    }

    @Override
    public Mono<ActionResult> insertData(MutateQuery mutateQuery) {
        DatabaseClient database = DatabaseClient.create(this.pool);
        return database.execute(SqlQueryConverter.convertMutationIntoInsertQuery(mutateQuery)).fetch().rowsUpdated()
                .map(rowCount -> new ActionResult(rowCount + " rows updated in " + mutateQuery.getDatabaseName() + "." +
                        mutateQuery.getTableName(), new ArrayList<String>()));
    }

    @Override
    public Mono<ActionResult> deleteData(MutateQuery mutateQuery) {
        DatabaseClient database = DatabaseClient.create(this.pool);
        return database.execute(SqlQueryConverter.convertMutationQuery(mutateQuery)).fetch().rowsUpdated()
                .map(rowCount -> new ActionResult(rowCount + " rows deleted in " + mutateQuery.getDatabaseName() + "." +
                        mutateQuery.getTableName() + " with " + mutateQuery.getIdFieldName() + " = " +
                        mutateQuery.getDataId(), new ArrayList<String>()));
    }

    private FieldType retrieveFieldType(String type) {
        String dataType = type.toLowerCase();
        if (dataType.contains(" ")) {
            dataType = dataType.substring(0, dataType.indexOf(" "));
        }
        if (dataType.contains("(")) {
            dataType = dataType.substring(0, dataType.indexOf("("));
        }
        switch (dataType) {
            case "bigint":
            case "bit":
            case "int":
            case "integer":
            case "mediumint":
            case "serial":
            case "smallint":
            case "tinyint":
                return FieldType.INTEGER;
            case "bool":
            case "boolean":
                return FieldType.BOOLEAN;
            case "box":
            case "line":
            case "lseg":
            case "point":
            case "polygon":
                return FieldType.GEO;
            case "date":
            case "datetime":
            case "timestamp":
                return FieldType.DATETIME;
            case "dec":
            case "decimal":
            case "double":
            case "float":
            case "float8":
            case "numeric":
            case "real":
                return FieldType.DECIMAL;
            case "enum":
            case "set":
            case "time":
            case "year":
                return FieldType.KEYWORD;
            case "json":
            case "jsonb":
                return FieldType.OBJECT;
            case "uuid":
                return FieldType.ID;
            default:
                return FieldType.TEXT;
        }
    }
}
