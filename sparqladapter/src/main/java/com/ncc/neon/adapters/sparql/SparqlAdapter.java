package com.ncc.neon.adapters.sparql;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.models.queries.ImportQuery;
import com.ncc.neon.models.queries.MutateQuery;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.results.ActionResult;
import com.ncc.neon.models.results.FieldTypePair;
import com.ncc.neon.models.results.TableWithFields;
import com.ncc.neon.models.results.TabularQueryResult;
import org.apache.jena.jdbc.remote.RemoteEndpointDriver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.*;
import java.util.List;
import java.util.Map;

public class SparqlAdapter extends QueryAdapter {

    private Connection conn = null;

    public SparqlAdapter(String host, String usernameFromConfig, String passwordFromConfig) {
        super("Sparql", host, usernameFromConfig, passwordFromConfig);

        try {
            RemoteEndpointDriver.register();
            // Expect host to be "datasetname@host"
            String[] datasetNameAndHost = host.split("@");
            this.conn = DriverManager.getConnection("jdbc:jena:remote:query=http://" + datasetNameAndHost[1] +
                    ":3030/" + datasetNameAndHost[0] + "/sparql&update=http://" + host + ":3030/" +
                    datasetNameAndHost[0] + "/update");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Mono<TabularQueryResult> execute(Query query) {

        Statement stmt = null;
        List<Map<String, Object>> results = null;
        try {
            // Need a statement
            stmt = this.conn.createStatement();

            // Make a query
            String sparqlQuery = SparqlQueryConverter.convertQuery(query);

            // Execute the query
            ResultSet rset = stmt.executeQuery(sparqlQuery);

            // Extract the results
            results = SparqlResultsConverter.convertResults(query, rset);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
        return Mono.just(new TabularQueryResult(results));
    }

    @Override
    public Flux<String> showDatabases() {
        return null;
    }

    @Override
    public Flux<String> showTables(String dbName) {
        return null;
    }

    @Override
    public Flux<String> getFieldNames(String databaseName, String tableName) {
        return null;
    }

    @Override
    public Flux<FieldTypePair> getFieldTypes(String databaseName, String tableName) {
        return null;
    }

    @Override
    public Flux<TableWithFields> getTableAndFieldNames(String databaseName) {
        return null;
    }

    @Override
    public Mono<ActionResult> importData(ImportQuery importQuery) {
        return null;
    }

    @Override
    public Mono<ActionResult> mutateData(MutateQuery mutate) {
        return null;
    }

    /**
     * Inserts the triple specified by the databasename, tablename, and dataID in the provided MutateQuery.
     *
     * @param mutate databaseName - Subject to be inserted, tableName - Predicate to be inserted, dataID - Object to
     *               be inserted
     * @return ActionResult detailing the outcome
     */
    @Override
    public Mono<ActionResult> insertData(MutateQuery mutate) {
        //TODO: Determine why sparql jdbc adapter doesn't work with this query
        Statement stmt = null;
        List<Map<String, Object>> results = null;
        try {
            // Need a statement
            stmt = this.conn.createStatement();

            // Make a query
            String sparqlQuery = SparqlQueryConverter.convertMutationInsertQuery(mutate);

            // Execute the query
            ResultSet rset = stmt.executeQuery(sparqlQuery);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }

        //TODO: set output results
        return null;
    }

    /**
     * Deletes the triple specified by the databasename, tablename, and dataID in the provided MutateQuery.
     *
     * @param mutate databaseName - Subject to be deleted, tableName - Predicate to be deleted, dataID - Object to
     *               be deleted
     * @return ActionResult detailing the outcome
     */
    @Override
    public Mono<ActionResult> deleteData(MutateQuery mutate) {
        //TODO: Determine why sparql jdbc adapter doesn't work with this query
        Statement stmt = null;
        List<Map<String, Object>> results = null;
        try {
            // Need a statement
            stmt = this.conn.createStatement();

            // Make a query
            String sparqlQuery = SparqlQueryConverter.convertMutationDeleteQuery(mutate);

            // Execute the query
            ResultSet rset = stmt.executeQuery(sparqlQuery);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }

        //TODO: set output results
        return null;
    }
}
