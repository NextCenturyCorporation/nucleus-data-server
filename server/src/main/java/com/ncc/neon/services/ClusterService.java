package com.ncc.neon.services;

import com.ncc.neon.models.queries.ClusterClause;
import com.ncc.neon.models.results.TabularQueryResult;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
public class ClusterService {

    private ClusterClause clusterClause;

    /**
     * Wraps the clustered results in a mono.
     *
     * @param tabularQueryResult the result of the unclustered query
     * @return a mono containing the new clustered results
     */
    public Mono<? extends TabularQueryResult> clusterIntoMono(TabularQueryResult tabularQueryResult) {
        this.clusterClause = null; // reset cluster clause
        return Mono.just(this.cluster(tabularQueryResult));
    }

    /**
     * Clusters the results from the query.
     *
     * TODO: Currently returns the same results i.e. method is a passthrough right now
     *
     * @param tabularQueryResult the result of the unclustered query
     * @return the new clustered results
     */
    public TabularQueryResult cluster(TabularQueryResult tabularQueryResult) {
        return tabularQueryResult;
    }

    /**
     * Used to set the cluster clause for the service. Only to be called right before
     * calling to cluster.
     *
     * @param clusterClause the clusterclause to be clustered on
     */
    public void setClusterClause(ClusterClause clusterClause) {
        this.clusterClause = clusterClause;
    }
}
