package com.ncc.neon.services;

import com.ncc.neon.models.queries.ClusterClause;
import com.ncc.neon.models.results.TabularQueryResult;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.*;

@Component
public class ClusterService {

    private static final String NUMBER_STEP = ".0001";
    private ClusterClause clusterClause;

    /**
     * Wraps the clustered results in a mono.
     *
     * @param tabularQueryResult the result of the unclustered query
     * @return a mono containing the new clustered results
     */
    public Mono<? extends TabularQueryResult> clusterIntoMono(TabularQueryResult tabularQueryResult) {
        return Mono.just(this.cluster(tabularQueryResult));
    }

    /**
     * Clusters the results from the query.
     *
     * @param tabularQueryResult the result of the unclustered query
     * @return the new clustered results
     */
    public TabularQueryResult cluster(TabularQueryResult tabularQueryResult) {
        if (this.clusterClause.getFieldType().equals("number")) {
            return this.aggregateNumber(tabularQueryResult);
        }

        this.clusterClause = null; // reset cluster clause
        return tabularQueryResult;
    }

    /**
     * Clusters results by number aggregation.
     *
     * @param tabularQueryResult the original results
     * @return the tabularqueryresult aggregated by numbers with the specifications given by the cluster clause
     */
    private TabularQueryResult aggregateNumber(TabularQueryResult tabularQueryResult) {
        List<List<Object>> clusters = this.clusterClause.getClusters();

        // establish keys
        String fieldNameKey = this.clusterClause.getFieldNames().get(0);
        String aggregationNameKey = this.clusterClause.getAggregationName();

        List<Map<String, Object>> data = tabularQueryResult.getData();

        // determine ordering
        Map<String, Object> first = data.get(0);
        Map<String, Object> last = data.get(data.size() - 1);
        BigDecimal firstGroup = new BigDecimal(first.get(fieldNameKey).toString()); // assume number because 'number' fieldType
        BigDecimal lastGroup = new BigDecimal(last.get(fieldNameKey).toString());
        String order = firstGroup.compareTo(lastGroup) == 1 ? "des" : "asc";

        // determine count
        int count = this.clusterClause.getCount() != 0 ? this.clusterClause.getCount() : 50;

        // data is small enough
        if (clusters == null && count > data.size()) {
            return tabularQueryResult;
        }

        // find new clusters
        List<Map<String, Object>> newData = getNewDataBins(fieldNameKey, firstGroup, lastGroup, count, clusters);

        // take care of extra keys that may be present
        Map<String, Object> extraKeySets = getExtraKeySetsMap(fieldNameKey, aggregationNameKey, data);

        // aggregate data into the new clusters
        aggregateNumbersInNewData(fieldNameKey, aggregationNameKey, data, newData, extraKeySets);

        this.clusterClause = null;
        return new TabularQueryResult(newData);
    }

    /**
     * Calculates the spacing between the different bins in the new data set results.
     *
     * @param fieldNameKey the key that corresponds with the field name
     * @param firstGroup the first group of data in the original result data
     * @param lastGroup the last group of data in the original result data
     * @param count how many bins there should be in the new data set
     * @param clusters clusters specified by the cluster clause if available
     * @return the new data set ready to be filled
     */
    private List<Map<String, Object>> getNewDataBins(String fieldNameKey, BigDecimal firstGroup, BigDecimal lastGroup,
                                                     int count, List<List<Object>> clusters) {
        List<Map<String, Object>> newData = new ArrayList<>();
        if (clusters == null) {
            BigDecimal gap = (lastGroup.subtract(firstGroup)).divide(new BigDecimal(count));
            BigDecimal step = new BigDecimal(NUMBER_STEP);
            BigDecimal currentBin = firstGroup;
            for (int i = 0; i < count; i++) {
                LinkedHashMap map = new LinkedHashMap<>();
                ArrayList range = new ArrayList<>();
                if (i != 0) {
                    range.add(currentBin.add(step));
                } else {
                    range.add(currentBin);
                }
                range.add(currentBin = currentBin.add(gap));
                map.put(fieldNameKey, range);
                newData.add(map);
            }
        } else {
            for (int i = 0; i < clusters.size(); i++) {
                LinkedHashMap map = new LinkedHashMap<>();
                ArrayList range = new ArrayList();
                range.add(new BigDecimal(clusters.get(i).get(0).toString()));
                range.add(new BigDecimal(clusters.get(i).get(1).toString()));
                map.put(fieldNameKey, range);
                newData.add(map);
            }
        }
        return newData;
    }

    /**
     * Setup any extra keys
     *
     * @param fieldNameKey fieldname key given by the cluster clause
     * @param aggregationNameKey aggregation name key given by the cluster clause
     * @param data original result data
     * @return the extra key sets map for storing additional keys
     */
    private Map<String, Object> getExtraKeySetsMap(String fieldNameKey, String aggregationNameKey,
                                                   List<Map<String, Object>> data) {
        Map<String, Object> datum = data.get(0);
        Map<String, Object> extraKeySets = new HashMap<>();
        for (String key : datum.keySet()) {
            if (!key.equals(aggregationNameKey) && !key.equals(fieldNameKey)) {
                extraKeySets.put(key, new HashSet<>());
            }
        }
        return extraKeySets;
    }

    /**
     * Traverses the original data and puts it in the correct bins in the new data.
     *
     * @param fieldNameKey the key that corresponds with the field name
     * @param aggregationNameKey the key that corresponds with the overall counts of each bin
     * @param data the original result data
     * @param newData the newly clustered data
     * @param extraKeySets the
     */
    private void aggregateNumbersInNewData(String fieldNameKey, String aggregationNameKey, List<Map<String,
            Object>> data, List<Map<String, Object>> newData, Map<String, Object> extraKeySets) {
        Iterator<Map<String, Object>> newDataIter = newData.iterator();
        int oldDataIndex = 0;
        while (newDataIter.hasNext()) {
            // reset the extra keys for each new bin
            for (String key : extraKeySets.keySet()) {
                extraKeySets.put(key, new HashSet<>());
            }

            // determine the boundaries and aggregated count for this bin
            BigDecimal currAgg = new BigDecimal("0");
            Map currNewBin = newDataIter.next();
            ArrayList newRange = (ArrayList) currNewBin.get(fieldNameKey);
            BigDecimal start = (BigDecimal) newRange.get(0);
            BigDecimal end = (BigDecimal) newRange.get(1);

            // traverse the old data a total of one time
            while (oldDataIndex < data.size()) {
                // retrieve the old data's field name
                BigDecimal oldBinValue = new BigDecimal(data.get(oldDataIndex).get(fieldNameKey).toString());

                // if old >= new start && old <= new end
                if (oldBinValue.compareTo(start) != -1
                        && oldBinValue.compareTo(end) != 1) {

                    // check extra keys
                    for (String key : extraKeySets.keySet()) {
                        ((Set) extraKeySets.get(key)).add(data.get(oldDataIndex).get(key));
                    }

                    // accumulate aggregate numbers
                    BigDecimal oldAgg = new BigDecimal(data.get(oldDataIndex).get(aggregationNameKey).toString());
                    currAgg = currAgg.add(oldAgg);

                    // move to the next data in the old results
                    oldDataIndex++;
                } else if (oldBinValue.compareTo(end) == 1) { // old > new end i.e. old bin moved past this curr new bin
                    break;
                }
            }

            // update the aggregated count
            currNewBin.put(aggregationNameKey, currAgg);

            // update the extra key sets
            for (String key : extraKeySets.keySet()) {
                currNewBin.put(key, extraKeySets.get(key));
            }
        }
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
