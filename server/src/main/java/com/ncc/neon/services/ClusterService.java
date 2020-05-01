package com.ncc.neon.services;

import com.ncc.neon.models.queries.ClusterClause;
import com.ncc.neon.models.results.TabularQueryResult;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

/**
 * The service in charge of performing all the clustering for results being returned.
 *
 * @author jlechner
 */
@Component
public class ClusterService {

    public static final int DEFAULT_TEXT_COUNT = 26;
    public static final int DEFAULT_NUMBER_COUNT = 50;
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
        String fieldType = this.clusterClause.getFieldType();

        // to keep the check simple, specialty cases are for text
        boolean isText = (fieldType.equals("text") || fieldType.equals("keyword")) ? true : false;

        List<List<Object>> clusters = this.clusterClause.getClusters();

        // establish keys
        String fieldNameKey = this.clusterClause.getFieldNames().get(0);
        String aggregationNameKey = this.clusterClause.getAggregationName();

        List<Map<String, Object>> data = tabularQueryResult.getData();

        // determine count
        // default counts
        BigDecimal count = isText ? new BigDecimal(DEFAULT_TEXT_COUNT) : new BigDecimal(DEFAULT_NUMBER_COUNT);
        if (this.clusterClause.getCount() != 0) {
            count = new BigDecimal(this.clusterClause.getCount());
        }

        // data is small enough
        if (clusters == null && count.compareTo(new BigDecimal(data.size())) == 1) {
            return tabularQueryResult;
        }

        // determine ordering
        Map<String, Object> first = data.get(0);
        Map<String, Object> last = data.get(data.size() - 1);
        BigDecimal firstGroup = null;
        BigDecimal lastGroup = null;
        if (!isText) {
            firstGroup = new BigDecimal(first.get(fieldNameKey).toString());
            lastGroup = new BigDecimal(last.get(fieldNameKey).toString());
        } else if (isText) {
            firstGroup = convertTextBinToNumber(first.get(fieldNameKey).toString());
            lastGroup = convertTextBinToNumber(last.get(fieldNameKey).toString());
        }
        String order = firstGroup.compareTo(lastGroup) == 1 ? "des" : "asc";

        // find new clusters
        List<Map<String, Object>> newData = getNewDataBins(fieldNameKey, isText, firstGroup, lastGroup, count, clusters);

        // take care of extra keys that may be present
        Map<String, Object> extraKeySets = getExtraKeySetsMap(fieldNameKey, aggregationNameKey, data);

        // aggregate data into the new clusters
        aggregateNumbersInNewData(fieldNameKey, isText, aggregationNameKey, data, newData, extraKeySets);

        // reset clusterclause
        this.clusterClause = null;

        // return new clustered results
        return new TabularQueryResult(newData);
    }

    /**
     * Calculates the spacing between the different bins in the new data set results.
     *
     * @param fieldNameKey the key that corresponds with the field name
     * @param isText whether the type of aggregation is text
     * @param firstGroup the first group of data in the original result data
     * @param lastGroup the last group of data in the original result data
     * @param count how many bins there should be in the new data set
     * @param clusters clusters specified by the cluster clause if available
     * @return the new data set ready to be filled
     */
    private List<Map<String, Object>> getNewDataBins(String fieldNameKey, boolean isText, BigDecimal firstGroup, BigDecimal lastGroup,
                                                     BigDecimal count, List<List<Object>> clusters) {
        List<Map<String, Object>> newData = new ArrayList<>();
        if (clusters == null) {
            BigDecimal gap = (lastGroup.subtract(firstGroup)).divide(count);
            BigDecimal step = null;
            if (!isText) {
                step = new BigDecimal(".0001");
            } else if (isText) {
                gap = gap.setScale(0, RoundingMode.DOWN);
                step = new BigDecimal("1");
            }
            BigDecimal currentBin = firstGroup;
            for (int i = 0; i < count.intValue(); i++) {
                LinkedHashMap map = new LinkedHashMap<>();
                ArrayList range = new ArrayList<>();
                if (!isText) {
                    if (i != 0) {
                        range.add(currentBin.add(step));
                    } else {
                        range.add(currentBin);
                    }
                    range.add(currentBin.add(gap));
                } else if (isText) {
                    if (i != 0) {
                        range.add(convertNumberToTextBin(currentBin = currentBin.add(step)));
                    } else {
                        range.add(convertNumberToTextBin(currentBin));
                    }
                    range.add(convertNumberToTextBin(currentBin.add(gap)));
                }
                currentBin = currentBin.add(gap);
                map.put(fieldNameKey, range);
                newData.add(map);
            }
        } else {
            for (int i = 0; i < clusters.size(); i++) {
                LinkedHashMap map = new LinkedHashMap<>();
                ArrayList range = new ArrayList();
                if (!isText) {
                    range.add(new BigDecimal(clusters.get(i).get(0).toString()));
                    range.add(new BigDecimal(clusters.get(i).get(1).toString()));
                } else if (isText) {
                    range.add(clusters.get(i).get(0).toString());
                    range.add(clusters.get(i).get(1).toString());
                }
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
     * @param isText whether the type of aggregation is text
     * @param aggregationNameKey the key that corresponds with the overall counts of each bin
     * @param data the original result data
     * @param newData the newly clustered data
     * @param extraKeySets the extra key sets map for storing additional keys
     */
    private void aggregateNumbersInNewData(String fieldNameKey, boolean isText, String aggregationNameKey,
                                           List<Map<String, Object>> data, List<Map<String, Object>> newData,
                                           Map<String, Object> extraKeySets) {
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
            ArrayList alo = (ArrayList) currNewBin.get(fieldNameKey);
            BigDecimal start = null;
            BigDecimal end = null;
            if (!isText) {
                start = (BigDecimal) alo.get(0);
                end = (BigDecimal) alo.get(1);
            } else if (isText) {
                start = convertTextBinToNumber(alo.get(0).toString());
                end = convertTextBinToNumber(alo.get(1).toString());
            }

            // traverse the old data a total of one time
            while (oldDataIndex < data.size()) {
                // retrieve the old data's field name
                BigDecimal oldBinValue = null;
                if (!isText) {
                    oldBinValue = new BigDecimal(data.get(oldDataIndex).get(fieldNameKey).toString());
                } else if (isText) {
                    oldBinValue = convertTextBinToNumber(data.get(oldDataIndex).get(fieldNameKey).toString());
                }

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
     * Converts the incoming text bin e.g. "a", "bd" into a number.
     *
     * @param text the bin to be converted
     * @return a BigDecimal corresponding to the index of the bin
     */
    private BigDecimal convertTextBinToNumber(String text) {
        final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";
        final int ALPHABET_LENGTH = 26;
        int total = 0;
        for (int i = text.length() - 1; i > -1; i--) {
            char c = text.charAt(i);
            total += (ALPHABET.indexOf(c) + 1) * Math.pow(ALPHABET_LENGTH, i);
        }

        return new BigDecimal(total);
    }

    /**
     * Converts the incoming number into a text bin e.g. "a", "bd".
     *
     * @param number the number corresponding to the text bin
     * @return returns the text bin as a String
     */
    private String convertNumberToTextBin(BigDecimal number) {
        final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";
        final int ALPHABET_LENGTH = 26;

        int intValue = number.intValue();

        int highestPower = 0;
        boolean limitNotExceeded = true;
        while (limitNotExceeded) {
            int test = intValue;
            for (int currPower = highestPower; currPower > -1; currPower--) {
                test = (int) (test - Math.pow(ALPHABET_LENGTH, currPower));
            }

            if (test < 0) {
                limitNotExceeded = false;
            } else {
                highestPower++;
            }
        }

        char[] text = new char[highestPower];
        int textIndex = 0;
        for (int currPower = highestPower; currPower > 0; currPower--) {
            for (int i = 1; i < ALPHABET_LENGTH + 1;i++) {
                if (intValue - i * Math.pow(ALPHABET_LENGTH, currPower - 1) < 0) {
                    text[textIndex] = ALPHABET.charAt(i - 2);
                    textIndex++;
                    break;
                }
            }
        }

        return String.valueOf(text);
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
