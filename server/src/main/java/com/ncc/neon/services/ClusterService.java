package com.ncc.neon.services;

import com.ncc.neon.models.ClusterType;
import com.ncc.neon.models.queries.ClusterClause;
import com.ncc.neon.models.results.TabularQueryResult;
import com.ncc.neon.util.DateUtil;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * The service in charge of performing all the clustering for results being returned.
 *
 * @author jlechner
 */
@Component
public class ClusterService {

    // text constants
    public static final int DEFAULT_TEXT_COUNT = 26;
    public static final String DEFAULT_TEXT_STEP = "1";

    // number constants
    public static final int DEFAULT_NUMBER_COUNT = 50;
    public static final String DEFAULT_NUMBER_STEP = ".0001";

    // datetime constants
    public static final int DEFAULT_DATETIME_COUNT = 50;
    public static final String DEFAULT_DATETIME_STEP = "1000";
    public static final String PRETTY_DATETIME_KEY = "pretty";

    // latlon constants
    public static final int DEFAULT_LATLON_COUNT = 12;
    public static final String DEFAULT_LATLON_STEP = "0.000000000000001";

    // alphabet constants
    private static final int ALPHABET_LENGTH = 26;
    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";

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
        ClusterType clusterType = retrieveClusterType(this.clusterClause.getFieldType());

        List<List<Object>> clusters = this.clusterClause.getClusters();

        // establish keys
        String fieldNameKey = this.clusterClause.getFieldNames().get(0);
        String aggregationNameKey = this.clusterClause.getAggregationName();

        List<Map<String, Object>> data = tabularQueryResult.getData();

        // determine count
        // default counts
        BigDecimal count = new BigDecimal(this.clusterClause.getCount());
        if (count.compareTo(new BigDecimal(0)) == 0) {
            switch (clusterType) {
                case LAT_LON:
                    count = new BigDecimal(DEFAULT_LATLON_COUNT);
                    break;
                case STRING:
                    count = new BigDecimal(DEFAULT_TEXT_COUNT);
                    break;
                case DATE:
                    count = new BigDecimal(DEFAULT_DATETIME_COUNT);
                    break;
                case NUMBER:
                    count = new BigDecimal(DEFAULT_NUMBER_COUNT);
                    break;
                default:
                    return tabularQueryResult;
            }
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
        if (!clusterType.equals(ClusterType.STRING)) {
            firstGroup = new BigDecimal(first.get(fieldNameKey).toString());
            lastGroup = new BigDecimal(last.get(fieldNameKey).toString());
        } else {
            firstGroup = convertTextBinToNumber(first.get(fieldNameKey).toString());
            lastGroup = convertTextBinToNumber(last.get(fieldNameKey).toString());
        }
        int order = firstGroup.compareTo(lastGroup) == 1 ? -1 : 1;

        // find new clusters
        List<Map<String, Object>> newData = null;
        if (!clusterType.equals(ClusterType.LAT_LON)) {
            newData = getNewDataBins(fieldNameKey, clusterType, firstGroup, lastGroup, count, clusters, order);

            // take care of extra keys that may be present
            Map<String, Object> extraKeySets = getExtraKeySetsMap(fieldNameKey, aggregationNameKey, data);

            // aggregate data into the new clusters
            aggregateNumbersInNewData(fieldNameKey, clusterType, aggregationNameKey, data, newData, extraKeySets, order);
        } else {
            newData = getNewLatLonDataBins(this.clusterClause.getFieldNames(), count, clusters);

            Map<String, Object> extraKeySets = getExtraLatLonKeySetsMap(this.clusterClause.getFieldNames(), aggregationNameKey, data);

            aggregateLatLonNumbersInNewData(this.clusterClause.getFieldNames(), aggregationNameKey, data, newData, extraKeySets);
        }

        // reset clusterclause
        this.clusterClause = null;

        // return new clustered results
        return new TabularQueryResult(newData);
    }

    /**
     * Calculates the spacing between the different bins in the new data set results.
     *
     * @param fieldNameKey the key that corresponds with the field name
     * @param clusterType the clustertype of the aggregation
     * @param firstGroup the first group of data in the original result data
     * @param lastGroup the last group of data in the original result data
     * @param count how many bins there should be in the new data set
     * @param clusters clusters specified by the cluster clause if available
     * @param order 1 for ascending, -1 for descending
     * @return the new data set ready to be filled
     */
    private List<Map<String, Object>> getNewDataBins(String fieldNameKey, ClusterType clusterType, BigDecimal firstGroup, BigDecimal lastGroup,
                                                     BigDecimal count, List<List<Object>> clusters, int order) {
        List<Map<String, Object>> newData = new ArrayList<>();
        if (clusters == null) {
            BigDecimal orderModifier = new BigDecimal(order);
            BigDecimal gap = (lastGroup.subtract(firstGroup)).divide(count);
            BigDecimal step = null;
            switch (clusterType) {
                case STRING:
                    gap = gap.setScale(0, RoundingMode.DOWN);
                    step = new BigDecimal(DEFAULT_TEXT_STEP).multiply(orderModifier);
                    break;
                case DATE:
                    gap = gap.setScale(0, RoundingMode.DOWN);
                    step = new BigDecimal(DEFAULT_DATETIME_STEP).multiply(orderModifier);
                    break;
                case NUMBER:
                default:
                    step = new BigDecimal(DEFAULT_NUMBER_STEP).multiply(orderModifier);
                    break;
            }
            BigDecimal currentBin = firstGroup;
            for (int i = 0; i < count.intValue(); i++) {
                LinkedHashMap map = new LinkedHashMap<>();
                ArrayList range = new ArrayList<>();
                switch (clusterType) {
                    case STRING:
                        if (i != 0) {
                            range.add(convertNumberToTextBin(currentBin = currentBin.add(step)));
                        } else {
                            range.add(convertNumberToTextBin(currentBin));
                        }
                        range.add(convertNumberToTextBin(currentBin.add(gap)));
                        break;
                    case DATE:
                        if (i != 0) {
                            range.add(currentBin.add(step));
                        } else {
                            range.add(currentBin);
                        }
                        range.add(currentBin.add(gap));
                        storePrettyDate(map, range);
                        break;
                    case NUMBER:
                    default:
                        if (i != 0) {
                            range.add(currentBin.add(step));
                        } else {
                            range.add(currentBin);
                        }
                        range.add(currentBin.add(gap));
                        break;
                }
                currentBin = currentBin.add(gap);
                map.put(fieldNameKey, range);
                newData.add(map);
            }
        } else {
            for (int i = 0; i < clusters.size(); i++) {
                LinkedHashMap map = new LinkedHashMap<>();
                ArrayList range = new ArrayList();
                if (!clusterType.equals(ClusterType.STRING)) { // not text
                    range.add(new BigDecimal(clusters.get(i).get(0).toString()));
                    range.add(new BigDecimal(clusters.get(i).get(1).toString()));
                } else if (clusterType.equals(ClusterType.STRING)) { // is text
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
     * Calculates the spacing between the different bins in the new data set results for the LatLon ClusterType.
     *
     * @param fieldNames the keys that correspond with the field names
     * @param longCount how many longitude bins there should be in the new data set
     * @param clusters clusters specified by the cluster clause if available
     * @return the new data set ready to be filled
     */
    private List<Map<String, Object>> getNewLatLonDataBins(List<String> fieldNames, BigDecimal longCount, List<List<Object>> clusters) {
        List<Map<String, Object>> newData = new ArrayList<>();

        String latKey = fieldNames.get(0);
        String longKey = fieldNames.get(1);

        BigDecimal latCount = longCount.divide(new BigDecimal(2));
        BigDecimal totalCount = longCount.multiply(latCount);

        BigDecimal step = new BigDecimal(DEFAULT_LATLON_STEP);

        BigDecimal longGap = new BigDecimal(360).divide(longCount);
        BigDecimal latGap = new BigDecimal(180).divide(latCount);

        if (clusters == null) {
            // lat first
            BigDecimal currentLatBin = new BigDecimal(-90); // latitude range starts at -90
            for (int i = 0; i < latCount.intValue(); i++) {
                ArrayList latRange = new ArrayList();
                if (i != 0) {
                    latRange.add(currentLatBin.add(step));
                } else {
                    latRange.add(currentLatBin);
                }
                latRange.add(currentLatBin = currentLatBin.add(latGap));

                // long second
                BigDecimal currentLongBin = new BigDecimal(-180); // longitude range starts at -180
                for (int j = 0; j < longCount.intValue(); j++) {
                    LinkedHashMap map = new LinkedHashMap();
                    ArrayList longRange = new ArrayList();
                    if (j != 0) {
                        longRange.add(currentLongBin.add(step));
                    } else {
                        longRange.add(currentLongBin);
                    }
                    longRange.add(currentLongBin = currentLongBin.add(longGap));
                    map.put(longKey, longRange);
                    map.put(latKey, latRange);
                    newData.add(map);
                }
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
            if (!key.equals(aggregationNameKey) && !key.equals(fieldNameKey) && !key.equals(PRETTY_DATETIME_KEY)) {
                extraKeySets.put(key, new HashSet<>());
            }
        }
        return extraKeySets;
    }

    /**
     * Setup any extra keys for the latlon clustertype
     *
     * @param fieldNameKeys fieldname keys given by the cluster clause
     * @param aggregationNameKey aggregation name key given by the cluster clause
     * @param data original result data
     * @return the extra key sets map for storing additional keys
     */
    private Map<String, Object> getExtraLatLonKeySetsMap(List<String> fieldNameKeys, String aggregationNameKey,
                                                         List<Map<String, Object>> data) {
        Map<String, Object> datum = data.get(0);
        Map<String, Object> extraKeySets = new HashMap<>();
        for (String key : datum.keySet()) {
            if (!key.equals(aggregationNameKey) && !key.equals(fieldNameKeys.get(0)) && !key.equals(fieldNameKeys.get(1))) {
                extraKeySets.put(key, new HashSet<>());
            }
        }
        return extraKeySets;
    }

    /**
     * Traverses the original data and puts it in the correct bins in the new data.
     *
     * @param fieldNameKey the key that corresponds with the field name
     * @param clusterType the clustertype of the aggregation
     * @param aggregationNameKey the key that corresponds with the overall counts of each bin
     * @param data the original result data
     * @param newData the newly clustered data
     * @param extraKeySets the extra key sets map for storing additional keys
     * @param order 1 for ascending, -1 for descending
     */
    private void aggregateNumbersInNewData(String fieldNameKey, ClusterType clusterType, String aggregationNameKey,
                                           List<Map<String, Object>> data, List<Map<String, Object>> newData,
                                           Map<String, Object> extraKeySets, int order) {
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
            BigDecimal start = null;
            BigDecimal end = null;
            switch (clusterType) {
                case STRING:
                    start = convertTextBinToNumber(newRange.get(0).toString());
                    end = convertTextBinToNumber(newRange.get(1).toString());
                    break;
                case DATE:
                case NUMBER:
                default:
                    start = (BigDecimal) newRange.get(0);
                    end = (BigDecimal) newRange.get(1);
                    break;
            }

            // traverse the old data a total of one time
            while (oldDataIndex < data.size()) {
                // retrieve the old data's field name
                BigDecimal oldBinValue = null;
                if (!clusterType.equals(ClusterType.STRING)) { // not text
                    oldBinValue = new BigDecimal(data.get(oldDataIndex).get(fieldNameKey).toString());
                } else if (clusterType.equals(ClusterType.STRING)) { // is text
                    oldBinValue = convertTextBinToNumber(data.get(oldDataIndex).get(fieldNameKey).toString());
                }

                // if old >= new start && old <= new end (for ascending)
                if (oldBinValue.compareTo(start) != (-1 * order)
                        && oldBinValue.compareTo(end) != (1 * order)) {

                    // check extra keys
                    for (String key : extraKeySets.keySet()) {
                        ((Set) extraKeySets.get(key)).add(data.get(oldDataIndex).get(key));
                    }

                    // accumulate aggregate numbers
                    BigDecimal oldAgg = new BigDecimal(data.get(oldDataIndex).get(aggregationNameKey).toString());
                    currAgg = currAgg.add(oldAgg);

                    // move to the next data in the old results
                    oldDataIndex++;
                } else if (oldBinValue.compareTo(end) == (1 * order)) { // old > new end i.e. old bin moved past this curr new bin (for ascending)
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
     * Traverses the original data and puts it in the correct bins in the new data for the latlon clusterType.
     * @param fieldNameKeys the keys that correspond with the field names
     * @param aggregationNameKey aggregation name key given by the cluster clause
     * @param data the original result data
     * @param newData the newly clustered data
     * @param extraKeySets the extra key sets map for storing additional keys
     */
    private void aggregateLatLonNumbersInNewData(List<String> fieldNameKeys, String aggregationNameKey, List<Map<String, Object>> data,
                                                 List<Map<String, Object>> newData, Map<String, Object> extraKeySets) {
        Iterator<Map<String, Object>> newDataIter = newData.iterator();
        while (newDataIter.hasNext()) {

            // reset the extra keys for each new bin
            for (String key : extraKeySets.keySet()) {
                extraKeySets.put(key, new HashSet<>());
            }

            // determine the boundaries and aggregated count for this bin
            Map currNewBin = newDataIter.next();
            BigDecimal currAgg = new BigDecimal("0");

            // get new lat range
            String latFieldKey = fieldNameKeys.get(0);
            ArrayList newLatRange = (ArrayList) currNewBin.get(latFieldKey);
            BigDecimal startLat = (BigDecimal) newLatRange.get(0);
            BigDecimal endLat = (BigDecimal) newLatRange.get(1);

            // get new long range
            String longFieldKey = fieldNameKeys.get(1);
            ArrayList newLongRange = (ArrayList) currNewBin.get(longFieldKey);
            BigDecimal startLong = (BigDecimal) newLongRange.get(0);
            BigDecimal endLong = (BigDecimal) newLongRange.get(1);

            Iterator<Map<String, Object>> dataIter = data.iterator();
            while (dataIter.hasNext()) {

                Map<String, Object> currData = dataIter.next();
                BigDecimal oldLatValue = new BigDecimal(currData.get(latFieldKey).toString());
                BigDecimal oldLongValue = new BigDecimal(currData.get(longFieldKey).toString());

                // if old >= new start && old <= new end (for lat and long)
                if (oldLatValue.compareTo(startLat) != -1
                        && oldLatValue.compareTo(endLat) != 1
                        && oldLongValue.compareTo(startLong) != -1
                        && oldLongValue.compareTo(endLong) != 1) {

                    // check extra keys
                    for (String key : extraKeySets.keySet()) {
                        // check extra keys
                        if (currData.get(key) != null) {
                            ((Set) extraKeySets.get(key)).add(currData.get(key));
                        }
                    }

                    // accumulate aggregate numbers
                    currAgg = currAgg.add(new BigDecimal(1));
                    dataIter.remove();
                }
            }

            // update the extra key sets
            for (String key : extraKeySets.keySet()) {
                currNewBin.put(key, extraKeySets.get(key));
            }

            // update the aggregated count
            currNewBin.put(aggregationNameKey, currAgg);
        }
    }

    /**
     * Converts the incoming text bin e.g. "a", "bd" into a number.
     *
     * @param text the bin to be converted
     * @return a BigDecimal corresponding to the index of the bin
     */
    private BigDecimal convertTextBinToNumber(String text) {
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

    private ClusterType retrieveClusterType(String type) {
        switch(type) {
            case "number":
                return ClusterType.NUMBER;
            case "keyword":
            case "text":
                return ClusterType.STRING;
            case "date":
                return ClusterType.DATE;
            case "latlon":
                return ClusterType.LAT_LON;
            default:
                return null;
        }
    }

    /**
     * Takes a bin for clustering and a range of dates and updates the bin with pretty ranges for those dates.
     *
     * @param bin the bin to be updated with pretty date ranges
     * @param range the range of dates as BigDecimals
     */
    private void storePrettyDate(Map bin, ArrayList<BigDecimal> range) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");
        ArrayList prettyDateRange = new ArrayList();
        for (int i = 0; i < 2; i++) {
            BigDecimal date = range.get(i);
            ZonedDateTime zonedDateTime = DateUtil.transformMillisecondsToDate(Long.parseLong(date.toString()));
            String prettyDate = formatter.format(zonedDateTime);
            prettyDateRange.add(prettyDate);
        }
        bin.put(PRETTY_DATETIME_KEY, prettyDateRange);
    }
}
