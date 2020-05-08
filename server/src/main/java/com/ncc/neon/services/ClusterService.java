package com.ncc.neon.services;

import com.ncc.neon.models.queries.ClusterClause;
import com.ncc.neon.models.results.FieldType;
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
        FieldType fieldType = retrieveFieldType(this.clusterClause.getFieldType());

        List<List<Object>> clusters = this.clusterClause.getClusters();

        // establish keys
        String fieldNameKey = this.clusterClause.getFieldNames().get(0);
        String aggregationNameKey = this.clusterClause.getAggregationName();

        List<Map<String, Object>> data = tabularQueryResult.getData();

        // determine count
        // default counts
        BigDecimal count = new BigDecimal(this.clusterClause.getCount());
        if (count.compareTo(new BigDecimal(0)) == 0) {
            switch (fieldType) {
                case KEYWORD:
                case TEXT:
                    count = new BigDecimal(DEFAULT_TEXT_COUNT);
                    break;
                case DATETIME:
                    count = new BigDecimal(DEFAULT_DATETIME_COUNT);
                    break;
                case DECIMAL:
                default:
                    count = new BigDecimal(DEFAULT_NUMBER_COUNT);
                    break;
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
        if (!fieldType.equals(FieldType.KEYWORD) && !(fieldType.equals(FieldType.TEXT))) {
            firstGroup = new BigDecimal(first.get(fieldNameKey).toString());
            lastGroup = new BigDecimal(last.get(fieldNameKey).toString());
        } else {
            firstGroup = convertTextBinToNumber(first.get(fieldNameKey).toString());
            lastGroup = convertTextBinToNumber(last.get(fieldNameKey).toString());
        }
        int order = firstGroup.compareTo(lastGroup) == 1 ? -1 : 1;

        // find new clusters
        List<Map<String, Object>> newData = getNewDataBins(fieldNameKey, fieldType, firstGroup, lastGroup, count, clusters, order);

        // take care of extra keys that may be present
        Map<String, Object> extraKeySets = getExtraKeySetsMap(fieldNameKey, aggregationNameKey, data);

        // aggregate data into the new clusters
        aggregateNumbersInNewData(fieldNameKey, fieldType, aggregationNameKey, data, newData, extraKeySets, order);

        // reset clusterclause
        this.clusterClause = null;

        // return new clustered results
        return new TabularQueryResult(newData);
    }

    /**
     * Calculates the spacing between the different bins in the new data set results.
     *
     * @param fieldNameKey the key that corresponds with the field name
     * @param fieldType the fieldtype of the aggregation
     * @param firstGroup the first group of data in the original result data
     * @param lastGroup the last group of data in the original result data
     * @param count how many bins there should be in the new data set
     * @param clusters clusters specified by the cluster clause if available
     * @param order 1 for ascending, -1 for descending
     * @return the new data set ready to be filled
     */
    private List<Map<String, Object>> getNewDataBins(String fieldNameKey, FieldType fieldType, BigDecimal firstGroup, BigDecimal lastGroup,
                                                     BigDecimal count, List<List<Object>> clusters, int order) {
        List<Map<String, Object>> newData = new ArrayList<>();
        if (clusters == null) {
            BigDecimal orderModifier = new BigDecimal(order);
            BigDecimal gap = (lastGroup.subtract(firstGroup)).divide(count);
            BigDecimal step = null;
            switch (fieldType) {
                case KEYWORD:
                case TEXT:
                    gap = gap.setScale(0, RoundingMode.DOWN);
                    step = new BigDecimal(DEFAULT_TEXT_STEP).multiply(orderModifier);
                    break;
                case DATETIME:
                    gap = gap.setScale(0, RoundingMode.DOWN);
                    step = new BigDecimal(DEFAULT_DATETIME_STEP).multiply(orderModifier);
                    break;
                case DECIMAL:
                default:
                    step = new BigDecimal(DEFAULT_NUMBER_STEP).multiply(orderModifier);
                    break;
            }
            BigDecimal currentBin = firstGroup;
            for (int i = 0; i < count.intValue(); i++) {
                LinkedHashMap map = new LinkedHashMap<>();
                ArrayList range = new ArrayList<>();
                if (!fieldType.equals(FieldType.KEYWORD) && !(fieldType.equals(FieldType.TEXT))) { // not text

                } else if (fieldType.equals(FieldType.KEYWORD) || (fieldType.equals(FieldType.TEXT))) { // is text

                }
                switch (fieldType) {
                    case KEYWORD:
                    case TEXT:
                        if (i != 0) {
                            range.add(convertNumberToTextBin(currentBin = currentBin.add(step)));
                        } else {
                            range.add(convertNumberToTextBin(currentBin));
                        }
                        range.add(convertNumberToTextBin(currentBin.add(gap)));
                        break;
                    case DATETIME:
                        if (i != 0) {
                            range.add(currentBin.add(step));
                        } else {
                            range.add(currentBin);
                        }
                        range.add(currentBin.add(gap));
                        storePrettyDate(map, range);
                        break;
                    case DECIMAL:
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
                if (!fieldType.equals(FieldType.KEYWORD) && !(fieldType.equals(FieldType.TEXT))) { // not text
                    range.add(new BigDecimal(clusters.get(i).get(0).toString()));
                    range.add(new BigDecimal(clusters.get(i).get(1).toString()));
                } else if (fieldType.equals(FieldType.KEYWORD) || (fieldType.equals(FieldType.TEXT))) { // is text
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
            if (!key.equals(aggregationNameKey) && !key.equals(fieldNameKey) && !key.equals(PRETTY_DATETIME_KEY)) {
                extraKeySets.put(key, new HashSet<>());
            }
        }
        return extraKeySets;
    }

    /**
     * Traverses the original data and puts it in the correct bins in the new data.
     * @param fieldNameKey the key that corresponds with the field name
     * @param fieldType the fieldtype of the aggregation
     * @param aggregationNameKey the key that corresponds with the overall counts of each bin
     * @param data the original result data
     * @param newData the newly clustered data
     * @param extraKeySets the extra key sets map for storing additional keys
     * @param order 1 for ascending, -1 for descending
     */
    private void aggregateNumbersInNewData(String fieldNameKey, FieldType fieldType, String aggregationNameKey,
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
            switch (fieldType) {
                case KEYWORD:
                case TEXT:
                    start = convertTextBinToNumber(newRange.get(0).toString());
                    end = convertTextBinToNumber(newRange.get(1).toString());
                    break;
                case DATETIME:
                case DECIMAL:
                default:
                    start = (BigDecimal) newRange.get(0);
                    end = (BigDecimal) newRange.get(1);
                    break;
            }

            // traverse the old data a total of one time
            while (oldDataIndex < data.size()) {
                // retrieve the old data's field name
                BigDecimal oldBinValue = null;
                if (!fieldType.equals(FieldType.KEYWORD) && !(fieldType.equals(FieldType.TEXT))) { // not text
                    oldBinValue = new BigDecimal(data.get(oldDataIndex).get(fieldNameKey).toString());
                } else if (fieldType.equals(FieldType.KEYWORD) || (fieldType.equals(FieldType.TEXT))) { // is text
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

    private FieldType retrieveFieldType(String type) {
        switch (type) {
            case "boolean":
                return FieldType.BOOLEAN;
            case "byte":
            case "integer":
            case "long":
            case "short":
                return FieldType.INTEGER;
            case "date":
                return FieldType.DATETIME;
            case "number":
            case "double":
            case "float":
            case "half_float":
            case "scaled_float":
                return FieldType.DECIMAL;
            case "geo-point":
            case "geo-shape":
                return FieldType.GEO;
            case "keyword":
                return FieldType.KEYWORD;
            case "nested":
            case "object":
                return FieldType.OBJECT;
            case "text":
            default:
                return FieldType.TEXT;
        }
    }

    /**
     * Takes a bin for clustering and a range of dates and updates the bin with pretty ranges for those dates.
     *
     * @param bin the bin to be updated with pretty date ranges
     * @param range the range of dates as BigDecimals
     */
    private void storePrettyDate(Map bin, ArrayList<BigDecimal> range) {
        System.out.println();
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
