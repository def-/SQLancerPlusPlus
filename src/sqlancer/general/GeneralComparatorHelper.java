package sqlancer.general;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.general.GeneralProvider.GeneralGlobalState;
import sqlancer.general.gen.AutoIndexSelectHelper;
import sqlancer.general.gen.AutoIndexSelectHelper.IndexedSelectResult;

/**
 * Helper class that extends ComparatorHelper functionality with
 * auto-index-selects support for the General provider.
 */
public final class GeneralComparatorHelper {

    private GeneralComparatorHelper() {
    }

    /**
     * Gets the result set first column as string, optionally routing
     * through an indexed view if auto-index-selects is enabled.
     *
     * @param queryString The SELECT query to execute
     * @param errors Expected errors to ignore
     * @param state The global state
     * @return List of string results from the first column
     * @throws SQLException If query execution fails
     */
    public static List<String> getResultSetFirstColumnAsString(
            String queryString,
            ExpectedErrors errors,
            GeneralGlobalState state) throws SQLException {

        if (AutoIndexSelectHelper.isEnabled(state)) {
            return executeWithIndexedView(queryString, errors, state);
        } else {
            return ComparatorHelper.getResultSetFirstColumnAsString(queryString, errors, state);
        }
    }

    /**
     * Gets combined result set from three queries, optionally routing
     * through indexed views if auto-index-selects is enabled.
     *
     * @param firstQueryString First SELECT query
     * @param secondQueryString Second SELECT query
     * @param thirdQueryString Third SELECT query
     * @param combinedString Output list for combined query strings
     * @param asUnion Whether to combine as UNION ALL
     * @param state The global state
     * @param errors Expected errors to ignore
     * @return Combined result set
     * @throws SQLException If query execution fails
     */
    public static List<String> getCombinedResultSet(
            String firstQueryString,
            String secondQueryString,
            String thirdQueryString,
            List<String> combinedString,
            boolean asUnion,
            GeneralGlobalState state,
            ExpectedErrors errors) throws SQLException {

        if (AutoIndexSelectHelper.isEnabled(state)) {
            return getCombinedResultSetWithIndexedViews(
                    firstQueryString, secondQueryString, thirdQueryString,
                    combinedString, asUnion, state, errors);
        } else {
            return ComparatorHelper.getCombinedResultSet(
                    firstQueryString, secondQueryString, thirdQueryString,
                    combinedString, asUnion, state, errors);
        }
    }

    private static List<String> executeWithIndexedView(
            String queryString,
            ExpectedErrors errors,
            GeneralGlobalState state) throws SQLException {

        IndexedSelectResult result = AutoIndexSelectHelper.executeSelectThroughIndexedView(
                queryString, state, errors);

        if (!result.isSuccess()) {
            if (errors.errorIsExpected(result.getErrorMessage())) {
                throw new IgnoreMeException();
            }
            throw new SQLException(result.getErrorMessage());
        }

        return result.getResults();
    }

    private static List<String> getCombinedResultSetWithIndexedViews(
            String firstQueryString,
            String secondQueryString,
            String thirdQueryString,
            List<String> combinedString,
            boolean asUnion,
            GeneralGlobalState state,
            ExpectedErrors errors) throws SQLException {

        List<String> secondResultSet;
        if (asUnion) {
            // For UNION queries, create a view for the combined query
            String unionString = firstQueryString + " UNION ALL " + secondQueryString + " UNION ALL "
                    + thirdQueryString;
            combinedString.add(unionString);
            secondResultSet = executeWithIndexedView(unionString, errors, state);
        } else {
            // Execute each query through its own indexed view
            secondResultSet = new ArrayList<>();
            secondResultSet.addAll(executeWithIndexedView(firstQueryString, errors, state));
            secondResultSet.addAll(executeWithIndexedView(secondQueryString, errors, state));
            secondResultSet.addAll(executeWithIndexedView(thirdQueryString, errors, state));
            combinedString.add(firstQueryString);
            combinedString.add(secondQueryString);
            combinedString.add(thirdQueryString);
        }
        return secondResultSet;
    }
}
