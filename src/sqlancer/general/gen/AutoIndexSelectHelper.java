package sqlancer.general.gen;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.general.GeneralProvider.GeneralGlobalState;

/**
 * Helper class that creates an indexed view for a SELECT query, executes
 * the query through the view, and then cleans up. This allows testing
 * the database's behavior when queries go through indexed views.
 *
 * For each SELECT query:
 * 1. Creates a view with the query body
 * 2. Creates an index on that view
 * 3. Executes SELECT from the view
 * 4. Drops the view
 */
public final class AutoIndexSelectHelper {

    private static final int MAX_ROWS_LIMIT = 100000;

    private AutoIndexSelectHelper() {
    }

    /**
     * Result of executing a SELECT through an indexed view.
     */
    public static class IndexedSelectResult {
        private final List<String> results;
        private final boolean success;
        private final String errorMessage;

        public IndexedSelectResult(List<String> results) {
            this.results = results;
            this.success = true;
            this.errorMessage = null;
        }

        public IndexedSelectResult(String errorMessage) {
            this.results = null;
            this.success = false;
            this.errorMessage = errorMessage;
        }

        public List<String> getResults() {
            return results;
        }

        public boolean isSuccess() {
            return success;
        }

        public String getErrorMessage() {
            return errorMessage;
        }
    }

    /**
     * Executes a SELECT query through an indexed view.
     *
     * @param selectQuery The original SELECT query
     * @param globalState The global state
     * @param errors Expected errors to ignore
     * @return The result of the query execution
     */
    public static IndexedSelectResult executeSelectThroughIndexedView(
            String selectQuery,
            GeneralGlobalState globalState,
            ExpectedErrors errors) {

        String viewUuid = UUID.randomUUID().toString().replace("-", "");
        String viewName = "v" + viewUuid;
        String indexName = "idx" + viewUuid;

        // Determine number of columns by analyzing the query or using a simple approach
        Integer numColumns = null;
        try {
            numColumns = getColumnCount(selectQuery, globalState);
        } catch (SQLException e) {
            // If we can't determine column count, we'll create view without explicit columns
        }

        String createViewSql = generateCreateViewSql(selectQuery, viewName, numColumns, globalState);
        String createIndexSql = generateCreateIndexSql(viewName, indexName, numColumns, globalState);
        String viewSelectSql = generateViewSelectSql(viewName, numColumns);
        String dropViewSql = generateDropViewSql(viewName, globalState);

        List<String> results = new ArrayList<>();

        // Log the original query and generated SQL
        logOriginalQuery(globalState, selectQuery);

        try (Statement stmt = globalState.getConnection().createStatement()) {
            // Log and create view
            logGeneratedSql(globalState, createViewSql);
            try {
                stmt.execute(createViewSql);
            } catch (SQLException e) {
                if (!errors.errorIsExpected(e.getMessage())) {
                    // View creation failed, fall back to direct query
                    return executeDirectQuery(selectQuery, globalState, errors);
                }
                return executeDirectQuery(selectQuery, globalState, errors);
            }

            // Log and create index
            logGeneratedSql(globalState, createIndexSql);
            try {
                stmt.execute(createIndexSql);
            } catch (SQLException e) {
                // Index creation failure is not critical, continue with view query
                // Some databases may not support indexes on views
            }

            // Log and execute select from view
            logGeneratedSql(globalState, viewSelectSql);
            stmt.setMaxRows(MAX_ROWS_LIMIT);
            try (ResultSet rs = stmt.executeQuery(viewSelectSql)) {
                while (rs.next()) {
                    String value = rs.getString(1);
                    if (value != null) {
                        value = value.replaceAll("[\\.]0+$", "");
                    }
                    results.add(value);
                }
            } catch (SQLException e) {
                if (!errors.errorIsExpected(e.getMessage())) {
                    // Query failed, try to drop view and return error
                    tryDropView(stmt, dropViewSql, globalState);
                    return new IndexedSelectResult(e.getMessage());
                }
                tryDropView(stmt, dropViewSql, globalState);
                return new IndexedSelectResult(e.getMessage());
            }

            // Drop view
            tryDropView(stmt, dropViewSql, globalState);

            // If we hit the limit, return error to skip comparison
            if (results.size() == MAX_ROWS_LIMIT) {
                return new IndexedSelectResult("Row limit exceeded");
            }
            return new IndexedSelectResult(results);
        } catch (SQLException e) {
            return new IndexedSelectResult(e.getMessage());
        }
    }

    private static void tryDropView(Statement stmt, String dropViewSql, GeneralGlobalState globalState) {
        try {
            logGeneratedSql(globalState, dropViewSql);
            stmt.execute(dropViewSql);
        } catch (SQLException ignored) {
            // Ignore drop errors
        }
    }

    /**
     * Logs the generated SQL to both the state log and the current logger output.
     */
    private static void logGeneratedSql(GeneralGlobalState globalState, String sql) {
        globalState.getState().logStatement(sql);
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent("-- [auto-index-selects] " + sql);
        }
    }

    /**
     * Logs the original query that will be executed through an indexed view.
     */
    public static void logOriginalQuery(GeneralGlobalState globalState, String originalQuery) {
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent("-- [auto-index-selects] Original query: " + originalQuery);
        }
    }

    private static IndexedSelectResult executeDirectQuery(
            String selectQuery,
            GeneralGlobalState globalState,
            ExpectedErrors errors) {
        List<String> results = new ArrayList<>();
        try (Statement stmt = globalState.getConnection().createStatement()) {
            stmt.setMaxRows(MAX_ROWS_LIMIT);
            try (ResultSet rs = stmt.executeQuery(selectQuery)) {
                while (rs.next()) {
                    String value = rs.getString(1);
                    if (value != null) {
                        value = value.replaceAll("[\\.]0+$", "");
                    }
                    results.add(value);
                }
            }
            // If we hit the limit, return error to skip comparison
            if (results.size() == MAX_ROWS_LIMIT) {
                return new IndexedSelectResult("Row limit exceeded");
            }
            return new IndexedSelectResult(results);
        } catch (SQLException e) {
            if (errors.errorIsExpected(e.getMessage())) {
                return new IndexedSelectResult(e.getMessage());
            }
            return new IndexedSelectResult(e.getMessage());
        }
    }

    /**
     * Attempts to determine the number of columns in a SELECT query result.
     */
    private static Integer getColumnCount(String selectQuery, GeneralGlobalState globalState) throws SQLException {
        // Try to execute the query with LIMIT 0 to get column metadata
        String limitedQuery = selectQuery;
        if (!selectQuery.toUpperCase().contains("LIMIT")) {
            limitedQuery = selectQuery + " LIMIT 0";
        }

        try (Statement stmt = globalState.getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(limitedQuery)) {
            return rs.getMetaData().getColumnCount();
        }
    }

    /**
     * Generates CREATE VIEW SQL with column aliasing to avoid ambiguity.
     */
    private static String generateCreateViewSql(
            String selectQuery,
            String viewName,
            Integer numColumns,
            GeneralGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE VIEW ");
        sb.append(viewName);

        if (numColumns != null && numColumns > 0) {
            sb.append("(");
            for (int i = 1; i <= numColumns; i++) {
                if (i > 1) {
                    sb.append(", ");
                }
                sb.append("a").append(i);
            }
            sb.append(")");
        }

        sb.append(" AS ");
        sb.append(selectQuery);
        return sb.toString();
    }

    /**
     * Generates CREATE INDEX SQL for the view.
     */
    private static String generateCreateIndexSql(
            String viewName,
            String indexName,
            Integer numColumns,
            GeneralGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE INDEX ");
        sb.append(indexName);
        sb.append(" ON ");
        sb.append(viewName);
        sb.append("(");

        if (numColumns != null && numColumns > 0) {
            for (int i = 1; i <= numColumns; i++) {
                if (i > 1) {
                    sb.append(", ");
                }
                sb.append("a").append(i);
            }
        } else {
            // Default index on first column if we don't know the schema
            sb.append("a1");
        }

        sb.append(")");
        return sb.toString();
    }

    /**
     * Generates SELECT SQL from the view.
     */
    private static String generateViewSelectSql(String viewName, Integer numColumns) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");

        if (numColumns != null && numColumns > 0) {
            for (int i = 1; i <= numColumns; i++) {
                if (i > 1) {
                    sb.append(", ");
                }
                sb.append("a").append(i);
            }
        } else {
            sb.append("*");
        }

        sb.append(" FROM ");
        sb.append(viewName);
        return sb.toString();
    }

    /**
     * Generates DROP VIEW SQL.
     */
    private static String generateDropViewSql(String viewName, GeneralGlobalState globalState) {
        return "DROP VIEW " + viewName;
    }

    /**
     * Checks if auto-index-selects is enabled in the global state.
     */
    public static boolean isEnabled(GeneralGlobalState globalState) {
        return globalState.getDbmsSpecificOptions().autoIndexSelects;
    }

    /**
     * Result of executing a boolean count query through an indexed view.
     */
    public static class BooleanCountResult {
        private final int count;
        private final boolean success;
        private final String errorMessage;

        public BooleanCountResult(int count) {
            this.count = count;
            this.success = true;
            this.errorMessage = null;
        }

        public BooleanCountResult(String errorMessage, boolean isError) {
            this.count = -1;
            this.success = !isError;
            this.errorMessage = errorMessage;
        }

        public int getCount() {
            return count;
        }

        public boolean isSuccess() {
            return success;
        }

        public String getErrorMessage() {
            return errorMessage;
        }
    }

    /**
     * Executes a SELECT query through an indexed view and counts rows where
     * the first column is TRUE.
     *
     * @param selectQuery The original SELECT query
     * @param globalState The global state
     * @param errors Expected errors to ignore
     * @return The count of rows where first column is TRUE
     */
    public static BooleanCountResult executeBooleanCountThroughIndexedView(
            String selectQuery,
            GeneralGlobalState globalState,
            ExpectedErrors errors) {

        String viewUuid = UUID.randomUUID().toString().replace("-", "");
        String viewName = "v" + viewUuid;
        String indexName = "idx" + viewUuid;

        Integer numColumns = null;
        try {
            numColumns = getColumnCount(selectQuery, globalState);
        } catch (SQLException e) {
            // Fall through with null
        }

        String createViewSql = generateCreateViewSql(selectQuery, viewName, numColumns, globalState);
        String createIndexSql = generateCreateIndexSql(viewName, indexName, numColumns, globalState);
        String viewSelectSql = generateViewSelectSql(viewName, numColumns);
        String dropViewSql = generateDropViewSql(viewName, globalState);

        int count = 0;

        // Log the original query and generated SQL
        logOriginalQuery(globalState, selectQuery);

        try (Statement stmt = globalState.getConnection().createStatement()) {
            // Create view
            logGeneratedSql(globalState, createViewSql);
            try {
                stmt.execute(createViewSql);
            } catch (SQLException e) {
                if (!errors.errorIsExpected(e.getMessage())) {
                    return new BooleanCountResult(e.getMessage(), true);
                }
                return new BooleanCountResult(e.getMessage(), false);
            }

            // Create index (non-critical)
            logGeneratedSql(globalState, createIndexSql);
            try {
                stmt.execute(createIndexSql);
            } catch (SQLException e) {
                // Index creation failure is not critical
            }

            // Execute select and count booleans
            logGeneratedSql(globalState, viewSelectSql);
            stmt.setMaxRows(MAX_ROWS_LIMIT);
            int rowsProcessed = 0;
            try (ResultSet rs = stmt.executeQuery(viewSelectSql)) {
                while (rs.next()) {
                    rowsProcessed++;
                    count += rs.getBoolean(1) ? 1 : 0;
                }
            } catch (SQLException e) {
                tryDropView(stmt, dropViewSql, globalState);
                if (!errors.errorIsExpected(e.getMessage())) {
                    return new BooleanCountResult(e.getMessage(), true);
                }
                return new BooleanCountResult(e.getMessage(), false);
            }

            // Drop view
            tryDropView(stmt, dropViewSql, globalState);

            // If we hit the limit, return failure to skip comparison
            if (rowsProcessed == MAX_ROWS_LIMIT) {
                return new BooleanCountResult(null, true);
            }
            return new BooleanCountResult(count);
        } catch (SQLException e) {
            return new BooleanCountResult(e.getMessage(), true);
        }
    }

    /**
     * Executes a SELECT query through an indexed view and counts total rows.
     *
     * @param selectQuery The original SELECT query
     * @param globalState The global state
     * @param errors Expected errors to ignore
     * @return The count of rows
     */
    public static BooleanCountResult executeRowCountThroughIndexedView(
            String selectQuery,
            GeneralGlobalState globalState,
            ExpectedErrors errors) {

        String viewUuid = UUID.randomUUID().toString().replace("-", "");
        String viewName = "v" + viewUuid;
        String indexName = "idx" + viewUuid;

        Integer numColumns = null;
        try {
            numColumns = getColumnCount(selectQuery, globalState);
        } catch (SQLException e) {
            // Fall through with null
        }

        String createViewSql = generateCreateViewSql(selectQuery, viewName, numColumns, globalState);
        String createIndexSql = generateCreateIndexSql(viewName, indexName, numColumns, globalState);
        String viewSelectSql = generateViewSelectSql(viewName, numColumns);
        String dropViewSql = generateDropViewSql(viewName, globalState);

        int count = 0;

        // Log the original query and generated SQL
        logOriginalQuery(globalState, selectQuery);

        try (Statement stmt = globalState.getConnection().createStatement()) {
            // Create view
            logGeneratedSql(globalState, createViewSql);
            try {
                stmt.execute(createViewSql);
            } catch (SQLException e) {
                if (!errors.errorIsExpected(e.getMessage())) {
                    return new BooleanCountResult(e.getMessage(), true);
                }
                return new BooleanCountResult(e.getMessage(), false);
            }

            // Create index (non-critical)
            logGeneratedSql(globalState, createIndexSql);
            try {
                stmt.execute(createIndexSql);
            } catch (SQLException e) {
                // Index creation failure is not critical
            }

            // Execute select and count rows
            logGeneratedSql(globalState, viewSelectSql);
            stmt.setMaxRows(MAX_ROWS_LIMIT);
            try (ResultSet rs = stmt.executeQuery(viewSelectSql)) {
                while (rs.next()) {
                    count++;
                }
            } catch (SQLException e) {
                tryDropView(stmt, dropViewSql, globalState);
                if (!errors.errorIsExpected(e.getMessage())) {
                    return new BooleanCountResult(e.getMessage(), true);
                }
                return new BooleanCountResult(e.getMessage(), false);
            }

            // Drop view
            tryDropView(stmt, dropViewSql, globalState);

            // If we hit the limit, return failure to skip comparison
            if (count == MAX_ROWS_LIMIT) {
                return new BooleanCountResult(null, true);
            }
            return new BooleanCountResult(count);
        } catch (SQLException e) {
            return new BooleanCountResult(e.getMessage(), true);
        }
    }
}
