package sqlancer.general.ast;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.general.GeneralErrorHandler;
import sqlancer.general.GeneralLearningManager.SQLFeature;
import sqlancer.general.GeneralProvider.GeneralGlobalState;
import sqlancer.general.GeneralSchema.GeneralCompositeDataType;
import sqlancer.general.GeneralSchema.GeneralDataType;
import sqlancer.general.learner.GeneralFragments;
import sqlancer.general.learner.GeneralFragments.GeneralFragmentChoice;
import sqlancer.general.learner.GeneralStringBuilder;

public class GeneralBinaryOperator implements Operator {

    private final String name;

    private static final String CONFIG_NAME = "operators.txt";
    private static final SQLFeature FEATURE = SQLFeature.OPERATOR;

    private static Map<String, GeneralCompositeDataType> operators = initOperators();
    private static GeneralBinaryOperatorFragments fragments = new GeneralBinaryOperatorFragments();

    private static final class GeneralBinaryOperatorFragments extends GeneralFragments {
        GeneralBinaryOperatorFragments() {
            super();
        }

        @Override
        public synchronized String genLearnStatement(GeneralGlobalState globalState) {
            setLearn(true);
            return getQuery(globalState).getQueryString();
        }

        @Override
        public List<String> genValStatements(GeneralGlobalState globalState, String key, String choice,
                String databaseName) {
            List<String> queries = new ArrayList<>();
            queries.add(String.format("SELECT NULL %s NULL;", choice));
            return queries;
        }

        @Override
        public String getConfigName() {
            return CONFIG_NAME;
        }

        @Override
        public String getStatementType() {
            return "OPERATOR";
        }

        @Override
        protected String getVariables() {
            return "";
        }

        @Override
        public SQLFeature getFeature() {
            return FEATURE;
        }

        @Override
        public void updateFragmentsFromLearner(GeneralGlobalState globalState) {
            super.updateFragmentsFromLearner(globalState);
            loadOperatorsFromFragments(globalState);
        }

        @Override
        protected void parseFragments(String... s) {
            String key = s[0];

            StringBuffer fmtString = new StringBuffer();
            List<GeneralFragmentVariable> vars = new ArrayList<>();
            Pattern pattern = Pattern.compile("<([^>]*)>");
            Matcher matcher = pattern.matcher(s[1]);

            String content = "";

            while (matcher.find()) {
                content = matcher.group(1);
                matcher.appendReplacement(fmtString, "%s");
                vars.add(GeneralFragmentVariable.valueOf(content.toUpperCase()));
            }
            matcher.appendTail(fmtString);

            addFragment(key, fmtString.toString(), vars);
        }
    }

    @Override
    public String getTextRepresentation() {
        return toString();
    }

    public GeneralBinaryOperator(String name) {
        this.name = name;
    }

    public static Operator getRandomByOptions(GeneralErrorHandler handler) {
        Operator op;
        String node;
        do {
            // String opName = Randomly.fromList(operators.values());
            String opName = Randomly.fromList(new ArrayList<>(operators.keySet()));
            op = new GeneralBinaryOperator(opName);
            node = "BINOP" + op.toString();
        } while (!handler.getCompositeOption(node) || !Randomly.getBooleanWithSmallProbability());
        handler.addScore(node);
        return op;
    }

    public static Operator getRandomByType(GeneralErrorHandler handler, GeneralCompositeDataType type) {
        Operator op;
        String node;
        do {
            // get all op from operators where key is type
            List<String> availOp = new ArrayList<>();
            for (String opName : operators.keySet()) {
                if (operators.get(opName).equals(type)) {
                    availOp.add(opName);
                }
            }
            // if no op found, return null
            if (availOp.isEmpty()) {
                // System.out.println("No operator found for type: " + type);
                return null;
            }
            // op = new GeneralBinaryOperator(opName);
            op = new GeneralBinaryOperator(Randomly.fromList(availOp));
            node = "BINOP" + op.toString();
        } while (!handler.getCompositeOption(node) || !Randomly.getBooleanWithSmallProbability());
        handler.addScore(node);
        return op;
    }

    public static Map<String, GeneralCompositeDataType> getOperators() {
        return operators;
    }

    public static GeneralFragments getFragments() {
        return fragments;
    }

    @Override
    public String toString() {
        return name;
    }

    public static SQLQueryAdapter getQuery(GeneralGlobalState globalState) {
        GeneralStringBuilder<GeneralBinaryOperatorFragments> sb = new GeneralStringBuilder<>(globalState, fragments);
        sb.append("CREATE TABLE TEST_TABLE (a INT, b INT);");
        sb.append("SELECT * FROM TEST_TABLE WHERE a ", 0);
        sb.append("b;");
        sb.append(String.format(" -- Hint: operator return a boolean value\n"));
        return new SQLQueryAdapter(sb.toString());
    }

    private static Map<String, GeneralCompositeDataType> initOperators() {
        Map<String, GeneralCompositeDataType> ops = new HashMap<>();
        //ops.put("$$$", GeneralDataType.BOOLEAN.get());
        ops.put("<>", GeneralDataType.BOOLEAN.get());
        ops.put("!=", GeneralDataType.BOOLEAN.get());
        ops.put("<", GeneralDataType.BOOLEAN.get());
        ops.put(">", GeneralDataType.BOOLEAN.get());
        ops.put("<=", GeneralDataType.BOOLEAN.get());
        ops.put(">=", GeneralDataType.BOOLEAN.get());
        ops.put("AND", GeneralDataType.BOOLEAN.get());
        ops.put("OR", GeneralDataType.BOOLEAN.get());
        return ops;
    }

    public static void loadOperatorsFromFragments(GeneralGlobalState globalState) {
        // load operators from fragments
        HashMap<String, GeneralCompositeDataType> ops = new HashMap<>();
        if (globalState.getOptions().debugLogs()) {
            System.out.println("Loading operators from fragments...");
            System.out.println("Operators: " + fragments.getFragments().keySet());
        }
        for (String fragment : fragments.getFragments().keySet()) {
            List<GeneralFragmentChoice> choices = fragments.getFragments().get(fragment);
            for (GeneralFragmentChoice choice : choices) {
                try {
                    ops.put(choice.toString(globalState), GeneralCompositeDataType.getByName(fragment));
                } catch (IllegalArgumentException e) {
                    System.out.println("IllegalArgumentException: " + e.getMessage());
                    System.out.println("Fragment: " + fragment);
                    System.out.println("Choice: " + choice.toString());
                    throw new AssertionError("IllegalArgumentException: " + e.getMessage());
                }
            }
        }
        operators.putAll(ops);
    }
}
