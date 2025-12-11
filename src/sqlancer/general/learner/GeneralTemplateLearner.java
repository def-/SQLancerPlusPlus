package sqlancer.general.learner;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONObject;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import sqlancer.FeatureLearner;
import sqlancer.general.GeneralLearningManager.SQLFeature;
import sqlancer.general.GeneralProvider.GeneralGlobalState;

public class GeneralTemplateLearner implements FeatureLearner {

    private static final String CHAT_URL = "https://api.openai.com/v1/chat/completions";
    private final String apiKey = System.getenv("OPENAI_API_KEY");

    private String rawFragments = "";
    private final GeneralGlobalState globalState;
    private final SQLFeature feature;
    private final String template;
    private final String variables;
    private final String systemPrompt;
    private final String topic;

    // optional things
    private String examples = "";

    @Override
    public void learn() {
        String response = "";
        String reference = "";

        // get the documentation reference
        if (globalState.getDbmsSpecificOptions().useRetrievalAugmentation) {
            reference = retrieveSummarization();
        }
        response = getDialectFromReference(reference);

        rawFragments = process(response);
    }

    public GeneralTemplateLearner(GeneralGlobalState globalState, SQLFeature feature, String template, String variables,
            String systemPrompt, String topic) {
        this.globalState = globalState;
        this.feature = feature;
        this.template = template;
        this.variables = variables;
        this.systemPrompt = systemPrompt;
        this.topic = topic;
    }

    @Override
    public void update() {
    }

    public String process(String response) {
        String content = response;

        // get rows with the placeholders and alternatives
        StringBuilder processed = new StringBuilder();
        String[] rows = content.split("\n");
        for (int i = 1; i < rows.length - 1; i++) {
            // validate if it's a placeholder&alternative row
            processed.append(rows[i]);
            processed.append("\n");
        }
        System.out.println(processed.toString());
        return processed.toString();
    }

    // private String retrieveURL() {
    // String doc_url = "";
    // String model = "gpt-4o-mini";
    // String system = "This GPT acts as a web crawler and assistant to help users
    // find the correct URL or specific documentation related to Database Management
    // Systems (DBMS). It should efficiently search the web and provide accurate,
    // relevant URLs based on the user's query. The assistant will maintain a
    // professional and formal tone, ensuring that users receive the most pertinent
    // information. If the initial query is too broad or unclear, the assistant will
    // ask for further clarification to narrow down the search. The responses should
    // be concise, returning only the URL without any explanation.";
    // String user = String.format("URL for %s %s",
    // globalState.getDbmsNameForLearning(), stmt_type);
    // try {
    // doc_url = getChatGPTResponse(model, system, user);
    // } catch (IOException e) {
    // e.printStackTrace();
    // }
    // try {
    // doc_url = parseAndGetGPTContent(doc_url);
    // } catch (Exception e) {
    // e.printStackTrace();
    // }
    // return doc_url;
    // }

    private String retrieveSummarization() {
        try {
            // assume that the python environment is set up
            List<String> command = Arrays.asList("python3", "src/chat.py", "--dbms",
                    globalState.getDbmsNameForLearning(), "--feature", feature.toString(), "--topic", topic, "--learn");
            System.out.println("Execute " + command);
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.redirectErrorStream(true);
            Process p = pb.start();
            BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(p.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }

            int exitCode = p.waitFor();
            if (exitCode != 0) {
                System.out.println("Error: " + exitCode);
                System.err.println(sb.toString());
                return null;
            } else {
                return sb.toString();
            }

        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            return null;
        }
    }

    private String getDialectFromReference(String reference) {
        String response = "";
        String model = "gpt-4o";
        String system = systemPrompt;
        StringBuilder sb = new StringBuilder();
        sb.append("DBMS: ");
        sb.append(globalState.getDbmsNameForLearning());
        sb.append("\n\n");
        sb.append("Sketch:\n");
        sb.append(template);
        if (variables != "") {
            sb.append("\n");
            sb.append("Available variables and their descriptions:\n");
            sb.append(variables);
        }
        if (examples != "") {
            sb.append("\n");
            sb.append("Examples:\n");
            sb.append(examples);
        }
        sb.append("\n");
        sb.append("Reference: ");
        sb.append(reference);
        // String user = String.format("DBMS: %s\n" + //
        // "Template: %s\n", globalState.getDbmsNameForLearning(), template);
        // if (variables != "") {
        // user += "Available variables and their descriptions:\n" + variables;
        // }
        // if (examples != "") {
        // user += "Examples:\n" + examples;
        // }
        // user += "Reference: " + reference;
        // user += "Note: Please do not call functions in DBMS that would bring
        // randomness to the query. Function calls should be deterministic.";
        String user = sb.toString();
        if (globalState.getOptions().debugLogs()) {
            System.out.println("User prompt:");
            System.out.println(user);
        }
        try {
            response = getChatGPTResponse(model, system, user);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            response = parseAndGetGPTContent(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }

    private String getChatGPTResponse(String model, String system, String user) throws IOException {
        if (apiKey == null) {
            System.err.println("OPENAI_API_KEY environment variable not set");
            return "";
        }
        OkHttpClient client = new OkHttpClient.Builder().connectTimeout(60, TimeUnit.SECONDS)
                .readTimeout(60, TimeUnit.SECONDS).build();

        JSONObject json = new JSONObject();

        json.put("model", model);

        // manage the messages
        JSONArray messages = new JSONArray();

        JSONObject message1 = new JSONObject();
        message1.put("role", "system");
        message1.put("content", system);

        JSONObject message2 = new JSONObject();
        message2.put("role", "user");
        message2.put("content", user);
        messages.put(message1);
        messages.put(message2);

        json.put("messages", messages);

        RequestBody body = RequestBody.create(json.toString(), MediaType.parse("application/json; charset=utf-8"));

        Request request = new Request.Builder().url(CHAT_URL).post(body).addHeader("Content-Type", "application/json")
                .addHeader("Authorization", "Bearer " + apiKey).build();
        Response response = client.newCall(request).execute();
        return response.body().string();
    }

    private String parseAndGetGPTContent(String response) {
        JSONObject json = new JSONObject(response);
        JSONArray choices = json.getJSONArray("choices");
        JSONObject choice = choices.getJSONObject(0);
        JSONObject message = choice.getJSONObject("message");
        return message.getString("content");
    }

    public String getFragments() {
        return rawFragments;
    }

    public void setExamples(String examples) {
        this.examples = examples;
    }

    public String getExamples() {
        return examples;
    }
}
