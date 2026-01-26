## ShQveL component

You can also use LLM to synthesize dialect-specific SQL fragments to enhance the test generator. You need two additional arguments to enable it:

- `--enable-extra-features`: enable the external feature fragments
- `--enable-learning`: enable the learning of the feature fragments

Requirements:
- [OpenAI API Key](https://platform.openai.com/docs/api-reference/authentication)
- Python 3.12 or above

```bash
# Install the requirements for documentation retrieval
pip install -r requirements.txt
```

e.g., to test DuckDB with learning and extra features enabled, you can run:

```bash
OPENAI_API_KEY=YOUR_OPENAI_API_KEY java -jar target/sqlancer-2.0.0.jar --use-reducer --enable-extra-features --enable-learning --num-threads 1 --num-tries 200 general --database-engine duckdb --oracle WHERE
```