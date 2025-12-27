# Guardian Rasa Bot

This directory defines the small Rasa assistant that powers the guardian Q&A on the dashboard.
It classifies the intents that guardians ask about (balance, receipts, exams, greetings) so the Flask
handler can gather the latest context-aware data and respond with the correct school-specific values.

## Local training
1. Create a virtual environment (optional, but recommended) and activate it.
2. From the repo root, install the Python dependencies:

```bash
pip install -r requirements.txt
```

3. Train the Rasa model:

```bash
cd rasa_bot
rasa train
```

This produces a new model under `rasa_bot/models/`.

## Running the bot server
After training, start the REST channel so the Flask app can query it:

```bash
cd rasa_bot
rasa run --enable-api --port 5005
```

The guardian AI endpoint will call `{{ RASA_URL }}/model/parse` (see `utils/rasa_bot.py`). The
default `RASA_URL` is `http://localhost:5005`, but you can override it via the
`RASA_URL` environment variable before launching the web app.
