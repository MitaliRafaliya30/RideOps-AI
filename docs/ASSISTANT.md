# AI Assistant (M14)

`assistant/`: ask the RideOps Gold layer questions in plain English, including rider safety.
A LangGraph ReAct agent (via OpenRouter) writes SQL against Databricks, or calls a domain tool.

```
cd assistant
<venv>\Scripts\streamlit run app.py      # chat UI, shows the SQL / tool behind every answer
python eval_safety.py                     # 12-question evaluation, writes eval_results.json
```

## How it answers

| Question type | Path | Why |
|---|---|---|
| "Why was ride 01bd... flagged?" | **`explain_ride`** domain tool | Fixed, reviewed SQL: alerts with reasons and recommended action, risk minute by minute, ground-truth outcome |
| "What happened on 2026-05-08?" / "latest" | **`safety_briefing`** domain tool | Fixed SQL: volume, alert rate, alerts by type, top zones, recall, latest data quality run |
| Anything else | Text-to-SQL on 21 Gold tables | Flexible; guided by a semantic layer in the prompt |

**Semantic layer (`src/prompts.py`).** Table grains, join keys and metric definitions identical to
`ml/metrics.py`, the model card and the dashboard (recall, false alarms per 1,000, precision), plus honesty
rules: live rides have no ground truth (never a recall); training-week numbers are optimistic and must be
computed from the tables, then caveated; all GPS is simulated.

## Safety and governance

- **Read-only, Gold-only guardrails** on every executed query (`src/guardrails.py`): SELECT/WITH only, no write
  keyword anywhere (comments stripped first), no Bronze/Silver, no table outside the allow-list, LIMIT 1000.
- **Domain tools are injection-proof:** inputs must match a strict pattern (UUID ride id, ISO date) before
  reaching SQL, and values are bound as query parameters, never pasted into the SQL text.
- **Transparency:** the UI shows the SQL or tool call behind every answer.
- **Model is a setting** (`LLM_MODEL` in `.env`), default Claude Haiku.

## Evaluation (`eval_safety.py`)

12 golden questions. Numeric answers are graded against values **computed from Gold at eval time** (never
hard-coded, so they cannot go stale). Questions also check required caveats, required tool use, and a refused
write request.

| Run | Model | Result | Notes |
|---|---|---|---|
| 1 | Claude Haiku | 10/12 graded, stopped by OpenRouter credits (402) | Q11 answer was correct ("no failed checks"); the grader only looked for "0", fixed |
| 2 | Nemotron 3 Ultra (free) | **11/12**, avg 24 s, 0.9 tool calls per question | Q3 quoted the holdout recall from the instructions instead of computing it from the table |
| 2b | Nemotron 3 Ultra (free) | Q3 re-run after a prompt fix: **pass** | "compute from the tables first, then caveat" |

What the eval caught is the point: one grader bug and one real reasoning slip (an LLM preferring a number in
its instructions over the data), both fixed and re-checked. A free model was ~4x slower than Claude Haiku and
another free model (Gemma) failed to call tools at all.

## Tests (no LLM needed)

`tests/test_assistant.py`: guardrails reject writes, hidden writes after comments, Bronze/Silver and unknown
schemas; domain tools reject injection attempts and invalid dates, bind parameters, and produce the expected
story for a caught crash, a quiet live ride, and a day briefing.
