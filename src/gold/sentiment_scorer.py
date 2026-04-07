"""Groq-based investment sentiment scorer.

Scores a news article on a 0–10 scale for PEA-PME investment signal:
  0–2  hard negative (fraud, bankruptcy, AMF investigation, major loss)
  3–4  soft negative (profit warning, downgrade, lawsuit, key departure)
  5    neutral / no actionable investment signal
  6–7  soft positive (new contract, partnership, market expansion)
  8–10 hard positive (acquisition premium, record earnings, major order)

Returns: {"score": int, "reason": str}
Reason is one sentence grounded strictly in the article text.
"""

import json
import os
import re
import time

from groq import Groq, RateLimitError

MODEL = "llama-3.3-70b-versatile"

_SYSTEM_PROMPT = """You are an investment analyst assistant evaluating news articles about French PEA-PME listed companies.

Your task: assess whether a news article is positive or negative for a retail investor considering buying or holding shares in the mentioned company.

Scoring scale (0–10):
- 0–2: Hard negative — fraud, bankruptcy, AMF investigation, major financial loss, delisting risk
- 3–4: Soft negative — profit warning, key executive departure, lawsuit, analyst downgrade
- 5: Neutral — routine news, no clear investment signal, ambiguous
- 6–7: Soft positive — new contract win, partnership, market expansion, analyst upgrade
- 8–10: Hard positive — acquisition at premium, record earnings, major strategic order, turnaround confirmed

Rules:
- Score ONLY based on what is explicitly stated in the article. Do not use external knowledge.
- If the article does not contain enough information to assess, return 5.
- Respond ONLY with a JSON object: {"score": <int 0-10>, "reason": "<one sentence in English>"}
- The reason must cite specific facts from the article, not generic statements."""


def _extract_json(raw: str) -> dict:
    """Parse Groq response robustly — handles markdown fences and minor formatting issues."""
    # Strip markdown code fences if present
    cleaned = re.sub(r"```(?:json)?\s*|\s*```", "", raw).strip()
    # Extract first {...} block in case of trailing garbage
    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if not match:
        raise ValueError(f"no JSON object found in response: {raw!r}")
    return json.loads(match.group())


def score_article(title: str, summary: str, api_key: str | None = None) -> dict:
    """Score one article. Returns {"score": int, "reason": str}.

    Retries once on rate limit (waits 60s). Raises on other errors.
    """
    client = Groq(api_key=api_key or os.environ["GROQ_API_KEY"])
    user_content = f"Title: {title}\n\nSummary: {summary or '(no summary available)'}"

    for attempt in range(2):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                ],
                temperature=0.1,
                max_tokens=120,
            )
            raw = response.choices[0].message.content
            result = _extract_json(raw)
            score = int(result["score"])
            if not 0 <= score <= 10:
                raise ValueError(f"score out of range: {score}")
            return {"score": score, "reason": str(result["reason"])}

        except RateLimitError:
            if attempt == 0:
                time.sleep(60)
                continue
            raise

    raise RuntimeError("score_article failed after retry")
