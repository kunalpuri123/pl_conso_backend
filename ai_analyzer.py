import google.generativeai as genai
import json
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env safely
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)

# Use ONE standard key name everywhere
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

def analyze_output_with_gemini(file_path: str):
    # 1. Load excel
    df = pd.read_excel(file_path)

    total_rows = len(df)

    # 2. Limit rows to avoid token explosion
    sample_df = df.head(100)

    # 3. Convert to text
    table_text = sample_df.to_csv(index=False)

    prompt = f"""
You are a QA automation analyst.

This is an output Excel file of an automated validation system.

Total rows in file: {total_rows}

Here is a sample of the data:

{table_text}

Analyze this data and return STRICT JSON:

{{
  "summary": "...",
  "total_tested": {total_rows},
  "passed": number,
  "failed": number,
  "accuracy": number,
  "data_quality_score": number,
  "errors": [],
  "top_issues": [],
  "verdict": "PASS" | "WARN" | "FAIL"
}}

Rules:
- Return ONLY valid JSON
- No markdown
- No explanation
- No extra text
"""

    model = genai.GenerativeModel("gemini-3-flash-preview")

    response = model.generate_content(prompt)

    text = response.text.strip()

    # Sometimes Gemini wraps in ```json ... ```
    if text.startswith("```"):
        text = text.replace("```json", "").replace("```", "").strip()

    return json.loads(text)
