import os

import requests

url = "https://api.groq.com/openai/v1/chat/completions"
headers = {
    "Authorization": f"Bearer {os.environ['GROQ_API_KEY']}",
    "Content-Type": "application/json",
}
payload = {
    "model": "llama-3.3-70b-versatile",
    "temperature": 0,
    "response_format": {"type": "json_object"},
    "messages": [
        {"role": "system", "content": "Return only valid JSON."},
        {"role": "user", "content": 'Return {"ok": true}'},
    ],
}

response = requests.post(url, headers=headers, json=payload, timeout=60)
print(response.status_code)
print(response.text)
print(dict(response.headers))
