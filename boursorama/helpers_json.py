# boursorama/helpers_json.py
import html
import json
import re
from typing import Any, Optional


def extract_json_like(text: str) -> Any:
    """
    Tente de parser une chaîne qui contient du JSON inline ou du JSON encodé.
    """
    if not text:
        return {}

    text = html.unescape(text).strip()

    # 1) Essai direct
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # 2) Extraction d'un bloc JSON au milieu d'un texte plus large
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return {}

    return {}


def find_first_value(obj: Any, keys: list[str]) -> Optional[Any]:
    """
    Recherche récursivement la première valeur non vide associée à une clé donnée.
    """
    if isinstance(obj, dict):
        for key in keys:
            if key in obj and obj[key] not in (None, "", [], {}):
                return obj[key]

        for value in obj.values():
            found = find_first_value(value, keys)
            if found is not None:
                return found

    elif isinstance(obj, list):
        for item in obj:
            found = find_first_value(item, keys)
            if found is not None:
                return found

    return None