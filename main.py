import json
from ufc import get_event
import os


os.makedirs("events", exist_ok=True)
for i in range(150, 329):

    try:

        url = f"https://www.ufc.com/event/ufc-{i}"
        print("Buscando", url)

        event = get_event(url)

        with open(f"events/ufc_{i}.json", "w", encoding="utf-8") as f:
            json.dump(event, f, indent=4, ensure_ascii=False)

    except Exception as e:
        print("Erro:", e)