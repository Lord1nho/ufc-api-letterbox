import json
from ufc import get_event
import os
import requests
import psycopg2
from slugify import slugify
from dotenv import load_dotenv

def connection():
    # conexão com o banco (Supabase usa Postgres normal)
    load_dotenv()

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )

    cur = conn.cursor()
    print("Conectado ao banco")

    with open("events/ufc_324.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    location = data["location"] or None

    cur.execute("""
    INSERT INTO events (name, date, venue, location)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (name, date)
    DO UPDATE SET venue = EXCLUDED.venue
    RETURNING id
""", (
    data["name"],
    data["date"],
    data["venue"],
    data["location"] or None
))

    event_id = cur.fetchone()[0]

    # =========================
    # 2. PROCESSAR LUTAS
    # =========================
    for fight in data["fights"]:

        # converter round (string → int)
        round_value = int(fight["round"]) if fight["round"] else None

        # gerar slug do evento
        event_slug = slugify(data["name"])

        # nomes dos lutadores
        fighter1 = fight["red corner"]["name"]
        fighter2 = fight["blue corner"]["name"]

        # ordenar para evitar duplicidade (A vs B == B vs A)
        fighters = sorted([fighter1, fighter2])

        # gerar fight_key
        fight_key = f"{event_slug}_{slugify(fighters[0])}_{slugify(fighters[1])}"

        # inserir luta com UPSERT
        cur.execute("""
            INSERT INTO fights (event_id, weight_class, method, round, time, fight_key)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (fight_key)
            DO UPDATE SET
                method = EXCLUDED.method,
                round = EXCLUDED.round,
                time = EXCLUDED.time
            RETURNING id
        """, (
            event_id,
            fight["weightclass"],
            fight["method"],
            round_value,
            fight["time"],
            fight_key
        ))

        fight_id = cur.fetchone()[0]

        # =========================
        # 3. PROCESSAR LUTADORES
        # =========================
        for corner_key, fighter_data in [
            ("red", fight["red corner"]),
            ("blue", fight["blue corner"])
        ]:

            ranking = fighter_data["ranking"] or None
            odds = fighter_data["odds"] or None

            # UPSERT fighter
            cur.execute("""
                INSERT INTO fighters (name, ufc_link)
                VALUES (%s, %s)
                ON CONFLICT (ufc_link)
                DO UPDATE SET name = EXCLUDED.name
                RETURNING id
            """, (
                fighter_data["name"],
                fighter_data["link"]
            ))

            fighter_id = cur.fetchone()[0]

            # inserir relação (resultado da luta)
            cur.execute("""
                INSERT INTO fight_data (
                    fight_id,
                    fighter_id,
                    corner,
                    result,
                    ranking,
                    odds
                )
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                fight_id,
                fighter_id,
                corner_key,
                fighter_data["result"],
                ranking,
                odds
            ))
    conn.commit()    
    cur.close()
    conn.close()



def scrape_events(start=327, end=329):

    os.makedirs("events", exist_ok=True)

    for i in range(start, end):

        url = f"https://www.ufc.com/event/ufc-{i}"

        try:

            print("Buscando", url)

            event = get_event(url)

            if not event:
                print("Evento inválido:", url)
                continue

            with open(f"events/ufc_{i}.json", "w", encoding="utf-8") as f:
                json.dump(event, f, indent=4, ensure_ascii=False)

        except Exception as e:

            print("Erro no evento:", i)
            print("URL:", url)
            print("Erro:", e)

def log_fight_counts():

    log_lines = []

    for file in os.listdir("events"):

        if not file.endswith(".json"):
            continue

        path = f"events/{file}"

        try:

            with open(path, encoding="utf-8") as f:
                event = json.load(f)

            fights = event.get("fights", [])

            line = f"{file} | {event.get('name')} | fights: {len(fights)}"

            print(line)

            log_lines.append(line)

        except Exception as e:

            line = f"{file} | ERROR | {e}"

            print(line)

            log_lines.append(line)

    with open("fight_log.txt", "w", encoding="utf-8") as log:

        for line in log_lines:
            log.write(line + "\n")


def teste():
    
    event_id = 1

    url = f"https://d29dxerjsp82wz.cloudfront.net/api/v3/event/live/{event_id}.json"

    response = requests.get(url)
    data = response.json()

# tenta pegar o nome do evento
    event_name = data["LiveEventDetail"]["Name"]

# limpa caracteres inválidos para nome de arquivo
    event_name = event_name.replace(":", "").replace(" ", "_")

    filename = f"{event_name}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print("Arquivo salvo:", filename)   


def main():
    scrape_events()
    log_fight_counts()


if __name__ == "__main__":
    main()