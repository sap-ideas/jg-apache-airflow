# Airflow secrets: Fernet key + Variables (Teams webhook & iSeller token)

## What we use

| Purpose | Where to set | Priority when code reads it |
|--------|----------------|------------------------------|
| Power Automate Teams webhook | Airflow Variable `POWER_AUTOMATE_TEAMS_WEBHOOK_URL` | Variable → env `POWER_AUTOMATE_TEAMS_WEBHOOK_URL` (see `common.teams_powerautomate_report.resolve_power_automate_webhook_url`) |
| iSeller Pusat API token | Airflow Variable `access_token_pusat` | Variable → env `access_token_pusat` (see `pipeline_backfill_fulfillment.run_backfill_fulfillment`) |
| iSeller Mitra API token | Airflow Variable `access_token_mitra` | Variable → env `access_token_mitra` → file `.env` (see `iseller_mitra_dwh/pipelines/master_bundlings.get_access_token_mitra`). Tidak ada / expired → DAG berhenti + alert, tanpa fallback |
| **Encryption at rest** for Variables/Connections | `AIRFLOW__CORE__FERNET_KEY` in `.env` (loaded by `docker-compose` `env_file`) | Must be **the same** on every Airflow component (webserver, scheduler, workers) |

## 1) Fernet key (encrypt Variable values in the metadata DB)

1. Generate a key (one time per deployment — **back it up** somewhere safe, e.g. password manager):

   ```bash
   python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

2. Put it in **`.env`** (do **not** commit if the repo is public):

   ```env
   AIRFLOW__CORE__FERNET_KEY=<<paste the key here>>
   ```

3. **Restart** all Airflow services so they load the new env (scheduler, webserver, triggerer, etc.).

4. In the Airflow UI, if you still see a warning like *“empty cryptography key”* when running `airflow variables set`, the running container does not have `AIRFLOW__CORE__FERNET_KEY` set — fix env + restart.

> **Note:** If you change the Fernet key after Variables were already stored, old encrypted values may become unreadable. Keep one stable key per environment.

## 2) Set Variables in the Airflow UI

1. Open **Admin → Variables**.
2. **Add** (or edit):

   - **Key:** `POWER_AUTOMATE_TEAMS_WEBHOOK_URL`  
     **Value:** full Power Automate HTTP trigger URL (including `sig=`).

   - **Key:** `access_token_pusat`  
     **Value:** current iSeller Pusat bearer token (rotate every ~2 weeks as needed).

   - **Key:** `access_token_mitra`  
     **Value:** current iSeller Mitra bearer token. If missing/expired, the `iseller_mitra_dwh` DAG
     stops with an email + Teams alert (reason shown); fix the token, then clear the task.

3. Save.

After Fernet is configured, values are stored **encrypted** in the metadata database (not plaintext in the DB).

## 3) CLI (optional)

From a container that has DB connectivity:

```bash
airflow variables set POWER_AUTOMATE_TEAMS_WEBHOOK_URL 'https://...'
airflow variables set access_token_pusat '...'
airflow variables set access_token_mitra '...'
airflow variables get POWER_AUTOMATE_TEAMS_WEBHOOK_URL
```

## 4) Fallback via `.env` / Docker Compose

You can still put these in **`.env`** (already merged into containers via `env_file` in `docker-compose.yaml`):

- `POWER_AUTOMATE_TEAMS_WEBHOOK_URL=...`
- `access_token_pusat=...`
- `access_token_mitra=...`

Code prefers **Airflow Variable** when present; env is the fallback (useful for local runs without the metadata DB).

## 5) Lock down access (admin-only)

- Restrict who has **Admin** or **Variable edit** permissions in Airflow (RBAC).
- Prefer **separate** Airflow instances or roles for prod vs dev.
- Rotate **webhook URL** and **API token** if they ever leak (chat, screenshots, shared repos).

## 6) Python deps for DAGs (`pymysql`, etc.)

The stock `apache/airflow` image does not ship every driver your DAGs use (e.g. **`pymysql`** for `mysql+pymysql://`).

This repo **bakes** dependencies from `requirements.txt` into a small custom image:

- `Dockerfile.airflow` (extends `apache/airflow:2.8.1-python3.11` and runs `pip install -r requirements.txt` as user `airflow`)
- `docker-compose.yaml` points all Airflow services at image **`airflow-nexus:2.8.1-python3.11`**

**Why not `_PIP_ADDITIONAL_REQUIREMENTS`?** On Airflow 2.8, the init/bootstrap path can fail when it tries to `pip install` as **root**; building the image is the approach recommended in the Airflow Docker docs.

After you change `requirements.txt`, rebuild and recreate:

```bash
docker-compose build
docker-compose up -d
```

## 7) Docker Compose recreate issues (optional)

If `docker-compose` fails with `KeyError: 'ContainerConfig'` on recreate, that is a known **docker-compose v1 + newer Docker** mismatch. Options:

- Install **Docker Compose v2** (`docker compose` plugin) and use `docker compose up -d`, or  
- Remove stuck containers and bring the stack up again from a clean state (careful with volumes).
