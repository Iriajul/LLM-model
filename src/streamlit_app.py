import sys, os
from pathlib import Path

# ensure project root on sys.path
ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import time
import json
import decimal
import datetime
import requests
import streamlit as st

from src.workflow import app, WorkflowState
from src.config   import logger, db

API = os.environ.get("EXPORT_API_URL", "http://localhost:8000")

# — Create .streamlit/secrets.toml if missing —
secrets_dir  = Path(".streamlit")
secrets_dir.mkdir(exist_ok=True)
secrets_file = secrets_dir / "secrets.toml"
if not secrets_file.exists():
    secrets_file.write_text("""# Add your credentials below
DB_HOST="localhost"
DB_PORT="5432"
DB_USER="postgres"
DB_PASSWORD="Malbro16"
DB_NAME="postgres"
DB_SCHEMA="info"
GROQ_API_KEY="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"
LLM_MODEL_NAME="llama3-70b-8192"

EXPORT_API_URL="http://localhost:8000"
""")

# JWT auth state
if "token" not in st.session_state:
    st.session_state.token = None
if "show_register" not in st.session_state:
    st.session_state.show_register = False

# — username/Email/Password login or register —
if not st.session_state.token:
    st.title("🔒 Login Required")
    if not st.session_state.show_register:
        st.subheader("Login")
        login_id = st.text_input("Email or Username")
        login_password = st.text_input("Password", type="password")
        if st.button("Login"):
            if not login_id or not login_password:
                st.error("Please enter both Email/Username and Password.")
            else:
                r = requests.post(f"{API}/auth/login", json={"login": login_id, "password": login_password})
                if r.status_code == 200:
                    st.session_state.token = r.json()["access_token"]
                    st.success("Logged in!")
                    st.rerun()
                else:
                    st.error("Login failed. Check your credentials or register below.")
                    st.session_state.show_register = True
        st.info("Don't have an account?")
        if st.button("Go to Register"):
            st.session_state.show_register = True
        st.stop()
    else:
        st.subheader("Register")
        reg_username = st.text_input("Username")
        reg_email = st.text_input("Email")
        reg_password = st.text_input("Password", type="password")
        if st.button("Register"):
            if not reg_username or not reg_email or not reg_password:
                st.error("Please fill all fields to register.")
            else:
                r = requests.post(
                    f"{API}/auth/register",
                    json={"username": reg_username, "email": reg_email, "password": reg_password}
                )
                if r.status_code in (200, 201):
                    st.success("Registration successful! Please log in.")
                    st.session_state.show_register = False
                else:
                    st.error(f"Registration failed: {r.text}")
        if st.button("Back to Login"):
            st.session_state.show_register = False
        st.stop()

# — Main UI —
st.title("NL2SQL Query & Export")
st.subheader("Ask natural-language questions about your database")

# one-time “NLP2SQL” animation
if "anim" not in st.session_state:
    for i in range(len("NLP2SQL")+1):
        st.title("NLP2SQL"[:i])
        time.sleep(0.1)
    st.session_state.anim = True

# history
if "history" not in st.session_state:
    st.session_state.history = []

# query form
with st.form("qf"):
    question = st.text_area("Your question:", height=100)
    go = st.form_submit_button("Run & Export")

if go and question:
    with st.spinner("Processing…"):
        # 1) Run your NL2SQL workflow
        state = {
            "user_input": question,
            "messages": [],
            "db_schema": "",
            "generated_sql": "",
            "db_result": None,
            "raw_db_result": None,
            "error_message": None,
            "correction_attempts": 0,
            "token": st.session_state.token
        }
        final = app.invoke(state)
        elapsed = time.time() - st.session_state.get("_start", time.time())
        st.session_state["_start"] = time.time()

        msgs = final.get("messages") or []
        if not msgs:
            st.error("Workflow returned no messages")
        else:
            content = msgs[-1].content
            st.success("✅ Query executed")
            st.subheader("Answer")
            st.markdown(content, unsafe_allow_html=True)

            # show SQL & preview
            if sql := final.get("generated_sql"):
                with st.expander("Generated SQL"):
                    st.code(sql, language="sql")

            if raw := final.get("raw_db_result"):
                with st.expander("Data Preview"):
                    st.json(raw[:10] if isinstance(raw, list) else raw)

            # 2) Export: sanitize dates/decimals
            def _serial(o):
                if isinstance(o, (datetime.date, datetime.datetime)):
                    return o.isoformat()
                if isinstance(o, decimal.Decimal):
                    return float(o)
                return str(o)

            if not st.session_state.token:
                st.error("You must be logged in to export data.")
                st.stop()

            payload = json.dumps({"data": final.get("raw_db_result", [])},
                                 default=_serial,
                                 ensure_ascii=False)
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {st.session_state.token}"
            }
            r = requests.post(f"{API}/export", data=payload, headers=headers, timeout=15)
            if r.status_code == 403:
                st.error("Session expired or unauthorized. Please log in again.")
                st.session_state.token = None
                st.session_state.show_register = False
                st.rerun()
            elif r.status_code != 200:
                st.error(f"Export failed: {r.status_code} {r.text}")
            else:
                meta = r.json()
                st.info("📦 Export ready")

                # 3) Download protected files
                for label, path, mime in [
                    ("CSV", meta["csv_url"], "text/csv"),
                    ("Excel", meta["excel_url"], "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                ]:
                    file_r = requests.get(f"{API}{path}", headers=headers, timeout=15)
                    if file_r.status_code == 403:
                        st.error("Session expired or unauthorized. Please log in again.")
                        st.session_state.token = None
                        st.session_state.show_register = False
                        st.rerun()
                    elif file_r.status_code == 200:
                        st.download_button(f"⬇️ {label}",
                                           data=file_r.content,
                                           file_name=path.split("/")[-1],
                                           mime=mime)
                    else:
                        st.error(f"Failed to fetch {label}: {file_r.status_code}")

            # record history
            st.session_state.history.insert(0, {
                "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                "q": question,
                "sql": final.get("generated_sql",""),
                "ans": content,
                "t": f"{elapsed:.2f}s"
            })

# history panel
if st.session_state.history:
    st.divider()
    st.subheader("History")
    for h in st.session_state.history[:5]:
        with st.expander(f"{h['ts']}: {h['q']}"):
            st.markdown(f"**Answer:** {h['ans']}")
            st.code(h["sql"], language="sql")
            st.caption(f"Time: {h['t']}")

# sidebar
with st.sidebar:
    st.header("Settings & Tools")
    if st.button("Clear History"):
        st.session_state.history = []
        st.rerun()
    if st.button("Logout"):
        st.session_state.token = None
        st.session_state.show_register = False
        st.success("Logged out!")
        st.rerun()
    if st.button("Test DB"):
        from db_utils import safe_db_run
        res = safe_db_run(f"SELECT 1 AS ok FROM {st.secrets.get('DB_SCHEMA','info')}.users LIMIT 1")
        if isinstance(res, list): st.success("DB OK"); st.json(res[0])
        else: st.error(res)
    st.divider()
    st.caption(f"Export API: {API}")
    st.caption(f"DB Schema: {st.secrets.get('DB_SCHEMA','info')}")
    st.caption(f"LLM Model: {st.secrets.get('LLM_MODEL_NAME','llama3-70b-8192')}")