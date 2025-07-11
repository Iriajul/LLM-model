import os
import json
import requests
from .workflow import app, WorkflowState
from .config import logger

def json_serial(obj):
    import datetime
    import decimal
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

if __name__ == "__main__":
    logger.info("Starting NL2SQL application example...")

    # --- Example Usage ---
    #question = "What is the total revenue generated by each product category in last 10 years?"
    question = "List each customers most recent order along with their previous order date"
    logger.info(f"User Question: {question}")

    # Prepare the initial state for the graph
    initial_state: WorkflowState = {
        "user_input": question,
        "messages": [],
        "db_schema": "",
        "generated_sql": "",
        "db_result": None,
        "error_message": None,
        "correction_attempts": 0
    }

    try:
        # Invoke the LangGraph application
        final_state = app.invoke(initial_state)

        if final_state and final_state.get("messages"):
            final_message = final_state["messages"][-1]
            if hasattr(final_message, 'content'):
                print("\n--- Final Answer ---")
                print(final_message.content)
                logger.info(f"Final Answer: {final_message.content}")

                # --- Export logic ---
                raw_result = final_state.get("raw_db_result")
                if raw_result:
                    API = os.environ.get("EXPORT_API_URL", "http://localhost:8000")
                    # Always login first to avoid 403 on first export
                    login_payload = {
                        "login": os.environ.get("EXPORT_API_USER", "zed"),
                        "password": os.environ.get("EXPORT_API_PASS", "Malbro17")
                    }
                    resp = requests.post(f"{API}/auth/login", json=login_payload)
                    if resp.status_code == 200:
                        token = resp.json()["access_token"]
                        print("Obtained access token via login.")
                    else:
                        print("Failed to get token:", resp.text)
                        token = None

                    if not token:
                        print("No access token, cannot export.")
                    else:
                        headers = {
                            "Content-Type": "application/json",
                            "Authorization": f"Bearer {token}"
                        }
                        payload = json.dumps({"data": raw_result}, default=json_serial, ensure_ascii=False)
                        try:
                            r = requests.post(f"{API}/export", data=payload, headers=headers, timeout=15)
                            if r.status_code == 200:
                                meta = r.json()
                                print("\n--- Export URIs ---")
                                print("CSV:", f"{API}{meta['csv_url']}")
                                print("Excel:", f"{API}{meta['excel_url']}")
                                # --- Download logic (optional) ---
                                for label, url_key in [("CSV", "csv_url"), ("Excel", "excel_url")]:
                                    download_url = f"{API}{meta[url_key]}"
                                    download_headers = {"Authorization": f"Bearer {token}"}
                                    try:
                                        resp = requests.get(download_url, headers=download_headers, timeout=15)
                                        if resp.status_code == 200:
                                            filename = download_url.split("/")[-1]
                                            with open(filename, "wb") as f:
                                                f.write(resp.content)
                                            print(f"Downloaded {label} file as {filename}")
                                        else:
                                            print(f"Failed to download {label}: {resp.status_code} {resp.text}")
                                    except Exception as e:
                                        print(f"Download request failed for {label}: {e}")
                            else:
                                print(f"Export failed: {r.status_code} {r.text}")
                        except Exception as e:
                            print(f"Export request failed: {e}")
                else:
                    print("No raw_db_result to export.")
            else:
                logger.error("Final state message has no content.")
                print("\n--- Final State (No Content) ---")
                print(final_state)
        else:
            logger.error("Workflow finished without a final state or messages.")
            print("\n--- Workflow Error ---")
            print("The workflow did not produce a final result.")

    except Exception as e:
        logger.error(f"An error occurred during workflow execution: {e}", exc_info=True)
        print("\n--- Workflow Execution Error ---")
        print(f"An unexpected error occurred: {e}")

    logger.info("NL2SQL application example finished.")