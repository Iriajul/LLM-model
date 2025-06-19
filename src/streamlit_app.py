import streamlit as st
from workflow import app, WorkflowState
from config import logger, db
import time
import json
import os
from pathlib import Path
import random

# Create secrets directory if needed
secrets_dir = Path(".streamlit")
secrets_dir.mkdir(exist_ok=True)

# Create default secrets file if missing
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
LLM_MODEL_NAME = "llama3-70b-8192"
""")

# Create NLP2SQL animation effect
def show_nl2sql_animation():
    container = st.empty()
    nl2sql_text = "NLP2SQL"
    animation_text = ""
    
    for i in range(len(nl2sql_text) + 1):
        animation_text = nl2sql_text[:i]
        container.title(animation_text)
        time.sleep(0.2)
    
    time.sleep(0.5)
    container.empty()

# Show animation only once when app loads
if 'animation_shown' not in st.session_state:
    show_nl2sql_animation()
    st.session_state.animation_shown = True

st.title("NL2SQL Query Interface")
st.subheader("Ask natural language questions about your database")

# Initialize session state
if 'history' not in st.session_state:
    st.session_state.history = []

# Display warning if using placeholder secrets
if st.secrets.get("DB_HOST") == "your_host":
    st.warning("⚠️ Please configure your credentials in `.streamlit/secrets.toml`")

# User input section
with st.form("query_form"):
    question = st.text_area("Enter your question:", 
                          placeholder="e.g. Show top 10 customers by total purchases",
                          height=100)
    submitted = st.form_submit_button("Execute Query")

if submitted and question:
    with st.spinner("Processing your question..."):
        start_time = time.time()
        
        # Prepare workflow state
        initial_state = {
            "user_input": question,
            "messages": [],
            "db_schema": "",
            "generated_sql": "",
            "db_result": None,
            "raw_db_result": None,
            "error_message": None,
            "correction_attempts": 0
        }
        
        try:
            # Execute workflow
            final_state = app.invoke(initial_state)
            elapsed_time = time.time() - start_time
            
            # Handle results
            if final_state.get("messages"):
                final_message = final_state["messages"][-1]
                result_content = final_message.content if hasattr(final_message, 'content') else str(final_message)
                
                # Store in history
                history_entry = {
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "question": question,
                    "sql": final_state.get("generated_sql", ""),
                    "answer": result_content,
                    "time": f"{elapsed_time:.2f}s"
                }
                st.session_state.history.insert(0, history_entry)
                
                # Display results
                st.success("Query executed successfully!")
                st.subheader("Answer:")
                st.markdown(result_content)
                
                # Show SQL and data preview
                if sql := final_state.get("generated_sql"):
                    with st.expander("Generated SQL"):
                        st.code(sql, language="sql")
                
                if raw_result := final_state.get("raw_db_result"):
                    with st.expander("Data Preview"):
                        if isinstance(raw_result, list) and len(raw_result) > 0:
                            st.json(raw_result[:10])  # Show first 10 rows
                        else:
                            st.warning("No data returned from query")
                
                # Show performance metrics
                st.caption(f"Execution time: {elapsed_time:.2f} seconds | "
                          f"Correction attempts: {final_state.get('correction_attempts', 0)}")
            
            else:
                st.error("No results generated")
        
        except Exception as e:
            st.error(f"Processing failed: {str(e)}")
            logger.error(f"Streamlit execution error: {e}", exc_info=True)

# Query history section
if st.session_state.history:
    st.divider()
    st.subheader("Query History")
    
    for i, entry in enumerate(st.session_state.history[:5]):  # Show last 5 entries
        with st.expander(f"{entry['timestamp']}: {entry['question']}"):
            st.markdown(f"**Answer:** {entry['answer']}")
            st.code(f"SQL: {entry['sql']}", language="sql")
            st.caption(f"Execution time: {entry['time']}")

# Sidebar with additional options
with st.sidebar:
    st.header("Configuration")
    
    if st.button("Clear Query History"):
        st.session_state.history = []
        st.rerun()
    
    if st.button("Test Database Connection"):
        try:
            from db_utils import safe_db_run
            # Use your actual schema name from secrets
            schema_name = st.secrets.get("DB_SCHEMA", "info")
            test_query = f"SELECT 1 AS status"
            
            # Handle both success and error cases
            test_result = safe_db_run(test_query)
            
            if isinstance(test_result, list):
                st.success("Database connection working")
                if test_result and len(test_result) > 0:
                    st.json(test_result[0])  # Show first row as JSON
                else:
                    st.write("Test query returned no results")
            else:
                st.error(f"Connection test failed: {test_result}")
        except Exception as e:
            st.error(f"Connection failed: {str(e)}")
    
    # Get available tables and format names
    try:
        tables = db.get_usable_table_names()
        if tables:
            st.divider()
            st.subheader("Available Tables")
            for table in tables:
                # Format table names: remove underscores and capitalize words
                clean_name = table.replace('_', ' ').title()
                st.markdown(f"- **{clean_name}**")
        else:
            st.warning("No tables found in the database")
    except Exception as e:
        st.error(f"Failed to fetch tables: {str(e)}")
    
    st.divider()
    st.caption("NL2SQL Application v1.0")
    
    # Handle secrets more gracefully
    try:
        model_name = st.secrets.get("LLM_MODEL_NAME", "llama3-70b-8192")
        schema_name = st.secrets.get("DB_SCHEMA", "public")
    except Exception:
        model_name = "llama3-70b-8192"
        schema_name = "public"
    
    st.caption(f"LLM Model: {model_name}")
    st.caption(f"Database Schema: {schema_name}")