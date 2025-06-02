import streamlit as st
import pyodbc
import pandas as pd
import xml.etree.ElementTree as ET
from io import BytesIO
import time
import threading
import queue
 
st.set_page_config(layout="wide")
#st.title("SQL Query Plan Analyzer")
st.markdown("<h1 style='text-align: center;'>SQL Query Plan Analyzer</h1>", unsafe_allow_html=True)
#st.caption("Extracts and analyzes execution plan metrics from SQL Server queries")
 
# --- Session state for login ---
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
 
if "conn_status" not in st.session_state:
    st.session_state.conn_status = False
 
 
# --- Step 1: Login Screen ---
def login_screen():
    st.markdown("<h3 style='text-align: center;'>🔐 Login</h3>", unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("login_form", clear_on_submit=False):
            server = st.text_input("SQL Server", "")
            database = st.text_input("Database Name", "")
            username = st.text_input("Username", "")
            password = st.text_input("Password", "", type="password")
            login_btn = st.form_submit_button("Login & Connect")
            if login_btn:
                try:
                    conn_str = (
                        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                        f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
                    )
                    test_conn = pyodbc.connect(conn_str, timeout=5)
                    test_conn.close()
 
                    # Save credentials and set auth
                    st.session_state.conn_str = conn_str
                    st.session_state.conn_status = True
                    st.session_state.authenticated = True
                    st.session_state.server = server
                    st.session_state.database = database
                    st.session_state.username = username
                    st.session_state.password = password
 
                    st.success("✅ Login successful!")
                    st.rerun()
 
                except Exception as e:
                    st.error(f"❌ Login failed: {str(e)}")
 
    st.stop()  # Prevent further rendering until login is successful
 
 
# --- Step 2: Main App ---
if not st.session_state.authenticated:
    login_screen()
 
 
# --- After login, display main UI ---
query = st.text_area("Enter your SQL query", height=200)
timeout = 160
max_xml_size = 20
run = st.button("Generate Execution Plan")
 
if run and query:
    progress_bar = st.progress(0)
    status_text = st.empty()
 
    def execute_query_with_timeout(conn_str, query, result_queue, max_size_mb):
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            try:
                cursor.execute(f"SET QUERY_GOVERNOR_COST_LIMIT = {timeout * 1000}")
            except:
                pass
 
            cursor.execute("SET STATISTICS XML ON")
            try:
                cursor.execute(query)
                plans = []
                max_size_bytes = max_size_mb * 1024 * 1024
                total_xml_size = 0
 
                for _ in range(100):
                    try:
                        rows = cursor.fetchall()
                        for row in rows:
                            if row and len(row) > 0:
                                row_val = str(row[0])
                                if (row_val.strip().startswith('<?xml') or
                                    '<ShowPlanXML' in row_val or
                                    '<QueryPlan' in row_val):
 
                                    xml_size = len(row_val.encode('utf-8'))
                                    if total_xml_size + xml_size <= max_size_bytes:
                                        plans.append(row_val)
                                        total_xml_size += xml_size
                                    else:
                                        result_queue.put(("size_limit", plans))
                                        return
                    except pyodbc.ProgrammingError:
                        pass
                    if not cursor.nextset():
                        break
                result_queue.put(("success", plans))
            except pyodbc.Error as sql_err:
                result_queue.put(("error", str(sql_err)))
            try:
                cursor.execute("SET STATISTICS XML OFF")
            except:
                pass
            cursor.close()
            conn.close()
        except Exception as e:
            result_queue.put(("error", str(e)))
 
    result_queue = queue.Queue()
    thread = threading.Thread(target=execute_query_with_timeout, args=(st.session_state.conn_str, query, result_queue, max_xml_size))
    thread.daemon = True
    thread.start()
 
    start_time = time.time()
    elapsed = 0
    while thread.is_alive() and elapsed < timeout:
        progress = min(elapsed / timeout, 0.99)
        progress_bar.progress(progress)
        status_text.text(f"Executing query... ({elapsed:.1f}s / {timeout}s)")
        time.sleep(0.1)
        elapsed = time.time() - start_time
 
    if thread.is_alive():
        progress_bar.progress(1.0)
        status_text.error(f"Query timed out after {timeout} seconds.")
    else:
        progress_bar.progress(1.0)
        if not result_queue.empty():
            result_type, result_data = result_queue.get()
            if result_type == "error":
                status_text.error(f"Error executing query: {result_data}")
                plans = []
            elif result_type == "size_limit":
                status_text.warning(f"XML size limit reached ({max_xml_size}MB).")
                plans = result_data
                status_text.success(f"Found {len(plans)} execution plan(s)")
            else:
                plans = result_data
 
            if plans:
                try:
                    namespaces = {'sql': 'http://schemas.microsoft.com/sqlserver/2004/07/showplan'}
                    query_summaries = []
 
                    for plan_idx, plan_xml in enumerate(plans):
                        try:
                            cleaned_xml = plan_xml
                            if not cleaned_xml.strip().startswith('<?xml'):
                                xml_start = cleaned_xml.find('<?xml')
                                if xml_start >= 0:
                                    cleaned_xml = cleaned_xml[xml_start:]
 
                            root = ET.fromstring(cleaned_xml)
                            for stmt in root.findall(".//sql:StmtSimple", namespaces):
                                query_text = stmt.attrib.get("StatementText", "").strip().replace("\n", " ")
                                query_hash = stmt.attrib.get("QueryHash", "N/A")
                                statement_cost = stmt.attrib.get("StatementSubTreeCost", "N/A")
                                total_actual_rows = 0
                                total_cpu = 0
                                total_elapsed = 0
                                total_logical_reads = 0
                                total_physical_reads = 0
                                total_writes = 0
 
                                runtime_info = stmt.find(".//sql:QueryTimeStats", namespaces)
                                if runtime_info is not None:
                                    total_cpu = float(runtime_info.attrib.get("CpuTime", 0))
                                    total_elapsed = float(runtime_info.attrib.get("ElapsedTime", 0))
 
                                relops = stmt.findall(".//sql:RelOp", namespaces)
                                for relop in relops:
                                    metrics = relop.find(".//sql:RunTimeCountersPerThread", namespaces)
                                    if metrics is not None:
                                        total_actual_rows += int(float(metrics.attrib.get("ActualRows", 0)))
                                        if total_cpu == 0:
                                            total_cpu += float(metrics.attrib.get("CPUTime", 0))
                                        if total_elapsed == 0:
                                            total_elapsed += float(metrics.attrib.get("ElapsedTime", 0))
                                        total_logical_reads += int(float(metrics.attrib.get("LogicalReads", 0)))
                                        total_physical_reads += int(float(metrics.attrib.get("PhysicalReads", 0)))
                                        total_writes += int(float(metrics.attrib.get("Writes", 0)))
 
                                query_summaries.append({
                                    "Query": query_text[:100] + "..." if len(query_text) > 100 else query_text,
                                    "Statement Cost": statement_cost,
                                    "CPU Time (ms)": round(total_cpu, 2),
                                    "Elapsed Time (ms)": round(total_elapsed, 2),
                                    "Logical Reads": total_logical_reads,
                                    "Physical Reads": total_physical_reads,
                                    "Writes": total_writes,
                                    "Actual Rows": total_actual_rows
                                })
                        except Exception as e:
                            st.warning(f"Plan {plan_idx+1} parsing failed: {e}")
 
                    if query_summaries:
                        start_index = 1  # Change this to your desired starting index
                        df = pd.DataFrame(query_summaries, index=range(start_index, start_index + len(query_summaries)))
                        #df = pd.DataFrame(query_summaries)
                        if len(df) > 1:
                            total_row = {
                                "Query": "TOTAL",
                                "Statement Cost": "",
                                "CPU Time (ms)": df["CPU Time (ms)"].sum(),
                                "Elapsed Time (ms)": df["Elapsed Time (ms)"].sum(),
                                "Logical Reads": df["Logical Reads"].sum(),
                                "Physical Reads": df["Physical Reads"].sum(),
                                "Writes": df["Writes"].sum(),
                                "Actual Rows": df["Actual Rows"].sum()
                            }
                            total_df = pd.DataFrame([total_row], index=[df.index[-1] + 1])  # Add total at the next index
                            df = pd.concat([df, total_df])  # No `index=` argument here
 
                        st.subheader("SQL Plan Summary")
                        st.dataframe(df, use_container_width=True)
 
                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                            df.to_excel(writer, index=False, sheet_name='SQLPlanSummary')
                        st.download_button("📥 Download Excel Report", data=output.getvalue(), file_name="sqlplan_summary.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
 
                        with st.expander("View Raw XML"):
                            for i, plan in enumerate(plans):
                                st.text_area(f"Plan {i+1}", plan[:10000] + "..." if len(plan) > 10000 else plan, height=200)
 
                except Exception as e:
                    st.error(f"Failed to process execution plans: {str(e)}")
            else:
                st.warning("No execution plans found. Try with a simple SELECT query.")
 
        else:
            st.error("No response from execution thread.")
 
 
# --- Help & Troubleshooting ---
with st.expander("Help & Troubleshooting"):
    st.markdown("""
    ### Common Issues
    - Ensure your login has SELECT permissions.
    - Some queries (DDL, dynamic SQL) may not return execution plans.
    - Estimated plans are not supported — this tool gets **actual** execution plans.
 
    ### Metrics Explained
    - **CPU Time (ms):** Time spent processing by CPU.
    - **Elapsed Time (ms):** Total wall-clock duration.
    - **Logical Reads:** Pages read from memory.
    - **Physical Reads:** Pages fetched from disk.
    - **Writes:** Pages written to disk.
    - **Actual Rows:** Total rows processed.
 
    Try with:
    ```sql
    SELECT TOP 1000 * FROM YourTable
    ```
    """)