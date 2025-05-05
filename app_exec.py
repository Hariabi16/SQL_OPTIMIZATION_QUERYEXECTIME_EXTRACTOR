import streamlit as st
import xml.etree.ElementTree as ET
import io

def extract_execution_times(plan_xml):
    raw_data = plan_xml.read()
    try:
        xml_content = raw_data.decode('utf-16')
    except UnicodeDecodeError:
        xml_content = raw_data.decode('utf-8')

    root = ET.fromstring(xml_content)
    namespace = {'ns': 'http://schemas.microsoft.com/sqlserver/2004/07/showplan'}

    query_times = []
    total_time_ms = 0.0

    for stmt in root.findall(".//ns:StmtSimple", namespace):
        query_text = stmt.attrib.get('StatementText', 'N/A')

        query_plan = stmt.find('ns:QueryPlan', namespace)
        if query_plan is not None:
            query_time_stats = query_plan.find('ns:QueryTimeStats', namespace)
            if query_time_stats is not None:
                cpu_time = float(query_time_stats.attrib.get('CpuTime', 0))
                elapsed_time = float(query_time_stats.attrib.get('ElapsedTime', 0))
            else:
                cpu_time = 0
                elapsed_time = 0
        else:
            cpu_time = 0
            elapsed_time = 0

        elapsed_time_ms = elapsed_time
        cpu_time_ms = cpu_time

        query_info = {
            'query_text': query_text.strip(),
            'cpu_time_ms': cpu_time_ms,
            'elapsed_time_ms': elapsed_time_ms
        }
        query_times.append(query_info)
        total_time_ms += elapsed_time_ms

    return query_times, total_time_ms

def format_output(query_times, total_time_ms):
    output = io.StringIO()
    output.write("\n=== Query-wise Execution Time Details ===\n")
    for idx, query in enumerate(query_times, 1):
        output.write(f"\nQuery {idx}:\n")
        output.write(f"Elapsed Time   : {query['elapsed_time_ms']:.2f} ms\n")
        output.write(f"CPU Time       : {query['cpu_time_ms']:.2f} ms\n")
        output.write(f"Query Snippet  : {query['query_text'][:150]}...\n")

    output.write("\n========================================\n")
    output.write(f"Total Execution Time (Sum of Queries): {total_time_ms:.2f} ms\n")
    output.write("========================================\n")
    return output.getvalue()

# Streamlit App UI
st.title("SQLPlan Execution Time Extractor")
uploaded_file = st.file_uploader("Upload your .sqlplan file", type=['sqlplan', 'xml'])

if uploaded_file is not None:
    try:
        query_times, total_time_ms = extract_execution_times(uploaded_file)
        output_text = format_output(query_times, total_time_ms)
        
        st.success("Execution time details extracted successfully!")
        st.text_area("Execution Details", value=output_text, height=400)

        st.download_button("Download Result as .txt", output_text, file_name="execution_times.txt", mime="text/plain")

    except Exception as e:
        st.error(f"Failed to process file: {e}")
