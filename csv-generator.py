import streamlit as st
import csv
from elasticsearch import Elasticsearch
import json
from datetime import datetime
import time
import threading
from queue import Queue
import tempfile
import os
import warnings
from urllib3.exceptions import InsecureRequestWarning
import logging
import pytz  # For timezone handlinga

# ------------------ Fixes / Settings ------------------
# Suppress SSL warnings if verify_certs=False
warnings.simplefilter('ignore', InsecureRequestWarning)

# Suppress Tornado CancelledError logging
logging.getLogger("tornado").setLevel(logging.ERROR)

# Clean up orphaned temp files on startup
if 'cleanup_done' not in st.session_state:
    st.session_state.cleanup_done = True
    try:
        import glob
        temp_dir = tempfile.gettempdir()
        # Find old CSV files (older than 1 hour)
        csv_pattern = os.path.join(temp_dir, "tmp*.csv")
        current_time = time.time()
        for file_path in glob.glob(csv_pattern):
            try:
                file_age = current_time - os.path.getmtime(file_path)
                if file_age > 3600:  # 1 hour old
                    os.remove(file_path)
            except:
                pass
    except:
        pass

st.set_page_config(page_title="ES Data Extractor", page_icon="🔍", layout="wide")

# ------------------ Session State ------------------
for key, default in [('es_client', None), ('data_streams', []), ('fields', []), ('connected', False),
                     ('extracting', False), ('download_ready', False), ('csv_file', None), ('stats', None)]:
    if key not in st.session_state:
        st.session_state[key] = default


# ------------------ Helper Functions ------------------
def create_client(host, port, ssl, auth_type, api_key=None, user=None, pwd=None):
    url = f"{'https' if ssl else 'http'}://{host}:{port}"
    try:
        if auth_type == "API Key":
            return Elasticsearch([url], api_key=api_key, verify_certs=False, request_timeout=60)
        elif auth_type == "Basic Auth":
            try:
                return Elasticsearch([url], basic_auth=(user, pwd), verify_certs=False, request_timeout=60)
            except TypeError:
                return Elasticsearch([url], http_auth=(user, pwd), verify_certs=False, request_timeout=60)
        return Elasticsearch([url], verify_certs=False, request_timeout=60)
    except TypeError:
        return Elasticsearch([url], headers={"Authorization": f"ApiKey {api_key}"}, verify_certs=False, request_timeout=60)


def get_fields(props, prefix=''):
    fields = []
    for name, info in props.items():
        full = f"{prefix}{name}" if prefix else name
        ftype = info.get('type', 'object')
        if ftype == 'nested' or 'properties' in info:
            fields.append({'field': full, 'type': ftype})
            fields.extend(get_fields(info.get('properties', {}), f"{full}."))
        else:
            fields.append({'field': full, 'type': ftype})
    return fields


def get_file_size_mb(filepath):
    """Get file size in MB"""
    if os.path.exists(filepath):
        return os.path.getsize(filepath) / (1024 * 1024)
    return 0


# ------------------ Worker Function ------------------
def extract_to_csv(es, index, query, scroll_time, batch, include_meta, queue):
    temp = None
    try:
        start = time.time()
        
        # Get the _source parameter if specified
        source_param = query.get("_source")
        
        # Debug: Log what we're searching for
        if source_param:
            queue.put(('status', f'Scanning fields... (Will extract {len(source_param)} specific fields)'))
        else:
            queue.put(('status', 'Scanning fields... (Will extract ALL fields)'))
        
        # PASS 1: Scan through to collect all unique field names
        all_fieldnames = set()
        sample_size = min(1000, batch)  # Sample first 1000 docs to get field names
        
        # Get the _source parameter if specified
        source_param = query.get("_source")
        
        page = es.search(
            index=index,
            query=query.get("query", {"match_all": {}}),
            size=sample_size,
            _source=source_param,
            track_total_hits=True  # CRITICAL: Get accurate total count, not limited to 10000
        )
        
        hits_total = page['hits']['total']['value'] if isinstance(page['hits']['total'], dict) else page['hits']['total']
        queue.put(('total', hits_total))
        
        if hits_total == 0:
            queue.put(('error', 'No documents found'))
            return
        
        # Collect field names from sample
        for hit in page['hits']['hits']:
            doc = {'_id': hit['_id'], '_index': hit['_index'], '_score': hit.get('_score'), **hit['_source']} if include_meta else hit['_source']
            def collect_fields(obj, parent=''):
                for k, v in obj.items():
                    key = f"{parent}.{k}" if parent else k
                    if isinstance(v, dict):
                        collect_fields(v, key)
                    else:
                        all_fieldnames.add(key)
            collect_fields(doc)
        
        fieldnames_list = sorted(list(all_fieldnames))
        
        # PASS 2: Extract all data and write to CSV
        queue.put(('status', 'Extracting data...'))
        temp = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', newline='', encoding='utf-8')
        writer = csv.DictWriter(temp, fieldnames=fieldnames_list)
        writer.writeheader()
        
        total = 0
        last_update = time.time()
        
        # Start fresh scroll for full extraction
        page = es.search(
            index=index,
            query=query.get("query", {"match_all": {}}),
            scroll=scroll_time,
            size=batch,
            _source=source_param,  # Use the same _source parameter
            track_total_hits=True  # Get accurate total
        )
        
        sid = page['_scroll_id']
        hits = page['hits']['hits']
        
        while hits:
            for hit in hits:
                doc = {'_id': hit['_id'], '_index': hit['_index'], '_score': hit.get('_score'), **hit['_source']} if include_meta else hit['_source']

                flat = {}
                def flatten(obj, parent=''):
                    for k, v in obj.items():
                        key = f"{parent}.{k}" if parent else k
                        if isinstance(v, dict):
                            flatten(v, key)
                        elif isinstance(v, list):
                            flat[key] = json.dumps(v)
                        else:
                            flat[key] = v
                flatten(doc)

                row_fixed = {fn: flat.get(fn, "") for fn in fieldnames_list}
                writer.writerow(row_fixed)
                total += 1

            # Update progress every second
            current_time = time.time()
            if current_time - last_update > 1:
                queue.put(('progress', total, hits_total))
                queue.put(('heartbeat', current_time - start))
                last_update = current_time

            page = es.scroll(scroll_id=sid, scroll=scroll_time)
            sid = page['_scroll_id']
            hits = page['hits']['hits']

        es.clear_scroll(scroll_id=sid)
        temp.close()

        file_size = get_file_size_mb(temp.name)

        queue.put(('done', {
            'file': temp.name,
            'time': time.time() - start,
            'total': total,
            'batch': batch,
            'size_mb': file_size
        }))
    except Exception as e:
        queue.put(('error', str(e)))
        if temp:
            try:
                temp.close()
            except:
                pass


# ------------------ Streamlit UI ------------------
st.title("🔍 Elasticsearch Data Extractor")
st.markdown("Extract data streams directly to CSV")

# Sidebar: Connection
with st.sidebar:
    st.header("🔧 Connection")
    host = st.text_input("Host", "10.xx.xx.xx")
    port = st.number_input("Port", value=9200, min_value=1, max_value=65535)
    auth = st.selectbox("Auth", ["API Key", "Basic Auth", "None"])
    ssl = st.checkbox("SSL", False)

    if auth == "API Key":
        key = st.text_input("Key", value="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", type="password")
        user = pwd = None
    elif auth == "Basic Auth":
        user = st.text_input("User")
        pwd = st.text_input("Pass", type="password")
        key = None
    else:
        key = user = pwd = None

    if st.button("🔌 Connect"):
        try:
            es = create_client(host, port, ssl, auth, key, user, pwd)
            if not es.ping():
                es.info()

            st.session_state.es_client = es
            st.session_state.connected = True

            ds = es.indices.get_data_stream(name="*")
            streams = [
                {
                    'name': d.get('name'),
                    'time': d.get('timestamp_field', {}).get('name', '@timestamp'),
                    'count': len(d.get('indices', []))
                }
                for d in ds.get('data_streams', [])
                if not d.get('name', '').startswith('.')
            ]
            st.session_state.data_streams = streams
            st.success(f"✅ {len(streams)} streams found")
        except Exception as e:
            st.error(f"❌ Connection failed: {e}")
            st.session_state.connected = False

    st.markdown("**Status:**")
    st.write("✅ Connected" if st.session_state.connected else "⚠️ Disconnected")
    st.markdown("---")
    st.header("⚙️ Settings")
    batch = st.number_input("Batch size", value=5000, min_value=1000, max_value=50000)
    scroll = st.text_input("Scroll timeout", "5m")


# ------------------ Main Logic ------------------
if not st.session_state.connected:
    st.warning("⚠️ Connect first")
    st.stop()
if not st.session_state.data_streams:
    st.warning("⚠️ No streams available")
    st.stop()

# Stream selection
st.subheader("📊 Data Stream")
opts = [f"{s['name']} ({s['count']} indices)" for s in st.session_state.data_streams]
sel_map = {o: s['name'] for o, s in zip(opts, st.session_state.data_streams)}
sel = st.selectbox("Stream", opts)
stream = sel_map[sel]

# Fetch fields
try:
    mapping = st.session_state.es_client.indices.get_mapping(index=stream)
    all_fields = []
    for idx, data in mapping.items():
        all_fields.extend(get_fields(data['mappings'].get('properties', {})))

    seen = set()
    fields = []
    for f in all_fields:
        if f['field'] not in seen:
            seen.add(f['field'])
            fields.append(f)

    st.session_state.fields = fields
    with st.expander(f"📋 {len(fields)} fields"):
        for f in fields[:50]:
            st.text(f"{f['field']} ({f['type']})")
        if len(fields) > 50:
            st.text(f"... and {len(fields)-50} more")
except Exception as e:
    st.error(f"Fields error: {e}")
    fields = []

# Filters
col1, col2 = st.columns([3, 1])
with col1:
    st.subheader("⏰ Time Range")
    ttype = st.radio("Type", ["None", "Relative", "Absolute"], horizontal=True, help="Absolute time works like Kibana's time picker")
    
    # Initialize time filter variables
    tfilter = None
    tfield = "@timestamp"
    
    if ttype == "Relative":
        trange = st.selectbox("Range", ["15m", "30m", "1h", "2h", "4h", "12h", "24h", "7d", "30d"])
        tmap = {"15m": 15, "30m": 30, "1h": 60, "2h": 120, "4h": 240, "12h": 720, "24h": 1440, "7d": 10080, "30d": 43200}
        tfilter = {'gte': f'now-{tmap[trange]}m', 'lte': 'now'}
        dates = [f['field'] for f in fields if f['type'] in ['date', 'date_nanos']]
        tfield = st.selectbox("Time Field", dates, index=dates.index("@timestamp") if "@timestamp" in dates else 0) if dates else st.text_input("Time Field", "@timestamp")
        
        # Show what time range will be used
        st.info(f"📅 Will search from **now-{trange}** to **now** using field: `{tfield}`")
        
        # Test time filter button
        if st.button("🧪 Test Time Filter", help="Check how many documents match this time range"):
            try:
                test_query = {
                    'query': {
                        'range': {
                            tfield: tfilter
                        }
                    }
                }
                result = st.session_state.es_client.search(
                    index=stream,
                    **test_query,
                    size=0,
                    track_total_hits=True
                )
                total = result['hits']['total']['value']
                if total == 0:
                    st.warning(f"⚠️ No documents found in last {trange}. Try a larger time range.")
                else:
                    st.success(f"✅ Found {total:,} documents in last {trange}")
            except Exception as e:
                st.error(f"❌ Error testing time filter: {e}")
        
    elif ttype == "Absolute":
        st.markdown("**🕐 Absolute Time Range (Like Kibana)**")
        
        # Quick paste from Kibana option
        with st.expander("📋 Paste from Kibana (Quick)", expanded=False):
            st.write("Copy the exact time range from Kibana and paste here:")
            kibana_format = st.text_input(
                "Kibana time range",
                placeholder="Oct 28, 2025 @ 00:00:00.000 - Oct 28, 2025 @ 00:30:00.000",
                help="Copy the time range text from Kibana Discover top bar"
            )
            
            if kibana_format and st.button("Parse Kibana Time"):
                try:
                    import re
                    # Parse format like: "Oct 28, 2025 @ 00:00:00.000 - Oct 28, 2025 @ 00:30:00.000"
                    pattern = r'(\w+ \d+, \d+) @ (\d+:\d+:\d+)\.\d+ - (\w+ \d+, \d+) @ (\d+:\d+:\d+)\.\d+'
                    match = re.match(pattern, kibana_format)
                    
                    if match:
                        from datetime import datetime
                        start_date_str = match.group(1)
                        start_time_str = match.group(2)
                        end_date_str = match.group(3)
                        end_time_str = match.group(4)
                        
                        start_parsed = datetime.strptime(f"{start_date_str} {start_time_str}", "%b %d, %Y %H:%M:%S")
                        end_parsed = datetime.strptime(f"{end_date_str} {end_time_str}", "%b %d, %Y %H:%M:%S")
                        
                        st.success(f"✅ Parsed: {start_parsed} to {end_parsed}")
                        st.info("Now manually enter these dates/times in the fields below")
                    else:
                        st.error("Could not parse format. Use manual entry below.")
                except Exception as e:
                    st.error(f"Parse error: {e}")
        
        col_start, col_end = st.columns(2)
        
        with col_start:
            st.write("**From:**")
            start_date = st.date_input("Start Date", key="start_date")
            start_time = st.time_input("Start Time", value=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).time(), key="start_time")
            
        with col_end:
            st.write("**To:**")
            end_date = st.date_input("End Date", key="end_date")
            end_time = st.time_input("End Time", value=datetime.now().time(), key="end_time")
        
        # Combine date and time
        start_dt = datetime.combine(start_date, start_time)
        end_dt = datetime.combine(end_date, end_time)
        
        # Timezone handling
        tz_option = st.radio(
            "Timezone",
            ["Browser Local Time (Auto-detect)", "UTC", "Custom"],
            horizontal=True,
            help="Browser Local Time will auto-detect your timezone and convert to UTC like Kibana does"
        )
        
        if tz_option == "Browser Local Time (Auto-detect)":
            # Get the local timezone offset
            from datetime import timezone
            import time as time_module
            
            # Get local timezone offset in seconds
            if time_module.daylight:
                offset_sec = time_module.altzone
            else:
                offset_sec = time_module.timezone
            
            # Convert to hours (negative because timezone is opposite of UTC offset)
            offset_hours = -offset_sec / 3600
            
            # Create timezone-aware datetimes with local offset
            from datetime import timedelta
            local_tz = timezone(timedelta(hours=offset_hours))
            
            start_dt_aware = start_dt.replace(tzinfo=local_tz)
            end_dt_aware = end_dt.replace(tzinfo=local_tz)
            
            # Convert to UTC
            start_dt_utc = start_dt_aware.astimezone(timezone.utc)
            end_dt_utc = end_dt_aware.astimezone(timezone.utc)
            
            # Format as ISO string
            start_iso = start_dt_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            end_iso = end_dt_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            
            st.caption(f"🌍 Local Time (UTC{offset_hours:+.0f}) → UTC")
            st.caption(f"📍 {start_dt.strftime('%Y-%m-%d %H:%M:%S')} → {start_iso}")
            st.caption(f"📍 {end_dt.strftime('%Y-%m-%d %H:%M:%S')} → {end_iso}")
            
        elif tz_option == "UTC":
            # Treat input as already UTC, add milliseconds and Z
            start_iso = start_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            end_iso = end_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            st.caption(f"🌍 Treating input as UTC: {start_iso} to {end_iso}")
            
        else:  # Custom
            tz_list = ['Asia/Riyadh', 'Asia/Dubai', 'US/Eastern', 'US/Central', 'US/Mountain', 'US/Pacific', 'Europe/London', 'Europe/Paris', 'Europe/Berlin', 'Asia/Tokyo', 'Asia/Shanghai', 'Australia/Sydney', 'UTC']
            selected_tz = st.selectbox("Select Timezone", tz_list)
            
            # Create timezone-aware datetimes
            tz = pytz.timezone(selected_tz)
            start_dt_tz = tz.localize(start_dt)
            end_dt_tz = tz.localize(end_dt)
            
            # Convert to UTC
            start_dt_utc = start_dt_tz.astimezone(pytz.UTC)
            end_dt_utc = end_dt_tz.astimezone(pytz.UTC)
            
            # Format as ISO string with milliseconds
            start_iso = start_dt_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            end_iso = end_dt_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            st.caption(f"🌐 {selected_tz} → UTC: {start_iso} to {end_iso}")
        
        tfilter = {'gte': start_iso, 'lte': end_iso}
        
        # Time field selection
        dates = [f['field'] for f in fields if f['type'] in ['date', 'date_nanos']]
        tfield = st.selectbox("Time Field", dates, index=dates.index("@timestamp") if "@timestamp" in dates else 0) if dates else st.text_input("Time Field", "@timestamp")
        
        # Show summary
        duration = end_dt - start_dt
        st.info(f"📅 Range: **{start_dt.strftime('%Y-%m-%d %H:%M:%S')}** → **{end_dt.strftime('%Y-%m-%d %H:%M:%S')}** (Duration: {duration}) | Field: `{tfield}`")
        
        # Test time filter button
        if st.button("🧪 Test Time Filter", help="Check how many documents match this exact time range", key="test_absolute"):
            try:
                test_query = {
                    'query': {
                        'range': {
                            tfield: tfilter
                        }
                    }
                }
                
                with st.expander("📋 Time Filter Query (Elasticsearch DSL)", expanded=False):
                    st.json(test_query)
                    st.write(f"**Start (gte):** {tfilter['gte']}")
                    st.write(f"**End (lte):** {tfilter['lte']}")
                
                result = st.session_state.es_client.search(
                    index=stream,
                    **test_query,
                    size=0,
                    track_total_hits=True
                )
                total = result['hits']['total']['value']
                if total == 0:
                    st.warning(f"⚠️ No documents found in this time range.")
                    st.info("💡 Try: 1) Check timezone setting, 2) Verify date/time are correct, 3) Check time field name")
                else:
                    st.success(f"✅ Found **{total:,}** documents in this time range")
                    
                    # Show first and last document timestamps
                    sample_first = st.session_state.es_client.search(
                        index=stream,
                        **test_query,
                        size=1,
                        sort=[{tfield: "asc"}]
                    )
                    sample_last = st.session_state.es_client.search(
                        index=stream,
                        **test_query,
                        size=1,
                        sort=[{tfield: "desc"}]
                    )
                    
                    if sample_first['hits']['hits'] and sample_last['hits']['hits']:
                        first_ts = sample_first['hits']['hits'][0]['_source'].get(tfield)
                        last_ts = sample_last['hits']['hits'][0]['_source'].get(tfield)
                        st.caption(f"🔹 First document timestamp: {first_ts}")
                        st.caption(f"🔹 Last document timestamp: {last_ts}")
                        
            except Exception as e:
                st.error(f"❌ Error testing time filter: {e}")
                import traceback
                with st.expander("Error details"):
                    st.code(traceback.format_exc())

    st.markdown("---")
    st.subheader("🔍 Query Filter")
    
    # Query type selector
    query_type = st.radio(
        "Query Type",
        ["KQL (Kibana Query Language)", "Lucene Query", "Simple Match"],
        horizontal=True,
        help="Choose the query syntax you prefer"
    )
    
    # Show examples based on query type
    if query_type == "KQL (Kibana Query Language)":
        placeholder_text = """Examples:
host.name: "server1"
host.name: "server1" AND status: 200
user.name: "admin" OR user.name: "root"
response.status >= 400
NOT status: 500"""
    elif query_type == "Lucene Query":
        placeholder_text = """Examples:
host.name:server1
host.name:server1 AND status:200
user.name:(admin OR root)
response.status:[400 TO *]
-status:500"""
    else:  # Simple Match
        placeholder_text = """Examples:
server1 admin
error failed
success 200"""
    
    kql = st.text_area(
        "Query (optional)", 
        placeholder=placeholder_text,
        height=120,
        help="Leave empty to extract all documents"
    )
    
    # Validate query syntax and show warnings
    if kql and kql.strip():
        # Check for common mistakes
        warnings = []
        
        # Check for "field: * AND field >" pattern (common mistake)
        if ": *" in kql and ">" in kql:
            warnings.append("⚠️ Avoid using `field: * AND field > value`. Just use `field > value` or `field >= value`")
        
        # Check for mixing : * with other conditions on same field
        import re
        field_star_pattern = r'(\w+(?:\.\w+)*): \*'
        matches = re.findall(field_star_pattern, kql)
        for field in matches:
            if f"{field} >" in kql or f"{field} <" in kql or f"{field}:" in kql.replace(f"{field}: *", ""):
                warnings.append(f"⚠️ Field `{field}` has both existence check (`: *`) and other conditions. Remove the `: *` part.")
        
        # Show warnings
        if warnings:
            for warning in warnings:
                st.warning(warning)
            st.info("💡 Tip: Test your query using the '🧪 Test Query' button below to verify it returns the expected count.")
    
    # Add query help expander
    with st.expander("📖 Query Syntax Help"):
        if query_type == "KQL (Kibana Query Language)":
            st.markdown("""
            **KQL Syntax:**
            - **Exact match:** `field: "exact value"`
            - **AND operator:** `field1: value1 AND field2: value2`
            - **OR operator:** `field1: value1 OR field2: value2`
            - **NOT operator:** `NOT field: value`
            - **Wildcard:** `field: val*` or `field: *alue`
            - **Ranges:** `field >= 100` or `field < 500`
            - **Exists:** `field: *`
            """)
        elif query_type == "Lucene Query":
            st.markdown("""
            **Lucene Syntax:**
            - **Exact match:** `field:value` or `field:"exact phrase"`
            - **AND operator:** `field1:value1 AND field2:value2`
            - **OR operator:** `field1:value1 OR field2:value2`
            - **NOT operator:** `-field:value` or `NOT field:value`
            - **Wildcard:** `field:val*` or `field:?alue`
            - **Ranges:** `field:[100 TO 500]` or `field:{100 TO *}`
            - **Exists:** `_exists_:field`
            """)
        else:
            st.markdown("""
            **Simple Match:**
            - Searches for terms across all fields
            - Multiple words are treated as separate terms
            - Best for simple text searches
            - Example: `error failed` finds documents with "error" OR "failed"
            """)
    
    # Add test query button
    if kql and kql.strip():
        col_test1, col_test2 = st.columns(2)
        
        with col_test1:
            if st.button("🧪 Test Query Only", help="Test query without time filter", use_container_width=True):
                try:
                    # Build test query based on type
                    test_query = {}
                    
                    if query_type == "KQL (Kibana Query Language)":
                        test_query = {
                            'query': {
                                'bool': {
                                    'must': [
                                        {
                                            'query_string': {
                                                'query': kql.strip(),
                                                'analyze_wildcard': True,
                                                'default_operator': 'AND'
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    elif query_type == "Lucene Query":
                        test_query = {
                            'query': {
                                'query_string': {
                                    'query': kql.strip(),
                                    'analyze_wildcard': True
                                }
                            }
                        }
                    else:  # Simple Match
                        test_query = {
                            'query': {
                                'multi_match': {
                                    'query': kql.strip(),
                                    'fields': ['*'],
                                    'type': 'best_fields'
                                }
                            }
                        }
                    
                    # Test the query
                    test_result = st.session_state.es_client.search(
                        index=stream,
                        **test_query,
                        size=0,
                        track_total_hits=True,
                        timeout='30s'
                    )
                    
                    total_hits = test_result['hits']['total']['value']
                    
                    if total_hits == 0:
                        st.warning(f"⚠️ Query returned 0 results. Check your query syntax.")
                    else:
                        st.info(f"ℹ️ Query alone (no time filter): **{total_hits:,}** documents")
                    
                except Exception as e:
                    st.error(f"❌ Query error: {str(e)}")
        
        with col_test2:
            if st.button("🎯 Test Query + Time Filter", help="Test exactly what will be extracted", use_container_width=True):
                try:
                    import re
                    
                    # Build COMPLETE query with time filter (exactly what extraction will use)
                    test_query = {}
                    kql_query = kql.strip()
                    
                    # Check if query contains range operators (>, <, >=, <=)
                    range_pattern = r'(\w+(?:\.\w+)*)\s*(>=|<=|>|<)\s*(\d+)'
                    range_match = re.search(range_pattern, kql_query)
                    
                    if range_match and query_type == "KQL (Kibana Query Language)":
                        # Extract field, operator, and value
                        field = range_match.group(1)
                        operator = range_match.group(2)
                        value = int(range_match.group(3))
                        
                        # Convert to proper range query
                        range_query = {}
                        if operator == '>':
                            range_query = {'gt': value}
                        elif operator == '>=':
                            range_query = {'gte': value}
                        elif operator == '<':
                            range_query = {'lt': value}
                        elif operator == '<=':
                            range_query = {'lte': value}
                        
                        # Check if there are other conditions
                        remaining_query = re.sub(range_pattern, '', kql_query).strip()
                        
                        if remaining_query and remaining_query not in ['AND', 'OR']:
                            # Has other conditions
                            test_query['query'] = {
                                'bool': {
                                    'must': [
                                        {
                                            'query_string': {
                                                'query': remaining_query,
                                                'analyze_wildcard': True,
                                                'default_operator': 'AND'
                                            }
                                        }
                                    ],
                                    'filter': [
                                        {
                                            'range': {
                                                field: range_query
                                            }
                                        }
                                    ]
                                }
                            }
                        else:
                            # Only range query
                            test_query['query'] = {
                                'range': {
                                    field: range_query
                                }
                            }
                        
                        st.success(f"🔧 Converted: `{field} {operator} {value}` → Elasticsearch range query")
                    
                    elif query_type == "KQL (Kibana Query Language)":
                        test_query['query'] = {
                            'bool': {
                                'must': [
                                    {
                                        'query_string': {
                                            'query': kql_query,
                                            'analyze_wildcard': True,
                                            'default_operator': 'AND'
                                        }
                                    }
                                ]
                            }
                        }
                    elif query_type == "Lucene Query":
                        test_query['query'] = {
                            'query_string': {
                                'query': kql_query,
                                'analyze_wildcard': True
                            }
                        }
                    else:  # Simple Match
                        test_query['query'] = {
                            'multi_match': {
                                'query': kql_query,
                                'fields': ['*'],
                                'type': 'best_fields'
                            }
                        }
                    
                    # Add time filter if exists
                    if ttype != "None" and tfilter:
                        # Check if we already have a range query (httpStatusCode)
                        if test_query['query'].get('range'):
                            # We have a simple range query, need to wrap it in bool with filter
                            existing_range = test_query['query']['range']
                            test_query['query'] = {
                                'bool': {
                                    'filter': [
                                        {'range': existing_range},  # Keep httpStatusCode filter
                                        {'range': {tfield: tfilter}}  # Add time filter
                                    ]
                                }
                            }
                        elif 'bool' not in test_query['query']:
                            # No bool yet, create one with time filter
                            test_query['query'] = {'bool': {'filter': [{'range': {tfield: tfilter}}]}}
                        else:
                            # Already has bool, just append time filter
                            test_query['query']['bool'].setdefault('filter', []).append({'range': {tfield: tfilter}})
                    
                    # Show the exact query being tested
                    with st.expander("📋 Complete Query (Proper Elasticsearch DSL)", expanded=True):
                        st.json(test_query)
                        st.caption("This is the EXACT query that will be used for extraction")
                    
                    # Test the query
                    test_result = st.session_state.es_client.search(
                        index=stream,
                        **test_query,
                        size=0,
                        track_total_hits=True,
                        timeout='30s'
                    )
                    
                    total_hits = test_result['hits']['total']['value']
                    
                    if total_hits == 0:
                        st.warning(f"⚠️ Combined query returned 0 results.")
                        if ttype != "None":
                            st.info("💡 Try testing time filter separately or expanding time range")
                    else:
                        st.success(f"✅ **This is what will be extracted: {total_hits:,} documents**")
                        if ttype != "None":
                            st.info(f"ℹ️ Includes: Query filter + Time filter (last {trange if ttype == 'Relative' else 'custom range'})")
                    
                    # Show sample
                    if total_hits > 0:
                        sample = st.session_state.es_client.search(
                            index=stream,
                            **test_query,
                            size=3
                        )
                        
                        with st.expander("📄 Sample Results (first 3 documents)"):
                            for idx, hit in enumerate(sample['hits']['hits']):
                                st.write(f"**Document {idx+1}:**")
                                source = hit['_source']
                                # Show relevant fields
                                if 'httpStatusCode' in str(kql):
                                    st.write(f"- httpStatusCode: {source.get('httpStatusCode', 'N/A')}")
                                if tfield in source:
                                    st.write(f"- {tfield}: {source.get(tfield, 'N/A')}")
                                with st.expander("Full document"):
                                    st.json(source)
                                if idx < 2:
                                    st.markdown("---")
                    
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())

with col2:
    st.subheader("📁 Options")
    meta = st.checkbox("Include metadata")
    st.subheader("🎯 Fields")
    fsel = st.radio("Select", ["All", "Specific"])
    selected = []  # Initialize selected fields
    
    if fsel == "Specific" and fields:
        st.info(f"📋 Total fields available: {len(fields)}")
        
        # Initialize session state for selected fields
        if 'selected_fields' not in st.session_state:
            st.session_state.selected_fields = []
        
        # Create a searchable field name list for validation
        available_field_names = {f['field']: f['type'] for f in fields}
        
        # Method 1: Text search input (lightweight, works with any number of fields)
        st.markdown("**🔍 Search & Add Fields:**")
        
        search_input = st.text_input(
            "Search for field name",
            placeholder="Type field name (e.g., 'host.name', 'user.id')",
            key="field_search_input",
            help="Type to search, then click Add. Case-sensitive."
        )
        
        # Show matching suggestions as you type
        if search_input and len(search_input) >= 2:
            # Filter matching fields
            matches = [
                f['field'] for f in fields 
                if search_input.lower() in f['field'].lower()
            ]
            
            if matches:
                st.success(f"✅ Found {len(matches)} matching fields")
                
                # Show first 20 matches in a compact list
                with st.expander(f"View {min(len(matches), 20)} matching fields", expanded=True):
                    for match in matches[:20]:
                        field_type = available_field_names.get(match, 'unknown')
                        col1, col2 = st.columns([4, 1])
                        with col1:
                            st.text(f"{match} ({field_type})")
                        with col2:
                            if st.button("➕", key=f"add_{match}", help=f"Add {match}"):
                                if match not in st.session_state.selected_fields:
                                    st.session_state.selected_fields.append(match)
                                    st.success(f"Added: {match}")
                                    st.rerun()
                    
                    if len(matches) > 20:
                        st.info(f"Showing first 20 of {len(matches)} matches. Refine search to see more.")
            else:
                st.warning("❌ No matching fields found")
        elif search_input and len(search_input) < 2:
            st.info("Type at least 2 characters to search")
        
        # Quick add button for exact match
        col_add, col_clear = st.columns(2)
        with col_add:
            if st.button("➕ Add Field (Exact)", use_container_width=True, help="Add the exact field name you typed"):
                if search_input:
                    if search_input in available_field_names:
                        if search_input not in st.session_state.selected_fields:
                            st.session_state.selected_fields.append(search_input)
                            st.success(f"✅ Added: {search_input}")
                            st.rerun()
                        else:
                            st.warning("Already selected")
                    else:
                        st.error(f"❌ Field '{search_input}' does not exist in this index")
        
        with col_clear:
            if st.button("🔄 Clear Search", use_container_width=True):
                st.rerun()
        
        # Display currently selected fields
        if st.session_state.selected_fields:
            st.markdown("---")
            st.markdown(f"**✅ Selected Fields ({len(st.session_state.selected_fields)}):**")
            
            # Show in scrollable container with remove buttons
            for idx, field_name in enumerate(st.session_state.selected_fields):
                col1, col2 = st.columns([5, 1])
                with col1:
                    field_type = available_field_names.get(field_name, 'unknown')
                    st.text(f"{idx+1}. {field_name} ({field_type})")
                with col2:
                    if st.button("🗑️", key=f"remove_{idx}", help=f"Remove {field_name}"):
                        st.session_state.selected_fields.remove(field_name)
                        st.rerun()
            
            # Export selected fields
            if st.button("📋 Copy Selected Fields", use_container_width=True):
                selected_text = '\n'.join(st.session_state.selected_fields)
                st.code(selected_text, language=None)
        else:
            st.info("👆 No fields selected yet. Search and add fields above.")
        
        # Method 2: Bulk text input (for power users)
        st.markdown("---")
        st.markdown("**💨 Bulk Add (Paste Multiple Fields):**")
        
        with st.expander("Paste field names to add multiple at once"):
            fields_text = st.text_area(
                "Field names (comma or line separated)",
                placeholder="host.name, user.id, @timestamp\nor one per line",
                height=120,
                key="bulk_fields_input"
            )
            
            col_bulk_add, col_bulk_validate = st.columns(2)
            
            with col_bulk_add:
                if st.button("➕ Add All", use_container_width=True):
                    if fields_text.strip():
                        # Parse input
                        if ',' in fields_text:
                            new_fields = [f.strip() for f in fields_text.split(',') if f.strip()]
                        else:
                            new_fields = [f.strip() for f in fields_text.split('\n') if f.strip()]
                        
                        # Validate and add
                        valid_fields = [f for f in new_fields if f in available_field_names]
                        invalid_fields = [f for f in new_fields if f not in available_field_names]
                        
                        # Add valid fields (avoid duplicates)
                        added_count = 0
                        for f in valid_fields:
                            if f not in st.session_state.selected_fields:
                                st.session_state.selected_fields.append(f)
                                added_count += 1
                        
                        if added_count > 0:
                            st.success(f"✅ Added {added_count} fields")
                        if invalid_fields:
                            st.warning(f"⚠️ Skipped {len(invalid_fields)} invalid fields: {', '.join(invalid_fields[:5])}")
                        
                        st.rerun()
            
            with col_bulk_validate:
                if st.button("🔍 Validate Only", use_container_width=True):
                    if fields_text.strip():
                        if ',' in fields_text:
                            test_fields = [f.strip() for f in fields_text.split(',') if f.strip()]
                        else:
                            test_fields = [f.strip() for f in fields_text.split('\n') if f.strip()]
                        
                        valid = [f for f in test_fields if f in available_field_names]
                        invalid = [f for f in test_fields if f not in available_field_names]
                        
                        st.info(f"✅ Valid: {len(valid)} | ❌ Invalid: {len(invalid)}")
                        if invalid:
                            st.error(f"Invalid fields: {', '.join(invalid[:10])}")
        
        # Quick action buttons
        st.markdown("---")
        st.markdown("**⚡ Quick Actions:**")
        col_a, col_b, col_c = st.columns(3)
        
        with col_a:
            if st.button("📅 All Date Fields", use_container_width=True):
                date_fields = [f['field'] for f in fields if f['type'] in ['date', 'date_nanos']]
                st.session_state.selected_fields = date_fields
                st.success(f"Selected {len(date_fields)} date fields")
                st.rerun()
        
        with col_b:
            if st.button("🌐 All Fields", use_container_width=True):
                if len(fields) > 2000:
                    st.warning(f"⚠️ This will select all {len(fields)} fields. This may take a while.")
                st.session_state.selected_fields = [f['field'] for f in fields]
                st.success(f"Selected all {len(fields)} fields")
                st.rerun()
        
        with col_c:
            if st.button("🗑️ Clear All", use_container_width=True):
                count = len(st.session_state.selected_fields)
                st.session_state.selected_fields = []
                st.info(f"Cleared {count} fields")
                st.rerun()
        
        # Export all available fields for reference
        if st.button("📋 Export All Available Field Names", use_container_width=True):
            st.session_state['show_all_fields'] = not st.session_state.get('show_all_fields', False)
        
        if st.session_state.get('show_all_fields'):
            with st.expander("📋 All Available Fields (Copy to use in bulk add)", expanded=True):
                all_field_text = '\n'.join([f['field'] for f in fields])
                st.download_button(
                    label="💾 Download Field List",
                    data=all_field_text,
                    file_name=f"{stream}_fields.txt",
                    mime="text/plain"
                )
                st.code(all_field_text, language=None)
                st.info(f"Total: {len(fields)} fields")
        
        # Set selected to session state value
        selected = st.session_state.selected_fields

# Extract button
btn = st.button("🚀 Extract to CSV", disabled=st.session_state.extracting, use_container_width=True)

# Show extraction summary before starting
if not st.session_state.extracting and (kql or ttype != "None" or (fsel == "Specific" and selected)):
    with st.expander("📋 Extraction Summary", expanded=False):
        st.markdown("**What will be extracted:**")
        
        # Data stream
        st.write(f"🗂️ **Data Stream:** `{stream}`")
        
        # Time range
        if ttype != "None":
            if ttype == "Relative":
                st.write(f"⏰ **Time Range:** Last {trange}")
            elif ttype == "Calendar":
                st.write(f"⏰ **Time Range:** {sdt} to {edt}")
        else:
            st.write(f"⏰ **Time Range:** All time")
        
        # KQL filter
        if kql and kql.strip():
            st.write(f"🔍 **KQL Filter:** `{kql.strip()}`")
        else:
            st.write(f"🔍 **KQL Filter:** None (all documents)")
        
        # Fields
        if fsel == "Specific" and selected:
            st.write(f"🎯 **Fields:** {len(selected)} specific fields selected")
            with st.expander("View selected fields"):
                st.write(", ".join(selected))
        else:
            st.write(f"🎯 **Fields:** All fields")
        
        # Batch size
        st.write(f"📦 **Batch Size:** {batch:,} documents per batch")

if st.session_state.extracting:
    st.info("⏳ Extraction in progress...")

if btn:
    try:
        # Clean up any old files from previous extraction
        if 'csv_file' in st.session_state and st.session_state.csv_file:
            try:
                if os.path.exists(st.session_state.csv_file):
                    os.remove(st.session_state.csv_file)
                    st.info("🧹 Cleaned up previous extraction file")
            except:
                pass
        
        # Reset download state
        st.session_state.download_ready = False
        st.session_state.download_clicked = False
        
        query = {"query": {"match_all": {}}}
        
        # Parse and build query based on selected type
        if kql and kql.strip():
            kql_query = kql.strip()
            
            # Check if query contains range operators (>, <, >=, <=)
            import re
            # Pattern: field >= value or field > value etc.
            range_pattern = r'(\w+(?:\.\w+)*)\s*(>=|<=|>|<)\s*(\d+)'
            range_match = re.search(range_pattern, kql_query)
            
            if range_match and query_type == "KQL (Kibana Query Language)":
                # Extract field, operator, and value
                field = range_match.group(1)
                operator = range_match.group(2)
                value = int(range_match.group(3))
                
                # Convert to proper range query
                range_query = {}
                if operator == '>':
                    range_query = {'gt': value}
                elif operator == '>=':
                    range_query = {'gte': value}
                elif operator == '<':
                    range_query = {'lt': value}
                elif operator == '<=':
                    range_query = {'lte': value}
                
                # Check if there are other conditions
                remaining_query = re.sub(range_pattern, '', kql_query).strip()
                
                if remaining_query and remaining_query not in ['AND', 'OR']:
                    # Has other conditions - use bool query with range filter
                    query['query'] = {
                        'bool': {
                            'must': [
                                {
                                    'query_string': {
                                        'query': remaining_query,
                                        'analyze_wildcard': True,
                                        'default_operator': 'AND'
                                    }
                                }
                            ],
                            'filter': [
                                {
                                    'range': {
                                        field: range_query
                                    }
                                }
                            ]
                        }
                    }
                else:
                    # Only range query
                    query['query'] = {
                        'range': {
                            field: range_query
                        }
                    }
                
                st.info(f"🔧 Converted range query: `{field} {operator} {value}` → proper Elasticsearch range syntax")
            
            elif query_type == "KQL (Kibana Query Language)":
                query['query'] = {
                    'bool': {
                        'must': [
                            {
                                'query_string': {
                                    'query': kql_query,
                                    'analyze_wildcard': True,
                                    'default_operator': 'AND'
                                }
                            }
                        ]
                    }
                }
            elif query_type == "Lucene Query":
                query['query'] = {
                    'query_string': {
                        'query': kql_query,
                        'analyze_wildcard': True
                    }
                }
            else:  # Simple Match
                query['query'] = {
                    'multi_match': {
                        'query': kql_query,
                        'fields': ['*'],
                        'type': 'best_fields'
                    }
                }
        
        # Add time filter
        if ttype != "None" and tfilter:
            # Check if we already have a simple range query (httpStatusCode)
            if query['query'].get('range'):
                # We have a simple range query, need to wrap it in bool with filter
                existing_range = query['query']['range']
                query['query'] = {
                    'bool': {
                        'filter': [
                            {'range': existing_range},  # Keep httpStatusCode filter
                            {'range': {tfield: tfilter}}  # Add time filter
                        ]
                    }
                }
            elif 'bool' not in query['query']:
                # No bool yet, create one with time filter
                query['query'] = {'bool': {'filter': [{'range': {tfield: tfilter}}]}}
            else:
                # Already has bool, just append time filter
                query['query']['bool'].setdefault('filter', []).append({'range': {tfield: tfilter}})
        
        # Add field selection
        if fsel == "Specific" and selected and len(selected) > 0:
            query['_source'] = selected
            st.info(f"🎯 Will extract {len(selected)} specific fields")
        else:
            st.info(f"🎯 Will extract ALL fields")
        
        # Debug: Show the query being used
        with st.expander("🔍 Query Details (Debug)", expanded=True):
            st.write("**Time Filter:**")
            if ttype != "None" and tfilter:
                st.success(f"✅ Time filter ACTIVE on field: `{tfield}`")
                st.write(f"- Type: {ttype}")
                st.write(f"- Range: {tfilter}")
                if ttype == "Relative":
                    st.write(f"- Human readable: Last {trange}")
            else:
                st.warning("⚠️ No time filter - Will search ALL time")
            
            st.write("**Field Selection:**")
            st.write(f"- Mode: {fsel}")
            st.write(f"- Selected fields count: {len(selected)}")
            if selected:
                st.write(f"- Fields list:")
                st.code('\n'.join(selected[:20]), language=None)
                if len(selected) > 20:
                    st.write(f"... and {len(selected)-20} more")
            
            st.write("**Complete Query:**")
            st.json(query)
            
            st.write("**What Elasticsearch will receive:**")
            if '_source' in query:
                st.success(f"✅ _source parameter SET - Will fetch ONLY these {len(query['_source'])} fields")
                st.write("First 10 fields:", query['_source'][:10])
            else:
                st.warning("⚠️ _source parameter NOT SET - Will fetch ALL fields")
            
            st.write("**Query:**")
            st.json(query)
            
            st.write("**What will be sent to Elasticsearch:**")
            if '_source' in query:
                st.success(f"✅ Will fetch ONLY these {len(query['_source'])} fields: {query['_source'][:10]}")
            else:
                st.info("ℹ️ Will fetch ALL fields (no _source filter)")

        st.session_state.extracting = True
        q = Queue()
        t = threading.Thread(
            target=extract_to_csv,
            args=(st.session_state.es_client, stream, query, scroll, batch, meta, q),
            daemon=True
        )
        t.start()

        # Create enhanced progress display
        st.markdown("### 📊 Extraction Progress")
        
        # Main progress bar
        prog = st.progress(0, text="Initializing...")
        
        # Metrics display
        metric_cols = st.columns(4)
        total_metric = metric_cols[0].empty()
        current_metric = metric_cols[1].empty()
        speed_metric = metric_cols[2].empty()
        eta_metric = metric_cols[3].empty()
        
        # Status message
        stat = st.empty()
        
        # Variables for tracking
        total_records = 0
        start_time = time.time()

        while t.is_alive() or not q.empty():
            try:
                msg = q.get(timeout=0.1)
                if msg[0] == 'status':
                    stat.info(f"🔄 {msg[1]}")
                    prog.progress(0, text=msg[1])
                    
                elif msg[0] == 'total':
                    total_records = msg[1]
                    # Keep showing total throughout extraction
                    total_metric.metric("📋 Total Records", f"{total_records:,}")
                    stat.success(f"✅ Found {total_records:,} records to extract")
                    
                elif msg[0] == 'progress':
                    current, total = msg[1], msg[2]
                    
                    # Update total_records if not set
                    if total_records == 0:
                        total_records = total
                    
                    # Keep showing total metric
                    total_metric.metric("📋 Total Records", f"{total_records:,}")
                    
                    # Calculate metrics
                    percentage = (current / total * 100) if total > 0 else 0
                    elapsed = time.time() - start_time
                    speed = current / elapsed if elapsed > 0 else 0
                    remaining = total - current
                    eta_seconds = remaining / speed if speed > 0 else 0
                    
                    # Update progress bar
                    progress_value = min(current/total, 1.0) if total > 0 else 0
                    prog.progress(progress_value, text=f"Extracting: {percentage:.1f}%")
                    
                    # Update metrics
                    current_metric.metric("✅ Extracted", f"{current:,}", f"{percentage:.1f}%")
                    speed_metric.metric("⚡ Speed", f"{speed:.0f} docs/s")
                    
                    # Format ETA
                    if eta_seconds > 0:
                        if eta_seconds < 60:
                            eta_text = f"{eta_seconds:.0f}s"
                        elif eta_seconds < 3600:
                            eta_text = f"{eta_seconds/60:.1f}m"
                        else:
                            eta_text = f"{eta_seconds/3600:.1f}h"
                        eta_metric.metric("⏱️ ETA", eta_text)
                    
                    # Status message
                    stat.info(f"📤 Extracting: {current:,} / {total:,} records ({percentage:.1f}%) | Speed: {speed:.0f} docs/s")
                    
                elif msg[0] == 'heartbeat':
                    # Heartbeat already tracked in progress
                    pass
                    
                elif msg[0] == 'done':
                    prog.progress(1.0, text="✅ Complete!")
                    stat.success(f"🎉 Extraction complete! {msg[1]['total']:,} records in {msg[1]['time']:.2f}s")
                    st.session_state.csv_file = msg[1]['file']
                    st.session_state.stats = msg[1]
                    st.session_state.download_ready = True
                    st.session_state.extracting = False
                    break
                    
                elif msg[0] == 'error':
                    prog.progress(0, text="❌ Error")
                    stat.error(f"❌ Error: {msg[1]}")
                    st.session_state.extracting = False
                    break
            except:
                time.sleep(0.05)

        t.join(timeout=1)
    except Exception as e:
        st.error(f"Error: {e}")
        st.session_state.extracting = False


# ------------------ Download & Auto-Cleanup ------------------
if st.session_state.download_ready and st.session_state.csv_file:
    s = st.session_state.stats
    st.success(f"✅ {s['total']:,} records extracted in {s['time']:.2f}s")
    
    c1, c2, c3 = st.columns(3)
    c1.metric("Records", f"{s['total']:,}")
    c2.metric("Time", f"{s['time']:.2f}s")
    c3.metric("Speed", f"{s['total']/s['time']:.0f} docs/s" if s['time'] > 0 else "N/A")
    st.markdown("---")

    file_size_mb = s.get('size_mb', 0)
    st.info(f"💾 File size: {file_size_mb:.2f} MB")

    fname = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Read file in chunks to create data for download
    try:
        # Read the file with a reasonable chunk size
        CHUNK_SIZE = 1024 * 1024  # 1MB chunks
        file_data = bytearray()
        
        with open(st.session_state.csv_file, 'rb') as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                file_data.extend(chunk)
        
        csv_bytes = bytes(file_data)
        
        # Initialize download tracking
        if 'download_clicked' not in st.session_state:
            st.session_state.download_clicked = False
        
        # Create download button with on_click callback
        def on_download_click():
            st.session_state.download_clicked = True
        
        # Create download button
        st.download_button(
            label="📥 Download CSV",
            data=csv_bytes,
            file_name=fname,
            mime="text/csv",
            use_container_width=True,
            key="download_csv",
            on_click=on_download_click
        )
        
        # Auto-cleanup after download button is clicked
        if st.session_state.download_clicked:
            st.success("✅ Download started! Cleaning up server file...")
            
            # Delete the temp file
            try:
                if os.path.exists(st.session_state.csv_file):
                    os.remove(st.session_state.csv_file)
                    st.success("🧹 File automatically deleted from server!")
                
                # Clear session state
                st.session_state.csv_file = None
                st.session_state.download_ready = False
                st.session_state.download_clicked = False
                
                # Give user time to see message then rerun
                time.sleep(1.5)
                st.rerun()
                
            except Exception as e:
                st.warning(f"⚠️ Could not auto-delete file: {e}")
        else:
            st.caption("💡 Click download button - file will be automatically deleted from server after download starts")
        
    except MemoryError:
        st.error("❌ File is too large to download via browser")
        st.code(f"Server file path: {st.session_state.csv_file}")
        st.markdown("""
        **Manual download required:**
        ```bash
        scp user@host:{} ./
        ```
        """.format(st.session_state.csv_file))
    except Exception as e:
        st.error(f"❌ Error preparing download: {e}")
        st.code(f"Server file path: {st.session_state.csv_file}")
    
    st.markdown("---")
    
    # Manual cleanup option
    col_a, col_b = st.columns(2)
    
    with col_a:
        if st.button("🧹 Delete File Now", use_container_width=True):
            try:
                if os.path.exists(st.session_state.csv_file):
                    os.remove(st.session_state.csv_file)
                st.session_state.csv_file = None
                st.session_state.download_ready = False
                st.success("✅ File deleted successfully")
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"❌ Error deleting file: {e}")
    
    with col_b:
        if st.button("🔄 New Extraction", use_container_width=True):
            try:
                if st.session_state.csv_file and os.path.exists(st.session_state.csv_file):
                    os.remove(st.session_state.csv_file)
            except Exception:
                pass
            st.session_state.download_ready = False
            st.session_state.csv_file = None
            st.session_state.stats = None
            st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("**v4.6** | Auto-Delete After Download")
