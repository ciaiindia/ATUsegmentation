import streamlit as st
import pandas as pd
import openai
from difflib import SequenceMatcher
import re
import os
from dotenv import load_dotenv
import io

# Page configuration
st.set_page_config(
    page_title="Query Processing App",
    page_icon="🔍",
    layout="wide"
)

# Add title and description
st.title("Query Processing Application")
st.markdown("Process queries and filter NPIs based on specific conditions")

# Azure OpenAI setup
try:
    # Configure OpenAI with Azure settings
    openai.api_type = "azure"
    openai.api_version = "2023-05-15"  # Using a stable API version
    openai.api_base = st.secrets["secrets"]["AZURE_OPENAI_ENDPOINT"]
    openai.api_key = st.secrets["secrets"]["AZURE_OPENAI_API_KEY"]
    
    # Test the configuration
    try:
        response = openai.ChatCompletion.create(
            engine=st.secrets["secrets"]["AZURE_OPENAI_MODEL"],
            messages=[{"role": "user", "content": "test"}],
            max_tokens=5,
            deployment_id=st.secrets["secrets"]["AZURE_OPENAI_MODEL"]
        )
        st.success("Azure OpenAI connection initialized successfully!")
    except Exception as e:
        st.error(f"Error testing Azure OpenAI connection: {str(e)}")
        st.stop()
except Exception as e:
    st.error(f"Error initializing Azure OpenAI settings: {str(e)}")
    st.stop()

# Fixed file paths
MAPPING_FILE = "Segmentation Mapping.xlsx"
RAW_DATA_FILE = "27.12.2024_CSL Vifor Global ATU_Final raw data_v1.xlsx"

@st.cache_data
def load_mapping_data():
    """Load and cache mapping data"""
    try:
        mapping_df = pd.read_excel(MAPPING_FILE)
        mapping_df['Question Distinction'] = mapping_df['Question Distinction'].astype(str)
        mapping_df['Question sub type'] = mapping_df['Question sub type'].astype(str)
        return mapping_df
    except Exception as e:
        st.error(f"Error loading mapping file: {e}")
        return None


def get_correct_column_name(df, column_name):
    """
    Get the actual column name from DataFrame accounting for whitespace variations.
    """
    # Try exact match first
    if column_name in df.columns:
        return column_name
    
    # Try with/without trailing space
    with_space = f"{column_name} "
    without_space = column_name.strip()
    
    if with_space in df.columns:
        return with_space
    if without_space in df.columns:
        return without_space
        
    # If no match found, return None
    print(f"[WARNING] Could not find column '{column_name}' in DataFrame")
    return None

def create_column_mapping(query_columns, raw_data_columns):
    """
    Creates an improved mapping between query column format and raw data column format
    with better error handling and matching logic.
    """
    mapping = {}
    raw_columns_map = {col.strip().lower(): col for col in raw_data_columns}
    
    print("\n[DEBUG] Creating column mapping...")
    print(f"Raw data columns available: {len(raw_data_columns)} columns")
    
    for query_col in query_columns:
        # Clean and standardize the query column name
        clean_query_col = query_col.strip().lower()
        clean_query_col = clean_query_col.replace('|', '_')
        
        # Try exact match first
        if clean_query_col in raw_columns_map:
            mapping[query_col] = raw_columns_map[clean_query_col]
            print(f"Found exact match: {query_col} -> {raw_columns_map[clean_query_col]}")
            continue
            
        # Try partial matches
        partial_matches = []
        query_parts = clean_query_col.split('_')
        
        for raw_col_key, raw_col in raw_columns_map.items():
            # Check if all parts of the query column exist in the raw column
            if all(part in raw_col_key for part in query_parts):
                score = SequenceMatcher(None, clean_query_col, raw_col_key).ratio()
                partial_matches.append((raw_col, score))
        
        # Sort partial matches by score
        if partial_matches:
            partial_matches.sort(key=lambda x: x[1], reverse=True)
            best_match = partial_matches[0][0]
            mapping[query_col] = best_match
            print(f"Found partial match: {query_col} -> {best_match} (score: {partial_matches[0][1]:.2f})")
            
            # Show other potential matches for debugging
            if len(partial_matches) > 1:
                print("Other potential matches:")
                for match, score in partial_matches[1:3]:  # Show top 3
                    print(f"  - {match} (score: {score:.2f})")
        else:
            print(f"No match found for: {query_col}")
            
        # Try fuzzy matching as a last resort
        if query_col not in mapping:
            fuzzy_matches = []
            for raw_col_key, raw_col in raw_columns_map.items():
                score = SequenceMatcher(None, clean_query_col, raw_col_key).ratio()
                if score > 0.8:  # High threshold for fuzzy matching
                    fuzzy_matches.append((raw_col, score))
            
            if fuzzy_matches:
                best_fuzzy = max(fuzzy_matches, key=lambda x: x[1])
                mapping[query_col] = best_fuzzy[0]
                print(f"Found fuzzy match: {query_col} -> {best_fuzzy[0]} (score: {best_fuzzy[1]:.2f})")
    
    # Final validation of mappings
    for query_col, mapped_col in mapping.items():
        if mapped_col not in raw_data_columns:
            print(f"[WARNING] Mapped column '{mapped_col}' not found in raw data!")
            mapping.pop(query_col)
    
    print("\n[DEBUG] Final Column Mapping:")
    for query_col, raw_col in mapping.items():
        print(f"'{query_col}' -> '{raw_col}'")
    
    return mapping

# def load_raw_data(file_path):
#     """Load raw data with correct row and column handling"""
#     try:
#         # Read Excel file starting from row 2 (1-based index)
#         raw_data = pd.read_excel(file_path, skiprows=1)
        
#         print("\n[DEBUG] Available columns in raw data:")
#         print(raw_data.columns.tolist())
        
#         return raw_data
#     except Exception as e:
#         print(f"Error loading raw data: {e}")
#         return None

def filter_npi_based_on_query(raw_data, final_query):
    """Filters NPI based on the given arithmetic query condition."""
    
    # Extract parts of the query
    parts = final_query.split()
    
    # Find the comparison operator and value
    comparison_operators = ['>=', '<=', '>', '<', '=', '≥', '≤']
    operator = None
    value = None
    
    for i, part in enumerate(parts):
        if part in comparison_operators or part.strip() in comparison_operators:
            operator = part.strip()
            if i + 1 < len(parts):
                value = parts[i + 1]
            break
    
    # Extract original column names
    query_columns = [part for part in parts if '|' in part]
    
    # Create dynamic column mapping
    column_mapping = create_column_mapping(query_columns, raw_data.columns)
    
    # Map columns using the dynamic mapping
    columns = [column_mapping.get(col) for col in query_columns if column_mapping.get(col)]
    
    print(f"\n[DEBUG] Query Analysis:")
    print(f"Original Columns: {query_columns}")
    print(f"Mapped Columns: {columns}")
    print(f"Operator: {operator}")
    print(f"Value: {value}")
    
    if not columns:
        print("[ERROR] No valid column names found after mapping!")
        return None
    if not operator:
        print("[ERROR] No comparison operator found in the final query!")
        return None
    if not value:
        print("[ERROR] No numeric threshold value found in the final query!")
        return None
        
    try:
        value = float(value)
    except ValueError:
        print("[ERROR] Invalid numeric value in the query!")
        return None

    # Convert Unicode operators for pandas query while preserving original in output
    operator_mapping = {'≥': '>=', '≤': '<='}
    query_operator = operator_mapping.get(operator, operator)
    
    # Get correct NPI column name
    npi_col = get_correct_column_name(raw_data, 'NPI')
    if not npi_col:
        print("[ERROR] Could not find NPI column in data!")
        return None
        
    print(f"\n[DEBUG] Using NPI column: '{npi_col}'")
    
    # First, exclude rows with null values in any of the relevant columns
    valid_data = raw_data.copy()
    for col in columns:
        null_count = valid_data[col].isnull().sum()
        if null_count > 0:
            print(f"\n[INFO] Found {null_count} null values in column '{col}'")
            valid_data = valid_data[valid_data[col].notna()]
    
    initial_count = len(raw_data)
    after_null_filter_count = len(valid_data)
    excluded_count = initial_count - after_null_filter_count
    
    print(f"\n[DEBUG] Null value filtering:")
    print(f"Initial row count: {initial_count}")
    print(f"Rows after excluding nulls: {after_null_filter_count}")
    print(f"Excluded rows due to nulls: {excluded_count}")
    
    # Compute sum of relevant columns on the filtered data
    print("\n[DEBUG] Computing sum of columns:")
    for col in columns:
        print(f"Using column: '{col}'")
    valid_data["Query_Sum"] = valid_data[columns].sum(axis=1)

    # Apply the filter based on the arithmetic condition
    filtered_npi = valid_data.query(f"Query_Sum {query_operator} {value}")[npi_col]
    
    print(f"\n[DEBUG] Final filtering:")
    print(f"NPIs matching query condition: {len(filtered_npi)}")
    
    return filtered_npi

def save_to_excel(final_query, filtered_npi):
    """Saves the final query and filtered NPIs to an Excel file."""
    output_excel_file = "processed_queries.xlsx"
    
    # Convert NPIs to list and handle any formatting
    npi_list = filtered_npi.tolist()
    
    df = pd.DataFrame({
        "Final Query": [final_query],
        "Filtered NPIs": [", ".join(map(str, npi_list))]
    })
    
    try:
        # Check if file exists
        try:
            existing_df = pd.read_excel(output_excel_file, sheet_name="Processed Queries", engine='openpyxl')
            start_row = len(existing_df) + 1
        except FileNotFoundError:
            start_row = 0
            
        # Write to Excel using openpyxl engine
        with pd.ExcelWriter(output_excel_file, engine='openpyxl', mode='a' if start_row > 0 else 'w') as writer:
            if start_row == 0:
                df.to_excel(writer, sheet_name="Processed Queries", index=False)
            else:
                df.to_excel(writer, sheet_name="Processed Queries", index=False, header=False, startrow=start_row)
                
        print(f"\nSuccessfully saved {len(filtered_npi)} NPIs to {output_excel_file}")
        print(f"Query: {final_query}")
        
    except Exception as e:
        print(f"\nError saving to Excel: {str(e)}")
        print("Attempting to create new file...")
        
        try:
            # If appending fails, try creating a new file
            df.to_excel(output_excel_file, sheet_name="Processed Queries", index=False, engine='openpyxl')
            print(f"Successfully created new file: {output_excel_file}")
        except Exception as e2:
            print(f"Failed to create new file: {str(e2)}")
            return


# def load_mapping_data(file_path):
#     """Load and prepare mapping data from Excel file."""
#     try:
#         # Read Excel file instead of CSV
#         mapping_df = pd.read_excel(file_path)
#         # Convert columns to string type
#         mapping_df['Question Distinction'] = mapping_df['Question Distinction'].astype(str)
#         mapping_df['Question sub type'] = mapping_df['Question sub type'].astype(str)
#         return mapping_df
#     except Exception as e:
#         print(f"Error loading Excel file: {e}")
#         return None

def split_query(query):
    """Split query based on mathematical and arithmetic operations."""
    # Define operators including Unicode versions
    comparison_operators = ['>=', '<=', '>', '<', '=', '≥', '≤']
    arithmetic_operators = ['+', '-', '*', '/']

    # Extract comparison operator and value
    operator_match = None
    value_match = None
    left_side = query  # Default to full query
    
    # Find the rightmost comparison operator
    last_operator_index = -1
    last_operator = None
    
    # First check for Unicode operators
    unicode_operators = {'≥': '>=', '≤': '<='}
    for unicode_op, standard_op in unicode_operators.items():
        idx = query.rfind(unicode_op)
        if idx > last_operator_index:
            last_operator_index = idx
            last_operator = unicode_op  # Keep the Unicode operator
    
    # Then check standard operators if no Unicode operator found
    if last_operator_index == -1:
        for comp_op in comparison_operators:
            if comp_op not in unicode_operators.values():  # Skip the standard versions of Unicode operators
                idx = query.rfind(comp_op)
                if idx > last_operator_index:
                    last_operator_index = idx
                    last_operator = comp_op
    
    if last_operator_index != -1:
        left_side = query[:last_operator_index].strip()
        operator_match = last_operator
        value_match = query[last_operator_index + len(last_operator):].strip()
        value_match = value_match.strip('"').strip()  # Remove quotes and extra spaces

    # Now split left side into arithmetic expressions
    arithmetic_parts = []
    current_part = ""
    
    i = 0
    while i < len(left_side):
        if left_side[i] in arithmetic_operators:
            if current_part.strip():
                arithmetic_parts.append({
                    'query': current_part.strip().replace('"', ''),  # Remove quotes
                    'operator': None,
                    'value': None,
                    'arithmetic_op': left_side[i]
                })
            current_part = ""
        else:
            current_part += left_side[i]
        i += 1
    
    if current_part.strip():
        last_part = {
            'query': current_part.strip().replace('"', ''),  # Remove quotes
            'operator': operator_match,  # Preserve the original operator (Unicode or standard)
            'value': value_match,
            'arithmetic_op': None
        }
        arithmetic_parts.append(last_part)

    print(f"\n[DEBUG] Split Query Results:")
    print(f"Original Operator Found: {operator_match}")
    print(f"Value: {value_match}")
    
    return arithmetic_parts


def clean_text(text):
    """Clean text for better matching."""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = ' '.join(text.split())
    return text

def calculate_similarity(str1, str2):
    """Calculate string similarity ratio."""
    return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()

def get_matching_subtype(query, mapping_df):
    """Get matching Question sub type using OpenAI."""
    # Get all unique question subtypes, sorted by length (longest first to prioritize more specific matches)
    question_subtypes = sorted(
        mapping_df['Question sub type'].dropna().unique().tolist(),
        key=len,
        reverse=True
    )
    
    system_prompt = f"""Analyze the given query and match it to the most appropriate question sub type from the following list:
{', '.join(question_subtypes)}

Instructions:
1. Look for the most specific matching question sub type that fits the query context
2. Consider all aspects of the query (prescribing changes, clinical experience, product preferences, etc.)
3. Match to the exact format as shown in the list
4. Return the complete question sub type, including any detailed descriptions

Return only the exact matching question sub type from the list, without any explanation."""
    
    try:
        response = openai.ChatCompletion.create(
            engine=st.secrets["secrets"]["AZURE_OPENAI_MODEL"],
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query}
            ],
            temperature=0,
            max_tokens=150,
            deployment_id=st.secrets["secrets"]["AZURE_OPENAI_MODEL"]
        )
        matched_subtype = response.choices[0].message.content.strip()
        print(f"\nMatched question sub type for '{query}': {matched_subtype}")
        return matched_subtype
    except Exception as e:
        print(f"Error in OpenAI request: {e}")
        return None


def find_question_distinction(query, matched_subtype, mapping_df):
    """Find matching Question Distinction using GPT-4."""
    if pd.isna(matched_subtype) or matched_subtype is None:
        return None
        
    # Get all possible distinctions for the matched subtype
    possible_distinctions = mapping_df[
        mapping_df['Question sub type'] == matched_subtype
    ]['Question Distinction'].dropna().unique().tolist()
    
    system_prompt = f"""Given a query and a list of possible question distinctions, select the most appropriate distinction that matches the query's intent.

Available Distinctions:
{', '.join(possible_distinctions)}

Instructions:
1. Analyze the query's context, intent, and specific terminology
2. Compare against each available distinction
3. Select the single most appropriate match
4. Return the exact distinction text as shown in the list
5. If no appropriate match exists, return "None"

Return only the matching distinction text, without any explanation."""

    try:
        response = openai.ChatCompletion.create(
            engine=st.secrets["secrets"]["AZURE_OPENAI_MODEL"],
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query}
            ],
            temperature=0,
            max_tokens=100,
            deployment_id=st.secrets["secrets"]["AZURE_OPENAI_MODEL"]
        )
        
        matched_distinction = response.choices[0].message.content.strip()
        
        # Verify the matched distinction exists in our list
        if matched_distinction in possible_distinctions:
            return matched_distinction
        return None
        
    except Exception as e:
        print(f"Error in OpenAI request: {e}")
        return None

def process_query(user_query, mapping_file):
    """Main function to process user query."""
    mapping_df = load_mapping_data()
    if mapping_df is None:
        return "Error: Could not load mapping data"

    query_parts = split_query(user_query)
    if not query_parts:
        return "Error: Query could not be split correctly."

    processed_parts = []
    final_operator = None
    final_value = None
    
    for part in query_parts:
        subtype = get_matching_subtype(part['query'], mapping_df)
        question_distinction = find_question_distinction(part['query'], subtype, mapping_df)
        
        # Preserve the original operator (including Unicode)
        if part.get('operator'):
            final_operator = part['operator']  # Keep the original Unicode operator if present
            final_value = part['value']

        processed_parts.append({
            'original_query': part['query'],
            'matched_subtype': subtype,
            'question_distinction': question_distinction,
            'operator': part.get('operator'),
            'value': part.get('value'),
            'arithmetic_op': part.get('arithmetic_op')
        })

    formatted_parts = []
    for i, part in enumerate(processed_parts):
        if part['question_distinction']:
            formatted_part = part['question_distinction']
            formatted_parts.append(formatted_part)

            # Add arithmetic operator if exists and not the last part
            if i < len(processed_parts) - 1 and part.get('arithmetic_op'):
                formatted_parts.append(part['arithmetic_op'])

    # Create the base query
    final_query = " ".join(formatted_parts)
    
    # Append the comparison operator and value if they exist
    if final_operator and final_value:
        final_query = f"{final_query} {final_operator} {final_value}"

    print("\n[DEBUG] Final Formatted Query with original operator:", final_query)

    return {
        'processed_parts': processed_parts,
        'final_query': final_query
    }


def main():
    # Load both raw data and mapping data at startup
    try:
        # Load raw data with improved error handling
        try:
            raw_data = pd.read_excel(RAW_DATA_FILE, skiprows=1)
            
            # Verify if NPI column exists (case-insensitive check)
            npi_columns = [col for col in raw_data.columns if col.upper() == 'NPI']
            if not npi_columns:
                st.error("NPI column not found in the data. Available columns:")
                st.write(raw_data.columns.tolist())
                st.stop()
            
            # If multiple matches found, use the first one
            if len(npi_columns) > 1:
                st.warning(f"Multiple NPI columns found: {npi_columns}. Using {npi_columns[0]}")
            
            # Standardize the NPI column name
            raw_data = raw_data.rename(columns={npi_columns[0]: 'NPI'})
            
        except Exception as e:
            st.error(f"Error loading raw data file: {str(e)}")
            st.write("Please check if the file exists and is not corrupted.")
            st.stop()
            
        # Load mapping data
        mapping_data = load_mapping_data()
        if mapping_data is None:
            st.error("Failed to load mapping data. Please check the mapping file.")
            st.stop()
            
        # Verify data integrity
        if raw_data.empty:
            st.error("Raw data file is empty")
            st.stop()
            
        if 'NPI' not in raw_data.columns:
            st.error("NPI column not found after standardization")
            st.write("Available columns:", raw_data.columns.tolist())
            st.stop()
            
        # Store data in session state
        if 'raw_data' not in st.session_state:
            st.session_state.raw_data = raw_data
        if 'mapping_data' not in st.session_state:
            st.session_state.mapping_data = mapping_data
            
        # Show a summary of the data
        st.markdown("### Data Summary")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Records", len(raw_data))
        with col2:
            st.metric("Total NPIs", raw_data['NPI'].nunique())
        with col3:
            st.metric("Mapping Rules", len(mapping_data))
            
        # Optional data preview in expander
        with st.expander("View Data Preview"):
            tab1, tab2 = st.tabs(["Raw Data", "Mapping Data"])
            with tab1:
                st.dataframe(raw_data.head(), use_container_width=True)
            with tab2:
                st.dataframe(mapping_data.head(), use_container_width=True)

        # Query section with examples
        st.header("Query Processing")
        
        # Example queries in an expander
        with st.expander("View Example Queries"):
            st.markdown("""
            Here are some example queries you can try:
            1. Inperson sales rep visits of Veltassa and Lokelma combined is greater than or equal to 2
            2. Number of in-person visits from Veltassa sales representatives is at least 3
            3. At most 1 in-person visit from Lokelma sales representatives
            
            Click on any example to use it.
            """)
            
            if st.button("Use Example 1"):
                st.session_state.query = "Inperson sales rep visits of Veltassa and Lokelma combined is greater than or equal to 2"
            if st.button("Use Example 2"):
                st.session_state.query = "Number of in-person visits from Veltassa sales representatives is at least 3"
            if st.button("Use Example 3"):
                st.session_state.query = "At most 1 in-person visit from Lokelma sales representatives"
        
        # Initialize session state for query if not exists
        if 'query' not in st.session_state:
            st.session_state.query = ""
        
        # Query input with session state
        user_query = st.text_input(
            "Enter your query:",
            value=st.session_state.query,
            placeholder="e.g., Inperson sales rep visits of Veltassa and Lokelma combined is greater than or equal to 2",
            key="query_input"
        )
        
        # Process button with improved styling
        col1, col2, col3 = st.columns([1,2,1])
        with col2:
            if st.button("Process Query", use_container_width=True):
                with st.spinner("Processing query..."):
                    try:
                        # Define system prompt
                        system_prompt = """Analyze the given user query carefully and convert it into a structured arithmetic expression that fits the different conditions mentioned.
    
    Guidelines for Conversion:
    Identify Mathematical Operations:
    Recognize keywords like "at most," "at least," "more than," "less than," etc., and map them to the appropriate arithmetic operators:
    at most → ≤
    at least → ≥
    More than → >
    Less than → <
    Exactly → =
    Break Down Multiple Conditions:
    
    If the query contains multiple conditions connected by conjunctions (e.g., "and," "or," "but"), split them accordingly and structure each as an individual arithmetic component.
    Standardize Query Components:
    
    Convert qualitative statements into measurable parameters.
    Maintain consistency in terminology and ensure the transformed expression retains the original meaning.
    Example Conversion:
    User Query:
    "Find HCPs who report at most 1 inperson visit from Veltassa and Lokelma sales representatives in the past three months."
    
    Transformed Arithmetic Expression:
    "Number of Inperson Visits from Sales Representatives in the Past 3 Months for Veltassa + Number of Inperson Visits from Sales Representatives in the Past 3 Months for Lokelma ≤ 1"""
                        
                        # Process through OpenAI
                        st.subheader("Query Processing Steps")
                        with st.expander("1. Initial Query Transformation", expanded=True):
                            try:
                                response = openai.ChatCompletion.create(
                                    engine=st.secrets["secrets"]["AZURE_OPENAI_MODEL"],
                                    messages=[
                                        {"role": "system", "content": system_prompt},
                                        {"role": "user", "content": user_query}
                                    ],
                                    temperature=0,
                                    max_tokens=150,
                                    deployment_id=st.secrets["secrets"]["AZURE_OPENAI_MODEL"]
                                )
                                final_query = response.choices[0].message.content.strip()
                                st.write("Transformed Query:", final_query)
                                st.session_state['last_transformed_query'] = final_query
                            except Exception as e:
                                st.error(f"Error in OpenAI query transformation: {str(e)}")
                                st.stop()

                        # Process through mapping system
                        with st.expander("2. Query Mapping and Processing", expanded=True):
                            result = process_query(final_query, st.session_state.mapping_data)
                            if not isinstance(result, dict):
                                st.error("Query processing failed")
                                return

                            final_query = result['final_query']
                            st.write("Final Formatted Query:", final_query)

                        # Filter NPIs
                        with st.expander("3. Results", expanded=True):
                            filtered_npi = filter_npi_based_on_query(st.session_state.raw_data, final_query)
                            
                            if filtered_npi is not None and not filtered_npi.empty:
                                # Convert to list and display results
                                npi_list = filtered_npi.astype(str).tolist()
                                
                                # Create results container
                                results_container = st.container()
                                
                                with results_container:
                                    # Display summary metrics
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        st.metric("Matching NPIs", len(npi_list))
                                    with col2:
                                        match_percentage = (len(npi_list) / len(st.session_state.raw_data)) * 100
                                        st.metric("Match Percentage", f"{match_percentage:.1f}%")
                                    
                                    # Create a DataFrame for better display
                                    result_df = pd.DataFrame(npi_list, columns=['NPI'])
                                    
                                    # Display results in a nice table
                                    st.markdown("### Matching NPIs")
                                    st.dataframe(
                                        result_df,
                                        use_container_width=True,
                                        height=400
                                    )
                                    
                                    # Download section
                                    st.markdown("### Download Results")
                                    download_col1, download_col2 = st.columns(2)
                                    
                                    with download_col1:
                                        # CSV Download with counter
                                        csv_data = result_df.to_csv(index=False)
                                        st.download_button(
                                            label="📥 Download as CSV",
                                            data=csv_data,
                                            file_name=f"filtered_npis_{len(npi_list)}_results.csv",
                                            mime="text/csv",
                                            use_container_width=True
                                        )
                                    
                                    with download_col2:
                                        # Excel Download with counter
                                        buffer = io.BytesIO()
                                        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                                            result_df.to_excel(writer, index=False)
                                        
                                        st.download_button(
                                            label="📥 Download as Excel",
                                            data=buffer.getvalue(),
                                            file_name=f"filtered_npis_{len(npi_list)}_results.xlsx",
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                            use_container_width=True
                                        )
                            else:
                                st.warning("No matching NPIs found")
                                
                    except Exception as e:
                        st.error(f"Error processing query: {str(e)}")
                        st.error("Please check your query and try again")
                
    except Exception as e:
        st.error(f"Error in main function: {str(e)}")
        st.write("Current working directory:", os.getcwd())
        st.write("Files in current directory:", os.listdir())
        st.stop()
if __name__ == "__main__":
    main()
