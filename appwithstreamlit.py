import streamlit as st
import pandas as pd
from openai import AzureOpenAI
from difflib import SequenceMatcher
import re
import os
from dotenv import load_dotenv
import io

# Load environment variables
load_dotenv()

# Azure OpenAI setup with detailed error handling
def initialize_azure_client():
    try:
        # First try getting from Streamlit secrets
        credentials = {
            "endpoint": st.secrets.get("AZURE_OPENAI_ENDPOINT", "https://ciaiaiservices.openai.azure.com/"),
            "api_key": st.secrets.get("AZURE_OPENAI_API_KEY", "817dce22f5a548b8b11fe0b6a3cf2c36"),
            "model": st.secrets.get("AZURE_OPENAI_MODEL", "gpt-4o-20240513")
        }
        
        # For debugging - show what credentials we're using (mask API key)
        st.write("Using Azure OpenAI endpoint:", credentials["endpoint"])
        st.write("Using Azure OpenAI model:", credentials["model"])
        
        # Initialize the client with all required parameters
        client = AzureOpenAI(
            api_key=credentials["api_key"],
            azure_endpoint=credentials["endpoint"],
            api_version="2024-02-15-preview"  # Adding back the API version
        )
        
        # Test the client with a simple completion
        test_messages = [
            {"role": "user", "content": "Hello, are you working?"}
        ]
        completion = client.chat.completions.create(
            model=credentials["model"],
            messages=test_messages,
            temperature=0,
            max_tokens=10
        )
        
        st.success("Azure OpenAI client initialized successfully!")
        return client
        
    except Exception as e:
        st.error(f"Error initializing Azure OpenAI client: {str(e)}")
        st.error("Please check if your Azure OpenAI credentials are correctly set in Streamlit secrets.")
        st.stop()

# Initialize the client
client = initialize_azure_client()
st.set_page_config(
    page_title="Query Processing App",
    page_icon="🔍",
    layout="wide"
)

# Add title and description
st.title("Query Processing Application")
st.markdown("Process queries and filter NPIs based on specific conditions")

# Initialize session state
if 'processed_results' not in st.session_state:
    st.session_state.processed_results = None

# File upload section
st.header("Upload Files")
col1, col2 = st.columns(2)

with col1:
    mapping_file = st.file_uploader("Upload Mapping File (Excel)", type=['xlsx'])

with col2:
    raw_data_file = st.file_uploader("Upload Raw Data File (Excel)", type=['xlsx'])

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

def load_raw_data(file_path):
    """Load raw data with correct row and column handling"""
    try:
        # Read Excel file starting from row 2 (1-based index)
        raw_data = pd.read_excel(file_path, skiprows=1)
        
        print("\n[DEBUG] Available columns in raw data:")
        print(raw_data.columns.tolist())
        
        return raw_data
    except Exception as e:
        print(f"Error loading raw data: {e}")
        return None

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


def load_mapping_data(file_path):
    """Load and prepare mapping data from Excel file."""
    try:
        # Read Excel file instead of CSV
        mapping_df = pd.read_excel(file_path)
        # Convert columns to string type
        mapping_df['Question Distinction'] = mapping_df['Question Distinction'].astype(str)
        mapping_df['Question sub type'] = mapping_df['Question sub type'].astype(str)
        return mapping_df
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        return None

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
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": query}
        ]
        
        completion = client.chat.completions.create(
            model=azure_openai_model_gpt4,
            messages=messages,
            temperature=0,
            n=1,
            seed=1
        )
        matched_subtype = completion.choices[0].message.content.strip()
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
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": query}
        ]
        
        completion = client.chat.completions.create(
            model=azure_openai_model_gpt4,
            messages=messages,
            temperature=0,
            n=1,
            seed=1
        )
        
        matched_distinction = completion.choices[0].message.content.strip()
        
        # Verify the matched distinction exists in our list
        if matched_distinction in possible_distinctions:
            return matched_distinction
        return None
        
    except Exception as e:
        print(f"Error in OpenAI request: {e}")
        return None

def process_query(user_query, mapping_file):
    """Main function to process user query."""
    mapping_df = load_mapping_data(mapping_file)
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
    if not (mapping_file and raw_data_file):
        st.warning("Please upload all required files")
        return

    # Load raw data
    st.subheader("Raw Data Preview")
    try:
        raw_data = pd.read_excel(raw_data_file, skiprows=1)
        st.dataframe(raw_data.head())
    except Exception as e:
        st.error(f"Error loading raw data: {str(e)}")
        return

    # Query input
    st.header("Query Processing")
    user_query = st.text_input(
        "Enter your query:", 
        placeholder="e.g., Inperson sales rep visits of Veltassa and Lokelma combined is greater than or equal to 2"
    )

    if st.button("Process Query"):
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
                    messages = [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_query}
                    ]
                    
                    completion = client.chat.completions.create(
                        model=azure_openai_model_gpt4,
                        messages=messages,
                        temperature=0,
                        n=1,
                        seed=1
                    )
                    final_query = completion.choices[0].message.content.strip()
                    st.write("Transformed Query:", final_query)

                # Process through mapping system
                with st.expander("2. Query Mapping and Processing", expanded=True):
                    result = process_query(final_query, mapping_file)
                    if not isinstance(result, dict):
                        st.error("Query processing failed")
                        return

                    final_query = result['final_query']
                    st.write("Final Formatted Query:", final_query)

                # Filter NPIs
                with st.expander("3. Results", expanded=True):
                    filtered_npi = filter_npi_based_on_query(raw_data, final_query)
                    
                    if filtered_npi is not None and not filtered_npi.empty:
                        # Convert to list and display results
                        npi_list = filtered_npi.astype(str).tolist()
                        
                        # Display summary
                        st.success(f"Found {len(npi_list)} matching NPIs")
                        
                        # Create a DataFrame for better display
                        result_df = pd.DataFrame(npi_list, columns=['NPI'])
                        st.dataframe(result_df)
                        
                        # Download options
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            # CSV Download
                            csv_data = result_df.to_csv(index=False)
                            st.download_button(
                                label="Download NPIs as CSV",
                                data=csv_data,
                                file_name="filtered_npis.csv",
                                mime="text/csv"
                            )
                            
                        with col2:
                            # Excel Download
                            excel_buffer = pd.ExcelWriter('filtered_npis.xlsx', engine='openpyxl')
                            result_df.to_excel(excel_buffer, index=False)
                            excel_buffer.close()
                            
                            with open('filtered_npis.xlsx', 'rb') as f:
                                excel_data = f.read()
                            
                            st.download_button(
                                label="Download NPIs as Excel",
                                data=excel_data,
                                file_name="filtered_npis.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                        
                        # Display query summary
                        st.subheader("Query Summary")
                        summary_data = {
                            "Original Query": user_query,
                            "Transformed Query": final_query,
                            "Total NPIs Found": len(npi_list)
                        }
                        st.json(summary_data)
                        
                    else:
                        st.warning("No matching NPIs found")
                        
            except Exception as e:
                st.error(f"Error processing query: {str(e)}")
                st.error("Please check your query and try again")

if __name__ == "__main__":
    main()
