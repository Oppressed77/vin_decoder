import pandas as pd
import requests
import os


def read_vins_from_csv(file_path="to_be_decoded.csv", vin_column_name="VIN"):
    """
    Reads VINs from a specified CSV file.

    Args:
        file_path (str): The path to the CSV file.
        vin_column_name (str): The name of the column containing VINs.

    Returns:
        list: A list of VINs, or an empty list if an error occurs.
    """
    try:
        df = pd.read_csv(file_path)
        if vin_column_name in df.columns:
            vins = df[vin_column_name].dropna().astype(str).tolist()
            if not vins:
                print(f"Error: No VINs found in column '{vin_column_name}' in {file_path}.")
                return []
            return vins
        else:
            # Try using the first column if the specified column name is not found
            if not df.empty and len(df.columns) > 0:
                print(
                    f"Warning: VIN column '{vin_column_name}' not found. Using the first column '{df.columns[0]}' as VIN source.")
                vins = df.iloc[:, 0].dropna().astype(str).tolist()
                if not vins:
                    print(f"Error: No VINs found in the first column '{df.columns[0]}' in {file_path}.")
                    return []
                return vins
            else:
                print(f"Error: VIN column '{vin_column_name}' not found and the CSV is empty or has no columns.")
                return []
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return []
    except pd.errors.EmptyDataError:
        print(f"Error: The file {file_path} is empty.")
        return []
    except Exception as e:
        print(f"An unexpected error occurred while reading {file_path}: {e}")
        return []


def append_results_to_csv_with_rollover(raw_csv_batch_data, base_filename, current_file_idx, max_size_mb=500,
                                        existing_headers_string=None):
    """
    Appends a batch of raw CSV data to a CSV file, handling file rollover and headers.

    Args:
        raw_csv_batch_data (str): The raw CSV string data from an API batch.
        base_filename (str): The base name for output files (e.g., "decoded_vins_output").
        current_file_idx (int): The current index for the output file (0 for base, 1 for base1.csv, etc.).
        max_size_mb (int): The maximum file size in megabytes before rollover.
        existing_headers_string (str, optional): The header string of the current active file (stripped of newlines). Defaults to None.

    Returns:
        tuple: (updated_current_file_idx, updated_existing_headers_string, output_filename)
    """
    max_size_bytes = max_size_mb * 1024 * 1024

    # Determine current output filename
    if current_file_idx == 0:
        output_filename = f"{base_filename}.csv"
    else:
        output_filename = f"{base_filename}{current_file_idx}.csv"

    # Initial rollover check: if current file already exists and is too large
    if os.path.exists(output_filename) and os.path.getsize(output_filename) > max_size_bytes:
        print(
            f"File {output_filename} (size {os.path.getsize(output_filename) / (1024 * 1024):.2f}MB) already exceeds {max_size_mb}MB. Rolling over before write.")
        current_file_idx += 1
        existing_headers_string = None  # New file will need new headers
        # Update output_filename for the new current_file_idx
        if current_file_idx == 0:
            output_filename = f"{base_filename}.csv"
        else:
            output_filename = f"{base_filename}{current_file_idx}.csv"
        print(f"New output file will be {output_filename}")

    # Normalize raw_csv_batch_data and split into lines
    clean_raw_csv_batch_data = str(raw_csv_batch_data).strip()
    if not clean_raw_csv_batch_data:
        return current_file_idx, existing_headers_string, output_filename  # Return current output_filename even if nothing is written

    # Refined Line Preparation:
    temp_lines = clean_raw_csv_batch_data.splitlines()
    current_batch_lines_list = []
    for l in temp_lines:
        stripped_l = l.strip()
        if stripped_l:  # Only add non-empty lines
            current_batch_lines_list.append(stripped_l)

    if not current_batch_lines_list:
        return current_file_idx, existing_headers_string, output_filename

    header_line_of_this_batch = current_batch_lines_list[0]

    lines_to_actually_write = []
    final_headers_for_this_file = existing_headers_string
    file_exists_before_write = os.path.exists(output_filename)

    if not file_exists_before_write or not existing_headers_string:
        lines_to_actually_write = current_batch_lines_list
        final_headers_for_this_file = header_line_of_this_batch
    else:
        if header_line_of_this_batch == existing_headers_string:
            if len(current_batch_lines_list) > 1:
                lines_to_actually_write = current_batch_lines_list[1:]
        else:
            print(
                f"Warning: Batch headers for {output_filename} do not match existing file headers. Skipping this batch.")
            print(f"File headers: '{existing_headers_string}'")
            print(f"Batch headers: '{header_line_of_this_batch}'")

    if lines_to_actually_write:
        try:
            with open(output_filename, 'a') as f:
                for line in lines_to_actually_write:  # these lines are already stripped and non-empty
                    f.write(line + os.linesep)

            new_file_size = os.path.getsize(output_filename)
            if new_file_size > max_size_bytes:
                print(
                    f"File {output_filename} (size {new_file_size / (1024 * 1024):.2f}MB) now exceeds {max_size_mb}MB after writing. Next batch will use a new file index.")
                current_file_idx += 1
                final_headers_for_this_file = None
        except IOError as e:
            print(f"IOError saving results to {output_filename}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while saving results to {output_filename}: {e}")

    return current_file_idx, final_headers_for_this_file, output_filename


def remove_blank_rows_from_file(file_path):
    """
    Reads a file, removes lines that are empty or contain only whitespace,
    and overwrites the file with the cleaned content.

    Args:
        file_path (str): The path to the file to clean.
    """
    if not os.path.exists(file_path):
        print(f"Post-processing: File {file_path} not found. Skipping.")
        return

    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()

        # Filter out blank lines (lines that are empty after stripping whitespace)
        cleaned_lines = [line for line in lines if line.strip()]

        if len(cleaned_lines) == len(lines):
            print(f"Post-processing: No blank rows found in {file_path}. No changes made.")
        else:
            # Re-join lines, ensuring they all end with a proper newline.
            # The original lines in cleaned_lines still have their original newlines.
            # To ensure consistent os.linesep:
            processed_cleaned_lines = [line.rstrip('\r\n') + os.linesep for line in cleaned_lines]

            with open(file_path, 'w') as f:
                f.writelines(processed_cleaned_lines)
            print(f"Post-processing: Removed {len(lines) - len(cleaned_lines)} blank row(s) from {file_path}.")

    except IOError as e:
        print(f"Post-processing: IOError while processing {file_path}: {e}")
    except Exception as e:
        print(f"Post-processing: An unexpected error occurred while processing {file_path}: {e}")


def decode_vins_in_batches(vin_list, base_output_filename="decoded_vins_output", max_file_size_mb=2):
    """
    Decodes a list of VINs in batches using the NHTSA API and saves results incrementally.

    Args:
        vin_list (list): A list of VIN strings.
        base_output_filename (str): The base name for output JSON files.
        max_file_size_mb (int): Maximum size in MB for each JSON file before rollover.

    Returns:
        int: The total number of successfully decoded VINs.
    """
    if not vin_list:
        print("No VINs provided to decode.")
        return 0, []  # Return count and empty list of files

    total_successfully_decoded_vins = 0
    current_file_index = 0
    current_headers = None
    written_files = set()

    batch_size = 50
    num_batches = (len(vin_list) + batch_size - 1) // batch_size

    for i in range(num_batches):
        start_index = i * batch_size
        end_index = start_index + batch_size
        batch_vins = vin_list[start_index:end_index]

        vin_string = ';'.join(batch_vins)
        api_url = 'https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/'
        payload = {'format': 'csv', 'data': vin_string}

        print(f"Processing batch {i + 1}/{num_batches} ({len(batch_vins)} VINs)...")

        try:
            response = requests.post(api_url, data=payload, timeout=30)
            response.raise_for_status()

            raw_csv_data_from_batch = response.text

            if raw_csv_data_from_batch and raw_csv_data_from_batch.strip():

                processed_lines = [line.strip() for line in raw_csv_data_from_batch.strip().splitlines()]
                processed_lines = [line for line in processed_lines if line]

                num_data_lines_in_batch = len(processed_lines) - 1 if processed_lines else -1

                if num_data_lines_in_batch >= 0:  # Changed to >= 0 to handle header-only batches for file creation
                    current_file_index, current_headers, actual_filename_written = append_results_to_csv_with_rollover(
                        raw_csv_data_from_batch,
                        base_output_filename,
                        current_file_index,
                        max_file_size_mb,
                        current_headers
                    )
                    if os.path.exists(actual_filename_written):  # Ensure file was actually written
                        written_files.add(actual_filename_written)

                    if num_data_lines_in_batch > 0:
                        total_successfully_decoded_vins += num_data_lines_in_batch
                    elif num_data_lines_in_batch == 0 and len(processed_lines) == 1:
                        print(
                            f"Info: Batch {i + 1} returned CSV data with only a header line. CSV: {raw_csv_data_from_batch[:200]}")
                        if not current_headers:  # If we don't have headers yet, and this batch gave us one
                            header_line = processed_lines[0]
                            if header_line:
                                current_headers = header_line
                else:
                    print(
                        f"Warning: Batch {i + 1} returned CSV data but it appears to be empty or malformed after stripping. CSV: {raw_csv_data_from_batch[:200]}")
            else:
                print(
                    f"Warning: Batch {i + 1} returned no results or empty CSV. Response text: {raw_csv_data_from_batch}")

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error for batch {i + 1}: {e}")
            print(f"Response content: {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Request exception for batch {i + 1}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred during API call for batch {i + 1}: {e}")

    if total_successfully_decoded_vins == 0:
        print("No data was successfully decoded from any batch.")

    return total_successfully_decoded_vins, list(written_files)


if __name__ == '__main__':
    print("Starting VIN decoding process...")

    vins_to_decode = read_vins_from_csv(file_path="to_be_decoded.csv", vin_column_name="VIN")

    if not vins_to_decode:
        print("No VINs found or error in reading CSV. Exiting.")
    else:
        print(f"Successfully read {len(vins_to_decode)} VINs from CSV.")

        successfully_decoded_count, files_written_to = decode_vins_in_batches(vins_to_decode)

        if successfully_decoded_count == 0:
            print("VIN decoding process resulted in no data being successfully decoded and saved. Exiting.")
        else:
            print(f"Successfully decoded {successfully_decoded_count} VINs overall.")
            print(f"Data saved to the following file(s): {', '.join(files_written_to)}")
            # Post-process each written file to remove blank rows
            if files_written_to:
                print("Starting post-processing to remove blank rows from output files...")
                for f_path in files_written_to:
                    remove_blank_rows_from_file(f_path)
                print("Post-processing complete.")
            print("VIN decoding process completed successfully.")
