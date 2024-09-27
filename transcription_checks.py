'''python Questions_regarding_transcription.py /data/Root_content/Vaani/Response_to_questions/MegaPrecheckList/03-07-2023-transcription-01 
/data/Root_content/Vaani/Response_to_questions/MegaPrecheckList/Questions_regarding_transcription/result.tsv
'''

import os
import pandas as pd
import argparse
import csv

# Define error codes
error_codes = {
    "The folder contains multiple .tsv files.": "TRXN_E1",
    "The folder is empty.": "TRXN_E2",
    "The file does not have all column or not tab seperated.": "TRXN_E3",
    "The file has a transcript with a newline or tab character.": "TRXN_E4",
    "The file is completely empty.": "TRXN_E5",
    "The file is not tab-separated.": "TRXN_E6",
    "The file not following the format(transcriber <original_tsv_row> Transcription)": "TRXN_E7",
}

def check_column_types(row):
    try:
        #print(row)
        # Check if the first column is an integer
        if not isinstance(int(row[0]), int):
            return False

        # Check if the second column is a .jpg filename
        # if not (isinstance(row[1], str) and row[1].endswith('.jpg')):
        #     return False

        # Check if the third column is a .wav filename
        if not (isinstance(row[2], str) and row[2].endswith('.wav')):
            return False

        # Check if the fourth column is an integer
        if not isinstance(int(row[3]), int):
            return False

        # Check if the fifth and sixth columns are floats or integers
        if not (isinstance(float(row[4]), (float, int)) and isinstance(float(row[5]), (float, int))):
            return False

        # Check if the seventh column is a string
        if not isinstance(row[6], str):
            return False

        return True

    except (ValueError, TypeError):
        return False
    
def check_tsv_files(root_folder, error_log):
    # Dictionary to count the number of .tsv files in each folder
    tsv_file_count = {}
    
    # Walk through the directory
    for dirpath, dirnames, filenames in os.walk(root_folder):
        tsv_files = [file for file in filenames if file.endswith('.tsv')]
        
        if tsv_files:
            tsv_file_count[dirpath] = len(tsv_files)
        else:
            # Check if the directory is empty
            if not filenames:
                error_log.append((os.path.basename(dirpath), error_codes["The folder is empty."]))
        
        for file in tsv_files:
            file_path = os.path.join(dirpath, file)
            try:
                # Read the .tsv file without header
                df = pd.read_csv(file_path, sep='\t', header=None)

                
                if df.shape[1] != 7:
                    error_log.append((file, error_codes["The file does not have all column or not tab seperated."]))
                else:
                    # Check each row for correct data types
                    for i, row in df.iterrows():
                        if not check_column_types(row):
                            error_log.append((file, error_codes["The file not following the format(transcriber <original_tsv_row> Transcription)"]))
                            break

                    # Check for newline or tab characters in the 7th column
                    transcripts = df.iloc[:, 6]  # 7th column by index (0-based)
                    for i, transcript in enumerate(transcripts):
                        if pd.isna(transcript):
                            continue
                        if '\n' in transcript or '\t' in transcript:
                            error_log.append((file, error_codes["The file has a transcript with a newline or tab character."]))
            except pd.errors.EmptyDataError:
                # Handle completely empty files
                error_log.append((file, error_codes["The file is completely empty."]))
            except Exception as e:
                error_log.append((file, f"Could not read the file. Error: {e}"))
    
    # Check the number of .tsv files found in each folder
    for folder, count in tsv_file_count.items():
        if count != 1:
            error_log.append((os.path.basename(folder), error_codes["The folder contains multiple .tsv files."]))

def process_all_subfolders(main_root_folder, error_log):
    # Traverse the top-level directories in the main root folder
    for directory in os.listdir(main_root_folder):
        directory_path = os.path.join(main_root_folder, directory)
        if os.path.isdir(directory_path):  # Check if it's a directory
            # Process each subfolder within this top-level directory
            for subdirectory in os.listdir(directory_path):
                subdirectory_path = os.path.join(directory_path, subdirectory)
                if os.path.isdir(subdirectory_path):  # Check if it's a directory
                    check_tsv_files(subdirectory_path, error_log)

def save_error_log(error_log, output_file):
    # Convert error log to DataFrame
    error_df = pd.DataFrame(error_log, columns=["filename", "error"])
    # Save DataFrame to TSV file
    error_df.to_csv(output_file, sep='\t', index=False)

def main(root_folder, output_file):
    # Initialize error log
    error_log = []

    # Process all subfolders and collect errors
    process_all_subfolders(root_folder, error_log)

    # Save errors to the output TSV file
    save_error_log(error_log, output_file)

if __name__ == "__main__":
    # Argument parsing
    parser = argparse.ArgumentParser(description="Check .tsv files in subfolders and save errors to a TSV file.")
    parser.add_argument("root_folder", type=str, help="Root folder containing subfolders with .tsv files.")
    parser.add_argument("output_file", type=str, help="Output TSV file to save errors.")
    args = parser.parse_args()

    # Call main function with parsed arguments
    main(args.root_folder, args.output_file)
