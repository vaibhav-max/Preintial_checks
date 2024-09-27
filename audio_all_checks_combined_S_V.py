"""python combined_S_V.py --main_root_folder /data/Root_content/Vaani/Database/raw/shaip/31-05-2024 \
--phase1_tsv_folder /data/Root_content/Vaani/Response_to_questions/MegaPrecheckList/phase1_speakerid_uttids \
--txt_file_path /data/Root_content/Vaani/Response_to_questions/MegaPrecheckList/phase2_state_district_mapping/phase2_karya_state_district_mapping.txt \
--xls_file_path /data/Root_content/Vaani/Response_to_questions/MegaPrecheckList/Images_Phase2.xlsx \
--output_file_path /data/Root_content/Vaani/Response_to_questions/MegaPrecheckList/Vaibhav_Sumit/results
"""

import os
import pandas as pd
import argparse
from collections import defaultdict
from tqdm import tqdm 
from speaker_metadata_checks import check_speaker_metadata

# Error codes as specified
ERROR_CODES = {
    'State or district mismatch': 'SPK-E12',
    'Filename contains space': 'SPK-E13',
    'Incorrect number of underscores': 'SPK-E14',
    'Image district mismatch': 'SPK-E15',
    'Incorrect number of underscores in .txt': 'SPK-E16',
    'Repeated speaker ID': 'SPK-E17',
    'Repeated utterance ID': 'SPK-E18',
    'TSV file does not end with newline': 'SPK-E19',
    'Unicode character in TSV file': 'SPK-E20',
    'Mismatched Speaker ID from the meta-data': 'SPK-E21',
    'Speaker_ID not found in meta-data': 'SPK-E22',
    'No .txt file found (meta-data)': 'SPK-E23',
    'Non-numeric uttID': 'SPK-E24',
    'Not Present in database of images': 'SPK-E25',
    'Incorrect Audio Extension not .wav': 'SPK-E26',
    'Total duration out of range': 'SPK-E27',
    'Exception occurred': 'SPK-E99' 
}

def get_duration(file, file_path):
    duration_sum = 0
    try:
        if file.endswith('.tsv'):
            try:
                df = pd.read_csv(file_path, sep='\t', header=None)
                
                if df.empty:
                    raise ValueError("The file is empty")
                
                # Calculate the total duration for the current file
                duration_sum = (df.iloc[:, 4] - df.iloc[:, 3]).sum()
                #print(file_path, duration_sum)
                duration_sum
            
            except ValueError as e:
                print(f"Skipping file {file_path} as it is empty: {e}")
                log_exception(log_entries, file, e)
            except Exception as e:
                print(f"An error occurred with file {file_path}: {e}")
                log_exception(log_entries, file, e)
    except Exception as e:
        print(f"Error calculating total duration in {file_path}: {e}")
        log_exception(log_entries, file, e)
    
    # Convert total duration from seconds to hours
    return duration_sum / 3600

def questions_regarding_audio_tsv_formats(file_path, filename, unicode_chars):
    entries = defaultdict(list)
    try:
        base_name = os.path.basename(file_path)

        if filename.endswith('.wav') or filename.endswith('.tsv'):
            if ' ' in filename:
                entries[base_name].append(ERROR_CODES['Filename contains space'])

            underscore_count = filename.count('_')
            if underscore_count != 5:
                entries[base_name].append(ERROR_CODES['Incorrect number of underscores'])

            img = filename.split('_')[4]
            img_district = img.split('-')[0]
            district = filename.split('_')[1]

            if '-' in img and img_district != district:
                entries[base_name].append(ERROR_CODES['Image district mismatch'])

        if filename.endswith('.txt'):
            underscore_count = filename.count('_')
            if underscore_count != 2:
                entries[base_name].append(ERROR_CODES['Incorrect number of underscores in .txt'])

        if filename.endswith('.tsv'):
            try:
                with open(file_path, 'rb') as file:
                    file.seek(-1, os.SEEK_END)
                    last_char = file.read(1)
                    if last_char != b'\n':
                        entries[base_name].append(ERROR_CODES['TSV file does not end with newline'])
            except Exception as e:
                print(f"Error reading last character of TSV file {file_path}: {e}")
                log_exception(log_entries, filename, e)

            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    content = file.read()
                    for char in unicode_chars:
                        if char in content:
                            entries[base_name].append(ERROR_CODES['Unicode character in TSV file'])
            except Exception as e:
                print(f"Error reading TSV file {file_path}: {e}")
                log_exception(log_entries, filename, e)
    except Exception as e:
        print(f"Error processing formats in file {filename}: {e}")
        log_exception(log_entries, filename, e)

    return entries

def extract_state_district_names_from_txt_file(txt_file_path):
    try:
        state_district_data = pd.read_csv(txt_file_path, sep='\t', header=None, names=['State', 'District'])
        unique_states = set(state_district_data['State'].unique())
        unique_districts = set(state_district_data['District'].unique())
        return unique_states, unique_districts
    except Exception as e:
        print(f"Error extracting state and district names from TXT file {txt_file_path}: {e}")
        log_exception(log_entries, os.path.basename(txt_file_path), e)
    return set(), set()

def compare_state_district_names(filename, txt_unique_states, txt_unique_districts):
    unique_states = set()
    unique_districts = set()
    mismatched_filenames = []

    try:
        if filename.endswith(('.wav', '.tsv', '.txt')):
            state = filename.split('_')[0]
            district = filename.split('_')[1]
            unique_states.add(state)
            unique_districts.add(district)
            state_match = state in txt_unique_states
            district_match = district in txt_unique_districts
            if not state_match or not district_match:
                mismatched_filenames.append(filename)
    except Exception as e:
        print(f"Error extracting state and district names from TXT for file {filename}: {e}")
        log_exception(log_entries, filename, e)

    return mismatched_filenames

def verify_speaker_id_in_filenames(file_name, speaker_id):
    issues = defaultdict(list)
    try:
        if file_name.endswith(('.wav', '.tsv', '.txt')):
            file_speaker_id = (file_name.split('_')[2]).split('.')[0]
            if file_speaker_id != speaker_id:
                issues[file_name].append(ERROR_CODES['Mismatched Speaker ID from the meta-data'])
    except Exception as e:
        print(f"Error verifying speaker ID in file {file_name}: {e}")
        log_exception(log_entries, file_name, e)
    return issues

def extract_speaker_id_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            for line in file:
                if line.startswith("Speaker_ID:"):
                    return line.split(":")[1].strip()
    except Exception as e:
        print(f"Error extracting speaker ID from file {file_path}: {e}")
        log_exception(log_entries, os.path.basename(file_path), e)
    return None

def process_folders(folder_path, folder):
    issues = defaultdict(list)
    try:
        txt_file_path = None

        for file_name in os.listdir(folder_path):
            if file_name.endswith(".txt"):
                txt_file_path = os.path.join(folder_path, file_name)
                break

        if txt_file_path:
            speaker_id = extract_speaker_id_from_file(txt_file_path)
            if speaker_id:
                speaker_id_issues = verify_speaker_id_in_filenames(folder_path, speaker_id)
                for file, error_list in speaker_id_issues.items():
                    issues[file].extend(error_list)
            else:
                issues[folder_path].append(ERROR_CODES['Speaker_ID not found in meta-data'])
        else:
            issues[folder_path].append(ERROR_CODES['No .txt file found (meta-data)'])
    except Exception as e:
        print(f"Error processing file {file_name} for folders in {folder}: {e}")
        log_exception(log_entries, file_name, e)
    return issues

def is_numeric(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def check_uttID_in_folder(file):
    issues = defaultdict(list)
    try:
        if file.endswith(('.tsv', '.wav')):
            filename_parts = file.split('_')
            if len(filename_parts) > 3:
                uttID = filename_parts[3]
                if not is_numeric(uttID):
                    issues[file].append(ERROR_CODES['Non-numeric uttID'])
    except Exception as e:
        print(f"Error checking uttID in file {file}: {e}")
        log_exception(log_entries, file, e)
    return issues

def check_audio_extensions(file):
    issues = defaultdict(list)
    try:
        if file.lower().endswith(('.mp3', '.flac', '.aac', '.ogg', '.wma', '.alac')):
            issues[file].append(ERROR_CODES['Incorrect Audio Extension not .wav'])
    except Exception as e:
        print(f"Error checking audio extensions in file {file}: {e}")
        log_exception(log_entries, file, e)
    return issues

def extract_image_id(filename):
    parts = filename.split('_')
    image_id = (('_'.join(parts[4:6])).split('.')[0]) + '.jpg'
    return image_id

def file_to_image_mapping(file, file_image_mapping):
    try:
        if file.endswith(('.tsv', '.wav')):
            image_id = extract_image_id(file)
            file_image_mapping[file] = image_id
    except Exception as e:
        print(f"Error processing file {file}: {e}")
        log_exception(log_entries, file, e)
    return file_image_mapping

def check_image_ids_in_csv(image_ids, xls_file):
    try:
        # Read both sheets from the Excel file
        district_specific_df = pd.read_excel(xls_file, sheet_name='DistrictSpecificImages')
        generic_images_df = pd.read_excel(xls_file, sheet_name='GenericImages')

        #print(district_specific_df)
        # Combine the 'Filename' columns from both sheets
        combined_filenames = set(district_specific_df['Filename']).union(set(generic_images_df['Filename']))
        # print(combined_filenames)
        # Check for image ID presence in the combined set of filenames
        image_id_presence = {image_id: (image_id in combined_filenames) for image_id in image_ids}
        
        return image_id_presence
    except Exception as e:
        print(f"Error checking image IDs in CSV file {xls_file}: {e}")
        log_exception(log_entries, xls_file, e)
    return {}

def check_for_repeats_in_tsv(folder_path_phase1):
    speaker_utt_pairs_phase1 = set()
    try:
        file_paths = []
        for root, dirs, files in os.walk(folder_path_phase1):
            for file in files:
                file_paths.append(os.path.join(root, file))
        
        for idx, file_path in tqdm(enumerate(file_paths), total=len(file_paths), desc="Processing TSV files"):
            file_name = os.path.basename(file_path)
            
            try:
                df = pd.read_csv(file_path, sep='\t', chunksize=100000)
                for chunk in df:
                    for _, row in chunk.iterrows():
                        first_column = row.iloc[0]
                        if isinstance(first_column, str) and first_column.startswith('/'):
                            basename = os.path.basename(first_column)
                            parts = basename.split("_")
                            if len(parts) >= 4:
                                speakerid = parts[2]
                                uttid = parts[3]
                                speaker_utt_pairs_phase1.add((speakerid, uttid))
            except Exception as e:
                print(f"Skipping file {file_name} in TSV check: {e}")
                log_exception(log_entries, file_name, e)
    except Exception as e:
        print(f"Error checking for repeats in TSV folder {folder_path_phase1} and file {file_name}: {e}")
        log_exception(log_entries, file_name, e)
    
    return speaker_utt_pairs_phase1

def extract_speakerid_uttid(filename):
    parts = filename.split('_')
    speaker_id = parts[2]
    utt_id = parts[3]
    return speaker_id, utt_id

def add_spk_utt_ids(filename):
    try:
        if filename.endswith('.wav'):
            speaker_id, utt_id = extract_speakerid_uttid(filename)
            speaker_ids_phase2.add(speaker_id)
            utt_ids_phase2.add(utt_id)

    except Exception as e:
        print(f"Error checking for repeats in file {filename}: {e}")
        log_exception(log_entries, filename, e)

def run_pipeline(district_folder, speaker_folder):
    
    #print("run_pipeline", speaker_folder)
    all_files = os.listdir(speaker_folder)
    text_files = [file for file in all_files if file.endswith('.txt')]
    wav_files = [file for file in all_files if file.endswith('.wav')]
    tsv_files = [file for file in all_files if file.endswith('.tsv')]

    if len(text_files) == 1:
        fp = os.path.join(speaker_folder, text_files[0])
        pdf_path = os.path.join(speaker_folder, text_files[0].replace('.txt', '.pdf'))
        log_error = check_speaker_metadata(fp)
        i = '/'.join(district_folder.split('/')[-4:])
        s = '/'.join(speaker_folder.split('/')[-5:])
        d2.append([i, s, log_error[1]])
        d3.append([i, s, log_error[2]])
        if os.path.isfile(pdf_path):
            d4.append([i, s, True])
        else:
            log_error[0].append('Error: (PDF-E1)')
            d4.append([i, s, False])
        if len(wav_files) < 1:
            log_error[0].append('Error: (WAV-E1)')
        if len(tsv_files) < 1:
            log_error[0].append('Error: (TSV-E1)')
        if len(log_error[0]) != 0:
            d1.append([i, s, log_error[0]])
    else:
        i = '/'.join(i.split('/')[-4:])
        s = '/'.join(s.split('/')[-5:])
        er_ = ['Error: (TXT-E1)']
        if len(wav_files) < 1:
            er_.append('Error: (WAV-E1)')
        if len(tsv_files) < 1:
            er_.append('Error: (TSV-E1)')
        d1.append([i, s, er_])

def save_to_csv_run_pipeline(output_path):
    df1 = pd.DataFrame(d1)
    df2 = pd.DataFrame(d2)
    df3 = pd.DataFrame(d3)
    df4 = pd.DataFrame(d4)
    
    column_name = 2
    expanded_df = pd.json_normalize(df3[column_name])
    result_df3 = pd.concat([df3.drop(columns=[column_name]), expanded_df], axis=1)
    expanded_df = pd.json_normalize(df2[column_name])
    result_df2 = pd.concat([df2.drop(columns=[column_name]), expanded_df], axis=1)
    df2 = result_df2
    df3 = result_df3
    
    if not df1.empty:
        df1.columns = ['District', 'Speaker', 'Reason']
    df2 = df2.rename(columns={0: 'District', 1: 'Speaker'})
    df3 = df3.rename(columns={0: 'District', 1: 'Speaker'})
    df4 = df4.rename(columns={0: 'District', 1: 'Speaker', 2: 'PDF exists'})
    
    if not df1.empty:
        df1 = df1.merge(df4, on=['District', 'Speaker'], how='outer')
    df2 = df2.merge(df4, on=['District', 'Speaker'], how='outer')
    df3 = df3.merge(df4, on=['District', 'Speaker'], how='outer')
    
    if not df1.empty:
        df1 = df1.drop(['District', 'PDF exists'], axis=1)
        df1 = df1.dropna()
        df1.to_csv(os.path.join(output_path, 'speaker_metadata_preinitial_checks_report.tsv'), index=False, sep='\t')
    df2.to_csv(os.path.join(output_path, 'speaker_metadata_df_extras.csv'), index=False)
    df3.to_csv(os.path.join(output_path, 'speaker_metadata_flagged.csv'), index=False, sep='\t')

def log_exception(log_entries, file_path, e):
    log_entries[file_path].append(f"{ERROR_CODES['Exception occurred']}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Process audio and metadata checks.')
    parser.add_argument('--main_root_folder', type=str, help='Path to the main folder containing subfolders for Phase 2')
    parser.add_argument('--phase1_tsv_folder', type=str, help='Path to the folder containing Phase 1 TSV files')
    parser.add_argument('--txt_file_path', type=str, help='Path to the text file containing state and district information')
    parser.add_argument('--xls_file_path', type=str, help='Path to the XLS file with image mappings')
    parser.add_argument('--output_file_path', type=str, help='Path to save the output TSV file')
    args = parser.parse_args()

    unicode_characters_to_check = ['\r']
    
    print("Retrieving the Speaker and Utterance Ids for Phase1....")
    speaker_utt_pairs_phase1 = check_for_repeats_in_tsv(args.phase1_tsv_folder)
    speaker_ids_phase1 = {pair[0] for pair in speaker_utt_pairs_phase1}
    utt_ids_phase1 = {pair[1] for pair in speaker_utt_pairs_phase1}
    print(f"Speaker-Utterance pairs Phase 1: {len(speaker_utt_pairs_phase1)} pairs found.")

    txt_unique_states, txt_unique_districts = extract_state_district_names_from_txt_file(args.txt_file_path)
    
    total_duration_hours_all_folders = 0.0
    for state_folder in tqdm(os.listdir(args.main_root_folder), desc="Processing state folders"):
        state_folder_path = os.path.join(args.main_root_folder, state_folder)
        for district_folder in tqdm(os.listdir(state_folder_path), desc="Processing district folders"):
            district_folder_path = os.path.join(state_folder_path, district_folder)
            for speaker_folder in tqdm(os.listdir(district_folder_path), desc="Processing speaker folders"):
                speaker_folder_path = os.path.join(district_folder_path, speaker_folder)

                speaker_id_issues = process_folders(speaker_folder_path, speaker_folder)
                for file, error_list in speaker_id_issues.items():
                    log_entries[file].extend(error_list)

                run_pipeline(district_folder_path, speaker_folder_path)

                for file in os.listdir(speaker_folder_path):
                    file_path = os.path.join(speaker_folder_path, file)
                    try:
                        total_duration_hours = get_duration(file, file_path)
                        total_duration_hours_all_folders += total_duration_hours

                        format_issues = questions_regarding_audio_tsv_formats(file_path, file, unicode_characters_to_check)
                        for file, issues in format_issues.items():
                            log_entries[file].extend(issues)

                        mismatched_filenames = compare_state_district_names(file, txt_unique_states, txt_unique_districts)
                        if mismatched_filenames:
                            for filename in mismatched_filenames:
                                log_entries[filename].append(ERROR_CODES['State or district mismatch'])

                        add_spk_utt_ids(file)
                        
                        uttID_issues = check_uttID_in_folder(file)
                        for file, error_list in uttID_issues.items():
                            log_entries[file].extend(error_list)
                        
                        extension_issues = check_audio_extensions(file)
                        for file, error_list in extension_issues.items():
                            log_entries[file].extend(error_list)

                        file_to_image_mapping(file, file_image_mapping)

                    except Exception as e:
                        log_exception(log_entries, district_folder_path, e)

    
    image_id_presence = check_image_ids_in_csv(file_image_mapping.values(), args.xls_file_path)
    for file, image_id in file_image_mapping.items():
        if not image_id_presence[image_id]:
            log_entries[file].append(ERROR_CODES['Not Present in database of images'])
    
    print(f'Total Duration across all folders: {total_duration_hours_all_folders:.2f} hours')
    if total_duration_hours_all_folders < 300 or total_duration_hours_all_folders > 900:
        log_entries["Batch Duration not between 300 to 900 Hours"].append(ERROR_CODES['Total duration out of range'])

    repeated_speaker_ids = speaker_ids_phase1.intersection(speaker_ids_phase2)
    repeated_utt_ids = utt_ids_phase1.intersection(utt_ids_phase2)
    
    if repeated_speaker_ids:
        for speaker_id in repeated_speaker_ids:
            log_entries[f"Repeated_speaker_ID_{speaker_id}"].append('Repeated speaker ID')
    if repeated_utt_ids:
        for utt_id in repeated_utt_ids:
            log_entries[f"Repeated_utt_ID_{utt_id}"].append('Repeated utterance ID')

    save_to_csv_run_pipeline(args.output_file_path)

    final_log_entries = [{"File": file, "Issue": ', '.join(set(issues))} for file, issues in log_entries.items()]

    log_df = pd.DataFrame(final_log_entries, columns=["File", "Issue"])
    log_df.to_csv(os.path.join(args.output_file_path, 'Error_files.tsv'), sep='\t', index=False)

    print(f"Results saved to {args.output_file_path}")

if __name__ == "__main__":
    log_entries = defaultdict(list)
    file_image_mapping = {}
    speaker_ids_phase2 = set()
    utt_ids_phase2 = set()
    d1, d2, d3, d4 = [], [], [], []

    main()