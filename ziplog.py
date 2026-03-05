from datetime import datetime,date
import os
import zipfile

def zip_log_files(source_directory, output_zip_file):
    try:
        with zipfile.ZipFile(output_zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(source_directory):
                for file in files:
                    if file.endswith(".log"):
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, start=source_directory)
                        zipf.write(file_path, arcname)
                        print(f"Added: {file_path}")
        print(f"Log files successfully zipped into {output_zip_file}")
    except Exception as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":
    logs_dir = "logs"
    now = datetime.now()
    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
    output_zip = "logs"+formatted_date+".zip"
    zip_log_files(logs_dir, output_zip)
