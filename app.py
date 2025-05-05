import sys
import json
import schedule
import time
import pandas as pd
from os import environ, remove
from pathlib import Path
from ftplib import FTP_TLS

def get_ftp() -> FTP_TLS:
    # Get FTP details
    FTPHOST = environ["FTPHOST"]
    FTPUSER = environ["FTPUSER"]
    FTPPASS = environ["FTPPASS"]

    # Return authenticated FTP
    ftp = FTP_TLS(FTPHOST, FTPUSER, FTPPASS)
    ftp.prot_p()
    return ftp

def upload_to_ftp(ftp: FTP_TLS, file_source: Path):
    with open(file_source, "rb") as fp:  # Reading binary format as file
        ftp.storbinary(f"STOR {file_source.name}", fp)

def delete_local_file(file_path: Path):
    try:
        if file_path.exists():
            remove(file_path)
            print(f"Deleted local file: {file_path}")
        else:
            print(f"File not found: {file_path}")
    except Exception as e:
        print(f"Error deleting file {file_path}: {e}")

def read_csv(config: dict) -> pd.DataFrame:
    url = config["URL"]
    params = config["PARAMS"]
    return pd.read_csv(url, **params) #** All params are passed as keyword arguments

def pipeline():
    try:
        with open("config.json", "rb") as fp:
            config_list = json.load(fp)  # Load the list from config.json

        ftp = get_ftp()  # Get FTP connection

        # Ensure the config is a list
        if not isinstance(config_list, list):
            raise TypeError("The config.json file must contain a list of dictionaries.")

        # Loop through the dictionary in the list
        for entry in config_list:
            for key, config in entry.items():
                print(f"Processing configuration for: {key}")
                try:
                    # Read the CSV data into a DataFrame
                    df = read_csv(config=config)

                    # Save the DataFrame to a CSV file
                    output_file = f"{key}.csv"
                    df.to_csv(output_file, index=False)
                    print(f"{output_file} has been downloaded.")

                    # Upload the CSV file to the FTP server
                    upload_to_ftp(ftp, Path(output_file))
                    print(f"{output_file} has been uploaded to FTP server.")

                    # Delete the local CSV file
                    delete_local_file(Path(output_file))
                    print(f"{output_file} has been deleted.")
                except Exception as e:
                    print(f"Error processing {key}: {e}")
    except Exception as e:
        print(f"Error in pipeline: {e}")


if __name__ == "__main__":
    # Check if the script is run with a parameter
    if len(sys.argv) < 2:
        print("Error: Missing parameter. Use 'manual' or 'schedule'.")
        sys.exit(1)  # Exit the script with an error code

    param = sys.argv[1]

    if param == "manual":
        pipeline()

    elif param == "schedule":
        schedule.every().day.at("23:59").do(pipeline)

        print("Scheduler is running. Waiting for the scheduled time...")
    
        while True:
            print("Waiting for the scheduled time...")
            # Run the scheduled jobs
            schedule.run_pending()
            time.sleep(1)

    else:
        print("Invalid Parameter. Use 'manual' or 'schedule'. App is not running.")


