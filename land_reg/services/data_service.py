from land_reg.ingestion.csv_ingestor import ingest_csv_files
from land_reg.utils.file_utils import validate_csv_files
from config import config
from pathlib import Path


def process_directory(process_type: str) -> None:

    directory = Path(config.DATA_FILE_PATH)

    if not directory.is_dir():
        raise ValueError(f"The path {config.DATA_FILE_PATH} is not a directory or does not exist.")
    
    
    # Get all files in the directory
    documents = [file for file in directory.iterdir() if file.is_file()]
    
    # Convert to list of file names
    document_names = [file.name for file in documents]
    
    # Filter out depending on process_type
    if process_type == "both":
        pass
    elif process_type == "dom":
        # Filter out OCOD_ datasets - remove overseas companies datasets
        document_names = [file for file in document_names if not "OCOD_" in file]
        
    elif process_type == "for":
        # Filter out CCOD datasets - remove domestic companies datasets
        document_names = [file for file in document_names if not "CCOD_" in file]
        

    #Get set of unparsed and valid csv files
    fileNameArray = validate_csv_files(document_names)

    if len(fileNameArray) > 0:
        #iF THERE ARE FILES TO PARSE THEN...
        ingest_csv_files(fileNameArray)
        
    else:
        print("No CSV files found in the specified directory.")
       