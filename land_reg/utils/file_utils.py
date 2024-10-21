import os
from config import config

def read_parsed_files():

    parsedFiles = []
    paths = []

    #Import requirement.txt file lines
    with open(config.PARSED_FILE_PATH, 'r') as file:
        paths = file.readlines()
        

    parsedFiles = [os.path.normpath(path.strip()) for path in paths]

    return parsedFiles

#Returns all valid unparsed csv files
def validate_csv_files(filePathArray):

    filesToParse = set()

    parsedFiles = read_parsed_files()

    # Loop through each file in the filePathArray, and add file paths not already parsed
    for filePath in filePathArray:    

        root = config.DATA_FILE_PATH
        fullPath = root / filePath

        #Check valid file path
        if not filePath.endswith('.csv'):
            print(f'Invalid file path: {filePath}')
            continue

        # Check if file path is already parsed
        if str(fullPath) in parsedFiles:
            continue

        print(f'Staging to parse: {filePath}')
        filesToParse.add(fullPath)
            
    return filesToParse
        