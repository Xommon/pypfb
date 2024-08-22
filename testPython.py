testimport pfb
import os
import fastavro
import pandas as pd
from io import StringIO

# Load PFB .avro
print("--- Convert PFB to Frictioness Data Package ---")
conditionsMet = False
while conditionsMet == False:
    fileName = input("Enter PFB (.avro) name: ")
    if os.path.exists(fileName) == False:
        print("File cannot be found: " + fileName + "\n")
    elif fileName[-5:].lower() != ".avro":
        print("Invalid filename: " + fileName + "\n")
    else:
        conditionsMet = True

# Extract TSV files from PFB
def ReadAvro(fileName):
    with open(fileName, 'rb') as file:
        reader = fastavro.reader(file)
        records = [record for record in reader]
    return records

def AvroToTSV(records):
    # Conert records to a pandas DataFrame
    df = pd.DataFrame(records)

    # Convert DataFrame to TSV
    tsvBuffer = StringIO()
    df.to_csv(tsvBuffer, sep='\t', index=False)

    # Get TSV content as a string
    tsvContent = tsvBuffer.getvalue()

    return tsvContent

# Read the .avro file
records = ReadAvro(fileName)

# Convert the records to TSV format and store in an array
tsvFiles = [AvroToTSV(records)]

# Print file names
print("File converted into:", len(tsvFiles), "files.")