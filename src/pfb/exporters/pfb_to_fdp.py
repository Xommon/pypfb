import os
import sys
import json
import csv
from fastavro import reader as AvroReader
from frictionless import Package, Resource

# Establish working directory
print("--- Convert PFB to Frictionless Data Package ---")
scriptDirectory = os.path.dirname(os.path.abspath(sys.argv[0]))
os.chdir(scriptDirectory)

# Import .avro file
conditionsMet = False
while not conditionsMet:
    fileName = input("Enter PFB (.avro) name: ")
    if not os.path.exists(fileName):
        print("File cannot be found: " + fileName + "\n")
    elif fileName[-5:].lower() != ".avro":
        print("Invalid filename: " + fileName + "\n")
        print("Must be a .avro file.")
    else:
        conditionsMet = True

# Create a new folder with the same name as the file
if not os.path.exists(fileName[:-5]):
    os.mkdir(fileName[:-5])

# Read the Avro file and prepare the data
def ReadAvro(fileName):
    with open(fileName, "rb") as f:
        avroData = list(AvroReader(f))  # Read all the records in the Avro file
    return avroData

# Convert to Frictionless Data Package
def ConvertToFrictionless(avroData, fileName):
    # Create a Frictionless Resource with Avro data
    resource = Resource(data=avroData)
    
    # Create a Frictionless Package and add the resource
    package = Package(name=fileName[:-5])
    package.add_resource(resource)

    # Save the package as 'DataPackage.json' inside the newly created folder
    packagePath = os.path.join(fileName[:-5], "DataPackage.json")
    package.to_json(packagePath)

# Function to save all records as a single CSV file
def SaveRecordsAsCsv(avroData, fileName):
    csvFilePath = os.path.join(fileName[:-5], "records.csv")
    with open(csvFilePath, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=avroData[0].keys())  # Use keys from the first record as headers
        writer.writeheader()  # Write the headers (keys from the first record)
        writer.writerows(avroData)  # Write all the records

# Read the Avro file
avroData = ReadAvro(fileName)

# Convert the Avro data to a Frictionless Data Package
ConvertToFrictionless(avroData, fileName)

# Save all records in the folder as a single CSV file
SaveRecordsAsCsv(avroData, fileName)

# Update user, show where the files have been saved
print(f"'DataPackage.json' and 'records.csv' saved in directory: {os.path.join(os.getcwd(), fileName[:-5])}")
