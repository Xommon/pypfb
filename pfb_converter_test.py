import avro.schema
import avro.datafile
import avro.io
import csv

def avro_to_tsv(avro_file, tsv_prefix, records_per_file):
    with open(avro_file, 'rb') as avro_f:
        reader = avro.datafile.DataFileReader(avro_f, avro.io.DatumReader())
        schema = reader.datum_reader.writers_schema
        
        records = []
        file_count = 1

        for record in reader:
            records.append(record)
            if len(records) >= records_per_file:
                tsv_filename = f"{tsv_prefix}_{file_count}.tsv"
                write_to_tsv(tsv_filename, records, schema)
                file_count += 1
                records = []

        if records:
            tsv_filename = f"{tsv_prefix}_{file_count}.tsv"
            write_to_tsv(tsv_filename, records, schema)
        
        reader.close()

def write_to_tsv(filename, records, schema):
    with open(filename, 'w', newline='') as tsv_f:
        writer = csv.DictWriter(tsv_f, fieldnames=schema.names, delimiter='\t')
        writer.writeheader()
        writer.writerows(records)

# Example usage:
avro_file = 'input.avro'
tsv_prefix = 'output'
records_per_file = 100  # Adjust this number as needed
avro_to_tsv(avro_file, tsv_prefix, records_per_file)
