import glob
import json
import csv
from flatten_json import flatten

def write_to_csv(data, filename):
    """Write flattened data to CSV file"""
    if not data:
        return
    # Write the results to a `csv` file, one for one with the json file, including the header names.
    with open(filename,'w',newline='',encoding='utf-8') as csvfile:
        writer=csv.DictWriter(csvfile,fieldnames=data.keys())
        writer.writeheader()
        writer.writerow(data)
def main():
    # your code here
    # Crawl the `data` directory with `Python` and identify all the `json` files.
    src_json=glob.glob("**/*.json",recursive=True)
    all_json_data=[]
    for json_file in src_json:
        with open(json_file,'r') as f:
            # Load all the `json` files.
            data=json.load(f)  # Load JSON data as a dictionary
            # Flatten out the `json` data structure.
            flattened = flatten(data)
            all_json_data.append(flattened)
            csv_file=json_file.replace('.json','.csv')
            write_to_csv(flattened, csv_file)
    if all(all_json_data[0].keys()==e.keys() for e in all_json_data):
        with open("output_all_json.csv","w",newline='',encoding='utf-8') as csvfile:
            writer=csv.DictWriter(csvfile,fieldnames=all_json_data[0].keys())
            writer.writeheader()
            for data in all_json_data:
                writer.writerow(data)
    else:
        print("JSON files have different keys, cannot write to a single CSV file.")
    pass


if __name__ == "__main__":
    main()
