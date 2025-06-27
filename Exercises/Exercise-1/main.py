# chưa lấy đc điểm cộng 
import requests
import zipfile
import os
download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip"
]


def main():
    os.makedirs("downloads", exist_ok=True)
    os.makedirs("save_data", exist_ok=True)
    
    file_names= [x.split("/")[-1] for x in download_uris]
    for uri, file_name in zip(download_uris,file_names):
        response=requests.get(uri)
        print(f"Downloading {file_name} from {uri}")
        if response.status_code==200:
            with open (f"downloads/{file_name}","wb") as file:
                file.write(response.content)
        else:
            print(f"Failed to download {uri}, status code: {response.status_code}")
    
    file_save=[]
    for file_name in file_names:
        name_file_save=file_name.replace(".zip", ".csv")
        file_save.append(name_file_save)
    
    for file_source,file_destination in zip(file_names,file_save):
       with zipfile.ZipFile(f"downloads/{file_source}", 'r') as zip_ref:
           file_csv=zip_ref.read(file_destination)
           
           with open(f"save_data/{file_destination}","wb") as file:
                file.write(file_csv)
                os.remove(f"downloads/{file_source}")
    
    os.rmdir("downloads")
    pass


if __name__ == "__main__":
    main()
