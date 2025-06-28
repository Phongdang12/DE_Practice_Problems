import requests
import pandas as pd
from bs4 import BeautifulSoup


# find 01102599999.csv in "2024-01-19 14:54	8.7M "	 
def main():
    # Attempt to web scrap/pull down the contents of `https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/`
    url="https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    response=requests.get(url)
    if response.status_code == 200:
            soup=BeautifulSoup(response.content,"html.parser")
            table=soup.find('table')
            table_rows=soup.find_all('tr')
        # Analyze it's structure, determine how to find the corresponding file to `2024-01-19 10:27	` using Python.
        # with open("data.csv", "w", encoding='utf-8') as file:
            for row in table_rows[3:]:
                cells=[td.get_text().strip() for td in row.find_all('td')]
                if cells and len(cells) > 1 and cells[1] == "2024-01-19 14:54" and cells[2]=="8.7M":
                #    file.write(cells[0] + "\n")
                # Build the `URL` required to download this file, and write the file locally.
                   url =f"https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/{cells[0]}"
                   response = requests.get(url)
                   if response.status_code == 200:
                       with open(cells[0],"wb") as f:
                           f.write(response.content)
    # Open the file with `Pandas` and find the records with the highest `HourlyDryBulbTemperature`.
    table=pd.read_csv("01102599999.csv")
    temp_column = table["HourlyDryBulbTemperature"]
    max_temp = temp_column.max()
    max_temp_rows = table[table["HourlyDryBulbTemperature"] == max_temp]
    # Print this to stdout/command line/terminal.
    print(max_temp_rows)
        
    pass

if __name__ == "__main__":
    main()


