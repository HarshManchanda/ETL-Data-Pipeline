import json
import requests
import datetime

def json_scraper(url, file_name, bucket):
    print("Start Running...")
    response = requests.request("GET",url)
    json_data=response.json()

    with open(file_name,'w', encoding='utf-8') as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=4)

    print("End Running...")
    
today_Date = datetime.datetime.today()
today_Date_Formatted = today_Date.strftime ('%d-%m-%Y')


json_scraper('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Mumbai/today?unitGroup=metric&key=<your-key>&contentType=json','/home/harsh/Dags_script/json data/'+str(today_Date_Formatted)+'.json','data-mbfr')