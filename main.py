import gspread
import pandas as pd
from typing import Union
from fastapi import FastAPI, Response
from datetime import datetime,timedelta
import os,ast
from dotenv import load_dotenv
from pydantic import BaseModel
from tabulate import tabulate
import pandas as pd

load_dotenv()

service_account = {
    item:os.environ.get(item.upper()).replace('\\n', '\n') for item in ['type', 'project_id', 'private_key_id', 'private_key', 'client_email', 'client_id', 'auth_uri', 'token_uri', 'auth_provider_x509_cert_url', 'client_x509_cert_url', 'universe_domain']
}

gc = gspread.service_account_from_dict(service_account)

wb = gc.open_by_key(os.environ['INPUT_KEY'])
app = FastAPI()
shifts = {}
profile = wb.worksheet("インターン生請求書フォーマットリンク集").get_all_records()
profiledf = pd.DataFrame.from_records(profile,columns=profile[0].keys()).set_index("Slack_id")
teams = profiledf.set_index('氏名')['Team'].to_dict()




def process_person(data, year,person):
    global teams
    shift = {}
    month = data[1][0].replace("月", "").zfill(2)
    for item in data[1:]:
        date = item[1].replace("日", "").zfill(2)
        start = item[3]
        end = item[4]
        location = item[5]
        work_time = item[7]
        shift[f"{year}-{month}-{date}"] = {
            "start": start,
            "end": end,
            "location": location,
            "work_time": work_time,
            "team":teams.get(person,"unknown"),
        }
    return shift

def get_shift_data(month):
    result = {}
    split_input = month.split("-")
    year = split_input[0]
    month = split_input[1]
    if len(month)==2 and month[0]=="0":
        month = month[1]
    sheet_name = "{}年{}月".format(year,month)
    sheet = wb.worksheet(sheet_name)
    list_of_lists = sheet.get_all_values()
    if list_of_lists[1][0]=='名前ジャンプ':
        list_of_lists = [sublist[1:] for sublist in list_of_lists]
    chunked = [
        [item[i:i + 8] for i in range(0, len(item), 8)] for item in list_of_lists[1:]
    ]
    index_to_name = {index: item[1] for index, item in enumerate(chunked[0])}
    for chunk in chunked:
        for idx, item in enumerate(chunk):
            name = index_to_name[idx]
            if name in result.keys():
                result[name].append(item)
            else:
                result[name] = [item]
    person_shift = {}
    
    for person, item in result.items():
        person_shift[person] = process_person(item, year,person)

    return person_shift

def show_shift(var: str, person_shift: dict,keyword:str):
    assert keyword in ("person","date")
    df = pd.DataFrame.from_dict(person_shift, orient="index")
    try:
        if keyword=="person":
            load = {
                key: value
                for key, value in df.loc[var].to_dict().items()
                if value["start"] != "" and value["end"] != ""
            }
        elif keyword=="date":
            load = {
                key: value
                for key, value in df[var].to_dict().items()
                if value["start"] != "" and value["end"] != ""
            }
    except KeyError:
        load = {"error": f"{var} shift not exist"}
        if keyword=="date":
            load={}

    return load

def start_up():
    global shifts
    today_startup = datetime.today().strftime('%Y-%m-%d').split("-")
    month_startup = f"{today_startup[0]}-{today_startup[1]}"
    shifts[month_startup] = get_shift_data(month_startup)
    if int(today_startup[-1])>25:
        month_startup = f"{today_startup[0]}-{str(int(today_startup[1])+1)}"
        shifts[month_startup] = get_shift_data(month_startup)

start_up()
@app.get("/")
def read_root():
    return "Welcome, hello world"

def appropriate_hour(hour):
    if hour[:2] == "24":
        hour = "23:59"
    hour = hour.replace("：",":")
    if len(hour) > 5:
        return datetime.strptime(hour, "%H:%M:%S").time()
    else:
        return datetime.strptime(hour, "%H:%M").time()

def get_load(var,month,keyword):
    assert keyword in ("person","date")
    global shifts
    try:
        load = show_shift(var, shifts[month],keyword)
    except KeyError as e:
        shifts[month] = get_shift_data(month)
        load = show_shift(var, shifts[month],keyword)
    return load

def process_load(load,
                formatted_load,keyword,
                req_team=[''],
                start_time=datetime.strptime("00:01", "%H:%M").time(),
                end_time=datetime.strptime("23:59", "%H:%M").time(),
                ):
    table = []
    if req_team==['']:
        #default value for team
        req_team = set(teams.values())
        print(req_team)
    if len(load)==0 or "error" in load.keys():
        formatted_load.append(load.get("error", "No shift"))
    else:
        for var, item in load.items():
            team=item['team']
            if keyword == "date":
                start = appropriate_hour(item["start"])
                end = appropriate_hour(item["end"])
                if start <= start_time and end >= end_time:
                    continue
                if not team in req_team:
                    continue
            location = item["location"]
            if location=="":
                location = "不明"
            table.append({"　　":var,"開業":item['start'],"終業":item['end'],"勤務地":location,"稼働時間":f"{item['work_time']}時間","チーム":team})
    formatted_load.append(tabulate(table,headers='keys'))
    return formatted_load

class Shift(BaseModel):
    name: str
    start_time: str
    end_time: str
    work_time: str
    location: str

@app.get("/bydate/{date_start}/")
def show_shift_date(
    date_start: str, team: str ="",date_end="", start_time: str = "6", end_time: str = "18", q: Union[str, None] = None
):
    start_time = datetime.strptime(start_time, "%H").time()
    if end_time == "24":
        end_time = datetime.strptime("23:59", "%H:%M").time()
    else:
        end_time = datetime.strptime(end_time, "%H").time()
    team = team.split(",")
    shortcut = {"today": datetime.now().strftime("%Y-%m-%d"),
                "tomorrow": (datetime.now() + timedelta(1)).strftime("%Y-%m-%d"),
                "yesterday": (datetime.now() + timedelta(-1)).strftime("%Y-%m-%d")}
    if date_start in shortcut.keys():
        date_start = shortcut[date_start]
    if date_end=="":
        date_end=date_start
    formatted_load = []   
    dates = [str(date.date()) for date in pd.date_range(start=date_start,end=date_end)]
    for date in dates:
        formatted_load.append(f"{date}のシフトはこちらです")
        month = "{}-{}".format(*date.split("-")[:2])
        try:
            load = get_load(date,month,"date")
        except IndexError as e:
            formatted_load.append(f"{month}のシフトはエラー発生しました、ナイジェル・清野を報告してください")
            return Response("\n".join(formatted_load), media_type="text/plain")
        except gspread.exceptions.WorksheetNotFound as e:
            formatted_load.append(f"{e.args[0]}のシフトはインターン生シフト表にありませんでした。確認の上再開してください")
            return Response("\n".join(formatted_load), media_type="text/plain")
        process_load(load,formatted_load,"date",team,start_time,end_time)
        formatted_load.append("\n")
    return Response("\n".join(formatted_load), media_type="text/plain")

@app.get("/byperson/{persons}/")
def show_shift_person(persons: str, q: Union[str, None] = None, month: str = datetime.now().strftime("%Y-%m"),):
    formatted_load = []
    for person in persons.split(" "):
        person = profiledf.loc[person.replace('@','')]["氏名"]    
        formatted_load.append(f"{person}の{month}シフトはこちらです")
        try:
            load = get_load(person,month,"person")
        except KeyboardInterrupt as e:
            formatted_load.append(f"{month}のシフトはエラー発生しました、ナイジェル・清野を報告してください")
            return Response("\n".join(formatted_load), media_type="text/plain") 
        except gspread.exceptions.WorksheetNotFound as e:
            formatted_load.append(f"{e.args[0]}のシフトはインターン生シフト表にありませんでした。確認の上再開してください")
            return Response("\n".join(formatted_load), media_type="text/plain")
        process_load(load,formatted_load,"person")
        formatted_load.append("\n")
    return Response("\n".join(formatted_load), media_type="text/plain")

@app.get("/getshift/{month}")
def download_shift(month):
    global shifts
    shifts[month] = get_shift_data(month)
    return Response("shift updated",media_type="text/plain")

@app.get("/showshift/{month}")
def show_shift_all(month):
    shift = shifts[month]
    final_load = [f"{month}のAllシフト"]
    for person in shift:
        final_load.append(person)
        formatted_load = []
        load = get_load(person,month,"person")
        process_load(load,formatted_load,"person")
        formatted_load.append("\n")
        final_load.extend(formatted_load)
    return Response("\n".join(final_load), media_type="text/plain")
