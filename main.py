import gspread
import pandas as pd
from typing import Union
from fastapi import FastAPI, Response
from datetime import datetime,timedelta
import os,ast
from dotenv import load_dotenv

load_dotenv()
credentials = ast.literal_eval(os.environ["CREDENTIALS"])
authorized_user = ast.literal_eval(os.environ["AUTHORIZED_USER"])
gc, authorized_user = gspread.oauth_from_dict(credentials, authorized_user)

wb = gc.open_by_key(os.environ['input_key'])
app = FastAPI()
shifts = {}


def process_person(person, year):
    shift = {}
    month = person[1][0].replace("月", "").zfill(2)
    for item in person[1:]:
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
        person_shift[person] = process_person(item, year)

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

today = datetime.today().strftime('%Y-%m-%d').split("-")
month = f"{today[0]}-{today[1]}"
shifts[month] = get_shift_data(month)
if int(today[-1])>25:
    month = f"{today[0]}-{str(int(today[1])+1)}"
    shifts[month] = get_shift_data(month)

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
                start_time=datetime.strptime("00:01", "%H:%M").time(),
                end_time=datetime.strptime("23:59", "%H:%M").time()):
    if len(load)==0:
        if "error" in load.keys():
            formatted_load.append(load.get("error", "No shift"))
        else:
            formatted_load.append("No shift")
    else:
        for var, item in load.items():
            location = item["location"]
            if location=="":
                location = "不明"
            if keyword == "date":
                start = appropriate_hour(item["start"])
                end = appropriate_hour(item["end"])
                if start <= start_time and end >= end_time:
                    continue
            formatted_load.append(
                    f"{var} {item['start']} から {item['end']} まで、稼働時間: {item['work_time']}時間 勤務地:{location}"
                )
    return formatted_load

@app.get("/bydate/{date}/")
def show_shift_date(
    date: str, start_time: str = "6", end_time: str = "18", q: Union[str, None] = None
):
    start_time = datetime.strptime(start_time, "%H").time()
    if end_time == "24":
        end_time = datetime.strptime("23:59", "%H:%M").time()
    else:
        end_time = datetime.strptime(end_time, "%H").time()

    shortcut = {"today": datetime.now().strftime("%Y-%m-%d"),
                "tomorrow": (datetime.now() + timedelta(1)).strftime("%Y-%m-%d"),
                "yesterday": (datetime.now() + timedelta(-1)).strftime("%Y-%m-%d")}
    
    if date in shortcut.keys():
        date = shortcut[date]

    month = "{}-{}".format(*date.split("-")[:2])
    formatted_load = [f"{date}のシフトはこちらです"]
    try:
        load = get_load(date,month,"date")
    except IndexError as e:
        formatted_load.append(f"{month}のシフトは見つかりませんでした、ナイジェル・清野を報告してください")
        return Response("\n".join(formatted_load), media_type="text/plain")
    except gspread.exceptions.WorksheetNotFound as e:
        formatted_load.append(f"{e.args[0]}のシフトはインターン生シフト表にありませんでした。確認の上再開してください")
        return Response("\n".join(formatted_load), media_type="text/plain")
    formatted_load = "\n".join(process_load(load,formatted_load,"date",start_time,end_time))
    return Response(formatted_load, media_type="text/plain")



@app.get("/byperson/{person}/")
def show_shift_person(person: str, q: Union[str, None] = None, month: str = datetime.now().strftime("%Y-%m")):
    """
    need to fix this to handle no user
    """
    formatted_load = [f"{person}の{month}シフトはこちらです"]
    try:
        load = get_load(person,month,"person")
    except IndexError as e:
        formatted_load.append(f"{month}のシフトは見つかりませんでした、ナイジェル・清野を報告してください")
        return Response("\n".join(formatted_load), media_type="text/plain")
    except gspread.exceptions.WorksheetNotFound as e:
        formatted_load.append(f"{e.args[0]}のシフトはインターン生シフト表にありませんでした。確認の上再開してください")
        return Response("\n".join(formatted_load), media_type="text/plain")
    
    formatted_load = "\n".join(process_load(load,formatted_load,"person"))
    return Response(formatted_load, media_type="text/plain")

@app.get("/getshift/{month}")
def get_shift(month):
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
