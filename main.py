import gspread
import pandas as pd
from typing import Union
from fastapi import FastAPI, Response
from datetime import datetime
import os,ast




credentials = ast.literal_eval(os.environ["CREDENTIALS"])
authorized_user = ast.literal_eval(os.environ["AUTHORIZED_USER"])
gc, authorized_user = gspread.oauth_from_dict(credentials, authorized_user)

wb = gc.open_by_key(os.environ['input_key'])
app = FastAPI()

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


def get_person_shift_month(input_data):
    """
    The problem with sept is not input, input is easy to fix,
    but the type of sheet is slightly different
    let's ignore since we don't use it a lot as well
    """
    result = {}
    split_input = input_data.split("-")
    year = split_input[0]
    month = split_input[1]
    if len(month)>2 and month[0]=="0":
        month = month[1]
    sheet_name = "{}年{}月".format(year,month)
    sheet = wb.worksheet(sheet_name)
    list_of_lists = sheet.get_all_values()
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


def get_shift_person(person: str, person_shift: dict):
    """
    Handle no user
    """
    df = pd.DataFrame.from_dict(person_shift, orient="index")
    try:
        load = {
            key: value
            for key, value in df.loc[person].to_dict().items()
            if value["start"] != "" and value["end"] != ""
        }
    except KeyError:
        load = {"error": f"{person} shift not exist"}
    return load


def get_shift_date(date: str, person_shift: dict):
    df = pd.DataFrame.from_dict(person_shift, orient="index")
    load = {
        key: value
        for key, value in df[date].to_dict().items()
        if value["start"] != "" and value["end"] != ""
    }
    return load


@app.get("/")
def read_root():
    return {"Hello": "World"}


def appropriate_hour(hour):
    if hour[:2] == "24":
        hour = "23:59"
    if len(hour) > 5:
        return datetime.strptime(hour, "%H:%M:%S").time()
    else:
        return datetime.strptime(hour, "%H:%M").time()


@app.get("/bydate/{date}/")
def read_shift_date(
    date: str, start_time: str = "9", end_time: str = "18", q: Union[str, None] = None
):
    formatted_load = [f"{date}のシフトはこちらです"]
    start_time = datetime.strptime(start_time, "%H").time()
    if end_time == "24":
        end_time = datetime.strptime("23:59", "%H:%M").time()
    else:
        end_time = datetime.strptime(end_time, "%H").time()

    load = get_shift_date(date, get_person_shift_month(date))
    if len(load) == 0:
        formatted_load.append("No shift")
    else:
        for person, item in load.items():
            location = item["location"]
            start = appropriate_hour(item["start"])
            end = appropriate_hour(item["end"])
            if location == "":
                location = "不明"
            if start >= start_time and end <= end_time:
                formatted_load.append(
                    f"{person} {start} から {end} まで、稼働時間: {item['work_time']}時間 勤務地:{location}"
                )

    formatted_load = "\n".join(formatted_load)

    return Response(formatted_load, media_type="text/plain")


@app.get("/byperson/{month}/{person}")
def read_shift_person(month: str, person: str, q: Union[str, None] = None):
    """
    need to fix this to handle no user
    """
    formatted_load = [f"{person}の{month}シフトはこちらです"]
    load = get_shift_person(person, get_person_shift_month(month))
    if len(load) == 0 or "error" in load.keys():
        formatted_load.append(load.get("error", "No shift"))
    else:
        for date, item in load.items():
            location = item["location"]
            if location == "":
                location = "不明"
            formatted_load.append(
                f"{date} {item['start']} から {item['end']} まで、稼働時間: {item['work_time']}時間 勤務地:{location}"
            )
        # removing the total part
        if len(formatted_load) > 2:
            formatted_load.pop()
    formatted_load = "\n".join(formatted_load)

    return Response(formatted_load, media_type="text/plain")
