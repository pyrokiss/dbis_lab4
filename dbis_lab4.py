import csv
import sys
import pymongo
from pymongo import MongoClient
import re
import time
import os

file_name, host, port = sys.argv

data_file_test = ["Test2019.csv", "Test2020.csv"]
data_file_or = ["Odata2019File.csv", "Odata2020File.csv"]
counter_file = "string_number.txt"


pattern_mark = re.compile(r'^\d+,\d+$')  # шаблон для поиска чисел с запятой в данных
pattern_file = re.compile(r'\d{4}')  # шаблон для поиска года в названии файла

header =["Регіон,Рік,Максимальный бал з Історії України"]
pipeline = [{"$match": {"histTestStatus": "Зараховано"}},
            {"$group": {"_id": {"Region": "$REGNAME",
                                "Year": "$year"},
            "MaxBall": {"$max": "$histBall100"}}},
            {"$sort": {"_id.Region": 1,
                       "_id.Year": 1}}]

def result_csv(db, pipeline, head):
    query_result = db.zno_data.aggregate(pipeline)
    with open("result.csv", "w") as result:
        result.write(",".join([str(elem) for elem in head])+"\n")
        for exm in query_result:
            temp = [exm["_id"]["Region"], exm["_id"]["Year"], exm["MaxBall"]]
            result.write(",".join([str(elem) for elem in temp]) + "\n")

def time_csv(start):
    with open("time.txt", "w") as time_file:
        time_file.write("Время: {}".format(str(time.time() - start)))
        print("Время: ", (time.time() - start))

def main_task(data_list):
    for data_file in data_list:
        print(data_file)
        with open(data_file, newline='', encoding="cp1251") as csvfile:
            zno_data_obj = []
            reader = csv.reader(csvfile, delimiter=';')
            head = next(reader)
            head.insert(0, "year")
            data_year = re.findall(pattern_file, data_file)[0]
            with open(counter_file, "r") as counter1:
                result = int(counter1.read())
                for n, row in enumerate(reader):
                    row.insert(0, data_year)
                    zno_data = {}
                    # print(n)
                    if n >= result:
                        for b, i in enumerate(row):
                            if i == "null":
                                i = None
                            if "Birth" in head[b] and i != None:
                                i = int(i)
                            if "Ball" in head[b] and i != None:
                                if "Ball100" in head[b]:
                                    i = float(i.replace(",", "."))
                                else:
                                    i = int(i)
                            zno_data.update({head[b]: i})
                        zno_data_obj.append(zno_data)
                        # print(n)
                        if (n+1)%100 == 0 and n >=0:
                            print("n:", (n+1))
                            print('len: ',len(zno_data_obj))
                            db.zno_data.insert_many(zno_data_obj)
                            zno_data_obj.clear()
                            with open(counter_file, "w") as counter2:
                                counter2.write(str(n+1))

            print('len: ', len(zno_data_obj))
            db.zno_data.insert_many(zno_data_obj)
            zno_data_obj.clear()
            with open(counter_file, "w") as counter3:
                counter3.write(str(0))

try:
    client = MongoClient(str(host), port=int(port))
    db = client.zno_data_or
    res = db.command("serverStatus")
    print("Successful connection")
    files_list = os.listdir()
    if counter_file not in files_list:
        with open(counter_file, "w") as counter:
            counter.write(str(0))
    else:
        start_time = time.time()
        main_task(data_file_or)
        result_csv(db, pipeline, header)
        time_csv(start_time)
except pymongo.errors.ServerSelectionTimeoutError as S:
    print("Connection error")