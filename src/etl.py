import csv
from google.cloud import bigquery
import csv
import os
from datetime import datetime
from os import listdir
from os.path import isfile, join
from os import walk
import re


def main():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\Owner\Downloads\\Equus-c54a22fa4e39.json"
    client = bigquery.Client()

    mypath = "C:\\Users\\Owner\\Desktop\\DRFData\\2021_02\\"
    filenames = os.listdir(mypath)
    # dir_names=os.listdir(mypath)
    # for directory in dir_names:
    #     if directory != '.DS_Store':
    #         filenames=os.listdir(mypath+directory) ADD THIS OTHER NESTED FOR LOOP TO GO THROUGH ALL FILES
    for filename in filenames:
        client = bigquery.Client()
        newlist = re.split('([0-9]+)', filename)
        track_code = newlist[0]
        race_date = newlist[1]

        # SQL Queries
        select_row = "file_name"
        data_table = "master_file_admin"
        current_processed_file_names = list()
        query_job = client.query(f"select {select_row} from drf.{data_table} ")
        result = query_job.result()

        for row in result:
            current_processed_file_names.append(row[select_row])

        if filename not in current_processed_file_names:
            insert_command = "INSERT INTO drf.master_file_admin (file_name,race_date,track_code,process_date,data_source,processing_errors)"
            insert_command += "VALUES('" + filename + "','" + str(
                f"{race_date[0:4]}-{race_date[4:6]}-{race_date[6:8]}") + "','" + track_code + "','" + datetime.today().strftime(
                '%Y-%m-%d') + "','DRF','')"
            client.query(insert_command)

            # path = mypath+directory+'/'+filename IF YOU BRING BACK NESTED FOR LOOP USE THIS
            path = mypath + '/' + filename
            with open(path, 'r') as csvfile:
                header_results_final = list()  # list of CSVs
                race_data_results_final = list()  # list of CSVs
                exotic_wagering_results_final = list()  # list of CSVs
                attendance_results_final = list()  # list of CSVs
                comments_results_final = list()  # list of CSVs
                footnotes_results_final = list()  # list of CSVs
                starters_performance_results_final = list()  # list of CSVs
                # creating a csv reader object
                csvreader = csv.reader(csvfile)

                # for each row in the csv reader create a JSON object list
                for row in csvreader:
                    if row[0] == 'R':  # race_data
                        results_to_JSON = RaceDataDRFDataPoint(track_code, race_date, row[0], row[1], row[2], row[3],
                                                               row[4], row[5], row[6], row[7], row[8], row[9], row[10],
                                                               row[11], row[12], row[13], row[14], row[15], row[16],
                                                               row[17], row[18], row[19], row[20],
                                                               row[21], row[22], row[23], row[24], row[25], row[26],
                                                               row[27], row[28], row[29], row[30],
                                                               row[31], row[32], row[33], row[34], row[35], row[36],
                                                               row[37], row[38], row[39], row[40],
                                                               row[41], row[42], row[43], row[44], row[45], row[46],
                                                               row[47], row[48], row[49], row[50],
                                                               row[51], row[52], row[53], row[54], row[55], row[56],
                                                               row[57], row[58], row[59], row[60],
                                                               row[61], row[62], row[63])

                        JSON = results_to_JSON.ResultsToJson()
                        race_data_results_final.append(JSON)
                    if row[0] == 'E':  # exotic
                        results_to_JSON = ExoticWageringDataDRFDataPoint(track_code, race_date, row[0], row[1], row[2],
                                                                         row[3], row[4], row[5], row[6], row[7], row[8])

                        JSON = results_to_JSON.ResultsToJson()
                        exotic_wagering_results_final.append(JSON)
                    if row[0] == 'S':  # Starters performace data
                        results_to_JSON = StartersPerformanceDataDRFDataPoint(track_code, race_date, row[0], row[1],
                                                                              row[2], row[3], row[4], row[5], row[6],
                                                                              row[7], row[8], row[9], row[10],
                                                                              row[11], row[12], row[13], row[14],
                                                                              row[15], row[16], row[17], row[18],
                                                                              row[19], row[20],
                                                                              row[21], row[22], row[23], row[24],
                                                                              row[25], row[26], row[27], row[28],
                                                                              row[29], row[30],
                                                                              row[31], row[32], row[33], row[34],
                                                                              row[35], row[36], row[37], row[38],
                                                                              row[39], row[40],
                                                                              row[41], row[42], row[43], row[44],
                                                                              row[45], row[46], row[47], row[48],
                                                                              row[49], row[50],
                                                                              row[51], row[52], row[53], row[54],
                                                                              row[55], row[56], row[57], row[58],
                                                                              row[59], row[60],
                                                                              row[61], row[62], row[63], row[64],
                                                                              row[65], row[66], row[67], row[68],
                                                                              row[69], row[70],
                                                                              row[71], row[72], row[73], row[74],
                                                                              row[75], row[76], row[77], row[78],
                                                                              row[79], row[80],
                                                                              row[81], row[82], row[83], row[84],
                                                                              row[85], row[86], row[87], row[88],
                                                                              row[89], row[90],
                                                                              row[91])
                        JSON = results_to_JSON.ResultsToJson()
                        starters_performance_results_final.append(JSON)

            errors = client.insert_rows_json('drf.master_starters_performance_data', starters_performance_results_final)
            errors_3 = client.insert_rows_json('drf.master_race_data', race_data_results_final)
            errors_2 = client.insert_rows_json('drf.master_exotic_wagering', exotic_wagering_results_final)
            print(f"{filename} processed")


if __name__ == "__main__":
    main()