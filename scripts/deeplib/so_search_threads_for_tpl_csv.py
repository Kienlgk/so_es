"""
    Search SO threads mentioning the TPLs in their sentences.
    Input is a CSV file having unique TPLs gotten from task 4. Get list of TPL indexes
    Step 1 in collecting so sents
"""
import os
import gc
import time
import csv
import argparse
# pip install nltk
import nltk
from nltk.corpus import stopwords
nltk.download('punkt')
nltk.download('stopwords')
from nltk.tokenize import word_tokenize

from multiprocessing import Pool
from pprint import pformat

import sys

sys.path.append(os.sep.join(sys.path[0].split(os.sep)[:-1]))
print(sys.path)
from utils.es_client import ESClient
from config.config import config

def remove_stop_word_from_query(query):
    text_tokens = word_tokenize(query)
    tokens_without_sw = [word for word in text_tokens if (word.lower() not in stopwords.words()) and (word.lower() not in ['sdk', 'plugin'])]
    return " ".join(tokens_without_sw)

# def 

def load_csv(csv_path):
    # csv_path = "/app/additional_data/tpl_index.csv"
    tpls = []
    with open(csv_path, "r") as fp:
        reader = csv.reader(fp)
        for row in reader:
            tpls.append((row[0], row[2])) #(Standard Package, Lib Name)
    return tpls

def main(args):
    import json
    # searching
    # with open("/app/additional_data/tpl_list.json", "r") as fp:
    #     libs = json.load(fp)

    # result_output = "/app/search_result_ours/"
    result_output = args.so_out_dir
    if args.end < args.start:
        raise Exception("Error input arguments: End is larger than Start")
    libs = load_csv(args.tpl_csv_dir)
    libs = libs[args.start:args.end+1]
    INDEX_NAME = config.es.index_name
    es_client = ESClient()
    print(len(libs))
    print(INDEX_NAME)
    for lib_i, tpl_tuple in enumerate(libs):    
        package, lib = tpl_tuple
        # if lib == "Unknown":
        package_parts_string = " ".join([part for part in package[1:].split("/") if part not in ["com", "org", "net"]])
        if lib != "Unknown":
            package_parts_string = package_parts_string + " " + lib
        print(f"{package} {lib}: query {package_parts_string}")

        lib_i = lib_i + args.start
        if os.path.exists(f"{result_output}/{lib_i}.json"):
            print(f"{result_output}/{lib_i}.json is available" )
            continue
        query = remove_stop_word_from_query(package_parts_string)
        # print(query)
        thread_results, results_len = es_client.query(index_name=INDEX_NAME, search_string=query, op="or", field="search")

        predicted_ids = []
        for res in thread_results:
            if res['_id'] not in predicted_ids:
                predicted_ids.append(res['_id'])
        os.makedirs(f"{result_output}/", exist_ok=True)
        with open(f"{result_output}/{lib_i}.json", "w+") as fp:
            json.dump(predicted_ids, fp, indent=2)
        time.sleep(0.2)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    ## Required parameters  
    parser.add_argument("--start", "-s", default=None, type=int, required=True,
                        help="Starting TPL index")
    parser.add_argument("--end", "-e", default=None, type=int, required=True,
                        help="Ending TPL index" )
    parser.add_argument("--tpl_csv_dir", default=None, type=str, required=True,
                        help="Location of file tpl_index.csv")      
    parser.add_argument("--so_out_dir", default=None, type=str, required=True,
                        help="Directory for storing thread IDs for each TPL" )   
    args = parser.parse_args()
    main(args)