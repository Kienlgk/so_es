
import os
import json
import glob
from pprint import pprint
import time
import traceback
from tqdm.auto import tqdm
from config.config import config
from utils.logger import logger
from utils.utils import load_threads_code_from_json, get_nrof_lines
from utils.html2txt import html2txt, get_code_block
from so_thread import StackOverflowThread
# from so_thread_v2 import StackOverflowDATYSThread
# from so_thread_v3 import StackOverflowSearchThread


def bulk_thread_generator_from_large_json(INDEX_NAME):
    def convert_to_thread_json(thread_id, posts, thread_has_code):
        thread_json = {
            thread_id: posts,
            'thread_has_code': thread_has_code
        }
        return thread_json
    def prepare_document_for_bulk(index_name, thread_to_be_indexed):
        row = {
            "_id": thread_to_be_indexed.thread_id,
            "thread_id": thread_to_be_indexed.thread_id,
            "tags": thread_to_be_indexed.tags,
            "code": thread_to_be_indexed.code,
            "text": thread_to_be_indexed.text,
            "has_code": thread_to_be_indexed.has_code,
            "_index": index_name
        }
        return row
    threads_code = load_threads_code_from_json()
    for thread_id, thread_items in threads_code.items():
        try:
            posts = thread_items['posts']
            thread_has_code = thread_items['thread_has_code']
            thread_json = convert_to_thread_json(thread_id, posts, thread_has_code)
            thread = StackOverflowThread(thread_json)
            yield prepare_document_for_bulk(INDEX_NAME, thread)
        except Exception as e:
            logger.exception(e)
            break

def get_indexing_thread_dict(): # For test and debug
    empty_threads = ['49002529', '10037337', '46609234', '28268682', '10091654', '6912594', '7259123', '18770943', '4381759', '34078660', '18372818', '11959260', '6233745', '20280882', '12884232', '31870170', '7616169', '8764405', '3557144', '2419144', '5716267', '56200101', '9842189', '34646554', '32046502', '2136915', '54264349', '8490901', '8522268', '6457245', '63506943', '10937763', '52090846', '9347354', '8860770']
    with open(config.datys.tags, "r") as tag_fp:
        tags = json.load(tag_fp)
    with open(config.datys.title, "r") as title_fp:
        titles = json.load(title_fp)
    for thread_id in empty_threads:
        path = glob.glob(config.datys.thread_path+"/"+thread_id+".*")[0]
        with open(path, "r") as thread_fp:
            thread_content = thread_fp.read()
        thread = StackOverflowDATYSThread(thread_content, thread_id, titles, tags) 
        print(thread_id)
        pprint(thread.to_dict())
        exit()

# def check_lacking_text(): # For test and debug
#     thread_id = "2363408"
#     with open(config.datys.tags, "r") as tag_fp:
#         tags = json.load(tag_fp)
#     with open(config.datys.title, "r") as title_fp:
#         titles = json.load(title_fp)
#     path = glob.glob(config.datys.thread_path+"/"+thread_id+".*")[0]
#     with open(path, "r") as thread_fp:
#         thread_content = thread_fp.read()
#     print(thread_content)
#     thread = StackOverflowDATYSThread(thread_content, thread_id, titles, tags) 
#     print(thread_id)
#     pprint(thread.to_dict())

def bulk_thread_generator_from_jsonl(INDEX_NAME):
    def convert_to_thread_json(thread_id, posts, thread_has_code):
        thread_json = {
            thread_id: posts,
            'thread_has_code': thread_has_code
        }
        return thread_json

    def prepare_document_for_bulk(index_name, thread_to_be_indexed):
        row = {
            "_id": thread_to_be_indexed.thread_id,
            "thread_id": thread_to_be_indexed.thread_id,
            "tags": thread_to_be_indexed.tags,
            "code": thread_to_be_indexed.code,
            "text": thread_to_be_indexed.text,
            "text_lowercase_query": thread_to_be_indexed.text,
            "body": thread_to_be_indexed.body,
            "search": thread_to_be_indexed.search,
            "title": thread_to_be_indexed.title,
            "has_code": thread_to_be_indexed.has_code,
            "_index": index_name
        }
        return row
    # threads_code = load_top5libs_threads_from_json()
    java_thread_jsonl_path = "/app/stackoverflow_dump/java_posts.jsonl"
    err_threads_jsonl_path = f"/app/data/debug/{config.es.index_name}/err_java_posts.jsonl"
    os.makedirs(os.path.dirname(err_threads_jsonl_path), exist_ok=True)
    number_of_lines = get_nrof_lines(java_thread_jsonl_path)
    err_threads_jsonl_writer =  open(err_threads_jsonl_path, "w+")
    with open(java_thread_jsonl_path, "r") as fp:
        jsonl = fp.readline()
        progress_bar = tqdm(total=100)
        line_count = 0
        while jsonl:
            thread_info_json = json.loads(jsonl)
            for k, v in thread_info_json.items():
                thread_id = k
                thread_items = v
            thread_json ={}
            thread_has_code = False
            for post in thread_items:
                content = post["Body"]
                code_blocks = get_code_block(content)
                post["code"] = code_blocks
                content = str(content.encode('utf-8'))
                post["content"] = html2txt(content)
                if len(post["code"]) > 0:
                    thread_has_code = True
            thread_json[thread_id] = thread_items
            thread_json['thread_has_code'] = thread_has_code
            try:
                thread = StackOverflowThread(thread_json)
                yield prepare_document_for_bulk(INDEX_NAME, thread)
            except Exception as e:
                # traceback.print_exc()
                logger.exception(e)
                print(jsonl, file=err_threads_jsonl_writer)
                # break
            jsonl = fp.readline()
            line_count += 1
            if line_count % int(number_of_lines/100) == 0:
                time.sleep(0.2)
                progress_bar.update(1)
    err_threads_jsonl_writer.close()
            

