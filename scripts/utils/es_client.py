import os
import requests
import json
import re
import traceback
import csv
import time
import random
import shutil

from config.config import config

from pprint import PrettyPrinter as PP
pp = PP(depth=2)
pprint = pp.pprint
ES_SCROLL_SIZE = 20

class ESClient():
    def __init__(self, index=None, scroll_loop_limit=config.es.scroll_loop_limit, scroll_size=config.es.scroll_size, host=config.es.host, port=config.es.port):
        from elasticsearch import Elasticsearch
        from elasticsearch.helpers import streaming_bulk
        self.es = Elasticsearch([{'host': host, 'port': port}])
        self.scroll_time = '5m'
        self.scroll_size = scroll_size
        self.scroll_ids = []
        self.index_name = index
        self.scroll_loop_limit = scroll_loop_limit

    def get_querying_index(self, custom_index):
        if custom_index is not None:
            return custom_index
        elif self.index_name is not None:
            return self.index_name
        else:
            raise Exception("No index for querying.")
            return None


    def _append_results(self, new_data, exist_result_list):
        exist_result_list = exist_result_list + [each_doc for each_doc in new_data['hits']['hits']]
        return exist_result_list
    
    def _get_matching_list(self, index_name=None, body=None):
        assert body is not None
        result_list = []
        start = time.time()
        query_index = self.get_querying_index(index_name)
        scroll_id = None
        total_matches = 0
        is_err = False
        try:
            data = self.es.search(index=query_index, scroll=self.scroll_time, body=body, size=self.scroll_size, request_timeout=50)
            total_matches = data['hits']['total']
            # pp.pprint(data)
            scroll_id = data['_scroll_id']
            scroll_size = len(data['hits']['hits'])
            loop_count = 0
            while (scroll_size > 0) and (loop_count < self.scroll_loop_limit):
            # while (scroll_size > 0):
                result_list = self._append_results(data, result_list)
                data = self.es.scroll(scroll_id=scroll_id, scroll=self.scroll_time, request_timeout=50)
                # print(data)
                scroll_size = len(data['hits']['hits'])
                loop_count += 1
            end = time.time()
            self._del_scroll_id(scroll_id)
        except Exception as e:
            is_err = True
            end = time.time()
            traceback.print_exc()
            if scroll_id is not None:
                self._del_scroll_id(scroll_id)
            
            return result_list, total_matches, end-start, is_err
        # print("[Database] Query scroll time for : {:.3f}s".format(end-start))
        return result_list, total_matches, end-start, is_err


    def _del_scroll_id(self, scroll_id):
        try:
            self.es.clear_scroll(body={'scroll_id': scroll_id})
            return True
        except Exception as e:
            return False

    def query(self, index_name, search_string, op, field):
        query_body = {
            "query": {
                "bool": {"must": [
                    {"match": {f"{field}": 
                        {
                            "operator": op,
                            "query": search_string
                        }}
                    }
                ]}}
        }
        results, total_matches, req_time, is_err = self._get_matching_list(index_name, body=query_body)
        return results, total_matches, is_err
    
    def query_with_op(self, index_name, search_string, op, field):
        query_body = {
            "query": {
                "bool": {"must": [
                    {"match": {f"{field}": 
                        {
                            "operator": op,
                            "query": search_string
                        }}
                    }
                ]}}
        }
        results, total_matches, req_time = self._get_matching_list(index_name, body=query_body)
        return results, total_matches

    def get(self, doc_id, index_name=None):
        query_index = self.get_querying_index(index_name)
        res = self.es.get(index=query_index, id=doc_id)
        return res['_source']


    def doc_generator(self, index_name=None, get_all_docs=True, body=None):
        """
            Generator to get all documents of an index query: {"query": {"match_all": {}}}
        """
        if not get_all_docs:
            assert body is not None
        else:
            body = {"query": {"match_all": {}}}
        query_index = self.get_querying_index(index_name)
        data = self.es.search(index=query_index, scroll=self.scroll_time, body=body, size=self.scroll_size)
        for doc in data['hits']['hits']:
            yield doc
        
        scroll_id = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])
        loop_count = 0
        while (scroll_size > 0) and (loop_count < self.scroll_loop_limit):
            
            data = self.es.scroll(scroll_id=scroll_id, scroll=self.scroll_time)
            for doc in data['hits']['hits']:
                yield doc
            scroll_size = len(data['hits']['hits'])
            loop_count += 1
        
        self._del_scroll_id(scroll_id)
        