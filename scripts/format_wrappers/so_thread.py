
import os
import time
import re
import traceback
import csv
import json
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError as XmlParseError
from pprint import PrettyPrinter as PP
from pprint import pformat
pp = PP(depth=2)

from utils.logger import logger
# from es_client import ESClient
from utils.stackoverflow_processing import convert_tags_string_to_list, search_pattern
from utils.html2txt import html2txt



class StackOverflowThread:
    def __init__(self, thread_json):
        self.thread_json = thread_json
        self.has_code = self.thread_json["thread_has_code"]
        self.thread_id = self.get_thread_id()
        self.tags = self.get_thread_tags()
        self.title = self.get_thread_title()
        self.text = self.get_thread_text()
        self.code = self.get_thread_code()
        self.body = self.get_thread_body()
        self.search = self.get_searching_content()

    def get_thread_id(self):
        thread_keys = list(self.thread_json.keys())
        thread_keys.remove("thread_has_code")
        thread_id = thread_keys[0]
        return thread_id

    def get_thread_tags(self):
        for post in self.thread_json[self.thread_id]:
            if "Tags" in post:
                tags = convert_tags_string_to_list(post['Tags'])
                return tags
        return []
    def get_thread_body(self):
        list_posts = self.thread_json[self.thread_id]
        body = self.get_thread_title() + "\n"
        body += "\nSOPostSep\n".join([post['Body'] for post in list_posts])
        return body

    def get_thread_text(self):
        list_posts = self.thread_json[self.thread_id]
        for post in list_posts:
            if post["PostTypeId"] == "1":
                text += post["Title"] + "\n"
                try:
                    text += " ".join(self.tags) + "\n"
                except Exception as e:
                    text += " ".join(self.get_thread_tags()) + "\n"
            text += post["content"] + "\nSOPostSep\n"
        return text

    def is_err_log(self, code_blk):
        matches = search_pattern(content=code_blk, regex="at\s*.*\(.*\.java:[0-9]*\)|at\s*.*\(.*\.scala:[0-9]*\)")
        if len(matches) > 3:
            return True
        else:
            return False
    
    def is_xml(self, code_blk):
        try:
            tree = ET.fromstring(code_blk)
            return True
        except XmlParseError as parse_error:
            return False


    def get_thread_title(self):
        for post in self.thread_json[self.thread_id]:
            if "Title" in post:
                return post["Title"]
        return ""

    def get_thread_code(self):
        code = ""
        list_posts = self.thread_json[self.thread_id]
        for post in list_posts:
            # content = "&#xA;".join(.split("\n"))
            # pattern = re.compile("`(.*?)`")
            # matches = pattern.findall()
            matches = search_pattern(content=post['content'], regex="`(.*?)`")
            for match in matches:
                # print("match ", match)
                if match == "":
                    continue
                # try:
                # code += match[0] + " "
                code += match + " " # fix bug put character to index

                # except Exception as e:
                #     # print("type ", type(match))
                #     logger.info(pformat(match))
                #     logger.exception(e)
                #     raise e
            for code_blk in post['code']:
                if self.is_err_log(code_blk):
                    continue
                code += "\n" + code_blk + "\nCodeBlockSep\n"
            code += "\nSOPostSep\n"
        return code

    def get_searching_content(self):
        def remove_so_newline(content):
            content = re.sub(r'(&#xA;)+','\n', content)
            content = re.sub(r'(&lt;)+','<', content)
            content = re.sub(r'(&gt;)+','>', content)
            return content

        content = ""
        list_posts = self.thread_json[self.thread_id]
        content = "\nSOPostSep\n".join([post['Body'] for post in list_posts])
        content = remove_so_newline(content)
        
        def sub_func(matchobj):
            code = matchobj.group(1)
            if self.is_err_log(code):
                return ""
            elif self.is_xml(code):
                return ""
            else:
                return f"<code>{code}</code>"
        
        # searching_content = re.sub("<code>(.*?)</code>", sub_func, content, flags=re.DOTALL)
        searching_content = content
        searching_content = html2txt(searching_content, strip_code=False) # This raises NotImplementedError: subclasses of ParserBase must override error()
        return searching_content

    def __repr__(self):
        return f"thread_id: {self.thread_id}\nTags: {';'.join(self.tags)}"
    
    def to_dict(self):
        return {
            'thread_id': self.thread_id,
            'code': self.code,
            'text': self.text,
            'tags': ';'.join(self.tags),
            'has_code': self.has_code
        }


    

