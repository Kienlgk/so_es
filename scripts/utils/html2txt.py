"""
    This file is modified from https://github.com/baolingfeng/APIExing/blob/master/html2txt.py to be able to run with python 3.x
    Usage:
    from utils.html2txt import html2txt
    body_text = str(post['Body'].encode('utf-8'))
    text = html2txt(body_text)
"""

import re
from html.parser import HTMLParser
from html.entities import entitydefs

mycompile = lambda pat:  re.compile(pat,  re.UNICODE)
WS_RE = mycompile(r'  +')

Url_new = r"""((?:[a-z][\w-]+:(?:/{1,3}|[a-z0-9%])|www\d{0,3}[.]|[a-z0-9.\-]+[.‌​][a-z]{2,4}/)(?:[^\s()<>]+|(([^\s()<>]+|(([^\s()<>]+)))*))+(?:(([^\s()<>]+|(‌​([^\s()<>]+)))*)|[^\s`!()[]{};:'".,<>?«»“”‘’]))"""
AtMention = r'@[a-zA-Z0-9_]+'
html_tags = ['!--', '!DOCTYPE', 'a', 'abbr', 'acronym', 'address', 'applet', 'area', 'article', 'aside', 'audio', 'b', 'base', 'basefont', 'bdi', 'bdo', 'big', 'blockquote', 'body', 'br', 'button', 'canvas', 'caption', 'center', 'cite', 'code', 'col', 'colgroup', 'data', 'datalist', 'dd', 'del', 'details', 'dfn', 'dialog', 'dir', 'div', 'dl', 'dt', 'em', 'embed', 'fieldset', 'figcaption', 'figure', 'font', 'footer', 'form', 'frame', 'frameset', 'h1> - <h6', 'head', 'header', 'hr', 'html', 'i', 'iframe', 'img', 'input', 'ins', 'kbd', 'label', 'legend', 'li', 'link', 'main', 'map', 'mark', 'meta', 'meter', 'nav', 'noframes', 'noscript', 'object', 'ol', 'optgroup', 'option', 'output', 'p', 'param', 'picture', 'pre', 'progress', 'q', 'rp', 'rt', 'ruby', 's', 'samp', 'script', 'section', 'select', 'small', 'source', 'span', 'strike', 'strong', 'style', 'sub', 'summary', 'sup', 'svg', 'table', 'tbody', 'td', 'template', 'textarea', 'tfoot', 'th', 'thead', 'time', 'title', 'tr', 'track', 'tt', 'u', 'ul', 'var', 'video', 'wbr']
def squeeze_whitespace(s):
    new_string = WS_RE.sub(" ",s)
    return new_string.strip()

class MLStripper(HTMLParser):
    def __init__(self, strip_code=True):
        self.reset()
        self.fed = []
        self.entityref = re.compile('&[a-zA-Z][-.a-zA-Z0-9]*[^a-zA-Z0-9]')
        self.convert_charrefs = True
        self.strip_code = strip_code

    def handle_data(self, d):
        self.fed.append(d)

    def handle_starttag(self, tag, attrs):
        if not self.strip_code:
            if tag == "code":
                self.fed.append('<code>')
            elif tag== "pre":
                self.fed.append('<pre>')
            elif tag == "a":
                append_str = "<a"
                attrs = dict(attrs)
                for k,v in attrs.items():
                    append_str += f' {k}="{v}"'
                append_str += ">"
                self.fed.append(append_str)
        else:
            self.fed.append(' ')

    def handle_endtag(self, tag):
        if not self.strip_code:
            if tag == "code":
                self.fed.append('</code>')
            elif tag== "pre":
                self.fed.append('</pre>')
            elif tag == "a":
                self.fed.append("</a>")
        else:
            self.fed.append(' ')

    def handle_entityref(self, name):
        if entitydefs.get(name) is None:
            m = self.entityref.match(self.rawdata.splitlines()[self.lineno-1][self.offset:])
            entity = m.group()
            # semicolon is consumed, other chars are not.
            if entity is not None:
            	#print "entity is none"
                if entity[-1] != ';':
                    entity = entity[:-1]
                self.fed.append(entity)
            else:
                self.fed.append('')
        else:
            self.fed.append(' ')

    def get_data(self):
        self.close()
        return ''.join(self.fed)
    
def strip_tags(html, strip_code=True):
    s = MLStripper(strip_code=strip_code)
    if strip_code:
        html = re.sub(r'<code>', '`', html)
        html = re.sub(r'</code>', '`', html)
        html = re.sub(r'&#xA;&#xA;<pre.*?>.*?</pre>', '#CODE', html) # add .*? to match tag class
        # html = re.sub(r'\n\n<pre.*?>.*?</pre>', '#CODE', html) # add .*? to match tag class

        html = re.sub(r'<pre.*?>.*?</pre>', '#CODE', html) # add this line to handle code snippet only posts. 
    #html = re.sub(r'(`(?=\S)|(?<=\S)`)', '', html)
    html = re.sub(r'(&#xA;)+','\n', html)
    s.feed(html)
    return s.get_data()

def unicode_encoder(my_string):
    """
        replace unicode into Str to avoid:
        NameError: name 'unicode' is not defined
    """
    for i in my_string:
        try:
#             yield str.encode(i, 'utf-8')
            yield i.encode('utf-8')
        except UnicodeDecodeError:
            print("Encode utf-8 failed")
            yield ' ' # or another whitespaces

def html2txt(content, strip_code=True):
#     try:
#     pro = ''.join( unicode_encoder( strip_tags(content) ) )
    pro = ''.join(strip_tags(content, strip_code=strip_code))
#     
    if strip_code:
        pro = re.sub(r'^ +', '', pro)
        pro = re.sub(r'\n +', '\n', pro)
        pro = re.sub(r'[\n]+', '\n',pro)
        pro = squeeze_whitespace(pro)
    else:
        # pro = re.sub(r'\n <code>', '\n<code>',pro)
        # pro = re.sub(r'\n\n<code>', '\n\n<code>\n',pro)
        pro = re.sub(r'\n <pre><code>', '\n<pre><code>',pro)
        pro = re.sub(r'\n<pre><code>', '\n<pre><code>\n',pro)
        pro = re.sub(r'</code></pre>', '\n</code></pre>',pro)
        pro = re.sub(r'\n\n ', '\n\n',pro)
    #pro = re.sub(Url_new, '#URL', pro, flags=re.DOTALL)
#     pro = re.sub(AtMention, '@USER', pro)
    if len(pro) > 3 and pro[0] == 'b':
        if pro[1]=="\'" and pro[-1]=="\'": 
            return pro[2:-1]
        if pro[1]=="\"" and pro[-1]=="\"": 
            return pro[2:-1]
    return pro

def get_code_block(content):
    from xml.sax import saxutils as su
    content = "&#xA;".join(content.split("\n"))
    pattern = re.compile("<pre.*?><code>(.*?)</code></pre>")
    matches = pattern.findall(content)
    code_blocks = []
    for match in matches:
        match = "\n".join(match.split("&#xA;"))
        match = su.unescape(match)
        code_blocks.append(match)
    return code_blocks
