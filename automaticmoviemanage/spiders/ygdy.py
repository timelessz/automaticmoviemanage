# -*- coding: utf-8 -*-
import re
import urllib
from builtins import print

import pymysql
import scrapy
from parsel import Selector
from scrapy import Selector
from scrapy.utils.project import get_project_settings

from automaticmoviemanage.items import AutomaticmoviemanageItem


class YgdySpider(scrapy.Spider):
    name = 'ygdy'
    allowed_domains = ['ygdy8.net']
    start_urls = ['http://ygdy8.net/']
    # 每一个 spider 设置不一样的 pipelines
    custom_settings = {
        'ITEM_PIPELINES': {
            'automaticmoviemanage.pipelines.MoviescrapyPipeline': 100,
        },
        'DOWNLOAD_DELAY': 10
    }

    def __init__(self, *args, **kwargs):
        super(YgdySpider, self).__init__(*args, **kwargs)
        setting = get_project_settings()
        dbargs = dict(
            host=setting.get('MYSQL_HOST'),
            port=3306,
            user=setting.get('MYSQL_USER'),
            password=setting.get('MYSQL_PASSWD'),
            db=setting.get('MYSQL_DBNAME'),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
        )
        self.conn = pymysql.connect(**dbargs)
        self.base_url = 'http://www.ygdy8.net'

    def get_movie(self, title):
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM automovie.movie_has_scrapy_info where name = '" + title + "' and comefrom='ygdy'")
        return cur.fetchone()

    def start_requests(self):
        '''
        首先获取
        :return:
        '''
        spider_urls = {
            'oumei': {
                'url': 'http://www.ygdy8.net/html/gndy/oumei/list_7_%s.html',
                'start_url': 'http://www.ygdy8.net/html/gndy/oumei/index.html',
                'id': 1,
                'name': '欧美电影'
            },
            'dalu': {
                'url': 'http://www.ygdy8.net/html/gndy/china/list_4_%s.html',
                'start_url': 'http://www.ygdy8.net/html/gndy/china/index.html',
                'id': 4,
                'name': '大陆电影'
            },
            'rihan': {
                'url': 'http://www.ygdy8.net/html/gndy/rihan/list_6_%s.html',
                'start_url': 'http://www.ygdy8.net/html/gndy/rihan/index.html',
                'id': 2,
                'name': '日韩电影'
            },
        }

        '''
        首先获取第一页 然后获取总的数量 用来判断总共多少页面
        '''
        for i in spider_urls:
            start_url = spider_urls[i]['start_url']
            id = spider_urls[i]['id']
            name = spider_urls[i]['name']
            url = spider_urls[i]['url']
            request = scrapy.Request(url=start_url, callback=self.parse)
            request.meta['web'] = False
            request.meta['category'] = {'id': id, 'name': name, 'url': url}
            yield request

    def parse(self, response):
        sel = Selector(response)
        #  首先知道 总共有多少
        #  从谷歌中提取的xpath链接 需要排除出 tbody
        pageinfo = sel.xpath('string(//*[@id="header"]/div/div[3]/div[3]/div[2]/div[2]/div[2]/div)').extract_first()
        startpos = pageinfo.find('共')
        stoppos = pageinfo.find('页')
        page_num = pageinfo[startpos + 1:stoppos].strip()
        # 总页数
        print('总页数' + page_num)
        # 页面num
        # 首先解析出第一页的页面信息
        category = response.meta['category']
        sel = Selector(response)
        tables = sel.xpath('//*[@id="header"]/div/div[3]/div[3]/div[2]/div[2]/div[2]/ul/td/table')
        for table in tables:
            item = AutomaticmoviemanageItem()
            item['comefrom'] = 'ygdy'
            title = table.xpath('tr[2]/td[2]/b/a[2]')
            href = title.xpath('@href').extract_first()
            text = title.xpath('text()').extract_first()
            item['title'] = text.strip()
            item['name'] = self.parse_name(item['title'])
            if self.get_movie(item['name']) is None:
                item['region_id'] = category['id']
                item['region_name'] = category['name']
                parent_url = category['url']
                # 获取详细内容页面 的url 使用相对路径跟绝对路径
                if href:
                    # 把相对路径转换为绝对路径
                    href = urllib.parse.urljoin(parent_url, href)
                    item['href'] = href
                    request = scrapy.Request(url=href, callback=self.parse_content)
                    request.meta['web'] = True
                    request.meta['item'] = item
                    yield request
            else:
                print('**********************************')
                print(item['title'] + '电影已经爬取，放弃爬取数据')
                print('**********************************')
        category = response.meta['category']
        start_url = category['url']
        for i in range(2, 20):
            url = start_url % i
            request = scrapy.Request(url=url, callback=self.parse_list)
            request.meta['web'] = False
            request.meta['category'] = category
            yield request

    def parse_list(self, response):
        '''
        解析列表
        :param response:
        :return:
        '''
        category = response.meta['category']
        sel = Selector(response)
        tables = sel.xpath('//*[@id="header"]/div/div[3]/div[3]/div[2]/div[2]/div[2]/ul/td/table')
        for table in tables:
            item = AutomaticmoviemanageItem()
            item['comefrom'] = 'ygdy'
            title = table.xpath('tr[2]/td[2]/b/a[2]')
            href = title.xpath('@href').extract_first()
            text = title.xpath('text()').extract_first()
            item['title'] = text.strip()
            #  截取书名号
            item['name'] = self.parse_name(item['title'])
            if self.get_movie(item['name']) is None:
                # 首先需要查询下是不是已经有重复 重复的跳出
                item['region_id'] = category['id']
                item['region_name'] = category['name']
                parent_url = category['url']
                # 获取详细内容页面 的url 使用相对路径跟绝对路径
                if href:
                    href = urllib.parse.urljoin(parent_url, href)
                    item['href'] = href
                    request = scrapy.Request(url=href, callback=self.parse_content)
                    request.meta['item'] = item
                    yield request
            else:
                print('**********************************')
                print(item['title'] + '电影已经存在，放弃爬取数据')
                print('**********************************')

    def parse_name(self, title):
        '''
        title 相关
        :param title: 标题  用于截取数据
        :return:
        '''
        if '《' in title and '》' in title:
            startpos = title.find('《')
            stoppos = title.find('》')
            name = title[startpos + 1:stoppos].strip()
            return name
        return title

    def parse_content(self, response):
        '''
        解析页面的内容
        '''
        item = response.meta['item']
        sel = Selector(response)
        content_selector = sel.xpath('//*[@id="Zoom"]/span')
        download_link_a = Selector(text=content_selector.extract_first()).xpath('//a').extract()
        a_download_info = []
        for a in download_link_a:
            print('????????????????????????????????????')
            a_selector = Selector(text=a)
            text = a_selector.xpath('//a/text()').extract_first()
            pattern = r'thunder:.*(?=")'
            thunder_list = re.findall(pattern, a)
            href = ''
            if thunder_list and text:
                type_id = 3
                type_name = '迅雷下载'
                href = thunder_list[0]
            else:
                href = a_selector.xpath('//a/@href').extract_first()
                text = a_selector.xpath('//a/text()').extract_first()
                type_id = 0
                type_name = ''
                if 'magnet:' in href:
                    type_id = 1
                    type_name = '磁力下载'
                elif 'ed2k://' in href:
                    type_id = 2
                    type_name = '电驴下载'
                elif 'thunder://' in href:
                    type_id = 3
                    type_name = '迅雷下载'
                elif 'pan' in href:
                    type_id = 4
                    type_name = '百度云'
                # 百度云链接的话需要密码  这种情况下需要自己进行操作 获取密码
            if type_id:
                a_download_info.append(
                    {'href': href, 'pwd': '', 'text': text, 'type_id': type_id, 'type_name': type_name})
            # print(a_download_info)
        item['download_a'] = a_download_info
        # # 提取处内容来
        if content_selector:
            content = content_selector.extract_first()
            # 提取处图片来
            replace_pattern = r'<[img|IMG].*?>'  # img标签的正则式
            img_url_pattern = r'.+?src="(\S+)"'  # img_url的正则式
            need_replace_list = re.findall(replace_pattern, content)  # 找到所有的img标签
            img_list = []
            for tag in need_replace_list:
                img_list.append(re.findall(img_url_pattern, tag)[0])  # 找到所有的img_url
            item['imglist'] = img_list
            # 过滤掉 img
            return self.sub_content(content, item, a_download_info)

    def sub_content(self, content, item, a_download_info):
        '''
        截取相关电影的内容
        :return:
        '''
        all_field = [
            {'text': '片名', 'field': 'name'},
            {'text': '译名', 'field': 'alias_name'},
            {'text': '又名', 'field': 'alias_name'},
            {'text': '年代', 'field': 'ages'},
            {'text': '产地', 'field': 'country'},
            {'text': '类别', 'field': 'type'},
            {'text': '类型', 'field': 'type'},
            {'text': '语言', 'field': 'language'},
            {'text': '字幕', 'field': 'subtitle'},
            {'text': '上映日期', 'field': 'releasedate'},
            {'text': 'IMDb评分', 'field': 'imdbscore'},
            {'text': 'IMDB', 'field': 'imdburl'},
            {'text': 'IMDb链接', 'field': 'imdburl'},
            {'text': '豆瓣评分', 'field': 'doubanscore'},
            {'text': '豆瓣链接', 'field': 'doubanurl'},
            {'text': '文件格式', 'field': 'filetype'},
            {'text': '视频尺寸', 'field': 'screensize'},
            {'text': '文件大小', 'field': 'filesize'},
            {'text': '片长', 'field': 'length'},
            {'text': '时长', 'field': 'length'},
            {'text': '主演', 'field': 'starring'},
            {'text': '导演', 'field': 'director'},
            {'text': '简介', 'field': 'summary'}
        ]
        content = content[0:content.find('【下载地址】')]
        dr = re.compile(r'<[^>]+>', re.S)
        # 去除html 标签
        content = dr.sub('', content)
        # 清除空格
        content = content.replace(u'\u3000', u'')
        # 清除 &nbsp;
        content = content.replace(u'\xa0', u'')
        pre_content = content
        # 过滤掉script
        re_script = re.compile('<\s*script[^>]*>[^<]*<\s*/\s*script\s*>', re.I)  # Script
        pre_content = re_script.sub('', pre_content)
        # 替换掉原来有的链接
        re_a = re.compile('<\s*a[^>]*>[^<]*<\s*/\s*a\s*>', re.I)
        pre_content = re_a.sub('', pre_content)
        item['content'] = pre_content
        # 之前的url 这块是需要保存到之前的库中的数据
        content_list = content.split('◎')
        if len(content_list) < 2:
            '''
            解析数据异常
            '''
            for content_field in all_field:
                if not content_field['field'] in item.keys():
                    item[content_field['field']] = ''
            return item
        for content_field in content_list:
            # 清除\r\n
            content_field = content_field.strip(' \t\n\r')
            if not content_field:
                continue
            for field in all_field:
                if '简介' == field['text']:
                    if '简' in content_field and '介' in content_field:
                        content_field = self.replace_str(content_field, a_download_info)
                        fieldtext = content_field.replace(field['text'], '')
                        item[field['field']] = fieldtext.strip(' \t\n\r')
                elif field['text'] in content_field and content_field.find(field['text']) == 0:
                    fieldtext = content_field.replace(field['text'], '')
                    item[field['field']] = fieldtext.strip(' \t\n\r')
        # # 如果是空的字段需要置为空的字段
        for content_field in all_field:
            if not content_field['field'] in item.keys():
                item[content_field['field']] = ''
        if item['region_id'] != 4:
            # 别名跟名字互换
            name = item['name']
            alias_name = item['alias_name']
            item['alias_name'] = name
            item['name'] = alias_name
            if '/' in item['name']:
                names = item['name'].split('/')
                item['name'] = names[0]
                for pername in names:
                    item['alias_name'] = item['alias_name'] + '/' + pername
        # print(item)
        return item

    def replace_str(self, content_field, a_download_info):
        print('简介截取^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
        print(content_field)
        print(a_download_info)
        print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
        content_field = content_field.replace('【下载地址】', '')
        for pre_a_info in a_download_info:
            if pre_a_info['text'] is not None and pre_a_info['text'] in content_field:
                content_field = content_field.replace(pre_a_info['text'], '')
        return content_field.strip(' ')
