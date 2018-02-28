# -*- coding: UTF-8 -*-

import urllib
from math import ceil

import pymysql
import re
import scrapy
import time
from scrapy.selector import Selector
# 读取配置文件相关
from scrapy.utils.project import get_project_settings

from automaticmoviemanage.items import AutomaticmoviemanageItem

'''
思路：
该网站爬取迅雷铺中的内容然后解析到数据库中：
应该保存到数据库

1、原文链接。
2、原文中图片的链接 。
3、原文全部内容 包含图片的链接。

'''


class XunleipuSpider(scrapy.Spider):
    name = "xunleipu"

    # 每一个 spider 设置不一样的 pipelines
    custom_settings = {
        'ITEM_PIPELINES': {
            'automaticmoviemanage.pipelines.MoviescrapyPipeline': 100,
        },
        'DOWNLOAD_DELAY': 10
    }

    def __init__(self, *args, **kwargs):
        super(XunleipuSpider, self).__init__(*args, **kwargs)
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
        self.base_url = 'http://www.xlp2.com'

    def start_requests(self):
        '''
        首先获取
        :return:
        '''
        spider_urls = {
            'oumei': {
                'url': 'http://www.xlp2.com/category/1_%s.htm',
                'start_url': 'http://www.xlp2.com/category/1_1.htm',
                'id': 1,
                'name': '欧美电影'
            },
            'dalu': {
                'url': 'http://www.xlp2.com/category/4_%s.htm',
                'start_url': 'http://www.xlp2.com/category/4_1.htm',
                'id': 4,
                'name': '大陆电影'
            },
            'rihan': {
                'url': 'http://www.xlp2.com/category/2_%s.htm',
                'start_url': 'http://www.xlp2.com/category/2_1.htm',
                'id': 2,
                'name': '日韩电影'
            },
            'gangtai': {
                'url': 'http://www.xlp2.com/category/3_%s.htm',
                'start_url': 'http://www.xlp2.com/category/3_1.htm',
                'id': 3,
                'name': '港台电影'
            },
            'classics': {
                'url': 'http://www.xlp2.com/category/11_%s.htm',
                'start_url': 'http://www.xlp2.com/category/11_1.htm',
                'id': 5,
                'name': '经典电影'
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
            request.meta['category'] = {'id': id, 'name': name, 'url': url}
            yield request

    def analyse_doanload_linkinfo(self, href):
        type_id = 10
        type_name = '其他'
        if 'magnet:' in href:
            type_id = 1
            type_name = '磁力下载'
        elif 'ed2k://' in href:
            type_id = 2
            type_name = '电驴下载'
        elif 'ftp://' in href:
            type_id = 3
            type_name = '迅雷下载'
        elif 'pan' in href:
            type_id = 4
            type_name = '百度云'
        elif 'torrent' in href:
            type_id = 5
            type_name = '种子'
        return {'type_id': type_id, 'type_name': type_name}

    def parse(self, response):
        sel = Selector(response)
        #  这个地方有问题  http://www.xlp2.com/category/1_1.htm   应该获取的是 根目录
        #  首先知道 总共有多少
        #  从谷歌中提取的xpath链接 需要排除出 tbody
        # alllist_count = int(
        #     sel.xpath('//*[@id="classpage2"]/div[5]/table/tr[21]/td/table/tr/td/font[3]/text()').extract_first())
        # page_num = ceil(alllist_count / 20)
        # # 总的页面数量
        # print(alllist_count)
        # 页面num
        # print(page_num)
        # 首先解析出第一页的页面信息
        category = response.meta['category']
        sel = Selector(response)
        trs = sel.xpath('//*[@id="classpage2"]/div[5]/table/tr')
        i = 1
        for tr in trs:
            item = AutomaticmoviemanageItem()
            item['comefrom'] = 'xunleipu'
            title = tr.xpath('string(td[1]/a)').extract_first()
            item['title'] = title.replace(str(i), '', 1).strip()
            item['name'] = self.parse_name(item['title'])
            i = i + 1
            if self.get_movie(item['name']) is None:
                item['addtime'] = tr.xpath('string(td[2])').extract_first()
                item['region_id'] = category['id']
                item['region_name'] = category['name']
                parent_url = category['url']
                # 获取详细内容页面 的url 使用相对路径跟绝对路径
                relative_href = tr.xpath('td[1]/a/@href').extract_first()
                if relative_href:
                    # 把相对路径转换为绝对路径
                    href = urllib.parse.urljoin(parent_url, relative_href)
                    # href = self.base_url + relative_href
                    item['href'] = href
                    request = scrapy.Request(url=href, callback=self.parse_content)
                    request.meta['item'] = item
                    print(href)
                    yield request
            else:
                time.sleep(1)
                print('**********************************')
                print(item['title'] + '电影已经存在，放弃爬取数据')
                print('**********************************')
        category = response.meta['category']
        start_url = category['url']
        # for i in range(2,page_num+1)
        for i in range(2, 20):
            url = start_url % i
            print(url)
            request = scrapy.Request(url=url, callback=self.parse_list)
            request.meta['category'] = category
            yield request

    def get_movie(self, name):
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM movie_has_scrapy_info where name = '" + name + "' and comefrom='xunleipu'")
        return cur.fetchone()

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

    def parse_list(self, response):
        '''
        解析列表
        :param response:
        :return:
        '''
        category = response.meta['category']
        sel = Selector(response)
        trs = sel.xpath('//*[@id="classpage2"]/div[5]/table/tr')
        i = 1
        for tr in trs:
            item = AutomaticmoviemanageItem()
            item['comefrom'] = 'xunleipu'
            title = tr.xpath('string(td[1]/a)').extract_first()
            item['title'] = title.replace(str(i), '', 1).strip()
            # 首先需要查询下是不是已经有重复 重复的跳出
            item['name'] = self.parse_name(item['title'])
            i = i + 1
            if self.get_movie(item['name']) is None:
                item['addtime'] = tr.xpath('string(td[2])').extract_first()
                item['region_id'] = category['id']
                item['region_name'] = category['name']
                parent_url = category['url']
                # 获取详细内容页面 的url 使用相对路径跟绝对路径
                relative_href = tr.xpath('td[1]/a/@href').extract_first()
                if relative_href:
                    href = urllib.parse.urljoin(parent_url, relative_href)
                    # href = self.base_url + relative_href
                    item['href'] = href
                    request = scrapy.Request(url=href, callback=self.parse_content)
                    request.meta['item'] = item
                    print(href)
                    yield request
            else:
                time.sleep(1)
                print('**********************************')
                print(item['title'] + '电影已经存在，放弃爬取数据')
                print('**********************************')

    def parse_content(self, response):
        '''
        解析页面的内容
        '''
        item = response.meta['item']
        sel = Selector(response)
        # 这个地方要修改 为
        content_selector = sel.xpath('//*[@id="classpage2"]/div')
        content_sel = None
        for content in content_selector:
            content_info = content.extract()
            if ('片' in content_info and '名' in content_info) or ('简' in content_info and '介' in content_info):
                content_sel = content
                break
        html_text = content_sel.extract()
        download_link_a = Selector(text=html_text.lower()).xpath('//a')
        a_download_info = []
        for a in download_link_a:
            href = a.xpath('@href').extract_first()
            text = a.xpath('text()').extract_first()
            download_info = self.analyse_doanload_linkinfo(href)
            # 百度云链接的话需要密码  这种情况下需要自己进行操作 获取密码
            a_download_info.append({'href': href, 'pwd': '', 'text': text, 'type_id': download_info['type_id'],
                                    'type_name': download_info['type_name']})
        if not a_download_info:
            print(content_sel.extract())
            print(
                '/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////没有获取到电影下载链接///////////////////////////////////')
        item['download_a'] = a_download_info
        # 提取处内容来
        if content_sel:
            content = content_sel.extract()
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
        dr = re.compile(r'<[^>]+>', re.S)
        pre_content = content
        # 去除html 标签
        content = dr.sub('', content)
        # 清除空格
        content = content.replace(u'\u3000', u'')
        # item['content'] = self.replace_str(content, a_download_info)
        item['content'] = pre_content
        content_list = content.split('◎')
        if len(content_list) < 2:
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
        # 如果是空的字段需要置为空的字段
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
        return item

    def replace_str(self, content_field, a_download_info):
        content_field = content_field.replace('下载地址', '')
        for pre_a_info in a_download_info:
            content_field = content_field.replace(pre_a_info['text'], '')
        for ch in ['迅雷：', '电驴：', '磁力：', '网盘链接：', '密码：', '迅雷', '电驴', '磁力', '网盘链接', '密码', '\r\n']:
            if ch in content_field:
                content_field = content_field.replace(ch, "")
        return content_field
