# -*- coding: UTF-8 -*-
import pymysql
import re
import scrapy
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


class Hao6vSpider(scrapy.Spider):
    name = "hao6v"
    # 没有过来掉相关的js
    # 代码之类

    # 每一个 spider 设置不一样的 pipelines
    custom_settings = {
        'ITEM_PIPELINES': {
            'automaticmoviemanage.pipelines.MoviescrapyPipeline': 100,
        },
        'DOWNLOAD_DELAY': 10
    }

    def __init__(self, *args, **kwargs):
        super(Hao6vSpider, self).__init__(*args, **kwargs)
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
        self.base_url = 'http://www.hao6v.com/'

    def start_requests(self):
        '''
        首先获取
        :return:
        '''
        spider_urls = {
            'dianying': {
                'url': 'http://www.hao6v.com/dy/index_%s.html',
                'start_url': 'http://www.hao6v.com/dy/index.html',
                'type': 'dy'
            }
        }

        '''
        首先获取第一页 然后获取总的数量 用来判断总共多少页面
        '''
        for i in spider_urls:
            start_url = spider_urls[i]['start_url']
            url = spider_urls[i]['url']
            request = scrapy.Request(url=start_url, callback=self.parse)
            request.meta['url'] = url
            yield request

    def parse(self, response):
        sel = Selector(response)
        #  首先知道 总共有多少
        #  从谷歌中提取的xpath链接 需要排除出 tbody
        pageinfo = sel.xpath(
            'string(//*[@id="main"]/*[contains(@class,"col4")]/*[contains(@class,"box")]/*[contains(@class,"listpage")])').extract_first()
        startpos = pageinfo.find('/')
        stoppos = pageinfo.find('每页')
        page_num = pageinfo[startpos + 1:stoppos].strip()
        # 总页数
        print('总页数' + page_num)
        # 页面num
        # 首先解析出第一页的页面信息
        start_url = response.meta['url']
        sel = Selector(response)
        lis = sel.xpath(
            '//*[@id="main"]/*[contains(@class,"col4")]/*[contains(@class,"box")]/*[contains(@class,"list")]/li')
        for li in lis:
            item = AutomaticmoviemanageItem()
            item['comefrom'] = 'hao6v'
            title = li.xpath('a')
            href = title.xpath('@href').extract_first()
            text = title.xpath('text()').extract_first()
            if text is None:
                text = title.xpath('font/text()').extract_first()
            item['title'] = text.strip()
            item['name'] = self.parse_name(item['title'])
            if self.get_movie(item['name']) is None:
                # 获取详细内容页面 的url 使用相对路径跟绝对路径
                item['region_id'] = 0
                item['region_name'] = ''
                if href:
                    # 把相对路径转换为绝对路径
                    item['href'] = href
                    request = scrapy.Request(url=href, callback=self.parse_content)
                    request.meta['item'] = item
                    yield request
            else:
                print('**********************************')
                print(item['title'] + '电影已经存在，放弃爬取数据')
                print('**********************************')
        # for i in range(2, int(page_num) + 1):
        for i in range(2, 20):
            url = start_url % i
            request = scrapy.Request(url=url, callback=self.parse_list)
            yield request

    def get_movie(self, name):
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM automovie.movie_has_scrapy_info where name ='" + name + "' and comefrom='hao6v'")
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
        sel = Selector(response)
        lis = sel.xpath(
            '//*[@id="main"]/*[contains(@class,"col4")]/*[contains(@class,"box")]/*[contains(@class,"list")]/li')
        for li in lis:
            item = AutomaticmoviemanageItem()
            item['comefrom'] = 'hao6v'
            title = li.xpath('a')
            href = title.xpath('@href').extract_first()
            text = title.xpath('text()').extract_first()
            if text is None:
                text = title.xpath('font/text()').extract_first()
            item['title'] = text.strip()
            item['name'] = self.parse_name(item['title'])
            if self.get_movie(item['name']) is None:
                item['region_id'] = 0
                item['region_name'] = ''
                # 获取详细内容页面 的url 使用相对路径跟绝对路径
                if href:
                    # 把相对路径转换为绝对路径
                    item['href'] = href
                    request = scrapy.Request(url=href, callback=self.parse_content)
                    request.meta['item'] = item
                    yield request
            else:
                print('**********************************')
                print(item['title'] + '电影已经存在，放弃爬取数据')
                print('**********************************')

    def parse_content(self, response):
        '''
        解析页面的内容 解析每一个页面的数据
        '''
        item = response.meta['item']
        sel = Selector(response)
        # 这个地方要修改 为
        downloadtable_html = sel.xpath('//*[@id="endText"]/table').extract_first()
        download_link_a = Selector(text=downloadtable_html).xpath('//a')
        a_download_info = []
        for a in download_link_a:
            href = a.xpath('@href').extract_first()
            text = a.xpath('text()').extract_first()
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
                # 百度云链接的话需要密码  这种情况下需要自己进行操作 获取密码
            if text is not None:
                a_download_info.append(
                    {'href': href, 'pwd': '', 'text': text, 'type_id': type_id, 'type_name': type_name})
        item['download_a'] = a_download_info
        # 内容区域的html 信息截取出来
        content_selector = sel.xpath('//*[@id="endText"]')
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
            return self.sub_content(content, item)

    def sub_content(self, content, item):
        '''
        截取相关电影的内容
        :return:
        '''
        all_field = [
            # {'text': '片名', 'field': 'name'},
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
        dr = re.compile(r'<[^>]+>', re.S)
        pre_content = content
        # 去除html 标签
        content = dr.sub('', content)
        # 清除空格
        content = content.replace(u'\u3000', u'')
        # 清除 &nbsp;
        content = content.replace(u'\xa0', u'')
        # 之前的url 这块是需要保存到之前的库中的数据
        # 过滤掉script
        re_script = re.compile('<\s*script[^>]*>[^<]*<\s*/\s*script\s*>', re.I)  # Script
        pre_content = re_script.sub('', pre_content)
        # 替换掉原来有的链接
        re_a = re.compile('<\s*a[^>]*>[^<]*<\s*/\s*a\s*>', re.I)
        pre_content = re_a.sub('', pre_content)
        item['content'] = pre_content[18:pre_content.find('<p><strong>')]
        content = content[0:content.find('【下载地址】')].strip(' \t\n\r')
        content_list = content.split('◎')
        if len(content_list) < 2:
            '''
            解析数据异常
            '''
            # # 如果是空的字段需要置为空的字段
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
                        fieldtext = content_field.replace(field['text'], '')
                        item[field['field']] = fieldtext.strip(' \t\n\r')
                elif field['text'] in content_field and content_field.find(field['text']) == 0:
                    fieldtext = content_field.replace(field['text'], '')
                    item[field['field']] = fieldtext.strip(' \t\n\r')
        # # 如果是空的字段需要置为空的字段
        for content_field in all_field:
            if not content_field['field'] in item.keys():
                item[content_field['field']] = ''
        # 根据产地来区分电影的国家
        item = self.getMovieRegion(item)
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

    def getMovieRegion(self, item):
        countrylist = [
            {
                'region_id': 1,
                'region_name': '欧美电影',
                'list': ['法国', '美国', '挪威', '丹麦', '瑞典', '英国', '捷克', '荷兰', '波兰', '罗马尼亚', '德国', '奥地利', '加拿大', '挪威', '西班牙',
                         '匈牙利', '斯洛伐克', 'Canada', '爱尔兰', '意大利', '澳大利亚', '比利时', '芬兰', '土耳其', '新西兰', '俄罗斯', '保加利亚']
            },
            {
                'region_id': 2,
                'region_name': '日韩电影',
                'list': ['日本', '韩国', '朝鲜']
            },
            {
                'region_id': 3,
                'region_name': '港台电影',
                'list': ['中国香港', '香港', '中国台湾', '台湾', '澳门', '中国澳门']
            },
            {
                'region_id': 4,
                'region_name': '大陆电影',
                'list': ['中国大陆', '中国', '大陆']
            },
            # {
            #     'region_id': 5,
            #     'region_name': '经典电影'
            # },
            {
                'region_id': 6,
                'region_name': '印度电影',
                'list': ['印度', 'india']
            },
            {
                'region_id': 7,
                'region_name': '其他电影'
                #     剩余的都归类到其他电影中
            },
            {
                'region_id': 8,
                'region_name': '泰国电影',
                'list': ['泰国']
            }
        ]
        item['region_id'] = 7
        item['region_name'] = '其他电影'
        if 'country' not in item.keys() or not item['country']:
            return item
        movieCountry = item['country']
        got = False
        for region in countrylist:
            perlist = region['list']
            region_id = region['region_id']
            region_name = region['region_name']
            for country in perlist:
                if country in movieCountry:
                    item['region_id'] = region_id
                    item['region_name'] = region_name
                    got = True
                    break
            if got:
                break
        return item
