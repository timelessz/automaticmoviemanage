# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

# 1、首先需要获取当前是不是已经爬取过当前数据了  添加过得比对字段  然后更新。
# 2、获取电影的id 添加下载链接，添加之前首先比对下是不是已经有了。
# 3、获取电影的id 添加电影的图片集。
import re
from builtins import print

import pymysql
import time


class AutomaticmoviemanagePipeline(object):
    def process_item(self, item, spider):
        print(item)
        return item


class MoviescrapyPipeline(object):

    def __init__(self, conn, dbargs):
        self.conn = conn
        self.typeManage = MovieTypeManage(dbargs)
        self.movieManage = MovieManage(dbargs)

    # 从配置文件中读取数据
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        dbargs = dict(
            host=settings.get('MYSQL_HOST'),
            port=3306,
            user=settings.get('MYSQL_USER'),
            password=settings.get('MYSQL_PASSWD'),
            db=settings.get('MYSQL_DBNAME'),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
        )
        connection = pymysql.connect(**dbargs)
        return cls(connection, dbargs)

    # pipeline默认调用
    def process_item(self, item, spider):
        print('______________________________')
        print(item)
        print('______________________________')
        # 正确取到数据
        # 首先取出 封面图片
        currenttime = int(time.time())
        # 首先从数据库中取出
        # 相关定影的信息
        type_ids_string = ',,'
        if 'type' in item.keys():
            movietype_info = re.split('/| ', item['type'])
            print(movietype_info)
            type_ids_string = self.typeManage.getSetMovieType(movietype_info)
            print(type_ids_string)

        movieInfo = self.movieManage.searchMovieInfo(item['name'])
        item = self.movieManage.fieldSet(item)
        print('-----------------------------')
        print(movieInfo)
        print('-----------------------------')
        if movieInfo:
            movieId = movieInfo['id']
            movieName = movieInfo['name']
            # 表示存在该电影 找出不一致的地方然后更新 差找出不一致的字段然后更新
            diffResult = self.movieManage.diffField(movieInfo, item)
            print('==============================')
            print(diffResult)
            # print(movieInfo)
            # print(item)
            print('==============================')
            # 电影更新信息
            self.movieManage.updateMovieInfo(diffResult, movieId, movieName)
        else:
            # 表示不存在电影的情况 更新
            movieInfo = self.movieManage.addMovieInfo(item, type_ids_string)
            print('|||||||||||||||||||||||||||||||||')
            print(movieInfo)
            print('|||||||||||||||||||||||||||||||||')
            pass
            # 匹配操作下载链接 还有图片集
        if 'download_a' in item.keys():
            self.movieManage.addMovieDownload(item['download_a'], movieInfo['id'], movieInfo['name'], item['href'],
                                              item['comefrom'])
        if 'imglist' in item.keys():
            self.movieManage.addMovieImgset(item['imglist'], movieInfo['id'], movieInfo['name'], item['comefrom'],
                                            item['href'])
        # 修改电影是不是已经爬取了
        self.movieManage.changeMovieHasScrapy(movieInfo['name'], item['comefrom'])


class MovieManage(object):
    '''
    电影信息管理相关设置
    '''
    field = [{'text': '片名', 'field': 'name'},
             {'text': '又名', 'field': 'alias_name'},
             {'text': '又名', 'field': 'title'},
             {'text': '', 'field': 'coversrc'},
             {'text': '类别', 'field': 'type'},
             {'text': '片长', 'field': 'length'},
             {'text': '豆瓣评分', 'field': 'doubanscore'},
             {'text': '豆瓣链接', 'field': 'doubanurl'},
             {'text': 'IMDb评分', 'field': 'imdbscore'},
             {'text': 'IMDB', 'field': 'imdburl'},
             {'text': '导演', 'field': 'director'},
             {'text': '年代', 'field': 'ages'},
             {'text': '上映日期', 'field': 'releasedate'},
             {'text': '主演', 'field': 'starring'},
             {'text': '简介', 'field': 'summary'},
             {'text': '内容', 'field': 'content'},
             {'text': '语言', 'field': 'language'},
             {'text': '产地', 'field': 'country'}]

    # name, alias_name, title, coversrc, type, length, doubanscore, doubanurl,
    # imdburl, imdbscore, region_id, region_name, director, ages, releasedate, starring, summary, content,
    # tags, "language", country, created_at, updated_at

    def __init__(self, dbargs):
        self.conn = pymysql.connect(**dbargs)

    def fieldSet(self, item):
        # 页面设置
        for content_field in self.field:
            if not content_field['field'] in item.keys():
                item[content_field['field']] = ''
        return item

    def searchMovieInfo(self, name):
        '''
        根据电影名来获取电影数据
        :param name:
        :return: None|movieInfo 电影信息
        '''
        with self.conn.cursor() as cursor:
            selectsql = 'select * from movie_movie_list  WHERE name="%s"' % name
            cursor.execute(selectsql)
            return cursor.fetchone()

    def diffField(self, movieInfo, item):
        '''
        找出字段不一致的字段然后更新数据
        :param movieInfo: 数据库中电影相关信息
        :param item: 当前获取到的的数据信息
        :param flag: 相关标志 是btbtdy 还是其他平台
        :return:
        '''
        # 需要比对的字段信息
        fields = ['alias_name', 'length', 'doubanscore', 'doubanurl', 'director', 'ages', 'releasedate', 'starring',
                  'summary', 'language', 'country']
        result = {}
        # 需要排除 演员内详 btbtdy
        for perfield in fields:
            if perfield == 'starring':
                if movieInfo[perfield] == '内详':
                    result[perfield] = item[perfield]
                continue
            if movieInfo[perfield] and item[perfield]:
                # 都存在的情况下
                pass
            elif movieInfo[perfield]:
                # 数据库中存在的情况下
                pass
            else:
                # 新获取的数据中中存在的情况下
                result[perfield] = item[perfield]
        return result

    def addMovieInfo(self, item, type_ids_str):
        '''
        添加电影相关信息
        :param item: 电影的item 相关信息
        :param type_ids_str: 电影相关分类String
        :return:
        :todo  需要循环下是不是当前的爬取数据有些字段不存在
        '''
        currenttime = int(time.time())
        with self.conn.cursor() as cursor:
            cursor.execute(
                "insert into movie_movie_list (name, alias_name, title, coversrc, type, length, doubanscore, doubanurl, imdburl, imdbscore, region_id, region_name, director, ages, releasedate, starring, summary, content, language, country,href,comefrom,created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (item['name'], item['alias_name'], item['title'], item['coversrc'], type_ids_str, item['length'],
                 item['doubanscore'], item['doubanurl'], item['imdburl'], item['imdbscore'], item['region_id'],
                 item['region_name'], item['director'], item['ages'], item['releasedate'], item['starring'],
                 item['summary'], item['content'], item['language'], item['country'], item['href'], item['comefrom'],
                 currenttime, currenttime))
            self.conn.commit()
            movie_id = cursor.lastrowid
            return {'id': movie_id, 'name': item['name'], 'href': item['href']}

    def updateMovieInfo(self, diffresult, movieId, movieName):
        '''
        更新电影相关的字段  比较有难度的是比对字段信息
        :return:
        '''
        with self.conn.cursor() as cursor:
            fieldstring = ''
            for field in diffresult.keys():
                value = diffresult[field]
                if value:
                    perfield = " %s='%s'," % (field, value)
                    fieldstring = fieldstring + perfield
            if fieldstring:
                fieldsql = fieldstring[0:-1]
                updateSql = "UPDATE movie_movie_list SET %s WHERE id=%s" % (fieldsql, movieId)
                print('/////////////////////////////////////')
                print('更新电影数据' + movieName)
                print('/////////////////////////////////////')
                cursor.execute(updateSql)
                self.conn.commit()

    def addMovieDownload(self, downloadlink, movie_id, movie_name, href, comefrom):
        '''
        添加电影的下载链接相关数据
        :param downloadlink:
        :param movie_id:
        :param movie_name:
        :return:
        '''
        # 首先需要检查下是不是已经有了该下载链接
        # 添加电影的下载链接 这块可以单独封装函数
        add_download_sql = 'insert into movie_download_link(movie_id,movie_name,comefrom,type_name, type_id, href, text, pwd,pre_href,created_at, updated_at) VALUES '
        templatesql = "(%s,'%s','%s','%s',%s,'%s','%s','%s','%s',%s,%s)"
        insert_sql = ''
        currenttime = int(time.time())
        i = 1
        with self.conn.cursor() as cursor:
            for download in downloadlink:
                if download['href'] is None or self.searchDownloadIsexists(download, movie_id) is not None:
                    continue
                ######################
                # 需要获取下是不是  该下载链接已经存在 需要添加电影名称
                ######################
                download_sql = templatesql % (
                    movie_id, movie_name, comefrom, download['type_name'], download['type_id'], download['href'],
                    download['text'], download['pwd'], href, currenttime,
                    currenttime)
                if i == 1:
                    insert_sql = download_sql
                else:
                    insert_sql = insert_sql + ',' + download_sql
                i = i + 1
            if insert_sql:
                print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>.')
                print(add_download_sql + insert_sql)
                print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>.')
                cursor.execute(add_download_sql + insert_sql)
                self.conn.commit()

    def searchDownloadIsexists(self, download, movie_id):
        '''查看下是不是当前电影下载链接是不是已经有了'''
        with self.conn.cursor() as cursor:
            selectsql = 'select id from movie_download_link WHERE href="%s" and movie_id=%s' % (
                download['href'], movie_id)
            cursor.execute(selectsql)
            return cursor.fetchone()
        return None

    def addMovieImgset(self, imglist, movie_id, movie_name, comefrom, href):
        '''
        添加电影相关的图片集
        :return:
        '''
        # 首先需要检查下是不是已经有了该图片链接
        # print(imglist)
        with self.conn.cursor() as cursor:
            # 然后添加电影的图片链接 这块可以单独封装函数
            add_img_sql = 'insert into movie_imglist(movie_id,movie_name,imgsrc,comefrom,href,created_at,updated_at) VALUES'
            imgtemplatesql = "(%s,'%s','%s','%s','%s',%s,%s)"
            insert_img_sql = ''
            currenttime = int(time.time())
            i = 1
            for img in imglist:
                # 首先需要看下是不是已经存在
                if self.searchImgIsexists(img, movie_id) is None:
                    img_sql = imgtemplatesql % (movie_id, movie_name, img, comefrom, href, currenttime, currenttime)
                    if i == 1:
                        insert_img_sql = img_sql
                    else:
                        insert_img_sql = insert_img_sql + ',' + img_sql
                    i = i + 1
            if insert_img_sql:
                print('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
                print(add_img_sql + insert_img_sql)
                print('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
                cursor.execute(add_img_sql + insert_img_sql)
                self.conn.commit()

    def searchImgIsexists(self, img, movie_id):
        '''
        查看下是不是当前电影图片资源是不是已经有了
        :param download:
        :param movie_id:
        :return:
        '''
        print(img)
        with self.conn.cursor() as cursor:
            selectsql = 'select id from movie_imglist WHERE imgsrc="%s" and movie_id=%s' % (img, movie_id)
            cursor.execute(selectsql)
            return cursor.fetchone()
        return None

    def changeMovieHasScrapy(self, name, comefrom):
        '''
        修改电影是不是已经爬取
        :param comefrom:
        :return:
        :todo 首先需要查看下是不是已经存在该配置了没如果已经存在的 表示是第二次爬取的数据 如果是第一次爬取的数据直接添加
        后期需要添加update
        '''
        #  首先需要查下是不是已经包含了
        currenttime = int(time.time())
        with self.conn.cursor() as cursor:
            insertSql = "insert into automovie.movie_has_scrapy_info(name, comefrom, reget, addtime, updatetime) VALUES('%s','%s','10','%s','%s') " % (
                name, comefrom, currenttime, currenttime)
            print('}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}')
            print(insertSql)
            print('}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}')
            cursor.execute(insertSql)
            self.conn.commit()


class MovieTypeManage(object):
    '''
    电影分类管理相关
    '''

    def __init__(self, dbargs):
        self.conn = pymysql.connect(**dbargs)

    def getSetMovieType(self, movietype_info):
        '''
        电影相关类型的管理
        :param movietype_info:
        :return: 拼接好的分类id STRING
        '''
        type_info = []
        # 需要从数据库中 获取 电影的 类型：然后 匹配下电影的分类
        type_ids_str = ','
        sql = "select id,name from movie_type"
        print(sql)
        with self.conn.cursor() as cursor:
            print('dsadsa')
            cursor.execute(sql)
            type_info = cursor.fetchall()
            print(type_info)
            for movietype in movietype_info:
                # 数据库中爬取的分类
                if not movietype:
                    continue
                type_id = 0
                for typedict in type_info:
                    if typedict['name'] in movietype:
                        type_id = typedict['id']
                        break
                if type_id == 0:
                    # 表示某个字段没有匹配到需要添加到数据库中
                    currenttime = int(time.time())
                    typesql = 'insert into movie_type(`name`,`created_at`) VALUES ("%s","%s")' % (
                        movietype, currenttime)
                    cursor.execute(typesql)
                    self.conn.commit()
                    # 然后获取 插入的id 是多少
                    selectsql = 'select id,name from movie_type WHERE name="%s"' % movietype
                    cursor.execute(selectsql)
                    type_id = cursor.fetchone()['id']
                type_ids_str = type_ids_str + str(type_id) + ','
        print('????????????????????????????///////')
        print(type_ids_str)
        print('????????????????????????????///////')
        return type_ids_str
