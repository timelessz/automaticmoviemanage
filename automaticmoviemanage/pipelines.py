# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

# 1、首先需要获取当前是不是已经爬取过当前数据了  添加过得比对字段  然后更新。
# 2、获取电影的id 添加下载链接，添加之前首先比对下是不是已经有了。
# 3、获取电影的id 添加电影的图片集。
import re
import time

from automaticmoviemanage.dborm import getsession
from automaticmoviemanage.model.models import MovieType, MovieMovieList, MovieDownloadLink, MovieImglist, \
    MovieHasScrapyInfo


class MoviescrapyPipeline(object):

    def __init__(self, dbargs):
        self.DBSession = getsession(**dbargs)
        self.typeManage = MovieTypeManage(dbargs)
        self.movieManage = MovieManage(dbargs)

    # 从配置文件中读取数据
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        dbargs = dict(
            host=settings.get('MYSQL_HOST'),
            user=settings.get('MYSQL_USER'),
            password=settings.get('MYSQL_PASSWD'),
            db=settings.get('MYSQL_DBNAME')
        )
        return cls(dbargs)

    # pipeline默认调用
    def process_item(self, item, spider):
        # 正确取到数据
        # 首先取出 封面图片
        # 首先从数据库中取出
        if 'imglist' in item.keys():
            for imgsrc in item['imglist']:
                if 'coversrc' not in item.keys() or item['coversrc'] == '':
                    item['coversrc'] = imgsrc
                    break
        print('______________________________')
        print(item)
        print('______________________________')
        # 首先从数据库中取出
        # 相关定影的信息
        type_ids_string = ',,'
        if 'type' in item.keys():
            movietype_info = re.split('/| ', item['type'])
            type_ids_string = ''
            if movietype_info:
                type_ids_string = self.typeManage.getSetMovieType(movietype_info)
        movieInfo = self.movieManage.searchMovieInfo(item)
        item = self.movieManage.fieldSet(item)
        print('-----------------------------')
        print(movieInfo)
        print('-----------------------------')
        if movieInfo:
            movieId = movieInfo.id
            movieName = movieInfo.name
            # 表示存在该电影 找出不一致的地方然后更新 差找出不一致的字段然后更新
            diffResult = self.movieManage.diffField(movieInfo, item)
            print('==============================')
            print(diffResult)
            print('==============================')
            # 电影更新信息
            self.movieManage.updateMovieInfo(diffResult, movieId, movieName)
        else:
            # 表示不存在电影的情况 更新
            movieInfo = self.movieManage.addMovieInfo(item, type_ids_string)
            print('|||||||||||||||||||||||||||||||||')
            print(movieInfo)
            print('|||||||||||||||||||||||||||||||||')
            movieId = movieInfo['id']
            movieName = movieInfo['name']
            pass
            # 匹配操作下载链接 还有图片集
        if 'download_a' in item.keys():
            print(item['download_a'])
            self.movieManage.addMovieDownload(item['download_a'], movieId, movieName, item['href'],
                                              item['comefrom'])
        if 'imglist' in item.keys():
            self.movieManage.addMovieImgset(item['imglist'], movieId, movieName, item['comefrom'],
                                            item['href'])
        # 修改电影是不是已经爬取了
        self.movieManage.changeMovieHasScrapy(movieName, item['comefrom'])


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

    def __init__(self, dbargs):
        self.DBSession = getsession(**dbargs)

    def fieldSet(self, item):
        # 页面设置
        for content_field in self.field:
            if not content_field['field'] in item.keys():
                item[content_field['field']] = ''
        return item

    def searchMovieInfo(self, item):
        '''
        根据电影名来获取电影数据
        :param name:
        :return: None|movieInfo 电影信息
        '''
        name = item['name'] if item['name'] else item['title']
        movie = self.DBSession.query(MovieMovieList).filter_by(name=name).first()
        if movie:
            return movie
        return None

    def diffField(self, movieInfo, item):
        '''
        找出字段不一致的字段然后更新数据
        :param movieInfo: 数据库中电影相关信息
        :param item: 当前获取到的的数据信息
        :param flag: 相关标志 是btbtdy 还是其他平台
        :return:
        '''
        # 需要比对的字段信息
        fields = ['alias_name', 'length', 'doubanscore', 'doubanurl', 'coversrc', 'director', 'ages', 'releasedate',
                  'starring', 'summary', 'language', 'country']
        result = {}
        # 需要排除 演员内详 btbtdy
        for perfield in fields:
            dbfield = getattr(movieInfo, perfield)
            if perfield == 'starring':
                if dbfield == '内详' and item[perfield] != '内详':
                    result[perfield] = item[perfield]
                continue
            if dbfield and item[perfield]:
                # 都存在的情况下
                pass
            elif dbfield:
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
        movie = MovieMovieList(
            name=item['name'],
            alias_name=item['alias_name'],
            title=item['title'],
            coversrc=item['coversrc'],
            type=type_ids_str,
            length=item['length'],
            doubanscore=item['doubanscore'],
            doubanurl=item['doubanurl'],
            imdburl=item['imdburl'],
            imdbscore=item['imdbscore'],
            region_id=item['region_id'],
            region_name=item['region_name'],
            director=item['director'],
            ages=item['ages'],
            releasedate=item['releasedate'],
            starring=item['starring'],
            summary=item['summary'],
            content=item['content'],
            language=item['language'],
            country=item['country'],
            href=item['href'],
            comefrom=item['comefrom'],
            created_at=currenttime,
            updated_at=currenttime,
        )
        self.DBSession.add(movie)
        self.DBSession.commit()
        return {'id': movie.id, 'name': item['name'], 'title': item['title'], 'href': item['href']}

    def updateMovieInfo(self, diffresult, movieId, movieName):
        '''
        更新电影相关的字段  比较有难度的是比对字段信息
        :return:
        '''
        movie = self.DBSession.query(MovieMovieList).filter_by(id=movieId).first()
        for field in diffresult.keys():
            value = diffresult[field]
            if value:
                setattr(movie, field, value)
        self.DBSession.commit()
        print('/////////////////////////////////////')
        print('更新电影数据' + movieName)
        print('/////////////////////////////////////')

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
        currenttime = int(time.time())
        downloadlinkarr = []
        for download in downloadlink:
            if download['href'] is None or self.searchDownloadIsexists(download, movie_id) is not None:
                continue
            downloadlinkarr.append(
                MovieDownloadLink(
                    movie_id=movie_id,
                    movie_name=movie_name,
                    comefrom=comefrom,
                    type_name=download['type_name'],
                    type_id=download['type_id'],
                    href=download['href'],
                    pre_href=href,
                    text=download['text'],
                    pwd=download['pwd'],
                    created_at=currenttime,
                    updated_at=currenttime,
                ))
        print(downloadlinkarr)
        self.DBSession.add_all(downloadlinkarr)
        self.DBSession.flush()
        self.DBSession.commit()

    def searchDownloadIsexists(self, download, movie_id):
        '''查看下是不是当前电影下载链接是不是已经有了'''
        return self.DBSession.query(MovieDownloadLink).filter_by(href=download['href'], movie_id=movie_id).first()

    def addMovieImgset(self, imglist, movie_id, movie_name, comefrom, href):
        '''
        添加电影相关的图片集
        :return:
        '''
        # 首先需要检查下是不是已经有了该图片链接
        currenttime = int(time.time())
        imgflist = []
        for img in imglist:
            # 首先需要看下是不是已经存在
            if self.searchImgIsexists(img, movie_id) is not None:
                continue
            imgflist.append(MovieImglist(
                movie_id=movie_id,
                movie_name=movie_name,
                comefrom=comefrom,
                imgsrc=img,
                href=href,
                created_at=currenttime,
                updated_at=currenttime,
            ))
        print(imgflist)
        self.DBSession.add_all(imgflist)
        self.DBSession.flush()
        self.DBSession.commit()

    def searchImgIsexists(self, img, movie_id):
        '''
        查看下是不是当前电影图片资源是不是已经有了
        :param download:
        :param movie_id:
        :return:
        '''
        return self.DBSession.query(MovieImglist).filter_by(imgsrc=img, movie_id=movie_id).first()

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
        self.DBSession.add(
            MovieHasScrapyInfo(
                name=name,
                comefrom=comefrom,
                reget='10',
                addtime=currenttime,
                updatetime=currenttime,
            ))
        self.DBSession.commit()


class MovieTypeManage(object):
    '''
    电影分类管理相关
    '''

    def __init__(self, dbargs):
        self.DBSession = getsession(**dbargs)

    def getSetMovieType(self, movietype_info):
        '''
        电影相关类型的管理
        :param movietype_info:
        :return: 拼接好的分类id STRING
        '''
        type_info = []
        # 需要从数据库中 获取 电影的 类型：然后 匹配下电影的分类
        type_ids_str = ','
        type_info = self.DBSession.query(MovieType).with_entities(MovieType.id, MovieType.name).all()
        for movietype in movietype_info:
            # 数据库中爬取的分类
            if not movietype:
                continue
            type_id = 0
            for typedict in type_info:
                if typedict.name in movietype:
                    type_id = typedict.id
                    break
            if type_id == 0:
                # 表示某个字段没有匹配到需要添加到数据库中
                currenttime = int(time.time())
                type = MovieType(
                    name=movietype,
                    created_at=currenttime
                )
                self.DBSession.add(type)
                self.DBSession.commit()
                type_id = type.id
            type_ids_str = type_ids_str + str(type_id) + ','
        print('????????????????????????????///////')
        print(type_ids_str)
        print('????????????????????????????///////')
        return type_ids_str
