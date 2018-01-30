#! C:\Python36\python.exe
# coding:utf-8

'''
每一页间相隔30个偏移量
第一页：http://wz.sun0769.com/index.php/question/questionType?type=4&page=0
第二页：http://wz.sun0769.com/index.php/question/questionType?type=4&page=30

爬取内容：
编号 标题 状态 网友 时间 url
'''
import gevent
import gevent.monkey
import requests
from bs4 import BeautifulSoup
import pymongo

gevent.monkey.patch_all()  # 协程自动切换

MONGO_URL = '10.36.132.158'  # IP
MONGO_DB = 'sun'  # 数据库
MONGO_TABLE = 'sunInfo'  # 表名

client = pymongo.MongoClient(MONGO_URL)  # 创建pymongo对象
db = client[MONGO_DB]  # 创建数据库

N = 10  # 需要开启的协成数量


# 获取主要内容
def getContent(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/51.0.2704.63 Safari/537.36'}
    try:
        content = requests.get(url, headers=headers).content.decode('gbk')
        soup = BeautifulSoup(content, 'lxml')
    except UnicodeDecodeError:
        content = requests.get(url, headers=headers).content
        soup = BeautifulSoup(content, 'lxml')
    except:
        print('编码错误')
    return soup


# 获取重要信息
def getInfo(urllist):
    try:
        for url in urllist:
            soup = getContent(url)
            number = soup.find_all('td', attrs={'width': '53'})
            title = soup.find_all('a', class_='news14')
            state = soup.find_all('td', attrs={'width': '50'})
            name = soup.find_all('td', attrs={'width': '105'})
            date = soup.find_all('td', class_='t12wh')
            # print(len(number), len(title), len(state), len(name), len(date))
            for i in range(len(number)):
                info = {
                    'number': number[i].string,
                    'title': title[i]['title'],
                    'state': state[i].string,
                    'name': name[i].string,
                    'sdate': date[i].string,
                    'url': title[i]['href']
                }
                # print(info)
                saveToMongo(info)
    except Exception as e:
        print("获取信息失败：",e)


# 获取最大页数
def getMaxPage():
    try:
        url = 'http://wz.sun0769.com/index.php/question/questionType?type=4&page=0'
        soup = getContent(url)
        maxPage = soup.find_all('div', class_='pagination')[0].find_all('a')[-1]['href'][-5:]  # 最大页码偏移量
        return maxPage
    except:
        print('页数未知或页数补抓失败')


# 保存到MongoDB中
def saveToMongo(result):
    try:
        if db[MONGO_TABLE].insert(result):
            print('存入到MONGODB成功', result)
    except Exception as e:
        print('存储失败', e)


def getUrllist(page):
    xlist = []
    urllist = []
    for i in range(0, page + 1, 30):
        newurl = 'http://wz.sun0769.com/index.php/question/questionType?type=4&page=' + str(i)  # 创建新链接
        urllist.append(newurl)  # 将所有的url添加到列表
    for i in range(N): xlist.append([])  # 创建一个二维列表,用于分配任务
    for i in range(len(urllist)):
        xlist[i % N].append(urllist[i])  # 求模切割，得到分配后的url列表
    return xlist


def main():
    maxPage = int(getMaxPage())
    xlist = getUrllist(maxPage)  # 获得url分配列表
    tasklist = []  # 任务列表
    for i in range(N):
        tasklist.append(gevent.spawn(getInfo, xlist[i]))  # 创建10个协程
    gevent.joinall(tasklist)  # 添加到协程列表中


if __name__ == "__main__":
    main()
    print('------数据抓取完毕------')
