# coding=utf-8
import datetime
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
import json
import pymysql
import time


# ****获取新闻函数
def get_news(main_url, news_type):
    # 数据库mysql链接
    config = {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
        'password': '163512',
        'db': 'test',
        'charset': 'utf8',
        'cursorclass': pymysql.cursors.DictCursor,
    }
    conn = pymysql.connect(**config)
    # 配置谷歌浏览器选项
    chrome_options = Options()
    chrome_options.add_argument('headless')
    chrome_options.add_argument('disable-gpu')
    chrome_options.add_argument('disable-infobars')
    driver = webdriver.Chrome(chrome_options=chrome_options)
    driver.get(main_url)
    time.sleep(3)

    all_window_height = []
    all_window_height.append(driver.execute_script("return document.body.scrollHeight;"))
    while True:
        driver.execute_script("scroll(0,100000)")
        time.sleep(1)
        check_height = driver.execute_script("return document.body.scrollHeight;")
        if check_height == all_window_height[-1]:
            break
        else:
            all_window_height.append(check_height)
    main_soup = BeautifulSoup(driver.page_source.encode('utf-8'), 'html.parser')
    main_url_list = []
    main_item_ids = []

    ul_list = main_soup.find('div', attrs={'class': 'channel_mod'}).findAll('ul', attrs={'class': 'list'})
    li_list = []
    img_list = []
    for ul in ul_list:
        li_temp = ul.findAll('li')
        for lili in li_temp:
            li_list.append(lili)
    for li in li_list:
        img = li.find('img')
        try:
            img_list.append(img['src'])
        except:
            continue
        main_item_ids.append(li['id'].split('_', 1)[0])
        main_url_list.append(li.find('a')['href'])
    index = 0
    for item_url in main_url_list:
        id = main_item_ids[index]
        index = index + 1
        # 如果url中包含id 则是话题讨论 不爬取
        # 如果url不是html后缀 不爬取
        if 'id' in item_url or item_url.find('html') == -1:
            continue
        driver.get(item_url)
        soup = BeautifulSoup(driver.page_source.encode('utf-8'), 'html.parser')
        title = soup.title.string
        if title == '':
            continue
        # 获取视频信息
        video_flag = 0
        video_title = ''
        video_html = ''
        video_title_tag = soup.findAll('div', attrs={'class': 'video-title'})
        if len(video_title_tag) > 0:
            video_flag = 1
            video_title = video_title_tag[0].a.string
            video_html_tag = soup.findAll('video')[0]
            video_html = str(video_html_tag)
            if 'blob' in video_html:
                continue
        # 获取导语信息
        introduction = ''
        introduction_tags = soup.findAll('div', attrs={'class': 'introduction'})
        if len(introduction_tags) > 0:
            introduction = introduction_tags[0].string
        # 获取正文
        content = ''
        content_lines = soup.findAll('p', attrs={'class': 'one-p'})
        for line in content_lines:
            img = line.find('img')
            # 如果有图片 则保存图片链接
            if img is not None:
                content = content + img['src']
            else:
                for string in line.strings:
                    content = content + string
        if content == '':
            continue
        # 获取时间 新闻来源
        news_time = ''
        src = ''
        js_tags = soup.findAll('script')
        for js in js_tags:
            str_js = str(js)
            if 'window.DATA' in str_js:
                str_js = js.string
                str_js = str_js.replace('window.DATA', '')
                str_js = str_js.replace('=', '')
                str_js = str_js.replace('\n', '')
                str_js = str_js.replace('\t', '')
                data = json.loads(str_js)
                news_time = data['pubtime']
                src = data['media']
                break
        create_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # 插入数据
        cursor = conn.cursor()
        try:
            cursor.execute('insert into news values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                           [id, title, introduction, content, src, news_type, news_time, create_time, video_html,
                            video_title,
                            str(video_flag), img_list[index - 1]])
            conn.commit()
            print img_list[index - 1], '------', title
        except:
            continue
        finally:
            cursor.close()
    conn.close()
    driver.close()


# main*****************************************#
head_url = 'https://new.qq.com/ch/'
type_url = ["ent/", "milite/", "world/", "tech/", "finance/", "auto/", "fashion/", "photo/", "games/",
            "house/", "cul/", "comic/", "emotion/", "digi/", "health/", "life/", "visit/", "food/", "history/",
            "pet/"]
sports_url = 'https://new.qq.com/rolls/?ext=sports'
# 开启线程爬取每一个模块的新闻
# 参数
arg_list = []
for i in range(len(type_url)):
    t = type_url[i]
    arg_list.append([head_url + t, t[0:len(t) - 1]])
arg_list.append([sports_url, 'sports'])

# url
url_arg = []
# type
type_arg = []
for url in type_url:
    url_arg.append(head_url + url)
    type_arg.append(url[0:len(url) - 1])
url_arg.append(sports_url)
type_arg.append('sports')
with ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(get_news, url_arg, type_arg)
# for i in range(len(url_arg)):
#get_news(url_arg[0], type_arg[0])
