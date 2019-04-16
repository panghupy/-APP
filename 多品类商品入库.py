'''多线程抓取多品类商品'''

from concurrent.futures import ThreadPoolExecutor
import requests
import urllib3
import json
import re
import time
from DataTools.tools import MysqlHelper
import schedule

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

category_dict = {
    '食品': 'cid1=1320|12218',
    '个护清洁': 'cid1=16750|15901|1316',
    '家居家装': 'cid1=6196&cid2=6198|6197|6227|6214|6219|9182|11143',
    '母婴': 'cid1=1319',
    '图书': 'cid1=1713',
    '数码3c': 'cid1=652&cid2=654|829|828|981|4973|12345|12346',
    '家用电器': 'cid1=737',
    '女装': 'cid2=1343',
    '男装': 'cid2=1342',
    '运动': 'cid1=1318',
}


# 获取分享链接时,谁分享就用谁的cookie,必须重新抓包拿到cookie并替换
def getCode(skuid, spuid, title, name, pic, source='', subsoure='', g_tk='', type=1, ie='utf-8',
            callback='_jp0'):
    '''
    获取分享链接
    code_url = https://qwd.jd.com/fcgi-bin/qwd_itemshare?g_tk=&skuid=13932924889&spuid=13932924889&type=1&ie=utf-8&source=&subsource=&ispg=&title=%E7%A2%A7%E7%84%B6%E5%BE%B7%EF%BC%88BRITA%EF%BC%89%20%E8%BF%87%E6%BB%A4%E6%B0%B4%E5%A3%B6%20%E5%AE%B6%E7%94%A8%E5%87%80%E6%B0%B4%E5%A3%B6%E8%BF%87%E6%BB%A4%E6%B0%B4%E6%9D%AF%20%E5%87%80%E6%B0%B4%E5%99%A8%20%E5%85%89%E6%B1%90%E7%B3%BB%E5%88%973.5L%E8%93%9D%2B%E5%8E%BB%E6%B0%B4%E5%9E%A2%E4%B8%93%E5%AE%B6%E7%89%88%E6%BB%A4%E8%8A%AF3%E6%9E%9A&couponid=&name=%E7%A2%A7%E7%84%B6%E5%BE%B7%EF%BC%88BRITA%EF%BC%89%20%E8%BF%87%E6%BB%A4%E6%B0%B4%E5%A3%B6%20%E5%AE%B6%E7%94%A8%E5%87%80%E6%B0%B4%E5%A3%B6%E8%BF%87%E6%BB%A4%E6%B0%B4%E6%9D%AF%20%E5%87%80%E6%B0%B4%E5%99%A8%20%E5%85%89%E6%B1%90%E7%B3%BB%E5%88%973.5L%E8%93%9D%2B%E5%8E%BB%E6%B0%B4%E5%9E%A2%E4%B8%93%E5%AE%B6%E7%89%88%E6%BB%A4%E8%8A%AF3%E6%9E%9A&pic=%252F%252Fimg14.360buyimg.com%252Fn1%252Fjfs%252Ft1%252F29367%252F23%252F10102%252F271682%252F5c82bdd8E7ba5a226%252Ff73070378aa909e1.jpg&callback=__jp0
    :return:链接地址
    '''
    code_url = 'https://qwd.jd.com/fcgi-bin/qwd_itemshare?'
    headers = {
        'Host': 'qwd.jd.com',
        'Accept': '*/*',
        'Connection': 'keep - alive',
        'Cookie': '''__jda = 122270672.15522706742551428584085.1552270674.1552278164.1552290799.4;__jdb = 122270672.7.15522706742551428584085 | 4.1552290799;__jdc = 122270672;mba_muid = 15522706742551428584085.1.1552292303989;mba_sid = 1.7;jfShareSource = 1_10;__jdv = 122270672 | direct | - | none | - | 1552270674255;app_id = 161;apptoken = FCFFDA1CAA5E49B6FCE19191B99870C85A1F33C46D93A8777DAC53353DFE7EB2FCF253B76677EFC73864E8ED2DDAD97308B99FE93AEA15F82F6D59F7BEC4274A;client_type = apple;clientid = BCE09B26-F616-4FEF-855E-44313FE61377;jdpin = hengli1989;jxjpin = hengli1989;levelName = %E9%92%BB%E7%9F%B3%E7%94%A8%E6%88%B7;nickname=%E5%B0%8F%E4%BA%86%E4%B8%AA%E6%AD%A5;picture_url=http://storage.360buyimg.com/i.imageUpload/68656e676c693139383931333737393639323835343834_mid.jpg;pinType=1;pt_key=app_openAAFchcVSADAvLvZFBkdchPrIFyL_VDeBg66LMHlcqgdN4StKJvKO5zibnzxkJV0zWBdGVS3BmQk;pt_pin=hengli1989;pwdt_id=hengli1989;sid=36f5ea12f8bbcd7baacb48e4117a46fw;tgt=AAFchcVRAEBGbcQKnTXYHcU0IsuZdab-6kXiNFONtx1E7monBn1lTGY6R2TEO3KoOSdUu4ptg8ROPAZOBppJ-z-Y3_clAcCo;userLevel=105;wg_skey=zp0DCE2716E45D075B69DF6A63E91D72A654AEA874A935E6F0DF0F993D93EB0E4563E2133B3490C6E54B94E82596F2E1BFEA1AD4870FB067289D7AA1046D97591E;wg_uin=5698843128;wq_skey=zp0DCE2716E45D075B69DF6A63E91D72A654AEA874A935E6F0DF0F993D93EB0E4563E2133B3490C6E54B94E82596F2E1BFEA1AD4870FB067289D7AA1046D97591E;wq_uin=5698843128;login_mode=1;qwd_chn=99;qwd_schn = 2;cert=36959282057632495340319637016144''',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
        'Accept-Language': 'zh-cn',
        'Referer': 'https: // qwd.jd.com / pages / itemMidPage?sku = 13932924889 & requestId = & couponId = 99733179 & couponValue = 10',
        'Accept - Encoding': 'br, gzip, deflate'
    }
    params = {
        'g_tk': g_tk,
        'skuid': skuid,
        'spuid': spuid,
        'title': title,
        'name': name,
        'pic': pic,
        'source': source,
        'subsoure': subsoure,
        'type': type,
        'ie': ie,
        'callback': callback
    }
    result = requests.get(code_url, headers=headers, params=params, verify=False).content.decode()
    result = re.findall('{".+"}', result.replace('\n', '').replace(' ', ''))[0]
    item = json.loads(result)
    skuurl = item['skuurl']  # 分享链接
    return skuurl


# 商品入库
def write_data(skuid, title, code, price, discount, category, pubdate, img, is_top, mysqlHelper):
    params = [skuid, title, code, price, discount, category, pubdate, img, is_top]
    sql = 'insert into jingfenApp_goods(skuid, title, code, price, discount, category,pubdate, img,is_top) value(%s,%s,%s,%s,%s,%s,%s,%s,%s) '

    count = mysqlHelper.insert(sql, params)
    if count > 0:
        print('成功写入数据库')
        print('-' * 50)

    else:
        print('写入数据库失败')
        print('-' * 50)


# 采集数据
def crawl_data(params):
    category_name, code, page = params
    print(category_name)
    # 打印当前线程的名字
    # print(threading.current_thread().name)

    product_url = 'https://qwd.jd.com/cps/product/searchgoods?g_tk=1573873518&' + code + '&pageindex=' + str(
        page) + '&pagesize=20&deviceId=BCE09B26-F616-4FEF-855E-44313FE61377'

    headers = {
        'Host': 'qwd.jd.com',
        'Accept': 'application/json,text/plain,*/*',
        'Connection': 'keep-alive',
        'Cookie': 'jfShareSource=1_10; __jda=122270672.15522706742551428584085.1552270674.1554791641.1554793504.85; __jdb=122270672.1.15522706742551428584085|85.1554793504; __jdc=122270672; mba_muid=15522706742551428584085.1.1554793504877; mba_sid=1.1; app_id=161; apptoken=FCFFDA1CAA5E49B6FCE19191B99870C85A1F33C46D93A8777DAC53353DFE7EB2FCF253B76677EFC73864E8ED2DDAD97308B99FE93AEA15F82F6D59F7BEC4274A; client_type=apple; clientid=BCE09B26-F616-4FEF-855E-44313FE61377; jdpin=hengli1989; jxjpin=hengli1989; levelName=%E9%92%BB%E7%9F%B3%E7%94%A8%E6%88%B7; login_mode=1; nickname=%E5%B0%8F%E4%BA%86%E4%B8%AA%E6%AD%A5; picture_url=http://storage.360buyimg.com/i.imageUpload/68656e676c693139383931333737393639323835343834_mid.jpg; pinType=1; qwd_chn=99; qwd_schn=2; tgt=AAFchcVRAEBGbcQKnTXYHcU0IsuZdab-6kXiNFONtx1E7monBn1lTGY6R2TEO3KoOSdUu4ptg8ROPAZOBppJ-z-Y3_clAcCo; userLevel=105; wg_skey=zp0DCE2716E45D075B69DF6A63E91D72A654AEA874A935E6F0DF0F993D93EB0E4563E2133B3490C6E54B94E82596F2E1BFEA1AD4870FB067289D7AA1046D97591E; wg_uin=5698843128; wq_skey=zp0DCE2716E45D075B69DF6A63E91D72A654AEA874A935E6F0DF0F993D93EB0E4563E2133B3490C6E54B94E82596F2E1BFEA1AD4870FB067289D7AA1046D97591E; wq_uin=5698843128; __jdv=122270672|direct|-|none|-|1554776524647; cert=22498407246831411630820978026578; sk_history=1192014%2C3681693%2C6043917%2C11381318858%2C29149058461%2C23959050923%2C42254678190%2C4343105%2C; __wga=1554284077706.1554284021428.1554168879768.1552296901116.3.6; cid=8; retina=1; shshshfp=f100b68df911d231ed0c07ae6a4a9953; shshshfpb=s2nVf4WfJ4cszqPv36MoqMg%3D%3D; addrId_1=282826997; addrType_1=1; jdAddrId=1_72_2819_0; jdAddrName=%u5317%u4EAC_%u671D%u9633%u533A_%u4E09%u73AF%u5230%u56DB%u73AF%u4E4B%u95F4_; mitemAddrId=1_72_2819_0; mitemAddrName=%u5317%u4EAC%u671D%u9633%u533A%u4E09%u73AF%u5230%u56DB%u73AF%u4E4B%u95F4SOHO%u73B0%u4EE3%u57CEA%u5EA731%u5C423111; pt_key=app_openAAFcorw6ADC_-EScMSfcqE2YhPBIYpXuNMDF9Abd-78rHS1yDKBcOjQ74KegLIDy7l9K-N9bx48; pt_pin=hengli1989; pwdt_id=hengli1989; sid=2f385f5e65896239a93842a8686757ew; wq_addr=282826997%7C1_72_2819_0%7C%u5317%u4EAC_%u671D%u9633%u533A_%u4E09%u73AF%u5230%u56DB%u73AF%u4E4B%u95F4_%7C%u5317%u4EAC%u671D%u9633%u533A%u4E09%u73AF%u5230%u56DB%u73AF%u4E4B%u95F4SOHO%u73B0%u4EE3%u57CEA%u5EA731%u5C423111%7C116.477%2C39.90721; string123=0A9DD908CF5180D5AB72F489592B710A%25YN60124@o2; wqmnx=1bcd1c43jdm356a3e3a6hpea2b4e4130; shshshfpa=97c733c3-70e2-1ee2-b18c-0e5460f97328-1552296950; visitkey=36385134401151987; sc_width=375; webp=0',
        'User-Agent': 'Mozilla / 5.0(iPhone;CPUiPhoneOS12_0likeMacOSX) AppleWebKit / 605.1.15(KHTML, likeGecko) Mobile / 16A366(4329073152);jdapp;JXJ / 3.2.6',
        'Accept-Language': 'zh-cn',
        'Referer': 'https://qwd.jd.com/pages/',
        'Accept-Encoding': 'br,gzip,deflate'
    }

    response = requests.get(product_url, headers=headers, verify=False)
    if response.status_code == 200:
        # 拿到本页所有商品信息
        product_dict = response.content.decode()
        result = json.loads(product_dict)
        # print(result)
        return result, category_name


# 解析数据
def parse_data(data):
    # 连接数据库
    print('链接数据库---')
    mysqlHelper = MysqlHelper('localhost', 'root', '123456', 'jingdong_db')
    mysqlHelper.connect()
    result, category_name = data.result()
    try:
        for item in result['sku']:
            skuid = item['skuId']
            spuid = item['spuId']
            title = item['title']
            # print(title, category_name)

            # 注意：这里可能图片太大会出问题
            imgUrl = item['imgUrl']
            commissionPrice = item['commissionPrice']  # 佣金
            price = item['price']
            # 优惠券金额
            try:
                denomination = item['denomination']
            except:
                denomination = 0
            # 分享链接
            code = getCode(skuid=skuid, spuid=spuid, title=title, name=title, pic=imgUrl)
            print(code)
            # 商品券后价格
            discount = float(price) - float(denomination)
            #
            # 入库日期
            pubdate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())[:-9]

            # 默认不置顶
            is_top = False
            if denomination and discount > 0 and code:
                print('商品分类：', category_name)
                print('商品title：', title)
                print('商品skuId：', skuid)
                print('商品的spuId：', spuid)
                print('商品图片url：', imgUrl)
                print('商品佣金：', commissionPrice)
                print('商品价格：', price)
                print('商品券后价格：', discount)
                # print('商品分享链接：', code)
                print('TimeTag：', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                # 　符合条件的商品入库
                write_data(skuid, title, code, price, discount, category_name, pubdate, imgUrl, is_top, mysqlHelper)
    except:
        print('没有数据啦---')
    mysqlHelper.close()


def job():
    # ThreadPoolExecutor帮我们创建一个线程池)
    # 如何构造一个线程池(里面有是个子线程)
    pool = ThreadPoolExecutor(max_workers=10)

    for page in range(1, 30):
        for category_name, code in category_dict.items():
            print(category_name, code)
            # 执行完毕任务之后返回的结果
            handle = pool.submit(crawl_data, (category_name, code, page))
            # 回调函数
            handle.add_done_callback(parse_data)


job()
schedule.every(30).minutes.do(job)
while True:
    schedule.run_pending()
    time.sleep(1)
