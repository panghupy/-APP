'''将抓取到的京东商品，存入数据库中'''

# ! usr/bin/python3
import time
import requests
import json
import re
from DataTools.tools import MysqlHelper
import schedule
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# 获取分享链接时,谁分享就用谁的cookie,必须重新抓包拿到cookie并替换
def getCode(skuid, spuid, couponid, title, name, pic, source='', subsoure='', g_tk='', type=1, ie='utf-8',
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
        'couponid': couponid,
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


def get_prime_goods_sku(pageindex=1):
    '''
    获取精选商品sku
    product_url = 'https://qwd.jd.com/cps/product/searchgoodsrecommend?g_tk=2004852025&sourceId=2&pageindex=1&pagesize=20&deviceId=C038799B-E1CD-4ECF-A2C0-471295C360CC'

    :return:
    '''
    category_name = '精选'
    product_url = 'https://qwd.jd.com/cps/product/searchgoodsrecommend?g_tk=2004852025&sourceId=2&pageindex=' + str(
        pageindex) + '&pagesize=20&deviceId=C038799B-E1CD-4ECF-A2C0-471295C360CC'

    headers = {
        'Host': 'qwd.jd.com',
        'Accept': 'application/json,text/plain,*/*',
        'Connection': 'keep-alive',
        'Cookie': 'jfShareSource=1;__jda=122270672.1552218403159433363416.1552218403.1552267775.1552271241.6;__jdb=122270672.10.1552218403159433363416|6.1552271241;__jdc=122270672;mba_muid=1552218403159433363416.1.1552274669702;mba_sid=1.10;app_id=161;apptoken=4F47639D211592365A1530937DB18DE6D7DBDCBAC9B676C6082C8B2BBFB3394508B8149417AE76EAB7ED9E941D881454FDE980D780ED87165C11026A71489F2E;client_type=apple;clientid=C038799B-E1CD-4ECF-A2C0-471295C360CC;jdpin=%E6%B3%A1%E6%B3%A1430;jxjpin=%E6%B3%A1%E6%B3%A1430;levelName=%E9%93%9C%E7%89%8C%E7%94%A8%E6%88%B7;nickname=%E6%B3%A1%E6%B3%A1430;picture_url=;pinType=1;tgt=AAFchPiyAEC6nH0AF8gA1QAV_8Ife37VRCEQTdGOqHWiwjTzA7VzfifIh0rVpPbd0LQPzfV8HD1Ktg6cbgxI3Nw-RYMuD184;userLevel=56;wg_skey=zpF8519D9C82F3A7B2B249863D3171FAC42B8C8630367404879627AEF6BC056A775BBFE8AE49D7523E1F92D107E64601C9;wg_uin=5728198599;wq_skey=zpF8519D9C82F3A7B2B249863D3171FAC42B8C8630367404879627AEF6BC056A775BBFE8AE49D7523E1F92D107E64601C9;wq_uin=5728198599;login_mode=1;qwd_chn=99;qwd_schn=2;__jdv=122270672|direct|-|none|-|1552218403164;cert=26554251477147546467732489310991',
        'User-Agent': 'Mozilla / 5.0(iPhone;CPUiPhoneOS12_0likeMacOSX) AppleWebKit / 605.1.15(KHTML, likeGecko) Mobile / 16A366(4329073152);jdapp;JXJ / 3.2.6',
        'Accept-Language': 'zh-cn',
        'Referer': 'https://qwd.jd.com/pages/',
        'Accept-Encoding': 'br,gzip,deflate'
    }

    response = requests.get(product_url, headers=headers, verify=False)
    # 拿到本页所有商品信息
    product_dict = response.content.decode()
    result = json.loads(product_dict)
    return result, category_name


def write_data(skuid, title, code, price, discount, category, pubdate, img, is_top, mysqlHelper):
    params = [skuid, title, code, price, discount, category, pubdate, img, is_top]
    sql = 'insert into jingfenApp_goods(skuid, title, code, price, discount, category,pubdate, img,is_top) value(%s,%s,%s,%s,%s,%s,%s,%s,%s) '

    count = mysqlHelper.insert(sql, params)
    if count > 0:
        print('成功写入数据库')


# 构造商品数据
def job():
    '''入库标准：有优惠劵并且佣金大于3块钱'''
    print('程序开始')
    # 设置页码
    page = 20

    # 连接数据库

    mysqlHelper = MysqlHelper('localhost', 'root', '123456', 'jingdong_db')
    mysqlHelper.connect()

    for i in range(1, page + 1):
        result, category_name = get_prime_goods_sku(pageindex=i)
        # print(result)
        for item in result['sku']:
            skuid = item['skuId']
            spuid = item['spuId']
            title = item['title']
            # 注意：这里可能图片太大会出问题
            imgUrl = item['imgUrl']
            commissionPrice = item['commissionPrice']  # 佣金
            try:
                couponId = item['couponId']
            except:
                couponId = None
            price = item['price']
            # 优惠券金额
            try:
                denomination = item['denomination']
            except:
                denomination = 0
            # 分享链接
            code = getCode(skuid=skuid, spuid=spuid, couponid=couponId, title=title, name=title, pic=imgUrl)

            # 商品券后价格
            discount = float(price) - float(denomination)

            # 入库日期
            pubdate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())[:-9]

            # 默认不置顶
            is_top = False
            if denomination and discount > 0 and code and float(commissionPrice) > 3.0:
                print('商品分类：', category_name)
                print('商品title：', title)
                print('商品skuId：', skuid)
                print('商品的spuId：', spuid)
                print('商品couponId：', couponId)
                print('商品图片url：', imgUrl)
                print('商品佣金：', commissionPrice)
                print('商品价格：', price)
                print('商品券后价格：', discount)
                print('商品分享链接：', code)
                print('TimeTag：', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                print('-' * 50)
                # 　符合条件的商品入库
                write_data(skuid, title, code, price, discount, category_name, pubdate, imgUrl, is_top, mysqlHelper)
    mysqlHelper.close()


job()
# schedule.every(30).minutes.do(job)
# while True:
#     schedule.run_pending()
#     time.sleep(1)
