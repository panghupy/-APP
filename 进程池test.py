import requests
from concurrent.futures import ProcessPoolExecutor
import os

# 创建进程池
processPool = ProcessPoolExecutor()


# params:请求参数,发起请求,返回一个json串
def get_response_from_pamars(params):
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:58.0) Gecko/20100101 Firefox/58.0',
        'Referer': 'https://xueqiu.com/today/',
        'Cookie': 'aliyungf_tc=AQAAAA6/wRO86QkAyPjycuwcw1P1vi+C; xq_a_token=7443762eee8f6a162df9eef231aa080d60705b21; xq_a_token.sig=3dXmfOS3uyMy7b17jgoYQ4gPMMI; xq_r_token=9ca9ab04037f292f4d5b0683b20266c0133bd863; xq_r_token.sig=6hcU3ekqyYuzz6nNFrMGDWyt4aU; _ga=GA1.2.24748024.1531387202; _gid=GA1.2.1971059712.1531387202; u=181531387202822; Hm_lvt_1db88642e346389874251b5a1eded6e3=1531387203; device_id=9759f8a1f0076fa3f2a4e11064b8ffbc; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1531402774; _gat_gtag_UA_16079156_4=1'
    }
    url = 'https://xueqiu.com/v4/statuses/public_timeline_by_category.json'

    response = requests.get(url, headers=headers, params=params)
    print(response.status_code)
    result = response.json()
    return result


# 下载数据
def get_data(params):
    # 得到json数据
    result = get_response_from_pamars(params)
    print('开始下载数据')
    params["count"] = 15
    params["max_id"] = result["next_max_id"]
    # 返回下一页请求的参数和本页下载的数据
    print('结束下载')
    return params, result


# 解析数据
def parse_data(future):
    # 解析数据
    print('开始解析数据' + str(os.getpid()))
    result = future.result()[1]
    print(result)
    params = future.result()[0]
    handler = processPool.submit(get_data, params)
    handler.add_done_callback(parse_data)
    print('结束解析' + str(os.getpid()))


def main():
    print('主进程开启')
    params_list = [
        {'since_id': -1, 'max_id': -1, 'count': 10, 'category': -1},
        {'since_id': -1, 'max_id': -1, 'count': 10, 'category': 105},
        {'since_id': -1, 'max_id': -1, 'count': 10, 'category': 111},
        {'since_id': -1, 'max_id': -1, 'count': 10, 'category': 102},
        {'since_id': -1, 'max_id': -1, 'count': 10, 'category': 104},
        {'since_id': -1, 'max_id': -1, 'count': 10, 'category': 101},
        {'since_id': -1, 'max_id': -1, 'count': 10, 'category': 113},
        {'since_id': -1, 'max_id': -1, 'count': 10, 'category': 110},
    ]

    for params in params_list:
        handler = processPool.submit(get_data, params)
        handler.add_done_callback(parse_data)

    print('主进程结束')


if __name__ == '__main__':
    main()
