#coding=utf-8


import sys
reload(sys)
sys.setdefaultencoding("utf-8")


import pymongo
import requests
import redis
from lxml import etree
import json


'''
爬虫核心逻辑 
'''

class Spider():



    def __init__(self,userId):

        self.userId = userId
        self.url="http://xueqiu.com/friendships/groups/members.json?page=1&uid="+userId+"&gid=0"
        self.header={}

        #cookie要自己从浏览器获取
        self.header["User-Agent"]="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
        self.cookies={	"s":"98r16hkm0x", 
        				"xq_a_token":"2f845c8e8eb9741e471f54972befdf8fcaef5cf7",
        				"xqat":"2f845c8e8eb9741e471f54972befdf8fcaef5cf7", 
        				"xq_r_token":"c0143df3f6d332218fc8a5c51feee7fbee1b812e", 
        				"xq_token_expire=":"ri%20Aug%2012%202016%2001%3A07%3A16%20GMT%2B0800%20(CST)",
        				"xq_is_login":"1",
        				"u":"1096704025",
        				"bid":"1d4fe0c1de6530f46c0dd2fd29e11b7d_iqquuq0z", 
        				"snbim_minify":"true",
        				"Hm_lvt_1db88642e346389874251b5a1eded6e3":"468775231", 
        				"Hm_lpvt_1db88642e346389874251b5a1eded6e3":"1469009599", 
        				"__utmt":"1",
        				"__utma":"1.843734523.1468775245.1468847928.1469009600.5", 
        				"__utmb":"1.1.10.1469009600",
        				"__utmc":"1", 
        				"__utmz":"1.1468775245.1.1",
        				"utmcsr":"(direct)",
        				"utmccn":"(direct)",
        				"utmcmd":"(none)"
        				}



    #获得当前用户的所有关注用户信息，返回list
    def get_user_data(self):
        userInfoList = []
        followee_url=self.url
        try:
            get_html=requests.get(followee_url,cookies=self.cookies,headers=self.header,verify=False,timeout=10)
        except:
            print "requests get error!url="+followee_url
            return userInfoList

        if get_html.status_code==200:

            content=json.loads(get_html.text)
            count = content['count']
            page = content['page']
            maxPage = content['maxPage']
            userContent = content['users']

            curList = self.analy_profile(userContent)
            userInfoList.extend(curList)

            page = page+1
            while page <= maxPage:
                curUrl="http://xueqiu.com/friendships/groups/members.json?page="+str(page)+"&uid="+self.userId+"&gid=0"
                #print curUrl
                try:
                    curHtml=requests.get(curUrl,cookies=self.cookies,headers=self.header,verify=False,timeout=10)
                    if curHtml.status_code==200:
                        curContent=json.loads(curHtml.text)
                        curUserContent = curContent['users']
                        curList = self.analy_profile(curUserContent)
                        userInfoList.extend(curList)
                    else:
                        print '[ERROR]curHtml.status_code error !!!'
                except:
                    print "requests get error!url="+curUrl

                page = page+1

            
        else:
            print '[ERROR]get_html.status_code error !!!'


        return userInfoList

    #解析每一页的关注用户，json格式
    def analy_profile(self,userContent):

        userInfoList = []
        
        for curUser in userContent:
            user_id = curUser['id']
            user_name = curUser['screen_name']
            user_gender = curUser['gender']
            user_city = curUser['province']
            user_followerNum = curUser['followers_count']
            user_disNum = curUser['status_count']
            user_followNum = curUser['friends_count']

            userInfo = {'id':user_id,'name':user_name,'gender':user_gender,'city':user_city,'followNum':user_followNum,'followerNum':user_followerNum,'discusNum':user_disNum}

            global red

            if red.sadd('red_had_spider',user_id):
                red.lpush('red_to_spider',user_id)
                userInfoList.append(userInfo)
                #print 'insert id = '+str(user_id)+' name = '+user_name

        return userInfoList


#　核心模块,bfs宽度优先搜索
def BFS_Search():
    global red
    global post_info
    userInfoList = []

    while True:
        tempUser=red.rpop('red_to_spider')
        
        if tempUser==None:
            print 'empty'
            break

        result=Spider(tempUser)
        curList = result.get_user_data()
        userInfoList.extend(curList)
        print 'userId : '+tempUser+',cur list data number: '+str(len(curList))+',total data number : '+str(len(userInfoList))

        #每一千个数据向mongodb中插入，减少io次数。
        if len(userInfoList)>1000:
            print 'insert to mongodb,data number = '+str(len(userInfoList))
            post_info.insert_many(userInfoList)
            userInfoList = []

    return "ok"

#连接redis，如果redis中没有数据，插入一个种子用户（不明真相的群众）
red=redis.Redis(host='localhost',port=6379,db=1)
seedUser = "1955602780"
if red.sadd('red_had_spider',seedUser):
    red.lpush('red_to_spider',seedUser)

#连接mongodb数据库
connection = pymongo.MongoClient()
tdb = connection.Xueqiu
post_info = tdb.userInfo


#执行爬虫
BFS_Search()








