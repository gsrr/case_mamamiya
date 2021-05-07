import sys
import xlrd
from datetime import datetime
import traceback
import os
import time
import my_dist
import math

TXT_USERS = "output.users"
FILTER_USERS = "filter.users"
RELATION_FOLDER = "relation_folder"
NEO4J_RELATION_USERS= "neo4j_relation.users"
NEO4J_ID_USERS= "neo4j_id.users"


def debug_log(msg):
    if os.path.exists("debug.file") == False:
        return;
    print (msg)

def parse_user_data(date, content):
    try:
        ulist = []
        date = date.strip()
        if len(date) == 0:
            return
        date_list = date.split("/")
        date_list[0] = str(int(date_list[0]) + 1911)
        date = "/".join(date_list)
        obj = datetime.strptime(date, "%Y/%m/%d %H:%M:%S")
        ulist.append(str(obj))
        clist = content.split()
        for i in range(len(clist)):
            key,value = clist[i].split("=", 1)
            if "車號" in key or "身分證號" in key:
                ulist.append(value)
            if "LAT" == key:
                ulist.append(value)
            if "LON" == key:
                ulist.append(value)
        if len(ulist) != 4:
            return None
        return ulist
    except:
        #print (traceback.format_exc())
        return None

def xls2txt_do(fpath):
    users = []
    data = xlrd.open_workbook(fpath)
    for n in range(len(data.sheet_names())):
        table = data.sheets()[n]
        for i in range(table.nrows):
            length = len(table.row_values(i))
            data_date = table.row_values(i)[1]
            data_content = table.row_values(i)[length - 1]
            user = parse_user_data(data_date, data_content)
            if user != None:
                users.append(user)
    with open("output.users", "a") as fw:
        for u in users:
            data = ",".join(u)
            debug_log("%s --> %s"%(fpath, data))
            fw.write("%s\n"%data)

def xls2txt():
    if os.path.exists("output.users") == True:
        print ("output.users is exist, remove it first")
        return
    base = sys.argv[2]
    for f in os.listdir(sys.argv[2]):
        xls2txt_do(base + "/" + f)

def in_the_same_time(user, locs, limit):
    for loc in locs:
        if (user[2] - loc[2]) <= limit:
            return True
    return False


def filter_users():
    global TXT_USERS
    timefilter = int(sys.argv[2])

    dic = {}
    with open(TXT_USERS, "r") as fr:
        users = fr.readlines()
        for u in users:
            u = u.strip()
            u_items = u.split(",")
            obj = datetime.strptime(u_items[0], "%Y-%m-%d %H:%M:%S")
            if len(u_items) != 4:
                print ("data is not correct: %s\n"%(u))
                return
            if float(u_items[2]) == 0 and float(u_items[3]) == 0:
                continue
            user = [u_items[2], u_items[3], datetime.timestamp(obj)]
            u_items[1] = u_items[1].replace("-", "")
            if u_items[1].isalnum() == False:
                continue
            if u_items[1] in dic:
                if in_the_same_time(user, dic[u_items[1]], timefilter) == False:
                    dic[u_items[1]].append(user)
            else:
                dic[u_items[1]] = [user]

    with open(FILTER_USERS, "w") as fw:
        for key in dic:
            locs = []
            for loc in dic[key]:
                loc_str = ":".join([ str(item) for item in loc ])
                locs.append(loc_str)
            data = "%s=%s"%(key, ",".join(locs))
            debug_log("%s --> %s"%(key, data))
            fw.write("%s\n"%(data))

# convert name to index
def filter_users2():
    global TXT_USERS
    timefilter = int(sys.argv[2])

    dic = {}
    with open(TXT_USERS, "r") as fr:
        users = fr.readlines()
        for u in users:
            u = u.strip()
            u_items = u.split(",")
            obj = datetime.strptime(u_items[0], "%Y-%m-%d %H:%M:%S")
            if len(u_items) != 4:
                print ("data is not correct: %s\n"%(u))
                return
            if float(u_items[2]) == 0 and float(u_items[3]) == 0:
                continue
            user = [u_items[2], u_items[3], datetime.timestamp(obj)]
            if u_items[1] in dic:
                if in_the_same_time(user, dic[u_items[1]], timefilter) == False:
                    dic[u_items[1]].append(user)
            else:
                dic[u_items[1]] = [user]

    userid = 1
    with open(FILTER_USERS, "w") as fw:
        for key in dic:
            locs = []
            for loc in dic[key]:
                loc_str = ":".join([ str(item) for item in loc ])
                locs.append(loc_str)
            data = "%d=%s"%(userid, ",".join(locs))
            debug_log("%s --> %s"%(key, data))
            fw.write("%s\n"%(data))
            userid += 1

def get_relation_with_others(uid, dic, time_thres, dist_thres):
    ret = {}
    for loc1 in dic[uid]:
        for key in dic:
            if key == uid:
                continue
            ret[key] = 0

    for loc1 in dic[uid]:
        for key in dic:
            if key == uid:
                continue
            dic[key].sort(key = lambda x: x[2])
            for loc2 in dic[key]:
                t1 = float(loc1[2])
                t2 = float(loc2[2])
                if abs(t1 - t2) > time_thres:
                    #print ("(t1,t2) = (%d, %d) different timeline"%(t1, t2))
                    break
                x1 = float(loc1[0])
                y1 = float(loc1[1])
                x2 = float(loc2[0])
                y2 = float(loc2[1])
                dist = my_dist.getDistance(x1, y1, x2, y2)
                if dist < dist_thres:
                    ret[key] += 1

    return ret

def cal_relation():
    time_thres = int(sys.argv[2])
    dist_thres = int(sys.argv[3])
    dic = {}
    
    if os.path.exists(RELATION_FOLDER) == False:
        os.mkdir(RELATION_FOLDER)

    with open(FILTER_USERS, "r") as fr:
        data = fr.readlines()
        for line in data:
            line = line.strip()
            uid, locs = line.split("=")
            dic[uid] = []
            for loc in locs.split(","):
                items = loc.split(":")
                dic[uid].append(loc.split(":"))

    for key in dic.keys():
        rdic = get_relation_with_others(key, dic, time_thres, dist_thres)
        fpath = "%s/%s"%(RELATION_FOLDER, key)
        with open(fpath, "w") as fw:
            debug_log("%s --> %s"%(key, rdic))
            for rk in rdic:
                data = "%s:%d\n"%(rk, rdic[rk])
                fw.write(data)


def check_time_thres(u1, u2, time_thres):
    t1 = float(u1[3])
    t2 = float(u2[3])
    if abs(t1 - t2) > time_thres:
        return -1
    return 0

def check_dist_thres(u1, u2, dist_thres):
    x1 = float(u1[1])
    y1 = float(u1[2])
    x2 = float(u2[1])
    y2 = float(u2[2])
    dist = my_dist.getDistance(x1, y1, x2, y2)
    if dist < dist_thres:
        return 0
    return -1

def cal_relation2():
    time_thres = int(sys.argv[2])
    dist_thres = float(sys.argv[3])
    users = []
    
    i = 0
    print ("Read file : %s"%(FILTER_USERS))
    with open(FILTER_USERS, "r") as fr:
        data = fr.readlines()
        for line in data:
            line = line.strip()
            uid, locs = line.split("=")
            for loc in locs.split(","):
                items = loc.split(":")
                users.append([uid] + items)

    users.sort(key = lambda x: x[3])

    dic = {}
    for u in users:
        dic[u[0]] = {}

    for i in range(len(users)):
        u1 = users[i]
        uid1 = u1[0]
        print ("Get relation : (index, uid) = (%d, %s)"%(i, uid1))
        for j in range(i + 1, len(users)):
            u2 = users[j]
            uid2 = u2[0]
            if check_time_thres(u1, u2, time_thres) != 0:
                break

            if check_dist_thres(u1, u2, dist_thres) != 0:
                continue
           
            if uid2 not in dic[uid1]:
                dic[uid1][uid2] = 0

            if uid1 not in dic[uid2]:
                dic[uid2][uid1] = 0

            dic[uid1][uid2] += 1
            dic[uid2][uid1] += 1

    idmap = {}
    cnt = 1
    print ("Generate file : %s"%(NEO4J_ID_USERS))
    with open(NEO4J_ID_USERS, "w") as fw:
        for key in dic.keys():
            fw.write("%d,%s\n"%(cnt, key))
            idmap[key] = cnt
            cnt += 1

    print ("Generate file : %s"%(NEO4J_RELATION_USERS))
    with open(NEO4J_RELATION_USERS, "w") as fw:
        for k1 in dic:
            for k2 in dic[k1]:
                fw.write("%s,%s,%d\n"%(idmap[k1], idmap[k2], dic[k1][k2]))


def filter_by_relation():
    rcnt = int(sys.argv[2])
    ufpath = sys.argv[3]
    rfpath = sys.argv[4]

    ufw = open(ufpath, "w")
    rfw = open(rfpath, "w")

    dic = {}
    out_users = {}
    with open(NEO4J_ID_USERS, "r") as fr:
        data = fr.readlines()
        for line in data:
            line = line.strip()
            uid, name = line.split(",")
            dic[uid] = name
    
    with open(NEO4J_RELATION_USERS, "r") as fr:
        data = fr.readlines()
        for line in data:
            line = line.strip()
            items = line.split(",")
            if int(items[2]) < rcnt:
                continue
            print (line)
            rfw.write("%s\n"%line)
            out_users[items[0]] = 1
            out_users[items[1]] = 1

    for key in out_users:
        ufw.write("%s,%s\n"%(key, dic[key]))
     
    ufw.close()
    rfw.close()
        

def help():
    cmds = [
        "Convert xls to txt format : \n\tpython3 main.py xls2txt $1, $1=folder contains xls files",
        "Filter users by some rules : \n\tpython3 main.py filter_user $1, $1=time threshold",
        "Get relationship : \n\tpython3 main.py cal_relation2 $1 $2, $1=time threshold, $2=distance threshold", 
        "filter by relation : \n\tpython3 main.py filter_by_relation $1 $2 $3, $1=relation cnt, $2=output file for users, $3=output file for relation"
    ]
    print ("\n")
    for cmd in cmds:
        print (cmd)
        print ("\n")
    print ("\n")

def main():
    func = getattr(sys.modules[__name__], sys.argv[1])
    func()

if __name__ == "__main__":
    main()
