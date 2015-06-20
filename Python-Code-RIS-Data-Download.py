import urllib.request
import httplib2
import _thread
import time
import re

#########################################
# thread function for ris data download #
#########################################

def download(rrc,name):
    id = _thread.get_ident()
    print("start thread",id,"||",rrc,"-",name)
    print()

    # definition of start time
    date_start = (2010,1,1,0,0,0,0,0,0)
    date_start_absolut = time.mktime(date_start)
    date_start_localtime = time.localtime(date_start_absolut)
    date_start_directory = time.strftime("%Y.%m",date_start_localtime)
    date_start_compare = time.strftime("%Y%m%d",date_start_localtime)

    # definition of start doublet
    doublet_start = (2009,12,31,0,0,0,0,0,0)
    doublet_start_absolut = time.mktime(doublet_start)
    doublet_start_localtime = time.localtime(doublet_start_absolut)
    doublet_start_directory = time.strftime("%Y.%m",doublet_start_localtime)
    doublet_start_compare = time.strftime("%Y%m%d",doublet_start_localtime)

    # change directory if last bview file has been reached
    directory = True
    treffer = 0
    doublet = doublet_start_compare
    while directory == True:
        url = "http://data.ris.ripe.net/" + rrc + "/" + date_start_directory + "/"
        h = httplib2.Http(".cache")
        header, content = h.request(url,"GET")
        content_string = content.decode(encoding='utf-8')
        list_bview = re.findall(r"href\=\"(bview\.20[0-3][0-9][0-1][0-9][0-3][0-9]\..*?)\"",
                                str(content_string))
        list_bview.reverse()

        # find sequentially bview files in the directory
        for index in list_bview:
            if doublet == doublet_start_compare:
                pass
            elif index[6:14] == doublet:
                pass
            elif index[6:14] > doublet:
                date_start_absolut += 86400
                date_start_localtime = time.localtime(date_start_absolut)
                date_start_compare = time.strftime("%Y%m%d",date_start_localtime)
                treffer = 0

            # find bview files even if one or more days are missing
            compare = False
            while compare == False:
                if index[6:14] == date_start_compare:

                    # pick only the first bview file of the day
                    while treffer == 0:
                        doublet = index[6:14]
                        download_url = url + index
                        print("download_url:",download_url)
                        download_file = urllib.request.urlopen(download_url)
                        download_data = download_file.read()
                        with open("D:/Python/" + rrc + "/" + index,"wb") as code:
                            code.write(download_data)
                        treffer += 1
                    compare = True
                else:
                    if index[6:14] < date_start_compare:
                        directory = False
                        break
                    date_start_absolut += 86400
                    date_start_localtime = time.localtime(date_start_absolut)
                    date_start_compare = time.strftime("%Y%m%d",date_start_localtime)
            if directory == False:
                break

        # jump to the first bview file in the next directory (next month)
        date_directory_absolute = date_start_absolut + 86400
        date_directory_localtime = time.localtime(date_directory_absolute)
        date_start_directory = time.strftime("%Y.%m",date_directory_localtime)
    print("end of thread",id,"||",rrc,"-",name)
    return

#############################################
# main program for downloading RIS raw data #
#############################################

global ixp_dic
ixp_dic = {"rrc01":"LINX","rrc07":"Netnod","rrc10":"MIX","rrc12":"DE-CIX","rrc13":"MSK-IX"}
ixp_abbr = ixp_dic.keys()
ixp_name = ixp_dic.values()
id = _thread.get_ident()
print("start main program: ",id)
print()

# starting one thread for each remote route collector
for rrc in ixp_abbr:
    print("download RIS data from",rrc,ixp_dic[rrc])
    name = ixp_dic[rrc]
    _thread.start_new_thread(download,(rrc,name))
    time.sleep(0.1)
