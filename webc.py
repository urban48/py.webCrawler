#!/usr/bin/env python

import argparse
import logging
import urllib.request, urllib.error, urllib.parse
import queue
import threading
import time
import os
from time import strftime
import msvcrt
from lxml.html import parse

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.ERROR)

parser = argparse.ArgumentParser()
parser.add_argument('-u',type=str , help='start url',  required=True)
parser.add_argument('-t', type=int ,  help='number of threads',  required=True)
parser.add_argument('-l', type=int ,  help='set depth level',  required=False)
args = vars(parser.parse_args())

#---------------------global vars ---------------------------------

#thread safe queue that holds the links pending crewling
Ulinks_Q = queue.Queue()#SetQueue(0)

#list that holds links that been procceced
Plinks_L = []

#thread safe queue that holds the images pending analizing
Uimg_Q = queue.Queue()#SetQueue(0)

#list that holds image urls that been procceced
Pimg_L = []

#defult 
init_url = args['u']

#list that holds results
ImgHit_L = []

LinkThread_holder = []
ImgThread_holder = []

#links proccessed counter
Plink_counter = 0

#image proccessed counter
Pimg_counter = 0

#img hit counter
ImgHit_counter = 0

#set up crawl depth lvl
if(args['l'] is None):
    crawl_level = 0
else:
    crawl_level = args['l']
    
#sets the number of threads that will be runing   
thread_count = args['t']

start_time = time.time()

#sets the path of the debug logs
#log_path ='C:\\Users\\Urban\\Documents\\Projects\\Python\\mpwc\\'
#log_path = "C:\\srg\\projects\\python\\web crawler\\"
log_path = 'c:\\webc\logs\\'

#clears the log files
with open(log_path + 'report_full_img_list.txt', 'w') as f:
   f.write('')
with open(log_path + 'report_full_link_list.txt', 'w') as f:
   f.write('')
with open(log_path + 'report_404.txt', 'w') as f:
   f.write('')
with open(log_path + 'report_dup_img_list.txt', 'w') as f:
   f.write('')
with open(log_path + 'report_error_retrive_img.txt', 'a') as f:
   f.write('')

#debug counters
img_added = 0
dup_links = 0
dup_images = 0

#hex trings that may indicate that the image contains archive inside
zip_hex_pat ="\\x50\\x48\\x03\\x04\\x0A\\x00\\x00\\x00\\x00\\x00\\x00"
rar_hex_pat = "\\x52\\x61\\x72\\x21\\x0A\\x07\\x00\\xCF\\x90\\x73\\x00\\x00\\x0D\\x00\\x00\\x00"

#---------------------global vars ---------------------------------
	
	
class Crawler(object):

    def __init__(self,  depth=0):
        self.depth = depth
   
    def isValidUrl(self,url):
        try:
           req = urllib.request.urlopen(url)
        except ValueError as e:
            logging.error(e)
            return False, url
        except urllib.error.HTTPError as e:
            if 300 < e.code < 304:
                logging.warning(url + ' - is redirected  ')
                u = urllib.error.HTTPError.geturl(e)
                newUrl =  urllib.request.urljoin(url, u)
                return True, newUrl, urllib.request.urlopen(newUrl)
            if e.code == 404:
                logging.warning(url + " - " + str(e))
                with open(log_path + 'report_404.txt', 'a') as f:
                    f.write(url+'\n') 
                return False, url
            if e.code == 403:
                logging.warning(url + " - " + str(e))
                return False, url
            logging.debug(e)
            return False, url
        except urllib.error.URLError as e:
            logging.error(e)
            return False, url
        except Exception as e:
            print('\n\n' + str(e) + '\n\n')
        else:
            return True, url, req



    def retrivePageData(self, url):
        global Plink_counter, dup_links
        #absUrl = url;
        absImgUrl = '';
        lock = threading.Lock()



        try:
            dom = parse(url).getroot()
        except IOError as e:
            logging.error('[Crawler.retrivePageData] cant parse give url - ' + str(e))
            print('\n\n\n' + url + '\n\n\n')
            return
        else:
            Plinks_L.append(url)
            lock.acquire()
            Plink_counter +=1
            lock.release()  

        if(dom is None):
            logging.debug('dom is none')
            return
        
        links = dom.cssselect('a')

        #retrive images from the page and store in queue to be proccessed
        self.GetImages(dom)
        
        for link in links:
                
            link_adr = link.get('href')


            if(link_adr != "" and link_adr is not None):
                
                #ignore js links
                if(link_adr.find('javascript:') != -1):
                    continue
                #check if its a full link
                if(link_adr.find('http://') != -1):
                    absUrl = link_adr   
                else:
                    absUrl = urllib.parse.urljoin(link.base_url, link_adr)  
            #href is empty
            else:
                continue

            try:
                with open(log_path + 'report_full_link_list.txt', 'a') as f:
                    f.write(str(absUrl)+'\n')
            except UnicodeEncodeError as e:
                logging.info('failed to add entry to "report_full_link_list" log')
                

            try:                 
                Plinks_L.index(absUrl)
            except ValueError:          
                Ulinks_Q.put(absUrl)
            else:
               lock.acquire()
               dup_links +=1
               lock.release()
                



    def GetImages(self, dom):
        global img_added, dup_images
        lock = threading.Lock()
        
        #grab all images from a page
        images = dom.cssselect('img')
            
        for img in images:
            
            img_path = img.get('src')   

            if(img_path != '' and img_path is not None):
                                        
                if(img_path.find('http://') != -1):
                    absImgUrl = img_path   
                else:
                    absImgUrl = urllib.parse.urljoin(img.base_url, img_path) 

                try:
                    if(absImgUrl is not None and absImgUrl != ''):
                        Pimg_L.index(absImgUrl)
                except ValueError:
                    Uimg_Q.put(absImgUrl)
                    lock.acquire()
                    img_added +=1
                    lock.release()
                else:
                    lock.acquire()
                    dup_images +=1 
                    lock.release()
                    try:
                        with open(log_path + 'report_dup_img_list.txt', 'a') as f:
                            f.write(str(absImgUrl)+'\n')
                    except UnicodeEncodeError as e:
                        logging.info('failed to add entry to "report_dup_img_list" log')
                    continue


class ImagAnalizer(object):  
    
    def analize(self, img):
        global Pimg_counter, ImgHit_counter
        lock = threading.Lock()

        cr = Crawler()             
        result = cr.isValidUrl(img)
        
        if(result[0]):
            try:
                req = result[2]
            except IndexError as f:
                print('\n\n\n' + str(result) + '\n\n\n')
            else:
                imgHexData = str(req.read())
                Pimg_L.append(img)
                lock.acquire()
                Pimg_counter +=1
                lock.release()
        else:
            logging.error("cant retrive image - " + img)
            try:
                with open(log_path + 'report_error_retrive_img.txt', 'a') as f:
                    f.write(str(img)+'\n')
            except UnicodeEncodeError as e:
                logging.info('failed to add entry to "report_dup_img_list" log')     
            return
                
        if(imgHexData.find(zip_hex_pat) != -1 or imgHexData.find(rar_hex_pat) != -1):
            ImgHit_L.append(img)           
            #access shared resource
            lock.acquire()
            ImgHit_counter +=1
            lock.release()
 
                                               
            

class CrawlerWorker(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.crawler = Crawler()
        self.flag = True
        global crawl_level
        self.depth_counter = 0
        

    def run(self):
        sleep_counter = 0
        while(self.flag):
            if(crawl_level == 0)
                self.crawl()
            elif:
                if(self.depth_counter < crawl_level):
                    self.crawl()
            else:
                logging.info('crawl_level reached, stopping thread')
                self.stop()

                
    def crawl(self):
        if(not Ulinks_Q.empty()):
            link = Ulinks_Q.get()
            #check if link in the proccessed queue
            try:
                Plinks_L.index(link)
            except ValueError:
                #check url validity
                self.crawler.retrivePageData(link)
                self.depth_counter +=1
                       
            else:
                logging.debug("link already in proccessed list - " + link)

        else:
            #time out exit
            if(sleep_counter > 10):
                self.stop()
                logging.info('CrawlerWorker thread time out')
            #wait for 1 second and check the Queue again if empty
            time.sleep(1)
            sleep_counter +=1
            
     
    def stop(self):
        self.flag = False
        logging.debug('CrawlerWorker thread stopped')


class ImagAnalizerWorker(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.ImagAnalizer = ImagAnalizer()
        self.flag = True
        
    def run(self):
        sleep_counter = 0
        while(self.flag):
            if(not Uimg_Q.empty()):
                img = Uimg_Q.get()
                #check if img already in the proccessed list
                if(img is not None):
                    try:
                        Pimg_L.index(img)
                    except ValueError:                     
                        self.ImagAnalizer.analize(img)    
                    else:
                        logging.info("image already in proccessed list - " +img)
                else:
                    print('\n\n\nnone value in Uimg_Q\n\n\n')
            else:
                #time out exit
                if(sleep_counter > 5):
                    self.stop()
                    logging.info('ImagAnalizerWorker thread time out')
                #wait for 1 second and check the Queue again if empty
                time.sleep(1)
                sleep_counter +=1


    def stop(self):
        self.flag = False

class StatsDisplyWorker(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.flag = True
        
    def run(self):
        global Pimg_counter, Plink_counter
        while(self.flag):
            print('\n--------stats----------')
            print('\nPimg_counter: ' + str(Pimg_counter))
            print('\nPlink_counter: ' + str(Plink_counter))
            print('\nhits: ' + str(ImgHit_counter))
            print('\nimg added: ' + str(img_added))
            print('\ndup_links: ' + str(dup_links))
            print('\ndup_images: ' + str(dup_images))
            print ('\nUlinks_Q: ', Ulinks_Q.qsize())
            print('\n-----------------------\n')
            time.sleep(10)
            


    def stop(self):
        self.flag = False
        logging.info('stats thread stoped')


class kbControll(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.LinkThread_holder = LinkThread_holder
        
    def run(self):
        while True:
            char = msvcrt.getch()
            if char == 'q'.encode('ascii'):
                logging.critical('program stopped by user')
                self.exit()
            if char == 's'.encode('ascii'):
                logging.critical('crawler stopped')
                for thread in self.LinkThread_holder:
                    thread.stop()
                
    def exit(self):
        os._exit(1)

#python C:\Users\Urban\Documents\Projects\Python\mpwc\webc.py -u http://www.ynet.co.il -t 4
#start kbControll thread
kbC = kbControll()
kbC.setDaemon(True)
kbC.start()


#start stats report
statDisplyThread = StatsDisplyWorker()
statDisplyThread.setDaemon(True)
statDisplyThread.start()

#insert the root url into the queue
c = Crawler()
#retrive page date the given page
c.retrivePageData(args['u'], 1)



for i in range(thread_count):
    link_thread = CrawlerWorker()
    LinkThread_holder.append(link_thread)
    #link_thread.setDaemon(True)
    link_thread.start()
    



for i in range(thread_count):
    img_thread = ImagAnalizerWorker()
    ImgThread_holder.append(img_thread)
   # img_thread.setDaemon(True)
    img_thread.start()


#first crawl all over the site and collect the data
for thread in LinkThread_holder:
    thread.join()

#wait for the ceawler to finish then analize the images collected
for thread in ImgThread_holder:
    thread.join()
'''
result = c.isValidUrl('http://www.ynet.co.il')
if(result[0]):
    c.retrivePageData((result[1]).strip())
''' 
print('Completed at ' + strftime('%a, %d %b %Y %H:%M:%S')  + "\ntook: " + '%.2f' % (time.time() - start_time) + " seconds")

print('\n\n\n-----stats-final-----')
print('\nPimg_counter: ' + str(Pimg_counter))
print('\ncrawled links: ' + str(Plink_counter))
print('\nhits: ' + str(ImgHit_counter))
print('\nPlinks_L: ' + str(len(Plinks_L)))
print('\nimg added: ' + str(img_added))
print('\ndup_links: ' + str(dup_links))
print('\ndup_images: ' + str(dup_images))
print ('\nUlinks_Q: ', Ulinks_Q.qsize())
print('\n--------------------------\n')

for i in range(Ulinks_Q.qsize()):
    try:
        with open(log_path + 'report_links_left.txt', 'w') as f:
            f.write(str(Ulinks_Q.get())+'\n')
    except UnicodeEncodeError as e:
        logging.info('failed to add entry to "report_links_left" log')

#print('\n\n\nlinks: ' + str(Plink_counter),'\nimages:  ' + str(Pimg_counter))

#file = open('C:\\Users\\Urban\\Desktop\\output.txt', mode='w', encoding='utf-8')

#for item in Plinks_L:t
 #   file.write("%s\n" % item)
#file.close()
