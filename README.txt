about:
this is a multithreaded  web crawler written in python 3.2.2.
its purpose right now is to collect images and check them if they got hidden files inside
but that may change if i find better use for it..

dependencies:
lxml module

usage:
python webc.py  -u your url(make sure its a full url with - https/http)  -t number of threads -; depth level (optional, defult = 1)

example
python web.py -u http://www.rootwebsite.com -t 8 -l 5

keyboard control:
q - stop the program
s - stop the crawler threads
l - depth level
