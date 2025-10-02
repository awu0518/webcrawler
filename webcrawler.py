import logging
import tldextract
import math
import time
import urllib.request
from threading import Lock, Thread, Event
import socket
from rbloom import Bloom
from queue import PriorityQueue
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse, urljoin
from urllib.error import HTTPError
from ddgs import DDGS
from html.parser import HTMLParser 

# Configs and Global Variables
UA = "azw7225@nyu.edu"
TARGET_TOTAL = 1100
PARSER_TIMEOUT = 2
REQUEST_TIMEOUT = 2
THREADS = 40
LOG_BATCH = 5

DISALLOWED_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp", ".svg", ".ico",
    ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx",
    ".odt", ".ods", ".odp", ".rtf", ".txt",
    ".mp3", ".wav", ".aac", ".ogg", ".flac",
    ".mp4", ".avi", ".mov", ".mkv", ".webm",
    ".zip", ".tar", ".gz", ".bz2", ".7z", ".rar",
    ".iso", ".dmg", ".exe", ".msi", ".bin",
    ".js", ".css", ".json", ".xml", ".csv", ".tsv",
    ".yaml", ".yml"
}

STOP = Event()

logger = logging.getLogger(__name__)
socket.setdefaulttimeout(REQUEST_TIMEOUT)

domains: dict[str, int] = {}
suffixes: dict[str, int] = {}
priorityLock = Lock()

bloomFilter = Bloom(1_000_000, 0.1)

queue = PriorityQueue()
rpCache: dict[str, RobotFileParser] = {}
rpLock = Lock()

numParsed, numNotFound, numOtherErrors = 0, 0, 0
counterLock = Lock()

ioBuffer = ""
totalSize = 0
lastFlushed = time.time()
ioLock = Lock()

# Custom Redirect Handler
class HTTPRedirectHandler(urllib.request.HTTPRedirectHandler):
    """
    Overwritten method of redirect_request to check if the newurl is in the bloom filter already
    and to add the link to the bloom filter if not.
    """
    def redirect_request(self, req, fp, code, msg, hdrs, newurl):
        newurl = urljoin(req.full_url, newurl)

        if newurl in bloomFilter:
            raise Exception("Redirected link already in bloom filter")
        bloomFilter.add(newurl)
        
        return super().redirect_request(req, fp, code, msg, hdrs, newurl)

# Adds custom redirect handler and user agent to urllib.request calls
_opener = urllib.request.build_opener(
    HTTPRedirectHandler()
)
_opener.addheaders = [("User-Agent", UA)]
urllib.request.install_opener(_opener)

"""
Removes any anchors and queries from links
"""
def stripLink(link: str) -> str:
    newLink = link.split("#", 1)[0]
    newLink = newLink.split("?", 1)[0]
    return newLink

"""
Checks if a link contains the substring "cgi" or if it is a disallowed extension
"""
def validLink(link: str) -> bool:
    if not link: return False
    if "cgi" in link: return False

    lastSegment = urlparse(link).path.rsplit("/", 1)[-1]
    dot = lastSegment.rfind(".")
    if dot != -1 and lastSegment[dot:].lower() in DISALLOWED_EXTENSIONS:
        return False
    
    return True

# Custom HTML Link Parser
class HTMLLinkParser(HTMLParser):
    def __init__(self, baseLink, depth):
        super().__init__()
        self.baseLink = baseLink
        self.depth = depth
        self.timeout = time.time() + PARSER_TIMEOUT

    """
    Overwritten method for handle_starttag to raise TimeoutError if parsing takes too long,
    replace the baseLink of the class for <base> tags, and sending a cleaned link to 
    the queue if valid
    """
    def handle_starttag(self, tag, attrs):
        if time.time() > self.timeout:
            raise TimeoutError()
        attrs = dict(attrs)
        if tag == "base":
            newBase = attrs.get("href")
            if newBase: self.baseLink = stripLink(urljoin(self.baseLink, newBase))
        elif tag == "a" or tag == "area" or tag == "link":
            newLink = attrs.get("href")
            if validLink(newLink): 
                parseLink(stripLink(urljoin(self.baseLink, newLink)), self.depth)

"""
Checks if a link can be parsed via robots.txt

If a base link does not have a robotfileparser, it will attempt to read the file 
with the baseLink + "/robots.txt"

Once a base link has a robotfileparser, just returns the result of can_fetch()
or defaults to True for any failure
"""
def canParseRobot(link: str) -> bool:
    parsed = urlparse(link)
    baseLink = parsed.scheme + "://" + parsed.netloc
    
    with rpLock:
        rp = rpCache.get(baseLink)
        if not rp:
            rp = RobotFileParser()
            robolink = baseLink + "/robots.txt"

            try:
                rp.set_url(robolink)
                rp.read()
            except Exception: # in case of timeout
                pass

            rpCache[baseLink] = rp
    try:
        return rp.can_fetch(UA, link)
    except Exception: # in case of invalid robotfileparser, default True
        return True

"""
Checks if a link can be parsed after robots.txt and checking if the link was in the bloom filter already
If it continues, calculates an initial priority score and puts link into bloom filter and queue
"""   
def parseLink(link: str, depth: int) -> bool:
    extract = tldextract.extract(link)
    
    if not canParseRobot(link):
        return False  
    if link in bloomFilter: 
        return False

    bloomFilter.add(link)
    domain = domains.get(extract.domain, 0)
    suffix = suffixes.get(extract.suffix, 0)
    priority = 1 / math.log1p(domain + 1)
    if domain == 0: priority += 5
    if suffix == 0: priority += 10

    queue.put((-priority, 0, depth+1, link))

    return True

"""
Function for thread to obtain links from queue and parse them

First it will attempt to find the link with the actual highest priority, since priorities may have changed
since inserting. This is done by saving the old link, reinserting with an updated priority, and checking
if the links are the same after a second get

Next it updates the 

"""
def worker():
    global numParsed, numNotFound, numOtherErrors, ioBuffer, lastFlushed, totalSize

    # while True:
    while not STOP.is_set():
        linkToParse = ""
        prevLink = None
        extract = None
        priority = 0

        while True:
            priority, repeat, depth, linkToParse = queue.get()
            if prevLink and linkToParse == prevLink:
                break
            prevLink = linkToParse
            extract = tldextract.extract(linkToParse)
            with priorityLock:
                domain = domains.get(extract.domain, 0)
                suffix = suffixes.get(extract.suffix, 0)
                updatedPriority = 1 / math.log1p(domain + 1)
                if domain == 0: updatedPriority += 5
                if suffix == 0: updatedPriority += 10
                
            queue.put((-updatedPriority, repeat - 1, depth, linkToParse))

        with priorityLock:
            currDomain = domains.get(extract.domain, 0)
            currSuffix = suffixes.get(extract.suffix, 0)
            domains[extract.domain] = currDomain + 1
            suffixes[extract.suffix] = currSuffix + 1

        request = urllib.request.Request(linkToParse, headers={"User-Agent": UA})
        parsedLink = urlparse(linkToParse)
        base = parsedLink.scheme + "://" + parsedLink.netloc

        try: 
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                timeAccessed = time.localtime()
                if response.geturl() != linkToParse:
                    if validLink(response.geturl()) and not canParseRobot(stripLink(response.geturl())): 
                        continue
                if response.headers.get_content_type() != "text/html":
                    continue
                parser = HTMLLinkParser(base, depth)
                charset = response.headers.get_content_charset()
                content = response.read()
                try: 
                    parser.feed(content.decode(charset if charset else "utf-8", errors="replace"))
                except TimeoutError:
                    pass
                with ioLock:
                    ioBuffer += f"{linkToParse} successfully parsed; Time Accessed: {time.strftime('%H:%M:%S', timeAccessed)}, Priority: {-1 * priority}, Depth: {depth}, Return Code: {response.getcode()}, Size: {len(content)}\n"
                    totalSize += len(content)
                with counterLock:
                    numParsed += 1

        except HTTPError as e:
            timeAccessed = time.localtime()    
            with ioLock:
                ioBuffer += f"{linkToParse} not parsed; Time Accessed: {time.strftime('%H:%M:%S', timeAccessed)}, Priority: {-1 * priority}, Depth: {depth}, Return Code: {e.code}\n"
            if e.code == 404:
                with counterLock:
                    numNotFound += 1
            else:
                with counterLock:
                    numOtherErrors += 1
        except Exception as e:
            timeAccessed = time.localtime()  
            print(f"{linkToParse} failed to parsed; Time Accessed: {time.strftime('%H:%M:%S', timeAccessed)}, Priority: {-1 * priority}, Depth: {depth}, {e}")
                

        with ioLock:
            if time.time() > lastFlushed + LOG_BATCH:
                logger.info(ioBuffer)
                ioBuffer = ""
                lastFlushed = time.time()
        with counterLock:
            if numParsed + numNotFound + numOtherErrors >= TARGET_TOTAL:
                STOP.set()

def main():
    logging.basicConfig(filename='webcrawler.log', level=logging.INFO)
    logger.info("Starting web crawler")

    # get input from user for first search
    searchTerm = input()
    logger.info(f"User submitted {searchTerm}")

    with DDGS() as ddgs:
        results = ddgs.text(searchTerm, max_results=10)
        for r in results:
            if validLink(r["href"]):
                parseLink(r["href"], -1)

    startTime = time.time()
    for _ in range(THREADS):
        Thread(target=worker, daemon=True).start()

    try:
        while not STOP.is_set():
            time.sleep(0.2)
    except KeyboardInterrupt:
        STOP.set()

    endTime = time.time()
    time.sleep(10)

    logger.info(f"CRAWL COMPLETE: Time Taken: {endTime - startTime}, Total Size: {totalSize}, Num Parsed: {numParsed}, Num 404: {numNotFound}, Other Codes: {numOtherErrors}")

if __name__ == '__main__':
    main()