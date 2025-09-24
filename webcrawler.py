import logging
import tldextract
import math
import time
import urllib.request
import threading
from rbloom import Bloom
from queue import PriorityQueue
from collections import defaultdict
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse, urljoin
from urllib.error import HTTPError
from ddgs import DDGS # search from DuckDuckGo
from html.parser import HTMLParser 

UA = "azw7225@nyu.edu"

logger = logging.getLogger(__name__)

domains = defaultdict(int)
suffix = defaultdict(int)
bloomFilter = Bloom(1_000_000, 0.1)
queue = PriorityQueue()
rp_cache: dict[str, RobotFileParser] = {}

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

parseLock = threading.Lock()
numParsed = 0
notFoundLock = threading.Lock()
num404 = 0
otherErrorLock = threading.Lock()
otherErrors = 0

calcLock = threading.Lock()

TARGET_TOTAL = 10000
STOP = threading.Event()

def total_processed():
    with parseLock:
        p = numParsed
    with notFoundLock:
        n = num404
    with otherErrorLock:
        o = otherErrors
    return p + n + o

class HTMLLinkParser(HTMLParser):
    def __init__(self, baseLink, depth):
        super().__init__()
        self.baseLink = baseLink
        self.depth = depth
        self.timeout = time.time() + 1

    def handle_starttag(self, tag, attrs):
        if time.time() > self.timeout:
            raise TimeoutError()
        attrs = dict(attrs)
        if tag == "base":
            newBase = attrs.get("href")
            if newBase: self.baseLink = urljoin(self.baseLink, newBase)
        if tag == "a" or tag == "area" or tag == "link":
            newLink = attrs.get("href")
            if not newLink:
                return
            anchorIndex = newLink.find("#")
            if anchorIndex != -1:
                newLink = newLink[:anchorIndex]
            queryIndex = newLink.find("?")
            if queryIndex != -1:
                newLink = newLink[:queryIndex]

            if validNewLink(newLink):    
                parseLink(urljoin(self.baseLink, newLink), self.depth)
            
        
class HTTPRedirectHandler(urllib.request.HTTPRedirectHandler):
    def __init__(self):
        super().__init__()

    def redirect_request(self, req, fp, code, msg, hdrs, newurl):
        # logger.debug(f"REDIRECT: {req.full_url} to {newurl}")
        newurl = urljoin(req.full_url, newurl)
        new_req = super().redirect_request(req, fp, code, msg, hdrs, newurl)
        if new_req:
            bloomFilter.add(newurl)

        return new_req


# Set User-Agent for robotparser.read() and custom redirect handler
_opener = urllib.request.build_opener(
    HTTPRedirectHandler()
)
_opener.addheaders = [("User-Agent", UA)]
urllib.request.install_opener(_opener)

def canParseRobot(link: str) -> bool:
    parsed = urlparse(link)
    baseLink = parsed.scheme + "://" + parsed.netloc

    rp = rp_cache.get(baseLink)

    if not rp:
        rp = RobotFileParser()
        robolink = baseLink + "/robots.txt"
        rp.set_url(robolink)
        try:
            rp.read()
        except Exception as e:
            # logger.warning(f"robots.txt fetch failed for {baseLink}: {e}")
            pass

        rp_cache[baseLink] = rp

    return rp_cache[baseLink].can_fetch(UA, link)

def validNewLink(link: str) -> bool:
    if link.find("cgi") != -1:
        return False
    
    possibleFile = urlparse(link).path.rsplit("/", 1)[-1]
    extensionDot = possibleFile.rfind(".")
    if extensionDot != -1 and possibleFile[extensionDot:].lower() in DISALLOWED_EXTENSIONS:
        return False
    return True
        
def parseLink(link: str, depth: int) -> bool:
    extract = tldextract.extract(link)
    
    if not canParseRobot(link):
        return False  
    if link in bloomFilter: 
        return False

    bloomFilter.add(link)
    
    priority = (1 / math.log1p(domains[extract.domain] + 1)) + 5 * (1 / math.log1p(suffix[extract.suffix] + 1))
    queue.put((-priority, depth+1, link))

    return True

def worker():
    global numParsed, num404, otherErrors
    # while True:
    while not STOP.is_set():
        linkToParse = ""
        extract = None
        priority = 0

        while True:
            priority, depth, linkToParse = queue.get()
            extract = tldextract.extract(linkToParse)
            with calcLock:
                updatedPriority = (1 / math.log1p(domains[extract.domain] + 1)) + 5 * (1 / math.log1p(suffix[extract.suffix] + 1))
            if updatedPriority == priority * -1:
                break
            queue.put((-updatedPriority, depth, linkToParse))

        with calcLock:
            domains[extract.domain] += 1
            suffix[extract.suffix] += 1

        request = urllib.request.Request(linkToParse, headers={"User-Agent": UA})
        parsedLink = urlparse(linkToParse)
        base = parsedLink.scheme + "://" + parsedLink.netloc

        try: 
            with urllib.request.urlopen(request, timeout=1) as response:
                timeAccessed = time.localtime()
                if response.headers.get_content_type() != "text/html":
                    continue
                parser = HTMLLinkParser(base, depth)
                charset = response.headers.get_content_charset()
                try: 
                    parser.feed(response.read().decode(charset if charset else "utf-8", errors="replace"))
                except TimeoutError:
                    pass
                logger.info(f"{linkToParse} successfully parsed: \nTime Accessed: {time.strftime('%H:%M:%S', timeAccessed)}, Priority: {priority}, Depth: {depth}, Return Code: {response.getcode()}, Domain: {domains[extract.domain]}, Suffix: {suffix[extract.suffix]}")
                with parseLock:
                    numParsed += 1

        except HTTPError as e:
            timeAccessed = time.localtime()    
            logger.info(f"{linkToParse} unsuccessfully parsed: \nTime Accessed: {time.strftime('%H:%M:%S', timeAccessed)}, Priority: {priority}, Depth: {depth}, Return Code: {e.code}")
            if e.code == 404:
                with notFoundLock:
                    num404 += 1
            else:
                with otherErrorLock:
                    otherErrors += 1
        except UnicodeEncodeError:
            pass
        except Exception as e:
            pass

        if total_processed() >= TARGET_TOTAL:
            STOP.set()

def main():
    logging.basicConfig(filename='webcrawler.log', level=logging.INFO)
    logger.info("Starting web crawler")

    # get input from user for first search
    searchTerm = input()
    logger.info(f"User submitted {searchTerm}")

    with DDGS() as ddgs:
        results = ddgs.text(searchTerm, max_results=3)
        for r in results:
            parseLink(r["href"], -1)

    startTime = time.time()
    for _ in range(40):
        threading.Thread(target=worker, daemon=True).start()

    try:
        while not STOP.is_set():
            time.sleep(0.2)
    except KeyboardInterrupt:
        STOP.set()

    endTime = time.time()
    time.sleep(10)

    logger.info(f"CRAWL COMPLETE: Time Taken: {endTime - startTime}, Num Parsed: {numParsed}, Num 404: {num404}, Other Codes: {otherErrors}")

if __name__ == '__main__':
    main()