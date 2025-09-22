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

successLogger = logging.getLogger("successLogger")
successLogger.setLevel(logging.INFO)
fh1 = logging.FileHandler("successLogs.log")
successLogger.addHandler(fh1)
successLogger.propagate = False  # don’t duplicate to root

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

class HTMLLinkParser(HTMLParser):
    def __init__(self, baseLink, depth):
        super().__init__()
        self.baseLink = baseLink
        self.depth = depth

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "base":
            newBase = attrs.get("href")
            if newBase: self.baseLink = newBase
        if tag == "a" or tag == "area" or tag == "link":
            newLink = attrs.get("href")
            if not newLink:
                return
            if validNewLink(newLink):    
                parseLink(urljoin(self.baseLink, newLink), self.baseLink, self.depth)
            
        

class HTTPRedirectHandler(urllib.request.HTTPRedirectHandler):
    def __init__(self):
        super().__init__()

    def redirect_request(self, req, fp, code, msg, hdrs, newurl):
        logger.debug("REDIRECT: {req.full_url} to {newurl}")
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

def canParseRobot(link: str, baseLink: str) -> bool:
    rp = rp_cache.get(baseLink)

    if not rp:
        rp = RobotFileParser()
        robolink = baseLink + "/robots.txt"
        rp.set_url(robolink)
        try:
            rp.read()
        except Exception as e:
            logger.warning(f"robots.txt fetch failed for {baseLink}: {e}")

        rp_cache[baseLink] = rp

    return rp_cache[baseLink].can_fetch(UA, link)

def validNewLink(link: str) -> bool:
    if link.find("cgi") != -1:
        return False
    
    possibleFile = link.split("/")[-1].rfind(".")
    extension = link.split("/")[-1][possibleFile:]
    if possibleFile != -1 and extension in DISALLOWED_EXTENSIONS:
        logger.info(f"DISALLOWED EXTENSION FOUND: {link}")
        return False
    return True
        
def parseLink(link: str, baseLink: str, depth: int) -> bool:
    logger.info(f"Parsing link {link}")
    extract = tldextract.extract(link)
    
    if not canParseRobot(link, baseLink):
        logger.info("Not allowed to parse by robot.txt, exiting early")
        return False
    
    if link in bloomFilter: 
        logger.info("Link in bloom filter, exiting early")
        return False

    bloomFilter.add(link)

    priority = (1 / math.log1p(domains[extract.domain] + 1)) + 5 * (1 / math.log1p(domains[extract.suffix] + 1))
    logger.info(f"Priority {priority}, domain {extract.domain} : {domains[extract.domain]}, suffix {extract.suffix} : {suffix[extract.suffix]}")
    domains[extract.domain] += 1
    suffix[extract.suffix] += 1

    queue.put((-priority, depth+1, link))
    return True

def worker():
    # while True:
    for _ in range(100):
        priority, depth, linkToParse = queue.get()
        request = urllib.request.Request(linkToParse, headers={"User-Agent": UA})
        parsedLink = urlparse(linkToParse)
        base = parsedLink.scheme + "://" + parsedLink.netloc

        try: 
            with urllib.request.urlopen(request, timeout=5) as response:
                timeAccessed = time.localtime()
                if response.headers['Content-Type'].split(";")[0] != "text/html":
                    continue
                parser = HTMLLinkParser(base, depth)
                charset = response.headers.get_content_charset()
                parser.feed(response.read().decode(charset if charset else "utf-8", errors="replace"))
                successLogger.info(f"{linkToParse} successfully parsed: \nTime Accessed: {time.strftime("%H:%M:%S", timeAccessed)}, Priority: {priority}, Depth: {depth}, Return Code: {response.getcode()}")
        except HTTPError as e:
            logger.info(f"HTTPError {e.code} for {linkToParse}; skipping")
        except UnicodeEncodeError:
            logger.info(f"Dropping non-ASCII URL that cannot be encoded: {linkToParse}")
        except Exception as e:
            logger.exception(f"Unexpected error for {linkToParse}: {e}")


def main():
    logging.basicConfig(filename='webcrawler.log', level=logging.INFO)
    logger.info("Starting web crawler")

    # get input from user for first search
    searchTerm = input()
    logger.info(f"User submitted {searchTerm}")

    with DDGS() as ddgs:
        results = ddgs.text(searchTerm, max_results=3)
        for r in results:
            parsedLink = urlparse(r["href"])
            base = parsedLink.scheme + "://" + parsedLink.netloc
            parseLink(r["href"], base, -1)

    threads = []
    for _ in range(25):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    for i in range(25):
        threads[i].join()

    print(queue.qsize())
    

if __name__ == '__main__':
    main()