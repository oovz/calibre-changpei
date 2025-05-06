from datetime import datetime
import re
import json
from queue import Queue, Empty
from urllib.parse import parse_qs, urlparse, quote

from calibre.ebooks.metadata.book.base import Metadata
from calibre.ebooks.metadata.sources.base import Source
from calibre.ebooks.chardet import xml_to_unicode
from calibre.utils.cleantext import clean_ascii_chars
from lxml import etree
from lxml.html import tostring

# use id without "CP" prefix
CHANGPEI_BOOK_URL = "https://www.gongzicp.com/novel-%s.html"
CHANGPEI_BOOK_URL_PATTERN = re.compile(".gongzicp\\.com\\/novel-(\\d+).html")

# note that string passed-in will need to be url-encoded
CHANGPEI_SEARCH_API = "https://www.gongzicp.com/webapi/search/novels?k=%s&page=1"
# use id without "CP" prefix
CHANGPEI_NOVEL_INFO = "https://www.gongzicp.com/webapi/novel/novelInfo?id=%s"
CHANGPEI_CHAPTER_LIST = "https://www.gongzicp.com/webapi/novel/chapterGetList?nid=%s"

CHANGPEI_NO_BOOKCOVER_URL = "https://resourcecp-cdn.gongzicp.com/files/images/nocover.jpg"

PROVIDER_ID = "changpei"
PROVIDER_VERSION = (1, 0, 0)
PROVIDER_AUTHOR = "Otaro"

SOURCE_PUBLISHER = "长佩文学"

# class SearchResultIndexMetadataCompareKeyGen:
#     def __init__(self, mi):
#         self.extra = getattr(mi, 'search_result_index', 0)

#     def compare_to_other(self, other):
#         return self.extra - other.extra

#     def __eq__(self, other):
#         return self.compare_to_other(other) == 0

#     def __ne__(self, other):
#         return self.compare_to_other(other) != 0

#     def __lt__(self, other):
#         return self.compare_to_other(other) < 0

#     def __le__(self, other):
#         return self.compare_to_other(other) <= 0

#     def __gt__(self, other):
#         return self.compare_to_other(other) > 0

#     def __ge__(self, other):
#         return self.compare_to_other(other) >= 0


def parse_html(raw):
    try:
        from html5_parser import parse
    except ImportError:
        # Old versions of calibre
        import html5lib

        return html5lib.parse(raw, treebuilder="lxml", namespaceHTMLElements=False)
    else:
        return parse(raw)


# a metadata download plugin
class Changpei(Source):
    name = SOURCE_PUBLISHER  # Name of the plugin
    description = "Downloads metadata and covers from Changpei (gongzicp.com)."
    supported_platforms = [
        "windows",
        "osx",
        "linux",
    ]  # Platforms this plugin will run on
    author = PROVIDER_AUTHOR  # The author of this plugin
    version = PROVIDER_VERSION  # The version number of this plugin
    minimum_calibre_version = (5, 0, 0)
    capabilities = frozenset(["identify", "cover"])
    touched_fields = frozenset(
        [
            "title",
            "authors",
            "identifier:" + PROVIDER_ID,
            "comments",
            "publisher",
            "languages",
            "tags",
            "pubdate",
        ]
    )
    has_html_comments = True
    supports_gzip_transfer_encoding = True
    can_get_multiple_covers = True

    def __init__(self, *args, **kwargs):
        Source.__init__(self, *args, **kwargs)
        
    def get_json_from_response(self, raw):
        """Parse JSON response from API"""
        return json.loads(raw)
        
    def get_first_chapter_publish_date(self, log, novel_id, timeout=30):
        """Get the publish date of the first chapter from the chapter list API"""
        br = self.browser
        api_url = CHANGPEI_CHAPTER_LIST % novel_id
        
        try:
            raw = br.open_novisit(api_url, timeout=timeout).read().strip()
            
            # Parse JSON response
            data = self.get_json_from_response(raw.decode('utf-8'))
            
            # Check if API call was successful
            if 'code' in data and data['code'] == 200 and 'data' in data and 'list' in data['data']:
                chapters = data['data']['list']
                
                # If no chapters available
                if not chapters:
                    log.info(f"No chapters found for novel_id: {novel_id}")
                    return None
                
                # Get the first chapter (usually at index 0)
                # Some novels might have chapters sorted differently, so we find the chapter with order="1"
                first_chapter = None
                for chapter in chapters:
                    if chapter.get('order') == '1' and chapter.get('type') == 'item':
                        first_chapter = chapter
                        break
                
                # If we didn't find a chapter with order="1", just use the first chapter in the list
                if not first_chapter and chapters:
                    for chapter in chapters:
                        if chapter.get('type') == 'item':
                            first_chapter = chapter
                            break
                
                if first_chapter:
                    publish_date = first_chapter.get('public_date')
                    if publish_date:
                        log.info(f"First chapter publish date for novel_id {novel_id}: {publish_date}")
                        try:
                            return datetime.strptime(publish_date, "%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            log.error(f"Invalid date format: {publish_date}")
            
            return None
        except Exception as e:
            log.exception(f"Error getting first chapter publish date: {e}")
            return None

    def get_book_url(self, identifiers):
        cp_id = identifiers.get(PROVIDER_ID, None)
        if cp_id:
            return (PROVIDER_ID, cp_id, CHANGPEI_BOOK_URL % cp_id)
        return None

    def get_book_url_name(self, idtype, idval, url):
        return SOURCE_PUBLISHER

    #fixme
    def get_cached_cover_url(self, identifiers):
        # there's no cached cover url for changpei
        # cp_id = identifiers.get(PROVIDER_ID, None)
        # if cp_id:
        #     return JINJIANG_BOOKCOVER_URL % cp_id
        return None

    def id_from_url(self, url):
        res = CHANGPEI_BOOK_URL_PATTERN.findall(url)
        if len(res) == 1:
            return res[0]
        return None
    
    # def identify_results_keygen(self, title=None, authors=None, identifiers={}):
    #     def keygen(mi):
    #         return SearchResultIndexMetadataCompareKeyGen(mi)
    #     return keygen

    def identify(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers={},
        timeout=30,
    ):
        cp_id = identifiers.get(PROVIDER_ID, None)
        if cp_id:
            # Use API to get book details if we have an ID
            api_url = CHANGPEI_NOVEL_INFO % cp_id
            log.info("identify with changpei id (%s) from API url: %s" % (cp_id, api_url))
            br = self.browser
            try:
                raw = br.open_novisit(api_url, timeout=timeout).read().strip()
                
                # Parse JSON response
                data = self.get_json_from_response(raw.decode('utf-8'))
                
                # Check if API call was successful
                if 'code' in data and data['code'] == 200 and 'data' in data:
                    novel_data = data['data']
                    
                    # Extract metadata from API response based on the sample provided
                    title = novel_data.get('novel_name', '')
                    author = novel_data.get('author_nickname', '')
                    
                    # Get description - the API returns HTML formatted description
                    desc = novel_data.get('novel_info', '')
                    if not desc:
                        desc = novel_data.get('novel_desc', '')
                    
                    # Parse tags from the tag_list array
                    tags = novel_data.get('tag_list', [])
                    
                    # Get cover URL
                    cover_url = novel_data.get('novel_cover', '')
                    if not cover_url:
                        cover_url = CHANGPEI_NO_BOOKCOVER_URL
                    
                    # Get publish date from the first chapter's publish date instead of creation time
                    bPublishDate = self.get_first_chapter_publish_date(log, cp_id, timeout)
                    
                    # Create metadata object
                    mi = Metadata(title, [author])
                    mi.identifiers = {PROVIDER_ID: cp_id}
                    mi.comments = desc
                    mi.publisher = SOURCE_PUBLISHER
                    mi.language = "zh_CN"
                    mi.tags = tags
                    mi.url = CHANGPEI_BOOK_URL % cp_id
                    mi.cover = cover_url
                    mi.pubdate = bPublishDate  # This could be None if no chapters are published
                    
                    result_queue.put(mi)
                    return None
                else:
                    log.error("API call failed or no data found for ID: %s" % cp_id)
            except Exception as e:
                log.exception("Error using API: %s" % e)

            return None

        # Use search API
        # @TODO support pagination and sorting
        normalized_title = "" if title is None else title
        search_api_url = CHANGPEI_SEARCH_API % quote(normalized_title, encoding="utf-8")

        log.info("search with title (%s) from API url: %s" % (normalized_title, search_api_url))

        br = self.browser
        try:
            raw = br.open_novisit(search_api_url, timeout=timeout).read().strip()
            
            # Parse JSON search response
            data = self.get_json_from_response(raw.decode('utf-8'))
            
            # Check if search API call was successful
            if 'code' in data and data['code'] == 200 and 'data' in data and 'list' in data['data']:
                books = data['data']['list']
                log.info("found %d books from search API, first page has %d" % (data['data']['count'], len(books)))
                
                for i, book in enumerate(books):
                    novel_id = book.get('novel_id', '')
                    if not novel_id:
                        log.error("[%d] can't find book id from search result" % i)
                        continue
                    
                    novel_id = str(novel_id)  # Ensure it's a string
                    
                    # Extract metadata from search result
                    bTitle = book.get('novel_name', '')
                    bAuthor = book.get('novel_author', '')
                    
                    # Get cover URL
                    bCover = book.get('novel_cover', CHANGPEI_NO_BOOKCOVER_URL)
                    # Remove small style suffix if present
                    if "?x-oss-process=style/small" in bCover:
                        bCover = bCover.split("?x-oss-process=style/small")[0]
                    
                    # Get tags
                    bTags = book.get('novel_tag_arr', [])
                    
                    # Create book URL
                    bURL = CHANGPEI_BOOK_URL % novel_id
                    
                    mi = Metadata(bTitle, [bAuthor])
                    mi.identifiers = {PROVIDER_ID: novel_id}
                    mi.publisher = SOURCE_PUBLISHER
                    mi.language = "zh_CN"
                    mi.tags = bTags
                    mi.url = bURL
                    mi.cover = bCover
                    mi.comments = book.get('novel_desc', '')
                    mi.pubdate = None  # No publish date available from search API
                    
                    log.info(
                        "[%d] id (%s) title (%s) author (%s)" % (i, novel_id, bTitle, bAuthor)
                    )
                    
                    result_queue.put(mi)
                
                return None
            else:
                log.error("Search API call failed or returned invalid data")
        except Exception as e:
            log.exception("Error using search API: %s" % e)
        
        return None

    def download_cover(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers={},
        timeout=30,
        get_best_cover=False,
    ):

        cp_id = identifiers.get(PROVIDER_ID, None)
        if cp_id is None:
            log.info("No id found, running identify")
            rq = Queue()
            self.identify(
                log, rq, abort, title=title, authors=authors, identifiers=identifiers
            )
            if abort.is_set():
                return

            results = []
            while True:
                try:
                    results.append(rq.get_nowait())
                except Empty:
                    break

            if len(results) == 0:
                log.error("No result after running identify")
                return

            # get the first result
            cp_id = results[0].identifiers.get(PROVIDER_ID, None)

        if cp_id is None:
            log.error("No id found after running identify")
            return
        
        br = self.browser
        
        # First try to get cover from API
        api_url = CHANGPEI_NOVEL_INFO % cp_id
        log.info("Getting cover from API url: %s" % api_url)
        
        try:
            raw = br.open_novisit(api_url, timeout=timeout).read().strip()
            
            # Parse JSON response
            data = self.get_json_from_response(raw.decode('utf-8'))
            
            # Check if API call was successful and get cover URL
            if 'code' in data and data['code'] == 200 and 'data' in data:
                novel_data = data['data']
                
                # Get the cover URL from the correct field based on sample
                cover_url = novel_data.get('novel_cover', '')
                
                if cover_url:
                    log.info("Found cover URL from API: %s" % cover_url)
                    try:
                        cdata = br.open_novisit(cover_url, timeout=timeout).read()
                        if cdata:
                            result_queue.put((self, cdata))
                            return
                    except Exception as e:
                        log.exception("Failed to download cover from API: %s" % e)
            else:
                log.error("API call failed or no data found for ID when downloading cover: %s" % cp_id)
        except Exception as e:
            log.exception("Error using API for cover: %s" % e)

if __name__ == "__main__":
    # To run these test use: 
    # calibre-customize -b "./src"
    # calibre-debug -e "./src/__init__.py"
    from calibre.ebooks.metadata.sources.test import (
        test_identify_plugin,
        title_test,
        authors_test,
        tags_test,
    )

    # TODO: add test cases for cover download
    test_identify_plugin(
        Changpei.name,
        [
            (
                # TODO: custom cover test
                {
                    "identifiers": {"changpei": "1312354"},
                },
                [title_test("降水概率百分百", exact=True), authors_test(["芥菜糊糊"]), tags_test(['小甜饼', '年下', '搞笑', '攻胸真的很大', 'he', '受说话真的很色', '甜宠'])],
            ),
            (
                # TODO: custom cover test
                {
                    "identifiers": {"changpei": "154936"},
                },
                [title_test("荒野植被", exact=True), authors_test(["麦香鸡呢"]), tags_test(['破镜重圆'])],
            ),
            (
                {
                    "title": "降水"
                },
                [title_test("降水概率百分百", exact=True), authors_test(["芥菜糊糊"])],
            ),
        ],
    )
