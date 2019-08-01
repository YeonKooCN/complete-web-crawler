import logging
import re
import os
import lxml.html
from urllib.parse import urlparse
from corpus import Corpus
from collections import Counter, defaultdict

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """
    LONG_URL_LENGTH_LIMIT = 400
    REPEATING_SUBDIRECTORIES_LIMIT = 10
    ONE_QUERY_PARAM_FREQ_LIMIT = 200
    MULTIPLE_QUERY_PARAM_FREQ_LIMIT = 100

    def __init__(self, frontier):
        self.frontier = frontier
        self.corpus = Corpus()
        self.url_freq = defaultdict(int)

        # Analytics
        self.subdomain_url_count = defaultdict(int)
        self.most_valid_out_links_url = ""
        self.most_valid_out_links = 0
        self.downloaded_urls = set()
        self.invalid_urls = set()
        self.current_invalid_reason = ""

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.fetch_url(url)
            valid_out_links = 0

            for next_link in self.extract_next_links(url_data):
                if self.corpus.get_file_name(next_link) is not None:
                    if self.is_valid(next_link):
                        parsed = urlparse(next_link)
                        
                        self.frontier.add_url(next_link)
                        self.subdomain_url_count[parsed.hostname] += 1
                        valid_out_links += 1
                        self.downloaded_urls.add(next_link)
                    else:
                        self.invalid_urls.add((next_link, self.current_invalid_reason))

            if valid_out_links > self.most_valid_out_links:
                self.most_valid_out_links_url = url
                self.most_valid_out_links = valid_out_links
                
        # create text file of downloaded urls only if it does not exist
        if not os.path.isfile("downloaded_urls.txt"):
            self.analytics()

    def fetch_url(self, url):
        """
        This method, using the given url, should find the corresponding file in the corpus and return a dictionary
        containing the url, content of the file in binary format and the content size in bytes
        :param url: the url to be fetched
        :return: a dictionary containing the url, content and the size of the content. If the url does not
        exist in the corpus, a dictionary with content set to None and size set to 0 can be returned.
        """
        url_data = {
            "url": url,
            "content": None,
            "size": 0
        }

        file_name = self.corpus.get_file_name(url)
        if file_name is not None:
            with open(file_name, "rb") as file:
                file_content = b""
                for content in file:
                    file_content += content
                url_data["content"] = file_content
            url_data["size"] = os.path.getsize(file_name)
        
        return url_data

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        outputLinks = []

        html_document = lxml.html.document_fromstring(url_data["content"])
        html_document.make_links_absolute(url_data["url"])
        for element, attribute, link, pos in html_document.iterlinks():
            if attribute == "href":
                outputLinks.append(link)

        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        subdirectories = Counter(parsed.path.split("/"))
        shp_url = parsed.scheme + "://" + parsed.hostname + parsed.path

        # checks if the scheme is http or https
        if parsed.scheme not in set(["http", "https"]):
            self.current_invalid_reason = "URL scheme is not http or https."
            return False

        # filters out long urls which are usually invalid
        if len(parsed.path) > self.LONG_URL_LENGTH_LIMIT:
            self.current_invalid_reason = "URL is unusually long. May be a trap."
            return False

        # filters out traps with repeating subdirectories
        for sub_dir in subdirectories:
            if subdirectories[sub_dir] > self.REPEATING_SUBDIRECTORIES_LIMIT:
                self.current_invalid_reason = "Subdirectories in URL repeat too many times. May be a trap."
                return False

        # checks if crawler is making progess
        if shp_url in self.url_freq:
            # filter out traps with one query parameter (ex: session IDs)
            if parsed.query != "" and "&" not in parsed.query \
               and self.url_freq[shp_url] > self.ONE_QUERY_PARAM_FREQ_LIMIT:
                self.current_invalid_reason = "Dynamic URL caused crawler to not make progress."
                return False
            # filter out traps with multiple query parameters (ex: calendar traps)
            elif "&" in parsed.query and self.url_freq[shp_url] > self.MULTIPLE_QUERY_PARAM_FREQ_LIMIT:
                self.current_invalid_reason = "Dynamic URL caused crawler to not make progress."
                return False
        else:
            # filters out the http or https url duplicate of an already added
            # url to the frontier
            if parsed.scheme == "http" \
               and "https://" + parsed.hostname + parsed.path in self.url_freq:
                self.current_invalid_reason = "https version of url already processed."
                return False
            else:
                if "http://" + parsed.hostname + parsed.path in self.url_freq:
                    self.current_invalid_reason = "http version of url already processed."
                    return False
        self.url_freq[shp_url] += 1
        
        try:
            if ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower()):
                return True
            else:
                self.current_invalid_reason = "URL not in ics.uci.edu domain or is a file."
                return False

        except TypeError:
            print("TypeError for ", parsed)
            self.current_invalid_reason = "TypeError for " + parsed
            return False

    def analytics(self):
        print("Creating text file with download urls...")
        with open("downloaded_urls.txt", "w") as file:
            for url in self.downloaded_urls:
                file.write(url + "\n")
            file.write("\n")
        print("Text file with downloaded urls created.")
        print("File name: downloaded_urls.txt")
