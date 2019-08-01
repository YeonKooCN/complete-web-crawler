import json
import time
import io
import pickle
import math
import re
import os
from bs4 import BeautifulSoup
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from nltk import word_tokenize
from corpus import Corpus

class Index:

    # The corpus directory name
    WEBPAGES_RAW_NAME = "WEBPAGES_RAW"
    # The corpus JSON mapping file
    JSON_FILE_NAME = os.path.join(".", WEBPAGES_RAW_NAME, "bookkeeping.json")
    # The downloaded urls text file name after crawling has finished
    DOWNLOADED_URLS_FILE_NAME = "downloaded_urls.txt"
    # The index file name with only valid webpages
    INDEX_VALID_FILE_NAME = "index_valid_only.pickle"
    # The index file name with all webpages
    INDEX_ALL_FILE_NAME = "index.pickle"

    def __init__(self):
        self.data = {}
        self.webpage_count = 0
        self.valid_webpages_only = True
        self.corpus = Corpus()
        # Set up the list of stopwords for filtering
        self.stops = set(stopwords.words('english'))

    def __len__(self):
        return len(self.data)

    def importIndex(self, index_pickle_file_name):
        if index_pickle_file_name == self.INDEX_ALL_FILE_NAME:
            self.valid_webpages_only = False
        t0 = time.time()
        print ("Loading pickle file....................")
        with open(index_pickle_file_name,'rb') as handle:
            self.data = pickle.load(handle)
        print ("Time to load pickle: ", time.time() - t0, "seconds.")

    def buildIndex(self, valid_only=True):
        # check if index is not empty
        if self.data:
            self.data.clear()
            self.webpage_count = 0

        self.valid_webpages_only = valid_only
        pages = self.postingsList(self.valid_webpages_only)
        
        # Go through every page
        for docID in pages:
            for word in pages[docID]:
                if word in self.data:
                    self.data[word][docID] = pages[docID][word]
                else:
                    self.data[word] = {}
                    self.data[word][docID] = pages[docID][word]
        
        # change index values from term frequency to tf-idf score
        self.calculateDocScores()

    # Calculates the score for each document in the index given a query using
    # tf-idf weights 
    def calculateDocScores(self):
        for word in self.data:
            for docID in self.data[word]:
                self.data[word][docID] = (1 + math.log10(self.data[word][docID])) \
                                                  * (math.log10(self.webpage_count / len(self.data[word])))

    # Create an index of pages listed by docID containing a listing of all their terms
    def postingsList(self, valid_only):
        pages = {}
        if valid_only:
            with open(self.DOWNLOADED_URLS_FILE_NAME, "r") as file:
                for url in file:
                    file_name = self.corpus.get_file_name(url)
                    if file_name is not None:
                        self.webpage_count += 1
                        docID_list = file_name.split("\\")[-2:]
                        docID = docID_list[0] + "/" + docID_list[1]
                        pages[docID] = self.tokenizeFile(file_name)
        else:
            with open(self.JSON_FILE_NAME, "r") as file:
                try:
                    json_data = json.load(file)
                except ValueError:
                    json_data = {}
            for docID in json_data:
                self.webpage_count += 1
                pages[docID] = self.tokenizeFile(self.WEBPAGES_RAW_NAME + "/" + docID)
                
        return pages

    # Parse and tokenize each file
    def tokenizeFile(self, folder_path):
        t0 = time.time()
        tokenizer = RegexpTokenizer(r'\w+')
        wordsdict = {}

        with io.open(folder_path, 'r', encoding='utf-8') as doc:
            data = doc.read().encode('utf-8')
        # Parse it out with BS4
        soup = BeautifulSoup(data, 'html.parser')
        # Strip out some basic garbage
        [s.extract() for s in soup(['style', 'script', '[document]'])]
        # Only concerned with these tags
        for tag in ['b', 'h1', 'h2', 'h3', 'title', 'body', 'strong']:
            # For each tag we use find_all to get the text
            for text in soup.find_all(tag):
                # Get the text and throw out anything that isn't a letter
                text = text.getText()
                text = re.sub(r'[^a-zA-Z1-9]', " ", text)
                # Tokenize text using Regexp Tokenizer
                text_r = tokenizer.tokenize(text)
                # Convert all tokens to lower case
                text_l = [w.lower() for w in text_r]
                # Remove stopwords
                text_f = [word for word in text_l if word not in self.stops]
                # Each word is added to the words dict
                for word in text_f:
                    if word in wordsdict.keys():
                        # Normalize term frequency by dividing tf by the total number of indexable words in the document
                        wordsdict[word] += (1 / len(text_f))
                    # Create a new word in the words dict when there is no occurence
                    else:
                        # Normalize term frequency by dividing tf by the total number of indexable words in the document
                        wordsdict[word] = (1 / len(text_f))
        print ("Count: ", self.webpage_count)
        print ("Folder: ", folder_path)
        return wordsdict

    # The class is for writing pickle file to be store on local subdirectory
    def writeToFile(self):
        index_pickle_file_name = self.getIndexFileName()
        print ("Writing to file................")
        with open(index_pickle_file_name, 'wb') as handle:
            pickle.dump(self.data, handle)
        print ("Wrtiting to file completed!")
        print (f"Index file name: {index_pickle_file_name}. The file can be found under the subdirectory.")

    def getDocCount(self):
        return self.webpage_count

    def getIndexFileName(self):
        return self.INDEX_VALID_FILE_NAME if self.valid_webpages_only else self.INDEX_ALL_FILE_NAME
        

    
