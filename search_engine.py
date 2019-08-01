import pickle
import time
import sys
import os
import json
import io
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from nltk.tokenize import RegexpTokenizer


class SearchEngine:

    """
    The next two lines of code are from Project 2 corpus.py
    """
    WEBPAGES_RAW_NAME = "WEBPAGES_RAW"
    JSON_FILE_NAME = os.path.join(".", WEBPAGES_RAW_NAME, "bookkeeping.json")

    def __init__(self, index):
        self.index = index.data
        self.query = ""
        self.query_word_list = []
        self.single_word_result = []
        # Next few lines of codes are borrowed from Project 2 corpus.py
        # Author: @MustafaIbrahim
        self.file_url_map = json.load(open(self.JSON_FILE_NAME), encoding="utf-8")
        self.url_file_map = dict()
        for key in self.file_url_map:
            self.url_file_map[self.file_url_map[key]] = key
        # End of borrowed code


    def startEngine(self):
        """
        This method initiates the search egnine
        """
        print("------------------------------------------------------------")
        print("Initiating Search Engine..............")
        self.searchQuery()


    def searchQuery(self):
        """
        This method prompts user to enter search term
        """
        while True:
            print ("Please enter a search query:")
            print ("Enter number '0' to exit the program")
            # Convert all letters in the search term to lower case
            searchTerm = str(input()).lower()
            self.queryConverter(searchTerm)
            self.query = searchTerm
            print ("Search query entered:", self.query)
            self.printResult()
            print ("******************************************************")
            if self.query == "0":
                print ("Program shutting down... Have a good day!")
                print("------------------------------------------------------")
                
                sys.exit(0)


    def queryConverter(self, query):
        """
        Converts multiple word query into a list composed of single words
        """
        tokenizer = RegexpTokenizer(r'\w+')
        # Save the tokenized result to query_word_list
        self.query_word_list = tokenizer.tokenize(query)
        return self.query_word_list

    
    def similarityScore(self):
        """
        This function takes in a list of query and return a dictionary of cosine 
        scores in such fashion [docID]:'score'.
        For the sake of simplicity, similarity scores are only calculated using
        tf-idf values stored inside the index. This method is based on "Unweighted Query Terms"
        pseudo code covered in Week 7 Slides page 6
        """
        sim_score = {}
        # for each query term t
        for word in self.query_word_list:
            if word in self.index:
                # for each pair in postings list
                for docID in self.index[word]:
                    if docID in sim_score:
                        # do Scores[d] += document weight
                        score = self.index[word][docID]
                        sim_score[docID] += score
                    else:
                        sim_score[docID] = self.index[word][docID]
        return sim_score


    def rankDoc(self):
        """
        This method helps ranking the results and print out the ranked search results.
        """
        s_map = self.file_url_map
        score = self.similarityScore()
        doc = []
        i = 1
        count = 0
        sorted_score = sorted(score.items(), key=lambda x: (x[1]), reverse=True)
        # Iterate through every docID in the matching results pool
        for docID in sorted_score:
            # Append the URL information to the dictionary
            doc.append(s_map[docID[0]])
            count += 1
        for item in doc:
            print("[", i, "]", item)
            self.get_webpage_title(item)
            i += 1
            if i == 21:
                break
        print ("Total number of results:", count)

        return sorted_score


    def get_webpage_title(self, url):
        """
        This method takes in a single url and print title of the webpage parsed by BeautifulSoup
        """
        if url in self.url_file_map:
            docID = self.url_file_map[url]
        path = self.WEBPAGES_RAW_NAME + "/" + docID
        with io.open(path, 'r', encoding='utf-8') as doc:
            data = doc.read().encode('utf-8')
        soup = BeautifulSoup(data, 'html.parser')
        title_data = "None"
        if soup.title is not None:
            title_data = soup.title.string
        print ("Webpage Title:", title_data)
        print ("------------------------------------------------------------")
        


    def printResult(self):
        print ("Printing top 15 results for query:", self.query)
        self.rankDoc()

        
