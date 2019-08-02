import atexit
import logging
import time
import os

from crawler import Crawler
from frontier import Frontier

from index import Index
from search_engine import SearchEngine

if __name__ == "__main__":
    
    valid_only = True
    build_index = False

    # Ask the user how they would like to buil the index
    while True:
        user_input = str(input("Would you like to search through only valid websites? (Y/N): "))
        if user_input not in ["Y", "N", "y", "n"]:
            print("Invalid input.")
        else:
            if (user_input == "Y" or user_input == "y"):
                if not os.path.isfile("index_valid_only.pickle"):
                    # Crawl the provided webpages
                    # Configures basic logging
                    logging.basicConfig(format='%(asctime)s (%(name)s) %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                                        level=logging.INFO)

                    # Instantiates frontier and loads the last state if exists
                    frontier = Frontier()
                    frontier.load_frontier()
                    # Registers a shutdown hook to save frontier state upon unexpected shutdown
                    atexit.register(frontier.save_frontier)

                    # Instantiates a crawler object and starts crawling
                    crawler = Crawler(frontier)
                    crawler.start_crawling()
                    build_index = True
            else:
                valid_only = False
                if not os.path.isfile("index.pickle"):
                    build_index = True
            break

    index = Index()
    # Build the index if it does not exist
    if build_index:
        print("Building index...")
        total_t0 = time.time()
        index.buildIndex(valid_only)
        index.writeToFile()
        print ("-------------------------------------------------------------------------")
        print ("Size of index (unique words): ", len(index))
        print ("Total running time: ", (time.time() - total_t0), "seconds")
        print("Total number of webpages processed:", index.getDocCount())
        print ("-------------------------------------------------------------------------")
    else:
        if valid_only:
            index.importIndex("index_valid_only.pickle")
        else:
            index.importIndex("index.pickle")


    # Start the search engine
    search_engine = SearchEngine(index)
    search_engine.startEngine()
    

    
