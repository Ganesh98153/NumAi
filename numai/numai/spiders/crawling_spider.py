
import os
import re
from urllib.parse import urlparse, urlunparse
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.crawler import CrawlerProcess

class UrlCrawlerSpider(scrapy.Spider):
    name = "urlcrawler"
    start_urls = ["https://www.blackrock.com/us/individual/"]
    #start_urls = ["https://www.sharesansar.com/"]
    repetitive_pattern = re.compile(r'\/([a-zA-Z0-9\-]+)\/.*?\b\/\1\b')
    visited_urls = set()
    
    # custom_settings = {
    #     'DEPTH_LIMIT': 3
    # }
    def is_attachment(self, url):
        # Check if the URL points to an attachment
        return url.endswith(('.pdf', '.xls', '.xlsx', '.csv', '.doc', '.docx', '.ppt', '.pptx'))
    
    # def normalize_url(self, url):
    #     """Normalize the URL by sorting its path segments."""
    #     parsed_url = urlparse(url)
    #     path_segments = parsed_url.path.split('/')
    #     normalized_segments = sorted(set(path_segments))  # Sort and remove duplicates
    #     normalized_path = '/'.join(segment for segment in normalized_segments if segment)
    #     normalized_url = urlunparse(parsed_url._replace(path='/' + normalized_path))
    #     return normalized_url

    def parse(self, response):
        current_depth = response.meta.get('depth', 0)  # Get the current depth
        # Extract links using LinkExtractor, excluding social media links
        link_extractor = LinkExtractor(
            allow=["us/individual"],
            allow_domains=["blackrock.com"],
            deny=['sign-on','career', 'biographies','video'],
            deny_domains=['facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com', 'wikipedia.org', 'github.com',],
            
        )
        
        links = link_extractor.extract_links(response)
        
        for link in links:
            my_url = link.url
            removed_hash_my_url = str(my_url).split("#")[0]
            removed_hash_my_url = removed_hash_my_url.rstrip('/')
            #normalized_url = self.normalize_url(removed_hash_my_url)
            if self.repetitive_pattern.search(removed_hash_my_url):
                continue
            if removed_hash_my_url not in self.visited_urls:
                self.visited_urls.add(removed_hash_my_url)
                if self.is_attachment(removed_hash_my_url):
                    self.append_to_csv_attachment(my_url, current_depth + 1)
                else:
                    # Otherwise, follow the link to continue crawling
                    self.append_to_csv(my_url, current_depth + 1)
                    
                yield scrapy.Request(my_url, callback=self.parse, meta={'depth': current_depth + 1})

                # Append the URL to the master CSV file
                #self.append_to_csv(removed_hash_my_url, current_depth + 1)
            

    def append_to_csv(self, url, depth):
        # Append the URL to the master CSV file
        file_exists = os.path.isfile('master_links.csv')
        with open('master_links.csv', 'a') as f:
            if not file_exists:
                f.write('url,depth\n')  # Write header if file does not exist
            f.write(f"{url},{depth}\n")
            
    def append_to_csv_attachment(self, url, depth):
        # Append the URL to the attachment CSV file
        file_exists = os.path.isfile('master_links_attach.csv')
        with open('master_links_attach.csv', 'a') as f:
            if not file_exists:
                f.write('url,depth\n')  # Write header if file does not exist
            f.write(f"{url},{depth}\n")

# To run the spider
if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(UrlCrawlerSpider)
    process.start()
        
    
    
    