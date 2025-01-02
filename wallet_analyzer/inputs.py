# Import libraries
import os
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv()

# Custom scrapy settings
custom_scrapy_settings = {
    "FEED_EXPORT_ENCODING": "utf-8", # UTF-8 deals with all types of characters
    "RETRY_TIMES": 3, # Retry failed requests up to 3 times
    "AUTOTHROTTLE_ENABLED": False, # Disables the AutoThrottle extension (recommended to be used if you are not using proxy services)
    "RANDOMIZE_DOWNLOAD_DELAY": False, # Should not be used with proxy services. If enabled, Scrapy will wait a random amount of time (between 0.5 * DOWNLOAD_DELAY and 1.5 * DOWNLOAD_DELAY) while fetching requests from the same website
    "CONCURRENT_REQUESTS": 15, # The maximum number of concurrent (i.e. simultaneous) requests that will be performed by the Scrapy downloader
    "DOWNLOAD_TIMEOUT": 120, # Setting the timeout parameter to 60 seconds as per the ScraperAPI documentation
    "ROBOTSTXT_OBEY": False, # Don't obey the Robots.txt rules
    "LOG_LEVEL": "DEBUG", # Set the level of logging to DEBUG
    # Zyte settings
    "DOWNLOAD_HANDLERS": {
        "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
        "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
    },
    "DOWNLOADER_MIDDLEWARES": {
        "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000,
    },
    "REQUEST_FINGERPRINTER_CLASS": "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter",
    "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    "ZYTE_API_KEY": os.getenv("ZYTE_API_KEY"),
    "ZYTE_API_LOG_REQUESTS": True,
    "ZYTE_API_TRANSPARENT_MODE": True,
    "ZYTE_API_SKIP_HEADERS": ["Cookie", "User-Agent"],
    "ZYTE_API_RETRY_POLICY": "retry_policies.CUSTOM_RETRY_POLICY"
}