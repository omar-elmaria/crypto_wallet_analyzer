# Import libraries
import scrapy
from inputs import custom_scrapy_settings
from helper_functions import *
import re
import json
import pandas as pd


class DexScreenerTopTradersSpider(scrapy.Spider):
    name = "dex_screener_top_traders"
    custom_settings = custom_scrapy_settings.copy() # Define the custom settings of the spider
    custom_settings["LOG_FILE"] = "dex_screener_top_traders.log"
    custom_settings["FEEDS"] = {
        'dex_screener_top_traders.json': {
            'format': 'json',
            'overwrite': True
        }
    }
    
    def start_requests(self):
        # Open the JSON file dex_screener_top_gainers.json
        self.logger.info("Opening the JSON file dex_screener_top_gainers.json")
        with open("dex_screener_top_gainers.json", "r") as f:
            # Load the JSON file
            df_top_gainers = json.load(f)
        
            # Change the top_gainers to a pandas data frame
            df_top_gainers = pd.DataFrame(df_top_gainers)

        # Send a request to each individual asset URL
        for asset_name, asset_url in zip(df_top_gainers["asset_name"], df_top_gainers["asset_url"]):
            # Send a request to the asset URL
            self.logger.info(f"Sending a request to the asset name {asset_name} with URL: {asset_url}")
            yield scrapy.Request(
                url=asset_url,
                callback=self.parse_top_traders,
                meta={
                    "zyte_api_automap": {
                        "browserHtml": True,
                        "javascript": True,
                        "actions": [
                            # Wait for the Top Traders Button
                            {
                                "action": "waitForSelector",
                                "timeout": 10,
                                "onError": "return",
                                "selector": {
                                    "type": "xpath",
                                    "value": "//button[text() = 'Top Traders']",
                                    "state": "attached"
                                }
                            },
                            # Click on the Top Traders Button
                            {
                                "action": "click",
                                "delay": 0,
                                "button": "left",
                                "onError": "return",
                                "selector": {
                                    "type": "xpath",
                                    "value": "//button[text() = 'Top Traders']",
                                    "state": "attached"
                                }
                            },
                        ]
                    },

                    # Meta data
                    "asset_name": asset_name,
                    "asset_url": asset_url
                }
            )

    def parse_top_traders(self, response):
        # Log a status message
        self.logger.info(f"Parsing the response of the asset {response.meta['asset_name']} with URL {response.meta['asset_url']}")
        
        # Extract the top traders list of results
        top_trader_results = response.xpath("//span[text() = 'bought']/../../following-sibling::div")

        # Extract the meta data
        asset_name = response.meta["asset_name"]
        asset_url = response.meta["asset_url"]

        # Parse the response
        for tr in top_trader_results:
            # Extract the trader bought amount in USD
            trader_bought_usd = tr.xpath(".//span[@class='chakra-text custom-rcecxm']/text()").get()
            trader_bought_usd = helper_normalize_numbers_in_txn_data(value=trader_bought_usd)

            # Extract the trader bought amount in crypto units
            trader_bought_crypto = tr.xpath(".//span[@class='chakra-text custom-rcecxm']/following-sibling::span/span[1]/text()").get()
            trader_bought_crypto = helper_normalize_numbers_in_txn_data(value=trader_bought_crypto)
            
            # Extract the number of buy TXNs
            trader_buy_txns = helper_normalize_numbers_in_txn_data(value=tr.xpath(".//span[@class='chakra-text custom-rcecxm']/following-sibling::span/span[3]/text()").get())
            trader_buy_txns = helper_treat_none_before_data_type_change(value=trader_buy_txns, data_type="int")

            # Extract the trader sold amount in USD
            trader_sold_usd = tr.xpath(".//span[@class='chakra-text custom-dv3t8y']/text()").get()
            trader_sold_usd = helper_normalize_numbers_in_txn_data(value=trader_sold_usd)
            
            # Extract the trader sold amount in crypto units
            trader_sold_crypto = tr.xpath(".//span[@class='chakra-text custom-dv3t8y']/following-sibling::span/span[1]/text()").get()
            trader_sold_crypto = helper_normalize_numbers_in_txn_data(value=trader_sold_crypto)
            
            # Extract the number of sell TXNs
            trader_sell_txns = helper_normalize_numbers_in_txn_data(value=tr.xpath(".//span[@class='chakra-text custom-dv3t8y']/following-sibling::span/span[3]/text()").get())
            trader_sell_txns = helper_treat_none_before_data_type_change(value=trader_sell_txns, data_type="int")
            
            # Extract the PnL
            trader_pnl = helper_normalize_numbers_in_txn_data(value=tr.xpath(".//div[@class='custom-1e9y0rl']/text()").get())
            trader_pnl = helper_normalize_numbers_in_txn_data(value=trader_pnl)

            # Extract the SOL scan URL
            sol_scan_url = tr.xpath(".//a[@aria-label='Open in block explorer']/@href").get()

            # Extract the wallet_address
            wallet_address = re.findall(pattern=r"(?<=account\/).*", string=sol_scan_url)[0]

            # Yield the output dictionary
            output_dict = {
                "asset_name": asset_name,
                "asset_url": asset_url,
                "trader_bought_usd": trader_bought_usd,
                "trader_bought_crypto": trader_bought_crypto,
                "trader_buy_txns": trader_buy_txns,
                "trader_sold_usd": trader_sold_usd,
                "trader_sold_crypto": trader_sold_crypto,
                "trader_sell_txns": trader_sell_txns,
                "trader_pnl": trader_pnl,
                "sol_scan_url": sol_scan_url,
                "wallet_address": wallet_address
            }

            yield output_dict