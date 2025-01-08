# Import libraries
import scrapy
from inputs import custom_scrapy_settings
import json
import pandas as pd

class GmgnAiWalletScreenerSpider(scrapy.Spider):
    name = "gmgn_ai_wallet_screener"
    custom_settings = custom_scrapy_settings.copy() # Define the custom settings of the spider
    custom_settings["LOG_FILE"] = "gmgn_ai_wallet_screener.log"
    custom_settings["FEEDS"] = {
        'gmgn_ai_wallet_screener.json': {
            'format': 'json',
            'overwrite': True
        }
    }
    base_url = "https://gmgn.ai/sol/address/{wallet_address}"
    max_retries = 1
    spider_actions = [
        {
            "action": "waitForSelector",
            "timeout": 10,
            "onError": "return",
            "selector": {
                "type": "xpath",
                "value": "//div[text() = 'Last 7D PnL']",
                "state": "attached"
            }
        },
        {
            "action": "click",
            "onError": "return",
            "selector": {
                "type": "xpath",
                "value": "//div[text() = '30d']",
                "state": "attached"
            }
        }
    ]
    
    def start_requests(self):
        # Open the JSON file dex_screener_top_traders.json and convert it to a pandas data frame
        self.logger.info("Opening the JSON file dex_screener_top_traders.json")
        with open("dex_screener_top_traders.json", "r") as f:
            # Load the JSON file
            df_raw_data = json.load(f)
        
            # Change the top_gainers to a pandas data frame
            df_raw_data = pd.DataFrame(df_raw_data)

            # Drop all columns that end with _raw
            df_raw_data = df_raw_data.loc[:, ~df_raw_data.columns.str.endswith('_raw')]
        
        # Change the data types of trader_bought_usd, trader_bought_crypto, trader_buy_txns, trader_sold_usd, trader_sold_crypto, trader_sell_txns, trader_pnl to numeric
        df_raw_data.loc[:, "trader_bought_usd":"trader_pnl"] = df_raw_data.loc[:, "trader_bought_usd":"trader_pnl"].apply(pd.to_numeric).round(2)

        # Filter the data frame to only include the rows where trader_bought_usd and trader_sold_usd are not null, then sort the data frame by trader_pnl in descending order
        df_top_traders = df_raw_data[(df_raw_data["trader_bought_usd"].notnull()) & (df_raw_data["trader_sold_usd"].notnull())].sort_values(by="trader_pnl", ascending=False)

        # Fill the NA trader_pnl values with 0
        df_top_traders["trader_pnl"] = df_top_traders["trader_pnl"].fillna(0)

        # Calculate the trader's percentage pnl
        df_top_traders["trader_pct_pnl"] = round((df_top_traders["trader_pnl"] / df_top_traders["trader_bought_usd"]) * 100, 2)

        # Rank the traders by their absolute PnL
        df_top_traders["abs_pnl_rank"] = df_top_traders["trader_pnl"].rank(ascending=False)
        
        # Rank the traders by their percentage-based PnL
        df_top_traders["pct_pnl_rank"] = df_top_traders["trader_pct_pnl"].rank(ascending=False)

        # Filter for the top 250 traders
        df_wallets_to_analyze = df_top_traders[df_top_traders["pct_pnl_rank"] <= pow(10, 6)].reset_index(drop=True)

        # Extract the full list of wallets
        full_list_of_wallets = list(df_wallets_to_analyze["wallet_address"].unique())

        for idx, wl in enumerate(full_list_of_wallets):
            request_counter = 1
            self.logger.info(f"Sending a request to the wallet address: {wl}, which is wallet {idx + 1} out of {len(full_list_of_wallets)}. Try {request_counter} out of {self.max_retries}.")
            yield scrapy.Request(
                url=self.base_url.format(wallet_address=wl),
                callback=self.parse_wallet_data,
                meta={
                    "zyte_api_automap": {
                        "browserHtml": True,
                        "javascript": True,
                        "actions": self.spider_actions
                    },
                    "wallet_address": wl,
                    "request_counter": request_counter,
                    "wallet_count": idx + 1,
                    "tot_num_wallets": len(full_list_of_wallets)
                }
            )
    
    def parse_wallet_data(self, response):
        # Extract the meta data
        resp_wallet_address = response.meta["wallet_address"]
        resp_request_counter = response.meta["request_counter"]
        resp_wallet_count = response.meta["wallet_count"]
        resp_tot_num_wallets = response.meta["tot_num_wallets"]

        # Print the raw logs of the Zyte API
        self.logger.info(f"Raw logs of the Zyte API for wallet address {resp_wallet_address}, which is wallet {resp_wallet_count} out of {resp_tot_num_wallets} --> {response.raw_api_response['actions']}")

        # Check if the page has been fully loaded
        check_page_load = response.xpath("//div[text() = 'Last 7D PnL']").get()
        if check_page_load is None and resp_request_counter < self.max_retries:
            resp_request_counter += 1
            self.logger.error(f"The page has not been fully loaded for the wallet address: {resp_wallet_address}, which is wallet {resp_wallet_count} out of {resp_tot_num_wallets}. Retrying the request {resp_request_counter} out of {self.max_retries}. URL: {response.url}")
            yield scrapy.Request(
                url=response.url,
                callback=self.parse_wallet_data,
                meta={
                    "zyte_api_automap": {
                        "browserHtml": True,
                        "javascript": True,
                        "actions": self.spider_actions
                    },
                    "wallet_address": resp_wallet_address,
                    "request_counter": resp_request_counter,
                    "wallet_count": resp_wallet_count,
                    "tot_num_wallets": resp_tot_num_wallets
                },
                dont_filter=True
            )
        else:
            # Print a status message
            self.logger.info(f"Processing the stats of the wallet address: {resp_wallet_address}, which is wallet {resp_wallet_count} out of {resp_tot_num_wallets}.")

            # Extract the wallet's gross profit
            tot_gross_profit = response.xpath("//div[text() = 'Total PnL']//following-sibling::div/text()").get()

            # Extract the wallet's total ROI
            tot_roi = response.xpath("//div[text() = 'Last 7D PnL']//following-sibling::div/text()").get()

            # Extract the win rate
            win_rate = response.xpath("//div[text() = 'Win Rate']//following-sibling::div/text()").get()

            # Create the output dictionary
            output_dict = {
                "wallet_address": resp_wallet_address,
                "tot_gross_profit": tot_gross_profit,
                "tot_roi": tot_roi,
                "win_rate": win_rate,
            }

            # Yield the output dictionary
            yield output_dict
