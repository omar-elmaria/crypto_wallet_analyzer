# Import libraries
import scrapy
from inputs import custom_scrapy_settings
import json
import pandas as pd

class DexCheckWalletScreenerSpider(scrapy.Spider):
    name = "dex_check_wallet_screener"
    custom_settings = custom_scrapy_settings.copy() # Define the custom settings of the spider
    custom_settings["LOG_FILE"] = "dex_check_wallet_screener.log"
    custom_settings["FEEDS"] = {
        'dex_check_wallet_screener.json': {
            'format': 'json',
            'overwrite': True
        }
    }
    base_url = "https://dexcheck.ai/app/wallet-analyzer/{wallet_address}"
    max_retries = 5

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
        df_wallets_to_analyze = df_top_traders[df_top_traders["pct_pnl_rank"] <= 250].reset_index(drop=True)

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
                        "browserHtml": True
                    },
                    "wallet_address": wl,
                    "request_counter": request_counter,
                    "wallet_count": idx + 1,
                    "tot_num_wallets": len(full_list_of_wallets)
                }
            )
    
    def parse_wallet_data(self, response):
        # Check if the page has been fully loaded
        tot_gross_profit = response.xpath("//button[text()='Gross Profit']/following-sibling::p/text()").get()
        if tot_gross_profit is None:
            request_counter = response.meta["request_counter"]
            request_counter += 1
            self.logger.error(f"The page has not been fully loaded for the wallet address: {response.meta['wallet_address']}. Retrying the request {request_counter} out of {self.max_retries}.")
            yield scrapy.Request(
                url=response.url,
                callback=self.parse_wallet_data,
                meta={
                    "zyte_api_automap": {
                        "browserHtml": True
                    },
                    "wallet_address": response.meta["wallet_address"],
                    "request_counter": request_counter
                }
            )
        else:
            # Print a status message
            self.logger.info(f"Processing the stats of the wallet address: {response.meta['wallet_address']}, which is wallet {response.meta['wallet_count']} out of {response.meta['tot_num_wallets']}.")

            # Extract the wallet's gross profit
            tot_gross_profit = response.xpath("//button[text()='Gross Profit']/following-sibling::p/text()").get()

            # Extract the realized gross profit
            realized_gross_profit = response.xpath("//button[text()='Gross Profit']/../div//p[text()='Realized']/following-sibling::p/span[1]/text()").get()
            
            # Extract the unrealized gross profit
            unrealized_gross_profit = response.xpath("//button[text()='Gross Profit']/../div//p[text()='Unrealized']/following-sibling::p/span[1]/text()").get()

            # Extract the wallet's total ROI
            tot_roi = response.xpath("//button[text()='Total ROI']/following-sibling::p/text()[1]").get()

            # Extract the realized ROI
            realized_roi = response.xpath("//button[text()='Total ROI']/../div//p[text()='Realized']/following-sibling::p/text()[1]").get()
            
            # Extract the unrealized ROI
            unrealized_roi = response.xpath("//button[text()='Total ROI']/../div//p[text()='Unrealized']/following-sibling::p/text()[1]").get()

            # Extract the win rate
            win_rate = response.xpath("//button[text()='Win Rate']/following-sibling::div/p/text()").get()

            # Extract the number of wins
            num_wins = response.xpath("//button[text()='Win Rate']/following-sibling::div//p[text()='Win']/following-sibling::p/text()").get()

            # Extract the number of losses
            num_losses = response.xpath("//button[text()='Win Rate']/following-sibling::div//p[text()='Lose']/following-sibling::p/text()").get()

            # Extract the trading volume
            trading_volume = response.xpath("//button[text()='Trading Volume']/following-sibling::p/text()[2]").get()

            # Extract the number of trades
            num_trades = response.xpath("//button[text()='Trades']/following-sibling::p/text()").get()

            # Extract the average trade size
            avg_trade_size = response.xpath("//button[text()='Avg. Trade Size']/following-sibling::p/span[1]/text()").get()

            # Create the output dictionary
            output_dict = {
                "wallet_address": response.meta["wallet_address"],
                "tot_gross_profit": tot_gross_profit,
                "realized_gross_profit": realized_gross_profit,
                "unrealized_gross_profit": unrealized_gross_profit,
                "tot_roi": tot_roi,
                "realized_roi": realized_roi,
                "unrealized_roi": unrealized_roi,
                "win_rate": win_rate,
                "num_wins": num_wins,
                "num_losses": num_losses,
                "trading_volume": trading_volume,
                "num_trades": num_trades,
                "avg_trade_size": avg_trade_size
            }

            # Yield the output dictionary
            yield output_dict