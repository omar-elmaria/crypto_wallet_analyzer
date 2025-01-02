# Import libraries
import scrapy
from inputs import custom_scrapy_settings
from helper_functions import *
from wallet_analyzer.items import DexScreenerTopGainers, DexScreenerTopTraders
import re

class DexScreenerCrawlerSpider(scrapy.Spider):
    name = "dex_screener_crawler"
    custom_settings = custom_scrapy_settings # Define the custom settings of the spider
    custom_settings["FEEDS"] = {
        'dex_screener_top_gainers.json': {
            'format': 'json',
            'overwrite': True,
            'item_classes': ["wallet_analyzer.items.DexScreenerTopGainers"]
        },
        'dex_screener_top_traders.json': {
            'format': 'json',
            'overwrite': True,
            'item_classes': ["wallet_analyzer.items.DexScreenerTopTraders"]
        }
    }
    base_url = "https://dexscreener.com/gainers/solana?min24HSells=30&min24HTxns=300&min24HVol=500000&minLiq=250000&minMarketCap=1000000&order=desc&rankBy=priceChangeH24" # Volume > 500k, Liquidity > 250k, MCap > 1M

    ## Start scraping
    def start_requests(self):
        # Send a request to the base URL
        yield scrapy.Request(
            url=self.base_url,
            callback=self.parse_top_gainers,
            meta={
                "zyte_api_automap": {
                    "browserHtml": True,
                },
            }
        )

    def parse_top_gainers(self, response):
        # Extract a list of results
        results = response.xpath("//div[@class='ds-dex-table ds-dex-table-top']/a")

        # Create an instance of DexScreenerTopGainers
        top_gainers_item_obj = DexScreenerTopGainers()

        # Parse the response
        for res in results[0:2]:
            # Extract the asset name
            asset_name = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-token']/span[contains(@class, 'ds-dex-table-row-base-token-symbol')]/text()").get()
            
            # Extract the asset name text
            asset_name_text = res.xpath(".//div[@class='ds-table-data-cell ds-dex-table-row-col-token']/div[@class='ds-dex-table-row-base-token-name']/span/text()[1]").get()
            
            # Extract the asset URL
            asset_url = "https://dexscreener.com" + res.xpath("./@href").get()

            # Extract the 24-hour gain rank
            asset_gain_rank = helper_treat_none_before_data_type_change(value=res.xpath(".//span[@class='ds-dex-table-row-badge-pair-no']/text()[2]").get(), data_type="int")
            
            # Extract the network
            asset_network = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-token']/img[@class='ds-dex-table-row-chain-icon']/@title").get()

            # Extract the DEX
            dex = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-token']/img[@class='ds-dex-table-row-dex-icon']/@title").get()
            
            # Extract the latest price in dollars
            asset_price = helper_treat_none_before_data_type_change(value=res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-price']/text()[2]").get(), data_type="float")
            
            # Extract the asset age in hours
            asset_age = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-pair-age']/span/text()").get()
            
            # Extract the asset's number of transactions in the last 24 hours
            asset_24_hr_txns = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-txns']/text()").get()
            asset_24_hr_txns = helper_treat_none_before_data_type_change(value=re.sub(pattern=",", repl="", string=asset_24_hr_txns), data_type="int")
            
            # Extract the asset's volume in the last 24 hours
            asset_24_hr_volume_in_mil = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-volume']/text()[2]").get()
            asset_24_hr_volume_in_mil = helper_normalize_millions_and_thousands_in_vol_liq_mcap(value=asset_24_hr_volume_in_mil)
            
            # Extract the asset's volume in the last 24 hours
            num_makers = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-makers']/text()").get()
            num_makers = helper_treat_none_before_data_type_change(value=re.sub(pattern=",", repl="", string=num_makers), data_type="int")
            
            # Extract the asset's price change in the last 5 minutes
            asset_price_change_l5m = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-price-change-m5']/span/text()").get()
            asset_price_change_l5m = helper_normalize_millions_and_billions_in_pct_gains(value=asset_price_change_l5m)
            
            # Extract the asset's price change in the last hour
            asset_price_change_l1h = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-price-change-h1']/span/text()").get()
            asset_price_change_l1h = helper_normalize_millions_and_billions_in_pct_gains(value=asset_price_change_l1h)
            
            # Extract the asset's price change in the last 6 hours
            asset_price_change_l6h = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-price-change-h6']/span/text()").get()
            asset_price_change_l6h = helper_normalize_millions_and_billions_in_pct_gains(value=asset_price_change_l6h)
            
            # Extract the asset's price change in the last 24 hours
            asset_price_change_l24h = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-price-change-h24']/span/text()").get()
            asset_price_change_l24h = helper_normalize_millions_and_billions_in_pct_gains(value=asset_price_change_l24h)
            
            # Extract the asset's liquidity
            asset_liquidity_in_mil = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-liquidity']/text()[2]").get()
            asset_liquidity_in_mil = helper_normalize_millions_and_thousands_in_vol_liq_mcap(value=asset_liquidity_in_mil)
            
            # Extract the asset's market cap
            asset_market_cap_in_mil = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-market-cap']/text()[2]").get()
            asset_market_cap_in_mil = helper_normalize_millions_and_thousands_in_vol_liq_mcap(value=asset_market_cap_in_mil)

            # Yield the output dictionary
            for var in [
                "asset_name", "asset_name_text", "asset_url",
                "asset_gain_rank", "asset_network", "dex",
                "asset_price", "asset_age", "asset_24_hr_txns",
                "asset_24_hr_volume_in_mil", "num_makers", "asset_price_change_l5m",
                "asset_price_change_l1h", "asset_price_change_l6h", "asset_price_change_l24h",
                "asset_liquidity_in_mil", "asset_market_cap_in_mil"
            ]:
                top_gainers_item_obj[var] = eval(var)

            yield top_gainers_item_obj
        
            # Send a request to the individual asset URL
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
        # Create an instance of DexScreenerTopTraders
        top_traders_item_obj = DexScreenerTopTraders()

        # Extract the top traders list of results
        top_trader_results = response.xpath("//span[text() = 'bought']/../../following-sibling::div")

        # Extract the meta data
        asset_name = response.meta["asset_name"]
        asset_url = response.meta["asset_url"]

        # Parse the response
        for tr in top_trader_results:
            # Extract the trader bought amount in USD
            trader_bought_usd = tr.xpath(".//span[@class='chakra-text custom-rcecxm']/text()").get()

            # Extract the trader bought amount in crypto units
            trader_bought_crypto = tr.xpath(".//span[@class='chakra-text custom-rcecxm']/following-sibling::span/span[1]/text()").get()
            
            # Extract the number of buy TXNs
            trader_buy_txns = tr.xpath(".//span[@class='chakra-text custom-rcecxm']/following-sibling::span/span[3]/text()").get()

            # Extract the trader sold amount in USD
            trader_sold_usd = tr.xpath(".//span[@class='chakra-text custom-dv3t8y']/text()").get()
            
            # Extract the trader sold amount in crypto units
            trader_sold_crypto = tr.xpath(".//span[@class='chakra-text custom-dv3t8y']/following-sibling::span/span[1]/text()").get()
            
            # Extract the number of sell TXNs
            trader_sell_txns = tr.xpath(".//span[@class='chakra-text custom-dv3t8y']/following-sibling::span/span[3]/text()").get()
            
            # Extract the PnL
            trader_pnl = tr.xpath(".//div[@class='custom-1e9y0rl']/text()").get()

            # Extract the SOL scan URL
            sol_scan_url = tr.xpath(".//a[@aria-label='Open in block explorer']/@href").get()

            # Extract the wallet_address
            wallet_address = re.findall(pattern=r"(?<=account\/).*", string=sol_scan_url)[0]

            # Yield the output dictionary
            for var in [
                "asset_name", "asset_url", "trader_bought_usd", "trader_bought_crypto", "trader_buy_txns",
                "trader_sold_usd", "trader_sold_crypto", "trader_sell_txns",
                "trader_pnl", "sol_scan_url", "wallet_address"
            ]:
                top_traders_item_obj[var] = eval(var)

            yield top_traders_item_obj