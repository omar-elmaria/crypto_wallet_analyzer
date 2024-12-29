# Import libraries
import scrapy
from inputs import custom_scrapy_settings
from wallet_analyzer.items import DexScreenerTopGainers
import re

class DexScreenerCrawlerSpider(scrapy.Spider):
    name = "dex_screener_crawler"
    custom_settings = custom_scrapy_settings # Define the custom settings of the spider
    base_url = "https://dexscreener.com/gainers/solana?min24HSells=30&min24HTxns=300&min24HVol=500000&minLiq=250000&minMarketCap=1000000&order=desc&rankBy=priceChangeH24" # Volume > 500k, Liquidity > 250k, MCap > 1M

    ## Helper functions
    def normalize_millions_and_thousands_in_vol_liq_mcap(self, value):
        if value.find("M") != -1:
            value = float(re.sub(pattern="M", repl="", string=value))
        elif value.find("B") != -1:
            value = float(re.sub(pattern="B", repl="", string=value)) * 1000
        elif value.find("K") != -1:
            value = float(re.sub(pattern="K", repl="", string=value)) / 1000
        else:
            value = float(value)
        
        return value
    
    def normalize_millions_and_billions_in_pct_gains(self, value):
        if value.find("M") != -1:
            value = float(re.sub(pattern="%|M|,", repl="", string=value)) * pow(10, 6)
        elif value.find("B") != -1:
            value = float(re.sub(pattern="%|B|,", repl="", string=value)) * pow(10, 9)
        elif value.find("K") != -1:
            value = float(re.sub(pattern="%|K|,", repl="", string=value)) * pow(10, 3)
        else:
            value = float(re.sub(pattern="%|,", repl="", string=value))
        
        return value

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

        # Create an instance of ProductItem
        top_gainers_item_obj = DexScreenerTopGainers()

        # Parse the response
        for res in results:
            # Extract the asset name
            asset_name = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-token']/span[contains(@class, 'ds-dex-table-row-base-token-symbol')]/text()").get()
            
            # Extract the asset name text
            asset_name_text = res.xpath(".//div[@class='ds-table-data-cell ds-dex-table-row-col-token']/div[@class='ds-dex-table-row-base-token-name']/span/text()[1]").get()
            
            # Extract the asset URL
            asset_url = "https://dexscreener.com" + res.xpath("./@href").get()

            # Extract the 24-hour gain rank
            asset_gain_rank = int(res.xpath(".//span[@class='ds-dex-table-row-badge-pair-no']/text()[2]").get())
            
            # Extract the network
            asset_network = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-token']/img[@class='ds-dex-table-row-chain-icon']/@title").get()

            # Extract the DEX
            dex = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-token']/img[@class='ds-dex-table-row-dex-icon']/@title").get()
            
            # Extract the latest price in dollars
            asset_price = float(res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-price']/text()[2]").get())
            
            # Extract the asset age in hours
            asset_age = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-pair-age']/span/text()").get()
            
            # Extract the asset's number of transactions in the last 24 hours
            asset_24_hr_txns = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-txns']/text()").get()
            asset_24_hr_txns = int(re.sub(pattern=",", repl="", string=asset_24_hr_txns))
            
            # Extract the asset's volume in the last 24 hours
            asset_24_hr_volume_in_mil = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-volume']/text()[2]").get()
            asset_24_hr_volume_in_mil = self.normalize_millions_and_thousands_in_vol_liq_mcap(value=asset_24_hr_volume_in_mil)
            
            # Extract the asset's volume in the last 24 hours
            num_makers = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-makers']/text()").get()
            num_makers = int(re.sub(pattern=",", repl="", string=num_makers))
            
            # Extract the asset's price change in the last 5 minutes
            asset_price_change_l5m = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-price-change-m5']/span/text()").get()
            asset_price_change_l5m = self.normalize_millions_and_billions_in_pct_gains(value=asset_price_change_l5m)
            
            # Extract the asset's price change in the last hour
            asset_price_change_l1h = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-price-change-h1']/span/text()").get()
            asset_price_change_l1h = self.normalize_millions_and_billions_in_pct_gains(value=asset_price_change_l1h)
            
            # Extract the asset's price change in the last 6 hours
            asset_price_change_l6h = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-price-change-h6']/span/text()").get()
            asset_price_change_l6h = self.normalize_millions_and_billions_in_pct_gains(value=asset_price_change_l6h)
            
            # Extract the asset's price change in the last 24 hours
            asset_price_change_l24h = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-price-change-h24']/span/text()").get()
            asset_price_change_l24h = self.normalize_millions_and_billions_in_pct_gains(value=asset_price_change_l24h)
            
            # Extract the asset's liquidity
            asset_liquidity_in_mil = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-liquidity']/text()[2]").get()
            asset_liquidity_in_mil = self.normalize_millions_and_thousands_in_vol_liq_mcap(value=asset_liquidity_in_mil)
            
            # Extract the asset's market cap
            asset_market_cap_in_mil = res.xpath("./div[@class='ds-table-data-cell ds-dex-table-row-col-market-cap']/text()[2]").get()
            asset_market_cap_in_mil = self.normalize_millions_and_thousands_in_vol_liq_mcap(value=asset_market_cap_in_mil)

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