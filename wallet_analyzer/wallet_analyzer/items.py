# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class DexScreenerTopGainers(scrapy.Item):
    asset_name = scrapy.Field()
    asset_name_text = scrapy.Field()
    asset_url = scrapy.Field()
    asset_gain_rank = scrapy.Field()
    asset_network = scrapy.Field()
    dex = scrapy.Field()
    asset_price = scrapy.Field()
    asset_age = scrapy.Field()
    asset_24_hr_txns = scrapy.Field()
    asset_24_hr_volume_in_mil = scrapy.Field()
    num_makers = scrapy.Field()
    asset_price_change_l5m = scrapy.Field()
    asset_price_change_l1h = scrapy.Field()
    asset_price_change_l6h = scrapy.Field()
    asset_price_change_l24h = scrapy.Field()
    asset_liquidity_in_mil = scrapy.Field()
    asset_market_cap_in_mil = scrapy.Field()

class DexScreenerTopTraders(scrapy.Item):
    asset_name = scrapy.Field()
    asset_url = scrapy.Field()
    trader_bought_usd = scrapy.Field()
    trader_bought_crypto = scrapy.Field()
    trader_buy_txns = scrapy.Field()
    trader_sold_usd = scrapy.Field()
    trader_sold_crypto = scrapy.Field()
    trader_sell_txns = scrapy.Field()
    trader_pnl = scrapy.Field()
    sol_scan_url = scrapy.Field()
    wallet_address = scrapy.Field()