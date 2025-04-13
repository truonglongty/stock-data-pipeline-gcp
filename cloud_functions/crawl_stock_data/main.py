from utils.polygon_api import crawl_ohlcs
from utils.alphavantage_api import crawl_news

def crawl_stock_data_entrypoint(request):
    """Entry Point for Cloud Function"""
    crawl_ohlcs()
    crawl_news()
    return "Crawled OHLCs and News successfully"
