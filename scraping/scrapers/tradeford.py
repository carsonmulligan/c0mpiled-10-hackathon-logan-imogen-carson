from .base import BaseScraper, Listing, SkipSite


class TradefordScraper(BaseScraper):
    slug = "tradeford"
    base_url = "http://www.tradeford.com/"

    def search(self, query: str) -> list[Listing]:
        search_url = self.build_search_url(query)
        response = self.fetch(search_url)
        if response is None:
            raise SkipSite("no response from tradeford search")
        listings = self.parse_generic_results(
            response.text,
            query=query,
            query_type="cas" if "-" in query else "keyword",
            result_selectors=[
                ".company-list li",
                ".products li",
                ".result-item",
                ".search-list li",
            ],
        )
        raw_html_path = None
        if listings:
            raw_html_path = self.save_sample(query, response.text)
            for listing in listings:
                listing.raw_html_path = raw_html_path
        return listings[:10]
