from .base import BaseScraper, Listing, SkipSite


class ChinaexporterScraper(BaseScraper):
    slug = "chinaexporter"
    base_url = "http://www.chinaexporter.com/"

    def search(self, query: str) -> list[Listing]:
        search_url = self.build_search_url(query)
        response = self.fetch(search_url)
        if response is None:
            raise SkipSite("no response from chinaexporter search")
        listings = self.parse_generic_results(
            response.text,
            query=query,
            query_type="cas" if "-" in query else "keyword",
            result_selectors=[
                ".productlist li",
                ".search-result",
                ".list-item",
                ".main-list li",
            ],
        )
        raw_html_path = None
        if listings:
            raw_html_path = self.save_sample(query, response.text)
            for listing in listings:
                listing.raw_html_path = raw_html_path
        return listings[:10]
