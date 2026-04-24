from .base import BaseScraper, Listing, SkipSite


class IndiamartExportScraper(BaseScraper):
    slug = "indiamart_export"
    base_url = "https://export.indiamart.com/"

    def search(self, query: str) -> list[Listing]:
        search_url = self.build_search_url(query)
        response = self.fetch(search_url)
        if response is None:
            raise SkipSite("no response from indiamart export search")
        listings = self.parse_generic_results(
            response.text,
            query=query,
            query_type="cas" if "-" in query else "keyword",
            result_selectors=[
                ".prod-list li",
                ".search-results li",
                ".lst li",
                ".card",
            ],
        )
        raw_html_path = None
        if listings:
            raw_html_path = self.save_sample(query, response.text)
            for listing in listings:
                listing.raw_html_path = raw_html_path
        return listings[:10]
