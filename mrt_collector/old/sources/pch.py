from datetime import datetime
import logging

from lib_utils.helper_funcs import get_hrefs, get_tags

from .source import Source


class PCH(Source):
    """Source for MRT RIB dumps from packet clearing house"""

    url = "https://www.pch.net/resources/Routing_Data/"

    @staticmethod
    def get_urls(dl_t: datetime) -> list:
        """Gets URLs of all RIB Dumps at Packet Clearing Houes"""

        logging.debug("Getting Packet Clearing House URLs")

        links = []
        # For each IPV version
        for ipv in [4, 6]:
            # Save the page of list of collector links
            page = dl_t.strftime(f"{PCH.url}IPv{ipv}_daily_snapshots/%Y/%m/")
            # Get all links to collectors from the page of collector links
            collector_links = PCH._get_collectors(page)
            # Validate that the number of links is correct
            PCH._validate_links(collector_links, page)
            for collector_link in collector_links:
                links.append(PCH._get_dump(page, collector_link, dl_t, ipv))

        return links

    @staticmethod
    def _get_collectors(collector_page_url) -> list:
        """Returns all the collector links at the following page"""

        # Get hrefs that are folders
        links = [x for x in get_hrefs(collector_page_url) if x[-1] == "/"]

        filtered_links = []
        # Only save links that can be seen after this specific href
        # If this looks flimsy it's fine, we validate the number of links
        save = False
        for link in links:
           if save:
                filtered_links.append(link)
           if collector_page_url.endswith(link):
                save = True
        return filtered_links

    @staticmethod
    def _validate_links(links, collector_page_url):
        """Validate that the proper number of collector links were found"""

        # Make sure the total links are equal to the filtered links
        total_links = get_tags(collector_page_url, "small")
        # [198 folders]
        total_links = int(total_links[0].text.split(" ")[0])

        assert total_links == len(links), "PCH links not collected properly"

    @staticmethod
    def _get_dump(page: str, collector_link: str, dl_time: datetime, ipv: str):
        """Returns the full link of the RIB dump at that collector"""

        # Get the RIB dump at this collector
        # page:
        # https://www.pch.net/resources/Routing_Data/
        # IPv6_daily_snapshots/2021/08/
        # collector_link:
        # route-collector.zrh2.pch.net/
        # dump:
        # route-collector.zrh2.pch.net-ipv6_bgp_routes.2021.08.09.gz
        fmt = f"{collector_link[:-1]}-ipv{ipv}_bgp_routes.%Y.%m.%d.gz"
        return page + collector_link + dl_time.strftime(fmt)
