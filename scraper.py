import re
import httpx
from bs4 import BeautifulSoup
from fastapi import HTTPException

# Target site configuration
BASE_URL = "https://animesalt.cx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://animesalt.cx/"
}

async def fetch_anime_catalog():
    """
    Scrapes the main page or catalog of animesalt.cx to get anime titles, 
    slugs/URLs, and thumbnail image links.
    """
    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=15.0) as client:
        try:
            response = await client.get(BASE_URL)
            if response.status_code != 200:
                print(f"Failed to fetch catalog: Status {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.text, 'html.parser')
            anime_list = []
            
            # Note: Selectors can be fine-tuned based on exact HTML tags of the site
            for item in soup.select('.anime-card, .item, .film-poster'): # General selectors for anime grids
                title_elem = item.find(['a', 'h3', 'div'], class_=re.compile('title|name', re.I))
                img_elem = item.find('img')
                link_elem = item.find('a', href=True)
                
                if title_elem and link_elem:
                    title = title_elem.get_text(strip=True)
                    url = link_elem['href']
                    if not url.startswith('http'):
                        url = BASE_URL + url
                    
                    thumbnail = img_elem.get('data-src') or img_elem.get('src') if img_elem else ""
                    
                    anime_list.append({
                        "title": title,
                        "url": url,
                        "thumbnail": thumbnail
                    })
            
            return anime_list
        except Exception as e:
            print(f"Error scraping catalog: {e}")
            return []

async def resolve_streaming_link(episode_url: str):
    """
    Resolves the target anime episode page to extract the underlying 
    HLS (.m3u8) streaming link hosted on ravok.buzz or similar sources.
    """
    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=15.0) as client:
        try:
            response = await client.get(episode_url)
            if response.status_code != 200:
                raise HTTPException(status_code=400, detail="Failed to fetch episode page.")
            
            html_content = response.text
            
            # Search for m3u8 links or ravok.buzz stream patterns inside the page scripts/JW Player config
            m3u8_pattern = r'https?://[^\s<>"]+?\.m3u8[^\s<>"]*'
            found_links = re.findall(m3u8_pattern, html_content)
            
            if found_links:
                # Return the first valid active HLS link
                return {"stream_url": found_links[0], "type": "hls"}
            
            # Alternative: Search for iframe or embedded player sources if direct m3u8 isn't in plaintext
            soup = BeautifulSoup(html_content, 'html.parser')
            iframe = soup.find('iframe', src=True)
            if iframe:
                iframe_src = iframe['src']
                # If it points to ravok.buzz or an embedded player, we can parse further or return it
                return {"stream_url": iframe_src, "type": "iframe"}
            
            raise HTTPException(status_code=404, detail="Streaming source (.m3u8) could not be resolved.")
            
        except Exception as e:
            print(f"Error resolving stream: {e}")
            raise HTTPException(status_code=500, detail=str(e))
