import asyncio
import aiohttp
import random
import validators
import pandas as pd
from datetime import datetime
from tqdm.asyncio import tqdm_asyncio
import chardet
import argparse
import sys

HOME = '~/Documents/GitHub/feat-eng-websites/src'
sys.path.append(HOME)
from helpers.snowflake_data_fetch import SnowflakeManager

### Country Selection (Change as Needed)
# country_code = 'BRA'  # Options: ARG, COL, MEX, CAN, JAM

### Initialize Snowflake Connection
snowflake_manager = SnowflakeManager()

### Rotating User-Agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:109.0) Gecko/20100101 Firefox/109.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:108.0) Gecko/20100101 Firefox/108.0",
]

### Configurations
BATCH_SIZE = 500
MAX_RETRIES = 3  # Retries for timeout errors
TIMEOUT = 20  # Increase timeout to 20s
CONCURRENT_REQUESTS = 5  # Reduce concurrency to avoid overload


### Function to fetch website content with retries
async def _fetch(session, url, country, semaphore):
    """Fetch HTML content for a single website with retries and error handling."""
    async with semaphore:
        if not validators.url(url):
            return url, None, "Invalid URL", country

        headers = {"User-Agent": random.choice(USER_AGENTS)}

        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(url, headers=headers, timeout=TIMEOUT) as response:
                    if response.status == 200:
                        raw_content = await response.read()  # Read as raw bytes

                        # Auto-detect encoding safely
                        detected_encoding = chardet.detect(raw_content)['encoding']
                        encoding = detected_encoding if detected_encoding else 'utf-8'

                        try:
                            html_content = raw_content.decode(encoding, errors="ignore")  # Decode safely
                        except UnicodeDecodeError:
                            html_content = raw_content.decode("utf-8", errors="ignore")  # Fallback

                        return url, html_content[:300000], "Success", country  # Limit size

                    elif response.status == 403:
                        return url, None, "Forbidden (403)", country
                    elif response.status == 404:
                        return url, None, "Not Found (404)", country
                    elif response.status in [500, 502, 503, 504]:
                        return url, None, f"Server Error ({response.status})", country
                    else:
                        return url, None, f"HTTP {response.status}", country

            except aiohttp.client_exceptions.ClientResponseError as e:
                if "Header value is too long" in str(e):
                    print(f"Skipping {url} due to large header issue.")
                    return url, None, "Error: Header Too Long", country
                else:
                    print(f"Error fetching {url}: {e}")
                    return url, None, f"Error: {e}", country

            except aiohttp.ClientConnectorCertificateError:
                return url, None, "No SSL Certificate", country
            except aiohttp.ClientConnectorError:
                return url, None, "Non-existent Domain", country
            except aiohttp.ClientPayloadError:
                return url, None, "Error: Content Encoding Issue", country
            except aiohttp.ClientOSError:
                return url, None, "Error: Connection Reset", country
            except aiohttp.ClientResponseError as e:
                return url, None, f"Error: {e}", country
            except asyncio.TimeoutError:
                if attempt < MAX_RETRIES - 1:
                    print(f"Retrying ({attempt + 1}/{MAX_RETRIES}) for {url} due to timeout...")
                    await asyncio.sleep(2)  # Wait 2 seconds before retrying
                else:
                    return url, None, "Timeout", country
            except Exception as e:
                return url, None, f"Error: {e}", country  # Catch any unexpected errors


### Function to fetch multiple websites in parallel
async def fetch_all(urls):
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)  # Create semaphore inside function

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [_fetch(session, url, country, semaphore) for url, country in urls]
        return await tqdm_asyncio.gather(*tasks, desc=f"Fetching Websites", total=len(urls))


### Run async fetcher and store results in Snowflake
def main(country_code, use_snowflake):
    if use_snowflake:
        ### Load website links from Snowflake
        query = f"""
            SELECT DISTINCT COUNTRY, WEBSITE_URL
            FROM DATABASE.SCHEMA.WEBSITES_INITIAL_TABLE
            WHERE COUNTRY = '{country_code}'
        """
        website_df = snowflake_manager.fetch_data(query)
        website_links = [(row["WEBSITE_URL"], row["COUNTRY"]) for _, row in website_df.iterrows()]
    else:
        ### Load website links from local CSV
        website_df = pd.read_csv(f"{HOME}/../data/websites_initial_table.csv")
        website_df = website_df[website_df["COUNTRY"] == country_code]
        website_links = list(zip(website_df["WEBSITE_URL"], website_df["COUNTRY"]))
    print(f"Total websites to process for {country_code}: {len(website_links)}")

    total_batches = len(website_links) // BATCH_SIZE + 1
    print(f"Total Batches for {country_code}: {total_batches}")

    for batch_num in range(total_batches):
        start = batch_num * BATCH_SIZE
        end = start + BATCH_SIZE
        batch = website_links[start:end]

        if not batch:
            continue

        print(f"\nProcessing batch {batch_num + 1}/{total_batches}... ({len(batch)} websites)")

        ### Force a fresh event loop to prevent "bound to a different event loop" error
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = loop.run_until_complete(fetch_all(batch))

        # Convert results to DataFrame
        df_results = pd.DataFrame(results, columns=["website", "html_content", "status", "country"])
        df_results["extracted_at"] = datetime.now()

        if use_snowflake:
            # Save to Snowflake table (append mode)
            csv_path = snowflake_manager.write_csv_local(df_results, f"WEBSITE_SCRAPED_DATA_{country_code}", path_to_save=f"{HOME}/../data")
            snowflake_manager.write_table(
                table_name=f"WEBSITE_SCRAPED_DATA_{country_code}",
                csv_file_path=csv_path,
                dataset=df_results,
                overwrite=False  # Ensures APPEND instead of OVERWRITE
            )
        else:
            # Save to local CSV file
            csv_path = f"{HOME}/../data/website_scraped_data_{country_code}.csv"
            df_results.to_csv(csv_path, mode='a', index=False, header=not batch_num)
            print(f"Results saved on CSV path: {csv_path}")

        print(f"Batch {batch_num + 1} complete! Data APPENDED to Snowflake.")

    print(f"\nScraping complete! Data stored in WEBSITE_SCRAPED_DATA for {country_code}.")


### Run the script
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch website content')
    parser.add_argument('-c', '--country_code',    default='BRA',   help='country to fetch website content for')
    parser.add_argument('--use_snowflake', action='store_true', help='if set, read/write from Snowflake; otherwise use CSV files')
    args = parser.parse_args()
    main(args.country_code, use_snowflake=args.use_snowflake)
