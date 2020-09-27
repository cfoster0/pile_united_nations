import time
import sys
import fire
import urllib3
import backoff
import pqdm.threads as threads
import pqdm.processes as processes
import lm_dataformat as lmd
from bs4 import BeautifulSoup

MIN_RECORD_NUMBER = 1
MAX_RECORD_NUMBER = 3884001

http = urllib3.PoolManager(headers={'User-Agent': 'PQDM/1.0'})

flatten = lambda l: [item for sublist in l for item in sublist]

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    r = None
    for i in range(0, len(lst), n):
        r = i + n
        yield lst[i:i + n]
    yield lst[r:]

class ScrapingException(BaseException):
    pass

class NetworkException(BaseException):
    pass

@backoff.on_exception(backoff.expo,
                      ScrapingException,
                      max_tries=6)
def scrape_pdf_list_item(number):
    url = 'https://digitallibrary.un.org/record/' + str(number) + '/files/'
    time.sleep(1.0)
    result = http.request('GET', url)
    if result.status == 200:
        data = result.data
        soup = BeautifulSoup(data, features="html.parser")
        links = soup.find_all('a', href=True)
        links = [link['href'] for link in links if '.pdf' in link['href']]
        return number, result.status, links
    elif result.status in [403, 429]:
        raise ScrapingException()
    elif result.status in [404, 410]:
        return number, result.status, []
    else:
        raise NetworkException()

def scrape_pdf_list(indices):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if len(indices) > 1:
        print("Checking UN Library for PDFs at indices ", indices[0], "to ", indices[-1], "...")
    elif len(indices) == 1:
        print("Checking UN Library for PDFs at index ", indices[0], "...")
    else:
        return [], []

    results = threads.pqdm([[index] for index in indices], scrape_pdf_list_item, n_jobs=4, argument_type='args')

    for result in results:
        if result is not tuple:
            raise result

    non_flattened = [links for number, status, links in results]

    return [str(index) for index in indices], flatten(non_flattened)

def scrape_pdf(url):
    return './pdfs'

def scrape_pdfs(url_list):
    return [scrape_pdf(url) for url in url_list]

def process_pdf(pdf):
    return 'Nothing', 'Nothing else'

def process_pdfs(pdf_list, archive):
    for pdf in pdf_list:
        text, meta = process_pdf(pdf)
        archive.add_data(text, meta=meta)
    archive.commit()
    return archive

def process(start=MIN_RECORD_NUMBER, end=MAX_RECORD_NUMBER, batch_size=2**5):
    try:
        with open("counts.dat", "r") as cf:
            watermark = int(cf.readlines()[-1].rstrip()) + 1
    except:
        watermark = start

    try:
        with open("urls.dat", "r") as uf:
            url_list = [line.rstrip() for line in uf.readlines()]
    except:
        url_list = []

    record_indices = [number for number in range(max(start, watermark), end)]

    with open("counts.dat", "a+") as cf, open("urls.dat", "a+") as uf:
        index_batches = chunks(record_indices, batch_size)
        for batch in index_batches:
            try:
                batch_index_list, batch_url_list = scrape_pdf_list(batch)
            except NetworkException:
                sys.exit("Error accessing records. Shutting down...")
            except ScrapingException:
                sys.exit("Error scraping records. May be due to rate-limiting. Please try again later, or with a proxy.")
            cf.writelines([item + '\n' for item in batch_index_list])
            uf.writelines([item + '\n' for item in batch_url_list])
            url_list.extend(batch_url_list)

    try:
        with open("processed.dat", "r") as pf:
            last_processed = pf.readlines()[-1].rstrip()
    except:
        last_processed = None

    if last_processed:
        last_processed_index = url_list.index(last_processed)
        url_list = [line.rstrip() for line in url_list[last_processed_index + 1:]]

    archive = lmd.Archive('out')

    with open("processed.dat", "a+") as pf:
        url_batches = chunks(url_list, batch_size)
        for batch in url_batches:
            print("Scraping PDF batch...")
            batch_pdf_list = scrape_pdfs(batch)
            print("Processing PDF batch...")
            process_pdfs(batch_pdf_list, archive)
            pf.writelines([item + '\n' for item in batch])

if __name__ == '__main__':
    fire.Fire(process)