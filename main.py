import fire
import urllib3
import pqdm.threads as threads
import pqdm.processes as processes
import lm_dataformat as lmd
from bs4 import BeautifulSoup

MIN_RECORD_NUMBER = 1
MAX_RECORD_NUMBER = 3884001

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    r = None
    for i in range(0, len(lst), n):
        r = i + n
        yield lst[i:i + n]
    yield lst[r:]

def scrape_pdf_list(indices):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    base_url = 'https://digitallibrary.un.org/record/'

    if len(indices) > 1:
        print("Checking UN Library for PDFs at indices ", indices[0], "to ", indices[-1], "...")
    elif len(indices) == 1:
        print("Checking UN Library for PDFs at index ", indices[0], "...")
    else:
        return [], []

    return [str(index) + '\n' for index in indices], [base_url + str(index) + '\n' for index in indices]

def scrape_pdf(url):
    return './pdfs'

def scrape_pdfs(url_list):
    return [scrape_pdf(url) for url in url_list]

def process_pdf(pdf):
    return '', None

def process_pdfs(pdf_list, archive):
    for pdf in pdf_list:
        text, meta = process_pdf(pdf)
        #archive.add_data(text, meta=meta)
        #archive.commit()
    return archive

def process(start=MIN_RECORD_NUMBER, end=MAX_RECORD_NUMBER, batch_size=2**16):
    try:
        with open("counts.dat", "r") as cf:
            watermark = int(cf.readlines()[-1]) + 1
    except:
        watermark = start

    try:
        with open("urls.dat", "r") as uf:
            url_list = uf.readlines()
    except:
        url_list = []

    record_indices = [number for number in range(max(start, watermark), end)]

    with open("counts.dat", "a+") as cf, open("urls.dat", "a+") as uf:
        index_batches = chunks(record_indices, batch_size)
        for batch in index_batches:
            batch_index_list, batch_url_list = scrape_pdf_list(batch)
            cf.writelines(batch_index_list)
            uf.writelines(batch_url_list)
            url_list.extend(batch_url_list)

    try:
        with open("processed.dat", "r") as pf:
            last_processed = pf.readlines()[-1]
    except:
        last_processed = None

    if last_processed:
        last_processed_index = url_list.index(last_processed)
        url_list = url_list[last_processed_index + 1:]

    archive = lmd.Archive('out')

    with open("processed.dat", "a+") as pf:
        url_batches = chunks(url_list, batch_size)
        for batch in url_batches:
            print("Scraping PDF batch...")
            batch_pdf_list = scrape_pdfs(batch)
            print("Processing PDF batch...")
            process_pdfs(batch_pdf_list, archive)
            pf.writelines(batch)

if __name__ == '__main__':
    fire.Fire(process)