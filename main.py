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
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def scrape_pdf_list(indices):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    base_url = 'https://digitallibrary.un.org/record/'

    print("Processing indices from ", indices[0], "to ", indices[-1])

    return [str(index) + '\n' for index in indices], [base_url + '\n' for _ in indices]

def scrape_pdf(url):
    return None

def scrape_pdfs(url_list):
    return [scrape_pdf(url) for url in url_list]

def process_pdf(pdf):
    return None, None

def process_pdfs(pdf_list, archive):
    for pdf in pdf_list:
        text, meta = process_pdf(pdf)
        ar.add_data(text, meta=meta)
        ar.commit()
    return archive

def process(start=MIN_RECORD_NUMBER, end=MAX_RECORD_NUMBER, batch_size=2**16):
    try:
        with open("counts.dat", "r") as cf:
            watermark = int(cf.readlines()[-1]) + 1
    except:
        watermark = start

    record_indices = [number for number in range(max(start, watermark), end)]

    with open("counts.dat", "a+") as cf, open("urls.dat", "a+") as uf:
        url_list = []
        index_batches = chunks(record_indices, batch_size)
        for batch in index_batches:
            batch_index_list, batch_url_list = scrape_pdf_list(batch)
            cf.writelines(batch_index_list)
            uf.writelines(batch_url_list)
            url_list.extend(batch_url_list)

    with open("downloads.dat", "a+") as df, open("processed.dat", "a+") as pf:
        pdf_list = []
        url_batches = chunks(url_list, batch_size)
        for batch in url_batches:
            batch_pdf_list = scrape_pdfs(batch)
            df.writelines(batch_pdf_list)
            pdf_list.extend(batch_pdf_list)

    with open("processed.dat", "a+") as pf:
        archive = process_pdfs(pdf_list, archive)

if __name__ == '__main__':
    fire.Fire(process)