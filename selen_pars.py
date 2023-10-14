import coloredlogs
import logging
from threading import Thread
from queue import Queue
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import requests_random_user_agent
import re
import lxml


requests.packages.urllib3.disable_warnings()
FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

logging.basicConfig(level=logging.DEBUG, format=FORMAT)
logger = logging.getLogger(__name__)
coloredlogs.install(logger=logger)


# Функція видалення дублікатів
def remove_dup_email(x):
    """
    This function removes duplicate email addresses
    :param x:
    :return:
    """
    return list(dict.fromkeys(x))


def remove_dup_phone(x):
    """
    This function removes duplicate phone
    :param x:
    :return:
    """
    return list(dict.fromkeys(x))


def get_email(html):
    """
    This function returns email address from html
    :param html:
    :return:
    """
    try:
        email = re.findall("[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", html)
        nodup_email = remove_dup_email(email)
        return [i for i in nodup_email]
    except Exception as e:
        logger.error(f"Email search error: {e}")


def get_phone(html):
    """
    This function returns the phone number
    :param html:
    :return:
    """
    try:
        phone_pattern = (r"(\+\d{1,2}\(\d{3}\)\ \d{3}\-\d{2}\-\d{2})|(\+\d{1,2}"
                         r"\(\d{3}\)\d{3}\-\d{2}\-\d{2})|(\+\d{12})|(\d{11})|"
                         r"(\+\d{1,2}\(\d{3}\)\d{3}\d{2}\d{2})|(\d{1,2}\(\d{3}\)"
                         r"\d{3}\d{2}\d{2})|(\+\d{1,2}\ \(\d{3}\)\ \d{3}[- ]\d{2}"
                         r"[- \d]\d{2})|(\d{10})|(\(\d{3}\)\ \d{3}\ \d{4})|(\d)"
                         r"(\(\d{3}\)\ \d{3}\ \d{2}\ \d{2})|(\d{3}\ \d{3}\ \d{4})")

        phone = re.findall(phone_pattern, html)

        nodup_phone = remove_dup_phone(phone)

        return [i for tup in nodup_phone for i in tup if i != '']
    except Exception as e:
        logger.error(f"Phone search error: {e}")


def read_file():
    """
    This function reads the file
    :return:
    """
    urls = []
    with open('web_urls.txt', 'r') as f:
        for line in f.readlines():
            url = line.strip()
            if not url.startswith('http'):
                url = 'https://' + url
            urls.append(url)
    return urls


def crawl(q, result):
    """
    This function is used to crawl the specified query string
    :param q:
    :param result:
    :return:
    """
    while not q.empty():
        url = q.get()
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")

            driver = webdriver.Chrome(options=chrome_options)
            driver.get(url[1])

            logger.info(f'Searched home URL: {url[1]}')

            info = BeautifulSoup(driver.page_source, 'lxml')
            emails_home = get_email(info.get_text())
            phones_home = get_phone(info.get_text())

            contacts_f = {'Website': url[1], 'Email': '', 'Phone': ''}

            try:
                contact_element = info.find('a', string=re.compile('contact', re.IGNORECASE))
                if contact_element:
                    contact = contact_element.get('href')
                    if 'http' in contact:
                        contact_url = contact
                    else:
                        contact_url = driver.current_url[0:-1] + "/" + contact

                    driver.get(contact_url)
                    contact_info = BeautifulSoup(driver.page_source, 'lxml').get_text()

                    logger.info(f'Searched contact URL: {driver.current_url}')

                    emails_contact = get_email(contact_info)
                    phones_contact = get_phone(contact_info)

                    emails_f = emails_home + emails_contact
                    phones_f = phones_home + phones_contact

                else:
                    emails_f = emails_home
                    phones_f = phones_home

                emails_f = remove_dup_email(emails_f)
                phones_f = remove_dup_phone(phones_f)

                contacts_f['Email'] = emails_f[0] if emails_f else ''
                contacts_f['Phone'] = phones_f[0] if phones_f else ''

                result[url[0]] = contacts_f

            except Exception as e:
                logger.error(f'Error in contact URL: {e}')
                result[url[0]] = {}

            finally:
                driver.quit()

        except Exception as e:
            logger.error(f"Request error in threads: {e}")
            result[url[0]] = {}
        finally:
            q.task_done()
            logger.debug(f"Queue task no {url[0]} completed.")
    return True


def main():
    """
    This function is called when the main thread is started
    :return:
    """
    urls = read_file()

    q = Queue(maxsize=0)
    num_threads = min(50, len(urls))
    results = [{} for x in urls]

    for i in range(len(urls)):
        q.put((i, urls[i]))

    for i in range(num_threads):
        logger.debug(f"Starting thread: {i}")
        worker = Thread(target=crawl, args=(q, results))
        worker.daemon = True
        worker.start()

    q.join()

    df = pd.DataFrame(results)
    excel_file = 'websites_info.xlsx'
    df.to_excel(excel_file, index=False)


if __name__ == "__main__":
    main()
