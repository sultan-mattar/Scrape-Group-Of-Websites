import pandas as pd
import time
import urllib.parse
import requests
from collections import deque
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

df = pd.read_csv('C:/Users/Sulta/Desktop/Online_Python/Marketing Hiring Florida.csv')
suffixes = [".com", ".org", ".net", ".gov", ".edu", ".mil", ".int", ".biz", ".info", ".pro",
            ".name", ".coop", ".museum", ".aero", ".travel", ".mobi", ".jobs", ".cat", ".asia",
            ".tel", ".xxx", ".post", ".kitchen", ".bike", ".clothing", ".gallery", ".solutions",
            ".camera", ".plumbing", ".estate", ".guru", ".ventures", ".academy", ".computer", ".center",
            ".de", ".au", ".company", ".management", ".systems", ".photography", ".technology", ".directory",
            ".us", ".uk", ".ca", ".graphics", ".today", ".diamonds", ".education", ".voyage", ".careers", ".academy",
            ".recipes", ".lighting", ".international", ".ninja", ".consulting", ".services", ".marketing", ".agency",
            ".capital", ".finance", ".investments", ".partners", ".construction", ".engineering", ".contractors", ".builders",
            ".estate", ".realtor", ".realestate", ".land", ".rentals", ".properties", ".architecture", ".design", ".development",
            ".digital", ".web", ".software", ".app", ".mobile", ".cloud", ".data", ".analytics", ".security", ".network", ".hosting",
            ".online", ".store", ".shop", ".ecommerce", ".blog", ".news", ".media", ".press", ".community", ".social", ".forum", ".education",
            ".university", ".college", ".school", ".academy", ".research", ".science", ".technology", ".health", ".medical", ".hospital", ".care",
            ".fitness", ".sports", ".art", ".gallery", ".music", ".theatre", ".film", ".video", ".entertainment", ".fashion", ".style", ".beauty", ".food",
            ".restaurant", ".coffee", ".beer", ".wine", ".travel", ".vacations", ".holidays", ".tourism", ".explore", ".adventure", ".family", ".pets", ".animals", ".environment",
            ".nature", ".garden", ".book", ".author", ".writing", ".publishing", ".marketing", ".business", ".entrepreneur", ".startup", ".consultant", ".coach", ".freelance", ".creative",
            ".designer", ".photographer", ".artist", ".musician", ".writer", ".journalist", ".engineer", ".developer", ".programmer", ".coder", ".gamer", ".geek", ".nerd", ".fan", ".sports", ".fitness",
            ".traveler", ".wanderer", ".explorer", ".dreamer", ".lover", ".friend", ".life"]

# Configure Selenium
webdriver_path = 'msedgedriver.exe'
options = webdriver.EdgeOptions()
options.use_chromium = True
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 OPR/98.0.0.0"')

driver = webdriver.Edge(webdriver_path, options=options)
time.sleep(2)

# Retry function with a maximum of 3 attempts
def retry_get(driver, url, max_attempts=3):
    attempts = 0
    while attempts < max_attempts:
        try:
            driver.get(url)
            break
        except Exception:
            print("Error occurred while accessing:", url)
            print("Retrying...")
            attempts += 1
            time.sleep(3)

# Iterate through company names
for index, row in df.iterrows():
    company_name = row['Company']

    if isinstance(company_name, float):
        company_name = str(company_name)

    # Search for the company website
    search_url = 'https://www.google.com/search?q=' + urllib.parse.quote(company_name.encode('utf-8'))
    retry_get(driver, search_url)

    try:
        # Extract the website link
        website_link_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.yuRUbf > a')))
        website_link = website_link_element.get_attribute('href')

        # Visit the company website
        retry_get(driver, website_link)
        time.sleep(5)
        url = driver.current_url
        time.sleep(2)
        df.at[index, 'Website'] = url
        print('The URL is: ' + str(url))

    except Exception as e:
        # Handle any exceptions that may occur
        print(f"An error occurred for company: {company_name}")
        print("Error:", e)

driver.quit()

# Save the modified DataFrame back to the original CSV file
df.to_csv('C:/Users/Sulta/Desktop/Online_Python/Marketing Hiring Florida.csv', index=False)


# Process emails for each company
for index, row in df.iterrows():
    company_url = row['Website']
    urls = deque([company_url])
    scraped_urls = set()
    emails = set()
    count = 0

    try:
        while urls:
            count += 1
            if count == 70:
                break
            url = urls.popleft()
            scraped_urls.add(url)

            parts = urllib.parse.urlsplit(url)
            base_url = '{0.scheme}://{0.netloc}'.format(parts)
            path = url[:url.rfind('/') + 1] if '/' in parts.path else url
            print('[%d] Processing %s' % (count, url))

            try:
                headers = {'Accept-Encoding': 'identity'}
                response = requests.get(url, headers=headers, stream=True)

            except Exception:
                print("Invalid URL: ", url)
                continue

            try:
                soup = BeautifulSoup(response.text, features="lxml")
                for anchor in soup.find_all("a"):
                    link = anchor.attrs.get('href', '')
                    if link.startswith('/'):
                        link = base_url + link
                    elif not link.startswith('http'):
                        link = path + link
                    if link not in urls and link not in scraped_urls:
                        urls.append(link)

                new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", response.text, re.I))
                emails.update(new_emails)

            except Exception as e:
                print(f"Error occurred while scraping website: {url}")
                print("Error:", e)
                continue

            filtered_emails = [email for email in emails if any(email.endswith(suffix) for suffix in suffixes)]
            filtered_emails = list(set(filtered_emails))  # Remove duplicates using a set
            print(filtered_emails)

            # Add emails to DataFrame only if there are emails
            if filtered_emails:
                email_column_name = 'Emails'
                if email_column_name in row:
                    existing_emails = row[email_column_name]
                    if existing_emails and isinstance(existing_emails, str):
                        existing_emails = existing_emails.split(', ')
                        existing_emails = list(set(existing_emails))  # Remove duplicates from existing emails
                        filtered_emails.extend(existing_emails)
                df.at[index, email_column_name] = ', '.join(filtered_emails)

            # Break the loop if three email addresses are found
            # if len(filtered_emails) >= 3:
            #     break
            # Save the modified DataFrame back to the original CSV file
            df.to_csv('C:/Users/Sulta/Desktop/Online_Python/Marketing Hiring Florida.csv', index=False)
    except KeyboardInterrupt:
        print('[-] Closing!')

