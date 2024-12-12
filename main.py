import os
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import ray

options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.binary_location = "/usr/bin/chromium-browser"

ray.init(ignore_reinit_error=True)

@ray.remote
def process_year(year, base_dir, output_dir, driver_path):
    driver = webdriver.Chrome(service=Service(driver_path), options=options)

    year_folder = os.path.join(base_dir, year)
    paper_file_path = os.path.join(year_folder, "paper.txt")
    if not os.path.exists(paper_file_path):
        print(f"File {paper_file_path} not found, skipping year {year}.")
        return

    year_output_folder = os.path.join(output_dir, year)
    os.makedirs(year_output_folder, exist_ok=True)

    metadata_folder = os.path.join(year_output_folder, "Metadata")
    bib_folder = os.path.join(year_output_folder, "BIB")
    pdf_folder = os.path.join(year_output_folder, "PDF")
    os.makedirs(metadata_folder, exist_ok=True)
    os.makedirs(bib_folder, exist_ok=True)
    os.makedirs(pdf_folder, exist_ok=True)

    with open(paper_file_path, "r") as paper_file:
        links = paper_file.read().splitlines()

    for link in links:
        driver.get(link)
        pdf_links = [a.get_attribute("href") for a in driver.find_elements(By.TAG_NAME, "a") if a.get_attribute("href") and ".pdf" in a.get_attribute("href")]
        json_links = [a.get_attribute("href") for a in driver.find_elements(By.TAG_NAME, "a") if a.get_attribute("href") and ".json" in a.get_attribute("href")]
        bib_links = [a.get_attribute("href") for a in driver.find_elements(By.TAG_NAME, "a") if a.get_attribute("href") and ".bib" in a.get_attribute("href")]

        for pdf_link in pdf_links:
            pdf_file = os.path.join(pdf_folder, os.path.basename(pdf_link))
            try:
                with open(pdf_file, "wb") as file:
                    file.write(requests.get(pdf_link).content)
                print(f"Downloaded PDF: {pdf_file}")
            except Exception as e:
                print(f"Failed to download PDF from {pdf_link}: {e}")

        for json_link in json_links:
            json_file = os.path.join(metadata_folder, os.path.basename(json_link))
            try:
                with open(json_file, "wb") as file:
                    file.write(requests.get(json_link).content)
                print(f"Downloaded JSON: {json_file}")
            except Exception as e:
                print(f"Failed to download JSON from {json_link}: {e}")

        for bib_link in bib_links:
            bib_file = os.path.join(bib_folder, os.path.basename(bib_link))
            try:
                with open(bib_file, "wb") as file:
                    file.write(requests.get(bib_link).content)
                print(f"Downloaded BIB: {bib_file}")
            except Exception as e:
                print(f"Failed to download BIB from {bib_link}: {e}")

    driver.quit()

def main():
    base_dir = "NeurIPS"
    output_dir = "NeurIPS-Dataset"
    os.makedirs(output_dir, exist_ok=True)

    years = sorted(os.listdir(base_dir))
    driver_path = "/usr/lib/chromium-browser/chromedriver"

    for i in range(0, len(years), 10):
        batch = years[i:i + 10]
        tasks = [process_year.remote(year, base_dir, output_dir, driver_path) for year in batch]
        ray.get(tasks)


if __name__ == "__main__":
    main()
