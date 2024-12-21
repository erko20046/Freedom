import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
driver.get('https://krisha.kz/prodazha/kvartiry/?page=0')

time.sleep(5)


titles = []
prices = []
cities = []
flat_building = []
map_complex = []
house_year = []
flat_floor = []
live_square = []

ad_urls = []


# 600 - 1200
num_pages = 800
ads_page = 600


while ads_page <= num_pages:
    ad_elements = driver.find_elements(By.CSS_SELECTOR, '.a-card.a-storage-live.ddl_product.ddl_product_link.not-colored.is-visible')
    ad_urls.extend([link.find_element(By.TAG_NAME, 'a').get_attribute('href') for link in ad_elements])

    # Печатаем собранные URL для отладки
    print(f"Collected URLs from page {ads_page}:")
    print(ad_urls)

    # Переход на следующую страницу (если доступна)
    try:
        next_button = driver.find_element(By.CSS_SELECTOR, 'a.paginator__btn.paginator__btn--next')
        next_page_url = next_button.get_attribute('href')
        driver.get(next_page_url)
        print(f"Navigating directly to: {next_page_url}")
        time.sleep(2)
        ads_page += 1
    except Exception as e:
        print(f"No more pages or error navigating to next page: {e}")
        break


ad_urls = list(set(ad_urls))


def parse_ad(url):
    try:
        driver.get(url)

        # Используем find_element и присваиваем значение None, если элемент не найден.
        title = driver.find_element(By.CLASS_NAME, 'offer__advert-title').text if driver.find_elements(By.CLASS_NAME, 'offer__advert-title') else None
        price = driver.find_element(By.CLASS_NAME, 'offer__price').text if driver.find_elements(By.CLASS_NAME, 'offer__price') else None
        city = driver.find_element(By.CLASS_NAME, 'offer__location').text if driver.find_elements(By.CLASS_NAME, 'offer__location') else None
        area = driver.find_element(By.XPATH, "//div[@data-name='live.square']//div[@class='offer__advert-short-info']").text if driver.find_elements(By.XPATH, "//div[@data-name='live.square']//div[@class='offer__advert-short-info']") else None
        floor = driver.find_element(By.XPATH, "//div[@data-name='flat.floor']//div[@class='offer__advert-short-info']").text if driver.find_elements(By.XPATH, "//div[@data-name='flat.floor']//div[@class='offer__advert-short-info']") else None
        building = driver.find_element(By.XPATH, "//div[@data-name='flat.building']//div[@class='offer__advert-short-info']").text if driver.find_elements(By.XPATH, "//div[@data-name='flat.building']//div[@class='offer__advert-short-info']") else None
        complex = driver.find_element(By.XPATH, "//div[@data-name='map.complex']//div[@class='offer__advert-short-info']").text if driver.find_elements(By.XPATH, "//div[@data-name='map.complex']//div[@class='offer__advert-short-info']") else None
        year = driver.find_element(By.XPATH, "//div[@data-name='house.year']//div[@class='offer__advert-short-info']").text if driver.find_elements(By.XPATH, "//div[@data-name='house.year']//div[@class='offer__advert-short-info']") else None


        titles.append(title)
        prices.append(price)
        cities.append(city)
        flat_building.append(building)
        map_complex.append(complex)
        house_year.append(year)
        flat_floor.append(floor)
        live_square.append(area)

        print(f"Collected ad: {title}, {city}")
    except Exception as e:
        print(f"Error parsing ad: {e}")



for url in ad_urls:
    if url:
        parse_ad(url)
    else:
        print("Found an empty URL, skipping...")


driver.quit()


df = pd.DataFrame({
    'Title': titles,
    'Price': prices,
    'City': cities,
    'Type of house': flat_building,
    'Map complex': map_complex,
    'House year': house_year,
    'Flat floor' : flat_floor,
    'live square': live_square,
})


df.to_csv('krisha_sell.csv', index=False)

print(df.head())
