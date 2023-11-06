import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
from selenium.webdriver.chrome.options import Options
import requests
from progress_bar import print_progress_bar
import re
def phase2_selenium_googlemap(data_phase2):
    # """
    # :param data_phase2: Dữ liệu chưa được gán địa chỉ (cột address) khi xử lý bằng OpenStreetMap
    # :return: Dữ liệu đươc gán địa chỉ
    # """
    print("Phase2: Selenium - RequestFromGoogleMap")
    latitude_phase2 = data_phase2['latitude']
    longitude_phase2 = data_phase2['longitude']
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    time.sleep(0.1)
    driver.get("https://www.google.com/maps")
    driver.implicitly_wait(20)

    total = len(latitude_phase2)
    for i in range(0, len(latitude_phase2)):
        print_progress_bar(i, total, 'RequestFromGoogleMap')
        time.sleep(0.1)
        try:
            search_box = driver.find_element(By.ID, "searchboxinput")
            search_box.send_keys(f"{latitude_phase2[i]}, {longitude_phase2[i]}")
            search_box.send_keys(Keys.ENTER)
            driver.implicitly_wait(10)
            time.sleep(0.1)
            place_address = driver.find_element(By.CSS_SELECTOR,
                                                "#QA0Szd > div > div > div.w6VYqd > div.bJzME.tTVLSc > div > div.e07Vkf.kA9KIf > div > div > div:nth-child(10) > div.Y4SsEe > div.LCF4w > span.JpCtJf > span").text
            driver.implicitly_wait(10)
            data_phase2['address'][i] = place_address
            time.sleep(0.1)
            clear_box = driver.find_element(By.CSS_SELECTOR, "#searchbox > div.lSDxNd > button")
            clear_box.click()
        except:
            driver.get("https://www.google.com/maps")
            driver.implicitly_wait(20)
            continue
    return data_phase2


def check_labeled_building_bts(data_phase2):
    # """
    # :param data_phase2: dữ liệu chứa địa chỉ được request từ Google Maps
    # :return: Bảng thêm cột status (1: là tên tòa nhà, bệnh viện, trường học,...; 0: là địa chỉ tuyệt đối, không chứa thông tin)
    # """
    print("Checking labeled bts data from Google Maps")
    desired_words = ['khu đô thị', 'tower', 'tòa', 'chung cư', 'building', 'vin', 'plaza', 'land', 'home', 'apartment',
                     'chung cu', 'căn hộ', 'center', 'nhà', 'KĐT',
                     'big c', 'văn phòng', 'bệnh viện', 'hotel', 'grand', 'khách sạn', 'trường', 'mall', 'bank', 'sun',
                     'garden', 'park', 'trung tâm', 'sky',
                     'pearl', 'công ty', 'ct', 'hh', 'win', 'house', 'town', 'holding', 'Riverside', 'department',
                     'mart', 'ngân hàng', 'cao ốc', 'Appartments', 'Khu thương mại',
                     'Tòa Nhà', 'Apartments', 'home', 'toa nha', 'celadon', 'block', 'gele', 'office', 'green',
                     'Tòa nhà']
    # Tạo biểu thức chính quy với các từ bạn muốn tìm kiếm, kết hợp chúng bằng toán tử |
    regex_pattern = '|'.join(map(re.escape, desired_words))
    # Lọc các hàng trong đó cột 'Address' chứa ít nhất một từ trong danh sách
    address_label = data_phase2['address']
    data_phase2['status'] = 0

    # Update cot status
    for i in range(len(address_label)):
        if data_phase2['address'].str.contains(regex_pattern, case=False, na=False, regex=True).iloc[i]:
            data_phase2.at[i, 'status'] = 1
    print("End Phase 2")
    return data_phase2