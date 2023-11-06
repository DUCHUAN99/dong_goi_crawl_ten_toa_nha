import time
import requests
from progress_bar import print_progress_bar


def get_places_of_interest(lat, lon):
    # 
    # :param lat: latitude
    # :param lon: longitude
    # :return: tòa nhà tương ứng với tọa độ
    # 
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {
        "format": "json",
        "lat": lat,
        "lon": lon,
        "addressdetails": 1,
    }
    response = requests.get(url, params=params)
    data = response.json()
    places_of_interest = []

    for key in ['building', 'hospital', 'factory', 'apartments', 'office', 'residential', 'aeroway', 'shop',
                'healthcare', 'leisure', 'amenity', 'industrial', 'tourism']:
        if key in data['address']:
            places_of_interest.append(data['address'][key])
    return places_of_interest


def phase1_openstreetmap(df):
    # 
    # :param df: Dữ liệu chưa được gán nhãn
    # :return: Dữ liệu đã được gán nhán tương ứng với lat, long (null khi OpenStreetMap không tìm thấy)
    # 
    print("Phase 1: Requesting From OpenStreetMap")
    latitude = df["latitude"]
    longitude = df["longitude"]
    total = len(latitude)
    try:
        for i in range(0, len(latitude)):
            print_progress_bar(i, total, 'RequestFromOpenStreetMap')
            time.sleep(0.1)
            places_of_interest = get_places_of_interest(latitude[i], longitude[i])
            if places_of_interest:
                df['address'][i] = places_of_interest[0]
                df['status'][i] = 1
            else:
                continue
    except Exception as e:
        print("ERROR" + str(e))
    results_phase1 = df[df['status'] == 1]
    data_phase2 = df[df['status'] != 1]
    print("End phase 1 ... ")
    return results_phase1, data_phase2
