from os.path import join, abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from load_data import *
from openstreetmap import *
from selenium_googlemap import *
import pandas as pd
import os
# warehouse_location = abspath('/apps/hive/warehouse/')


# # Cập nhật dữ liệu mới nhất từ npom.rims đến cell_lac_address
# update_daily_the_latest_data(spark)

# # Lấy những file chứa các trạm chưa được gán nhãn
# new_bts_ibs_address = get_unlabeled_bts(spark)
# phase1

new_bts_ibs_address = pd.read_csv('data_test.csv')


results_phase1, data_phase2 = phase1_openstreetmap(new_bts_ibs_address)
# # Phase2: Pass unlabeled data to Google Map (Sử dụng Selenium)
output_selenium = phase2_selenium_googlemap(data_phase2)

# # Check dữ liệu từ GGMap xem đã được gán nhãn building,... hay chưa; update vào cột status
results_phase2 = check_labeled_building_bts(output_selenium)

# # Kết hợp kết quả từ phase1 và phase2
merged_df = pd.concat([results_phase1, results_phase2], ignore_index=True)
# results = spark.createDataFrame(merged_df)
results = merged_df[['ci', 'lac', 'latitude', 'longitude', 'cell_name', 'address','status']]
# Kiểm tra xem tệp tin đã tồn tại hay không
if os.path.exists('results.csv'):
    # Nếu tệp tin đã tồn tại, xóa nó đi
    os.remove('results.csv')
results.to_csv('results.csv', index=False)


# # Cập nhật dữ liệu mới vào Hive
# results.write.mode("append") \
#     .insertInto("training.ibs_cell_lac_address", overwrite=False)