from pyspark.sql.functions import col


def update_daily_the_latest_data(spark):
    """
    :param spark: Spark from main
    :return: Cập nhật giá trị từ partition mới nhất của bảng npoms.rims vào cell_lac_address
    """
    # Load dữ liệu mới nhất từ bảng npom.rims
    latest_partition = spark.sql("SELECT max(date_stamp) FROM npom.rims").collect()[0][0]
    npom_rims = spark.sql(f"SELECT cell_name AS name, lac, ci AS cell, latitude, longitude FROM npom.rims WHERE date_stamp = {latest_partition} AND ibs_status=1")

    # Load dữ liệu cũ từ bảng cell_lac
    bts_ibs_address = spark.table('training.ibs_cell_lac_address')

    #Thực hiện join 2 bảng (chỉ lấy giá trị mới mà bảng cell_lac không có)
    fetch_table_daily = npom_rims.join(bts_ibs_address, (npom_rims['cell'] == bts_ibs_address['cell']) & (
            npom_rims['lac'] == bts_ibs_address['lac']), how='left') \
        .filter(bts_ibs_address['cell'].isNull() | bts_ibs_address['lac'].isNull()) \
        .select(npom_rims['cell'], npom_rims['lac'], npom_rims['latitude'], npom_rims['longitude'], npom_rims['name'],
                bts_ibs_address['address'], bts_ibs_address['status'])

    #Nối bảng mới vào cell_lac
    fetch_table_daily.write.mode("append") \
        .insertInto("training.ibs_cell_lac_address", overwrite=False)

def get_unlabeled_bts(spark):
    """
    :param spark:
    :return: Trả về dữ liệu chưa được gán nhãn status=0 (dữ liệu cũ) và status = null (với những dữ liệu mới đc update)
    """
    new_bts_ibs_address = spark.table('training.ibs_cell_lac_address')
    filtered_unlabeled_bts = new_bts_ibs_address.filter((col('status') == 0) | (col('status').isNull()))

    #Chuyển dữ liệu sang PandasDF()
    filtered_unlabeled_bts_df = filtered_unlabeled_bts.toPandas()
    return filtered_unlabeled_bts_df
