import pandas as pd
import logging


def read_data():
    try:
        data = pd.read_csv(
            "dags/data/Filedata_Data_Penjualan_dan_Pajak_BBM_di_DKI_Jakarta.csv"
        )
        return data
    except Exception as e:
        logging.error(f"could not read data {e}")
