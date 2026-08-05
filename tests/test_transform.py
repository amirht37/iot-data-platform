import pandas as pd
from transform import transform_data
def make_input_df(raw_id= 1,device_id = "pico_001",temperature= 20,humidity= 30):
    row= {
        "raw_id": raw_id,
        "source": "raspberry_pi_pico",
        "ingest_ts": "2026-06-04T17:55:01Z",
        "payload": {
            "device_id": device_id,
            "temperature": temperature,
            "humidity": humidity
        }
        
    }
    return pd.DataFrame([row])
def test_humidity_is_out_of_range():
    df_input= make_input_df(humidity=150)
    df_clean,df_quarantine = transform_data(df_input,run_id=1)
    assert len(df_quarantine) == 1
    assert df_quarantine.iloc[0]["reject_reason"] == "humidity_out_of_range"

