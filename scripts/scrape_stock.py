import requests
import csv
from datetime import datetime
api_key = "20PW8HQYMT7BCIEU"
symbol = "IBM"
interval = "5min"
function = "TIME_SERIES_INTRADAY"
output_size = "full"
extended_hours = "true"
url = (f"https://www.alphavantage.co/query?function={function}&symbol={symbol}"
       f"&interval={interval}&outputsize={output_size}&extended_hours={extended_hours}"
       f"&apikey={api_key}&datatype=csv")
response = requests.get(url)
if response.status_code == 200:
    filename = f"stock_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = f"/mnt/d/projects/stock_project/data/{filename}"  # Sử dụng đường dẫn trong container
    with open(filepath, "w", newline="") as f:
        f.write(response.text)
    print(f"Dữ liệu cho {symbol} đã được lưu thành công vào {filename}")
else:
    print(f"Lỗi: {response.status_code}, {response.text}")