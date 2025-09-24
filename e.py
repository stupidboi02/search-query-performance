from abc import abstractmethod
import pandas as pd
import json
import os

class Extract():
    def __init__(self, folder_name):
        self.folder_name = folder_name
        self.files_name = self.get_json_files()
        self.run = self.load_and_process()

    def get_json_files(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        folder_path = os.path.join(base_dir, self.folder_name)

        return [
            os.path.join(folder_path, f)
            for f in os.listdir(folder_path)
            if f.endswith(".json") and os.path.isfile(os.path.join(folder_path, f))
        ]

    def load_and_process(self):
        dfs = []

        for file_path in self.files_name:
            with open(file_path, "r", encoding="utf-8") as f:
                text = f.read()

            data = json.loads(text)   # parse JSON thành dict
            raw_data = data['raw_data']

            df = pd.json_normalize(raw_data)
            df = self.clean(df)   # gọi hàm làm sạch đặc thù

            dfs.append(df)

        return pd.concat(dfs, ignore_index=True)

    @abstractmethod
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

# Khởi tạo với folder "monthly"
class ExtractMonthly(Extract):
    def clean(self,df):
        df = df[[
        'asin','month_period',
        'searchQueryData.searchQuery','searchQueryData.searchQueryScore','searchQueryData.searchQueryVolume',
        'impressionData.totalQueryImpressionCount','impressionData.asinImpressionCount','impressionData.asinImpressionShare',
        'clickData.totalClickCount','clickData.totalClickRate','clickData.asinClickCount','clickData.asinClickShare',
        'cartAddData.totalCartAddCount','cartAddData.totalCartAddRate','cartAddData.asinCartAddCount','cartAddData.asinCartAddShare',
        'purchaseData.totalPurchaseCount','purchaseData.totalPurchaseRate','purchaseData.asinPurchaseCount','purchaseData.asinPurchaseShare']
        ].copy()
        
        df["month_period"] = pd.to_datetime(df["month_period"], format="%B %Y")
        df["month"] = df["month_period"].dt.month
        df["year"] = df["month_period"].dt.year

        # Làm gọn tên cột
        df.columns = [col.split(".")[-1] for col in df.columns]
        df.fillna(0, inplace=True)
        df.drop_duplicates(keep='first', inplace=True)
        # df.info()
        return df

class ExtractQuarterly(Extract):
    def clean(self, df):
        df = df[[
        'asin','quarter_period',
        'searchQueryData.searchQuery','searchQueryData.searchQueryScore','searchQueryData.searchQueryVolume',
        'impressionData.totalQueryImpressionCount','impressionData.asinImpressionCount','impressionData.asinImpressionShare',
        'clickData.totalClickCount','clickData.totalClickRate','clickData.asinClickCount','clickData.asinClickShare',
        'cartAddData.totalCartAddCount','cartAddData.totalCartAddRate','cartAddData.asinCartAddCount','cartAddData.asinCartAddShare',
        'purchaseData.totalPurchaseCount','purchaseData.totalPurchaseRate','purchaseData.asinPurchaseCount','purchaseData.asinPurchaseShare']
        ].copy()
        
        df["quarter"] = df["quarter_period"].str.split(" ").str[0]
        df["year"] = df["quarter_period"].str.split(" ").str[1]

        # Làm gọn tên cột
        df.columns = [col.split(".")[-1] for col in df.columns]
        df.fillna(0, inplace=True)
        df.drop_duplicates(keep='first', inplace=True)
        df.info()
        return df

# m = ExtractMonthly('monthly')
# df_m = m.run
q = ExtractQuarterly('quarterly')
df_q = q.run
