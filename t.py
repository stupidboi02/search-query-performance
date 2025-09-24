from abc import abstractmethod
import pandas as pd
class Transform():
    
    def __init__(self,df):
        self.df = df
        self.transform = self.transform()
        pass
    
    @abstractmethod
    def transform(self, df:pd.DataFrame) -> pd.DataFrame:
        pass

class TransformMonthly(Transform):
    def transform(self):
        df_agg = self.df.groupby(["asin","searchQuery", "year", "month"], as_index=False).agg({
            "searchQueryScore": "sum",
            "searchQueryVolume": "sum",
            "totalQueryImpressionCount": "sum",
            "asinImpressionCount": "sum",
            "asinImpressionShare": "sum",
            "totalClickCount": "sum",
            "totalClickRate": "sum",
            "asinClickCount": "sum",
            "asinClickShare": "sum",
            "totalCartAddCount": "sum",
            "totalCartAddRate": "sum",
            "asinCartAddCount": "sum",
            "asinCartAddShare": "sum",
            "totalPurchaseCount": "sum",
            "totalPurchaseRate": "sum",
            "asinPurchaseCount": "sum",
            "asinPurchaseShare": "sum",})
        return df_agg
 
class TransformQuarterly(Transform):
    def transform(self):
        df_agg = self.df.groupby(["asin","searchQuery","year", "quarter"], as_index=False).agg({
            "searchQueryScore": "sum",
            "searchQueryVolume": "sum",
            "totalQueryImpressionCount": "sum",
            "asinImpressionCount": "sum",
            "asinImpressionShare": "sum",
            "totalClickCount": "sum",
            "totalClickRate": "sum",
            "asinClickCount": "sum",
            "asinClickShare": "sum",
            "totalCartAddCount": "sum",
            "totalCartAddRate": "sum",
            "asinCartAddCount": "sum",
            "asinCartAddShare": "sum",
            "totalPurchaseCount": "sum",
            "totalPurchaseRate": "sum",
            "asinPurchaseCount": "sum",
            "asinPurchaseShare": "sum",})
        return df_agg