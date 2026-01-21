from app.core.data_loader import DataLoader

dl = DataLoader()

df = dl.load(r"D:\_TECK\1. CESSOES\2. CESSOES\_ 05.2025\05.05\CONTROLE CESSAO ROMA VINI - 05.05.xlsx")

print(df.shape)
print(df.columns.tolist())