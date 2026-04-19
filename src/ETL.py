import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json

def clean_noise_data(file_path) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    initial_rows = len(df)
    print(f"Xử lý nhiễu. Số dòng ban đầu: {initial_rows}")

    df_clean = df.drop_duplicates()

    # Xoá NaN
    critical_cols = ['Date', 'Sales_Amount', 'Quantity']
    df_clean = df_clean.dropna(subset=critical_cols)

    # Lọc nhiễu logic
    df_clean = df_clean[(df_clean['Sales_Amount'] > 0) & (df_clean['Quantity'] > 0)]

    # Xử lý Outlier cho Quantity bằng IQR
    Q1 = df_clean['Quantity'].quantile(0.25)
    Q3 = df_clean['Quantity'].quantile(0.75)
    IQR = Q3 - Q1
    upper_bound = Q3 + 1.5 * IQR

    # Giữ lại các dòng có Quantity hợp lý
    df_clean = df_clean[df_clean['Quantity'] <= upper_bound]

    final_rows = len(df_clean)
    print(f"Xử lý hoàn tất. Đã loại bỏ: {initial_rows - final_rows} dòng nhiễu.\n")
    
    return df_clean.copy()

def etl_pipeline(df):
    # Xóa cột Transaction_ID
    if 'Transaction_ID' in df.columns:
        df = df.drop(columns=['Transaction_ID'])
        
    # Chuyển đổi Date sang định dạng YYYYMM
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['Date'] = df['Date'].dt.strftime('%Y%m')
    
    # Xác định Dimension và Measure
    measures = ['Sales_Amount', 'Quantity']
    dimensions = [col for col in df.columns if col not in measures]
    
    # Tính Cardinality cho từng Dimension
    cardinalities = {col: df[col].nunique() for col in dimensions}
    sorted_dimensions = sorted(cardinalities.keys(), key=lambda k: cardinalities[k])
    
    # Integer Encoding
    mapping_dict = {}
    for col in sorted_dimensions:
        unique_vals = sorted(df[col].dropna().unique())
        
        col_mapping = {val: idx + 1 for idx, val in enumerate(unique_vals)}
        mapping_dict[col] = col_mapping
        
        df[col] = df[col].map(col_mapping)
    
    df_final = df[sorted_dimensions].copy()
    
    processed_data = df_final.to_numpy()
    sale_values = df['Sales_Amount'].to_numpy()
    quantity_values = df['Quantity'].to_numpy()
    
    return processed_data, mapping_dict, sorted_dimensions, sale_values, quantity_values

def compare_boxplot(file_path, cleaned_df):
    df_before = pd.read_csv(file_path)
    
    df_after = cleaned_df
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    sns.boxplot(data=df_before, y='Quantity', ax=axes[0], palette='Set1')
    axes[0].set_title('Trước xử lý nhiễu', fontsize=12, fontweight='bold')
    axes[0].set_ylabel('Quantity')

    sns.boxplot(data=df_after, y='Quantity', ax=axes[1], palette='Set2')
    axes[1].set_title('Sau xử lý nhiễu', fontsize=12, fontweight='bold')
    axes[1].set_ylabel('Quantity')
    
    plt.tight_layout()
    plt.show()

def export_to_csv(processed_array, mappings, dimensions, sale_values, quantity_values, file_name="pos_encoded.csv"):
    df_export = pd.DataFrame(processed_array, columns=list(dimensions))
    df_export["Sales_Amount"] = sale_values
    df_export["Quantity"] = quantity_values
    
    df_export.to_csv(file_name, index=False, encoding='utf-8-sig')
    print(f"Đã xuất dữ liệu encoded tại: {file_name}")

    # mapping file
    mapping_file = file_name.replace(".csv", "_mapping.json")
    
    clean_mappings = {}
    for col, mapping in mappings.items():
        clean_mappings[col] = {str(k): int(v) for k, v in mapping.items()}
        
    with open(mapping_file, 'w', encoding='utf-8') as f:
        json.dump(clean_mappings, f, ensure_ascii=False, indent=4)
        
    print(f"[*] Đã xuất file mapping tại: {mapping_file}")