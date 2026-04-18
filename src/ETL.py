import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def clean_noise_data(file_path) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    initial_rows = len(df)
    print(f"Xử lý nhiễu. Số dòng ban đầu: {initial_rows}")

    df_clean = df.drop_duplicates()

    # 2. Xoá NaN
    critical_cols = ['Date', 'Sales_Amount', 'Quantity']
    df_clean = df_clean.dropna(subset=critical_cols)

    # 3. Lọc nhiễu logic
    df_clean = df_clean[(df_clean['Sales_Amount'] > 0) & (df_clean['Quantity'] > 0)]

    # 4. Xử lý Outlier cho Quantity bằng IQR
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
    
    # Xác định đâu là Dimension và Measure
    measures = ['Sales_Amount', 'Quantity']
    dimensions = [col for col in df.columns if col not in measures]
    
    # Tính Cardinality cho từng Dimension
    cardinalities = {col: df[col].nunique() for col in dimensions}
    
    # Sắp xếp các Dimension theo Cardinality từ thấp đến cao
    sorted_dimensions = sorted(cardinalities.keys(), key=lambda k: cardinalities[k])
    
    # Integer Encoding
    mapping_dict = {}
    for col in sorted_dimensions:
        unique_vals = sorted(df[col].dropna().unique())
        
        col_mapping = {val: idx + 1 for idx, val in enumerate(unique_vals)}
        mapping_dict[col] = col_mapping
        
        df[col] = df[col].map(col_mapping)
        
    # Sorted Dimensions đứng trước, Measures đứng sau
    final_columns = sorted_dimensions + measures
    df_final = df[final_columns]
    
    processed_data = df_final.to_numpy()
    
    return processed_data, mapping_dict, final_columns

def compare_boxplot(file_path):
    df_before = pd.read_csv(file_path)
    
    df_after = clean_noise_data(file_path)
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    sns.boxplot(data=df_before, y='Quantity', ax=axes[0], palette='Set1')
    axes[0].set_title('Trước xử lý nhiễu', fontsize=12, fontweight='bold')
    axes[0].set_ylabel('Quantity')

    sns.boxplot(data=df_after, y='Quantity', ax=axes[1], palette='Set2')
    axes[1].set_title('Sau xử lý nhiễu', fontsize=12, fontweight='bold')
    axes[1].set_ylabel('Quantity')
    
    plt.tight_layout()
    plt.show()