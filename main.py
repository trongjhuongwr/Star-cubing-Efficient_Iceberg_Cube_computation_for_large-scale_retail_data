from src.ETL import clean_noise_data, etl_pipeline, compare_boxplot, export_to_csv

file_path = r".\\pos_data.csv"

df_clean = clean_noise_data(file_path)
processed_array, mappings, dimensions, sale_values, quantity_values = etl_pipeline(df_clean)

print("Thứ tự cột sau khi sắp xếp theo Cardinality:")
print(dimensions)

print("\nMảng 2D:")
print(processed_array[:2])

print("\nMapping Dictionary:")
print(mappings)

print("\nSale Values:")
print(sale_values)

print("\nQuantity Values:")
print(quantity_values)

export_to_csv(processed_array, mappings, dimensions, sale_values, quantity_values)
# compare_boxplot(file_path, df_clean)