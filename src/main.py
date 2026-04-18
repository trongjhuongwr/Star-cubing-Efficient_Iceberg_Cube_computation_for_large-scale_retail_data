from ETL import clean_noise_data, etl_pipeline, compare_boxplot

file_path = r".\test_data.csv"

df_clean = clean_noise_data(file_path)
compare_boxplot(file_path)

processed_array, mappings, col_names = etl_pipeline(df_clean)

print("Thứ tự cột sau khi sắp xếp theo Cardinality:")
print(col_names)

print("\nMảng 2D:")
print(processed_array[:2])

print("\nMapping Dictionary:")
print(mappings)