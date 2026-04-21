from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Iterable, List
import urllib.parse

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class DBManager:
	"""Manage SQL Server DDL/DML workflow for the retail star schema."""

	FACT_TARGET_COLUMNS: List[str] = [
		"Date_Code",
		"City_Code",
		"Category_Code",
		"Payment_Method_Code",
		"Region_Code",
		"Customer_Type_Code",
		"Quantity",
		"Sales_Amount",
	]

	FACT_COLUMN_ALIASES: Dict[str, List[str]] = {
		"Date_Code": ["Date_Code", "Date"],
		"City_Code": ["City_Code", "City"],
		"Category_Code": ["Category_Code", "Category"],
		"Payment_Method_Code": ["Payment_Method_Code", "Payment_Method"],
		"Region_Code": ["Region_Code", "Region"],
		"Customer_Type_Code": ["Customer_Type_Code", "Customer_Type"],
		"Quantity": ["Quantity"],
		"Sales_Amount": ["Sales_Amount"],
	}

	DIMENSION_TABLE_MAP: Dict[str, str] = {
		"Customer_Type": "Dim_Customer_Type",
		"Region": "Dim_Region",
		"Payment_Method": "Dim_Payment_Method",
		"Category": "Dim_Category",
		"City": "Dim_City",
		"Date": "Dim_Date",
	}

	def __init__(
		self,
		server: str,
		database: str,
		driver: str = "ODBC Driver 17 for SQL Server",
	) -> None:
		"""Create SQL Server engine using Windows Authentication."""

		odbc_str = (
			f"DRIVER={{{driver}}};"
			f"SERVER={server};"
			f"DATABASE={database};"
			"Trusted_Connection=yes;"
			"TrustServerCertificate=yes;"
		)
		encoded = urllib.parse.quote_plus(odbc_str)
		connection_url = f"mssql+pyodbc:///?odbc_connect={encoded}"

		self.engine: Engine = create_engine(
			connection_url,
			fast_executemany=True,
			future=True,
		)

	def execute_sql_file(self, sql_file_path: str | Path) -> None:
		"""Execute SQL statements from a file, splitting by semicolon."""

		path = Path(sql_file_path)
		if not path.exists():
			raise FileNotFoundError(f"Không tìm thấy file SQL: {path}")

		sql_text = path.read_text(encoding="utf-8-sig")
		sql_text = "\n".join(
			line for line in sql_text.splitlines() if line.strip().upper() != "GO"
		)
		statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]

		if not statements:
			print(f"[SQL] File rỗng hoặc không có câu lệnh hợp lệ: {path}")
			return

		with self.engine.begin() as connection:
			for idx, statement in enumerate(statements, start=1):
				connection.execute(text(statement))
				print(f"[SQL] Đã chạy câu lệnh {idx}/{len(statements)} từ {path.name}")

	def load_dimensions(self, mapping_json_path: str | Path) -> None:
		"""Load dimension mappings from JSON into all Dim_ tables."""

		path = Path(mapping_json_path)
		if not path.exists():
			raise FileNotFoundError(f"Không tìm thấy file mapping: {path}")

		with path.open("r", encoding="utf-8") as file:
			mappings = json.load(file)

		with self.engine.begin() as connection:
			for dim_name, table_name in self.DIMENSION_TABLE_MAP.items():
				if dim_name not in mappings:
					raise KeyError(f"Thiếu dimension '{dim_name}' trong mapping JSON")

				pairs = mappings[dim_name]
				records = [
					{"Code": int(code), "Name": str(name)}
					for name, code in pairs.items()
				]
				records.sort(key=lambda row: row["Code"])

				connection.execute(text(f"DELETE FROM dbo.{table_name}"))
				if records:
					connection.execute(
						text(
							f"INSERT INTO dbo.{table_name} (Code, Name) "
							"VALUES (:Code, :Name)"
						),
						records,
					)

				print(
					f"[DIM] Đã nạp {len(records):,} dòng vào bảng dbo.{table_name}"
				)

	def load_fact(self, fact_csv_path: str | Path, chunksize: int = 100000) -> None:
		"""Load encoded fact data from CSV into Fact_Transactions by chunks."""

		path = Path(fact_csv_path)
		if not path.exists():
			raise FileNotFoundError(f"Không tìm thấy file fact: {path}")

		total_rows = 0
		for chunk_idx, chunk in enumerate(
			pd.read_csv(path, chunksize=chunksize),
			start=1,
		):
			prepared = self._prepare_fact_chunk(chunk)
			prepared.to_sql(
				name="Fact_Transactions",
				con=self.engine,
				schema="dbo",
				if_exists="append",
				index=False,
				method=None,
			)

			total_rows += len(prepared)
			print(
				"[FACT] Chunk "
				f"{chunk_idx:,}: {len(prepared):,} dòng | Lũy kế: {total_rows:,} dòng"
			)

		print(f"[FACT] Hoàn tất nạp dữ liệu Fact_Transactions: {total_rows:,} dòng")

	def _prepare_fact_chunk(self, frame: pd.DataFrame) -> pd.DataFrame:
		"""Normalize source CSV columns into target fact schema columns."""

		normalized = frame.copy()
		normalized.columns = [col.replace("\ufeff", "").strip() for col in normalized.columns]

		rename_map: Dict[str, str] = {}
		for target_col, aliases in self.FACT_COLUMN_ALIASES.items():
			source_col = next((col for col in aliases if col in normalized.columns), None)
			if source_col is None:
				raise KeyError(
					"Thiếu cột bắt buộc trong file fact: "
					f"{target_col} (aliases: {aliases})"
				)
			rename_map[source_col] = target_col

		normalized = normalized.rename(columns=rename_map)
		normalized = normalized[self.FACT_TARGET_COLUMNS].copy()

		int_cols = [
			"Date_Code",
			"City_Code",
			"Category_Code",
			"Payment_Method_Code",
			"Region_Code",
			"Customer_Type_Code",
			"Quantity",
		]
		for col in int_cols:
			normalized[col] = pd.to_numeric(normalized[col], errors="raise").astype("int64")

		normalized["Sales_Amount"] = pd.to_numeric(
			normalized["Sales_Amount"], errors="raise"
		).astype("float64")

		return normalized


if __name__ == "__main__":
	SERVER = os.getenv("SQL_SERVER", r"localhost\SQLEXPRESS")
	DATABASE = os.getenv("SQL_DATABASE", "RetailIceberg")
	DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

	project_root = Path(__file__).resolve().parents[1]
	schema_sql = project_root / "sql" / "01_schema.sql"
	aggregate_sql = project_root / "sql" / "02_queries.sql"
	mapping_json = project_root / "data" / "processed" / "pos_encoded_mapping.json"
	fact_csv = project_root / "data" / "processed" / "pos_encoded.csv"

	print("=== BẮT ĐẦU PIPELINE DWH STAR SCHEMA ===")
	print(f"[INIT] Kết nối SQL Server: {SERVER} | Database: {DATABASE}")

	print("[INIT] Kiểm tra và tạo database nếu chưa tồn tại...")
	bootstrap = DBManager(server=SERVER, database="master", driver=DRIVER)
	safe_db_name = DATABASE.replace("]", "]]")
	with bootstrap.engine.begin() as connection:
		connection.execute(
			text(
				f"IF DB_ID('{DATABASE}') IS NULL "
				f"CREATE DATABASE [{safe_db_name}]"
			)
		)

	manager = DBManager(server=SERVER, database=DATABASE, driver=DRIVER)

	print("[STEP 1] Tạo schema và index...")
	manager.execute_sql_file(schema_sql)

	print("[STEP 2] Nạp dữ liệu dimension từ file mapping JSON...")
	manager.load_dimensions(mapping_json)

	print("[STEP 3] Nạp dữ liệu fact theo từng chunk...")
	manager.load_fact(fact_csv, chunksize=100000)

	print("[STEP 4] Build bảng aggregate cho BI...")
	manager.execute_sql_file(aggregate_sql)

	print("=== HOÀN TẤT PIPELINE ===")
