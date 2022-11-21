import os
import pandas as pd

combined_sheet = pd.DataFrame(
    {"Date": [], "Account": [], "Category": [], "Description": [], "Inflow": [], "Outflow": []})

directory = os.getcwd()
directory = directory + "/statement_files"

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    print(f"Processing {filename}")
    if os.path.isfile(f) and filename.startswith("AMZ"):
        df = pd.read_csv(f)
        col_map = {"Transaction Date": "Date"}
        df.rename(col_map, inplace=True, axis="columns")
        df["Inflow"] = df.Amount.where(df.Amount > 0, 0)
        df["Outflow"] = -1 * df.Amount.where(df.Amount < 0, 0)
        df["Account"] = "Amazon Card"
        combined_sheet = combined_sheet.append(df)
    elif os.path.isfile(f) and filename.startswith("CapitalOne"):
        df = pd.read_csv(f)
        col_map = {"Transaction Date": "Date"}
        df.rename(col_map, inplace=True, axis="columns")
        df["Account"] = "CapitalOne"
        combined_sheet = combined_sheet.append(df)
    elif os.path.isfile(f) and filename.startswith("Discover"):
        df = pd.read_csv(f)
        col_map = {"Trans. Date": "Date"}
        df.rename(col_map, inplace=True, axis="columns")
        df["Account"] = "Discover Card"
        combined_sheet = combined_sheet.append(df)
    elif os.path.isfile(f) and filename.startswith("USAAVisa"):
        df = pd.read_csv(f)
        col_map = {"date": "Date", "Cat": "Category"}
        df.rename(col_map, inplace=True, axis="columns")
        df["Account"] = "USAA Card"
        combined_sheet = combined_sheet.append(df)
    elif os.path.isfile(f) and filename.startswith("USAAChecking"):
        df = pd.read_csv(f)
        col_map = {"Transaction Date": "Date", "Cat": "Category"}
        df.rename(col_map, inplace=True, axis="columns")
        df["Account"] = "USAA Checking"
        combined_sheet = combined_sheet.append(df)
    elif os.path.isfile(f) and filename.startswith("BOA"):
        df = pd.read_csv(f)
        col_map = {"Posted Date": "Date", "Payee": "Description"}
        df.rename(col_map, inplace=True, axis="columns")
        df["Account"] = "BOA"
        combined_sheet = combined_sheet.append(df)
    else:
        print("Skipped...")

# Split the Amount columns into Inflow/Outflow
combined_sheet["Inflow"] = combined_sheet.Amount.where(combined_sheet.Amount > 0, 0)
combined_sheet["Outflow"] = -1 * combined_sheet.Amount.where(combined_sheet.Amount < 0, 0)

# Make all values in these columns positive, or 0 if None
combined_sheet["Inflow"] = combined_sheet.Inflow.abs()
combined_sheet["Outflow"] = combined_sheet.Outflow.abs()

# Strip out any columns we don't need
outfile = combined_sheet[["Date", "Account", "Category", "Description", "Inflow", "Outflow"]]
outfile.to_csv(f"{directory}/Combined_Card_Statements.csv", index=False)
