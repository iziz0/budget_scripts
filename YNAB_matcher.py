import os
import datetime

import pandas as pd

def combine_cc_statements(directory, start_date=None, end_date=None):
    if not start_date:
        start_date = datetime.datetime.now() - datetime.timedelta(days=90)
    if not end_date:
        end_date = datetime.datetime.now()

    combined_sheet = pd.DataFrame(
        {"Date": [], "Account": [], "Category": [], "Description": []})

    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)

        # Make sure it's a file
        if os.path.isfile(f):
            try:
                # Try to read it as CSV
                df = pd.read_csv(f)
            except:
                # If it isn't possible to read the file as csv
                print(f'{filename} is not a csv file.')

                # Move to the next item in the for loop
                continue
        else:
            # Move to the next item in the for loop
            continue

        if filename.startswith("AMZ"):
            col_map = {"Transaction Date": "Date", "Description": "Payee"}
            df.rename(col_map, inplace=True, axis="columns")
            df["Account"] = "Amazon Card"
        elif filename.startswith("CapitalOne"):
            col_map = {"Transaction Date": "Date", "Description": "Payee"}
            df.rename(col_map, inplace=True, axis="columns")
            df["Amount"] = -1 * df.Debit.where(df.Debit > 0)
            # If it doesn't match, leave the existing Amount. Otherwise, it will clear out the Debits.
            df["Amount"] = df.Credit.where(df.Credit > 0, df.Amount)
            df["Account"] = "CapitalOne"
        elif filename.startswith("Discover"):
            col_map = {"Trans. Date": "Date", "Description": "Payee"}
            df["Amount"] = -1 * df["Amount"]
            df.rename(col_map, inplace=True, axis="columns")
            df["Account"] = "Discover Card"
        elif filename.startswith("USAAVisa"):
            col_map = {"date": "Date", "Cat": "Category", "Original Description": "Payee"}
            df.rename(col_map, inplace=True, axis="columns")
            df["Account"] = "USAA Card"
        elif filename.startswith("USAAChecking"):
            col_map = {"Transaction Date": "Date", "Cat": "Category", "Original Description": "Payee"}
            df.rename(col_map, inplace=True, axis="columns")
            df["Account"] = "USAA Checking"
        elif filename.startswith("BOA"):
            col_map = {"Posted Date": "Date"}
            df.rename(col_map, inplace=True, axis="columns")
            df["Account"] = "BOA"
        else:
            # If it didn't match the filename
            print(f'Skipped processing {filename}.')
            continue

        # Make sure it isn't empty
        if not df.empty:
            # Filter out unwanted dates to keep the DataFrame as small as possible
            match = _filter_by_dates(df, "Date", start_date, end_date)
            combined_sheet = pd.concat([combined_sheet, match])


    # Split the Amount columns into Inflow/Outflow
    combined_sheet["Inflow"] = combined_sheet.Amount.where(combined_sheet.Amount > 0)
    combined_sheet["Outflow"] = combined_sheet.Amount.where(combined_sheet.Amount < 0)

    # Make all values in these columns positive, or 0 if None
    combined_sheet["Inflow"] = combined_sheet.Inflow.abs()
    combined_sheet["Outflow"] = combined_sheet.Outflow.abs()

    # Strip out any columns we don't need
    outfile = combined_sheet[["Date", "Account", "Category", "Payee", "Description",
                              "Inflow", "Outflow", "Status", "Amount"]]

    return outfile


def filter_ynab_data(file, start_date=None, end_date=None, days=90):
    """Makes Inflow/Outflow values numeric, Removes Checking and HSA data, only keeps rows between start/end dates"""
    if not start_date:
        start_date = datetime.datetime.now() - datetime.timedelta(days=days)
    if not end_date:
        end_date = datetime.datetime.now()

    df = pd.read_csv(file)
    df = _filter_by_dates(df, "Date", start_date, end_date)

    # Remove $ and , from the dollar amounts so we can convert them to numbers
    df["Inflow"] = pd.to_numeric(df["Inflow"].replace("[$,]", "", regex=True))
    df["Outflow"] = pd.to_numeric(df["Outflow"].replace("[$,]", "", regex=True))

    # Create an "Amount" column for the YNAB data. Outflow = negative Amount
    df["Amount"] = -1 * df.Outflow
    # If it doesn't match, leave the existing Amount. Otherwise, it will clear out the Debits.
    df["Amount"] = df.Inflow.where(df.Inflow > 0, df.Amount)
    df["Amount"] = pd.to_numeric(df["Amount"])

    df.drop(columns=["Flag", "Check Number", "Running Balance"], inplace=True)

    mask = ((df["Account"] == "Checking") | (df["Account"] == "HSA"))
    match = df.loc[mask]

    # Drop unwanted Accounts
    # df.drop(df[(df["Account"] == "Checking") & (df["Account"] == "HSA")].index, inplace=True)

    return match


def compare_sheets(ynab, stmts, merge_on_column="Amount", days_buffer=3):
    # Convert the strings to datetime objects
    ynab["Date"] = pd.to_datetime(ynab["Date"], infer_datetime_format=True)
    stmts["Date"] = pd.to_datetime(stmts["Date"], infer_datetime_format=True)

    # Group by Inflow/Outflow values. By default, this will not group empty values, which is what we want.
    ynab_amounts = ynab.groupby("Amount")
    statement_amounts = stmts.groupby("Amount")

    # Create empty dataframes
    all_matched_rows = pd.DataFrame()
    unmatched_stmt_rows = pd.DataFrame()

    column_list = ["Date", "Account", "Category", "Payee", "Description", "Inflow", "Outflow", "Status", "Amount"]

    # For each group in the Statement groups
    matched_ynab_rows = pd.DataFrame()

    for stmt_group_name, stmt_group in statement_amounts:
        try:
            # Try to get a matching YNAB stmt_group
            ynab_group = ynab_amounts.get_group(stmt_group_name)
        except KeyError as e:
            # If there is not a matching YNAB stmt_group, get() will throw a KeyError exception
            unmatched_stmt_rows = pd.concat([unmatched_stmt_rows, stmt_group])
            continue
        # If there is a match, iterate through the rows in the stmt_group
        # itertuples() allows you to reference the column names. iterrows() only gives the row values.
        for row in stmt_group.itertuples():
            stmt_date = row.Date
            start_date = stmt_date - datetime.timedelta(days=days_buffer)
            end_date = stmt_date + datetime.timedelta(days=days_buffer)
            mask = (ynab_group['Date'] > start_date) & (ynab_group['Date'] <= end_date)
            ynab_match = ynab_group.loc[mask]
            # row is a tuple, not a DataFrame. It needs to be converted to a df to get column names and merge.
            stmt_df = pd.DataFrame([list(row)[1:]], columns=column_list)
            stmt_df.fillna({"Inflow": 0.0, "Outflow": 0.0}, inplace=True)
            # If there is at least one matching row in the YNAB group
            if not ynab_match.empty:
                # print(f'Match on {row.Account}: {stmt_group_name}')
                # matched_stmt_rows = pd.concat([matched_stmt_rows, ynab_match, stmt_df])
                merged_rows = pd.merge(ynab_match,
                                       stmt_df,
                                       on=merge_on_column,
                                       sort=False,
                                       suffixes=("_YNAB", "_Statement"),
                                       )
                all_matched_rows = pd.concat([all_matched_rows, merged_rows])
                matched_ynab_rows = pd.concat([matched_ynab_rows, ynab_match])
            else:
                unmatched_stmt_rows = pd.concat([unmatched_stmt_rows, stmt_df])
    # Get all rows in ynab that never matched any statement row
    unmatched_ynab_rows = pd.concat([ynab, matched_ynab_rows]).drop_duplicates(keep=False)
    # unmatched_ynab_rows["Sheet"] = "YNAB"
    # unmatched_stmt_rows = pd.concat([unmatched_stmt_rows, unmatched_ynab_rows])

    return all_matched_rows, unmatched_ynab_rows, unmatched_stmt_rows


def _filter_by_dates(df: pd.DataFrame, col_name: str, start_date: datetime.datetime, end_date: datetime.datetime):
    """Returns only items where "col_name", is between start_date and end_date

    :param df DataFrame to filter
    :param col_name The name of the column containing a datetime to filter on
    :param start_date A datetime for the start of the range
    :param end_date A datetime for the end of the range

    :return DataFrame
    """
    # Convert the strings to datetime objects
    df[col_name] = pd.to_datetime(df[col_name], infer_datetime_format=True)

    mask = (df[col_name] > start_date) & (df[col_name] <= end_date)
    match = df.loc[mask]

    return match



if __name__ == '__main__':
    # Prompt for each input. If left blank, assign "." for the working dir.
    ynab_file = input("YNAB filepath [./YNAB_data.csv]: ") or "./YNAB_data.csv"
    statement_folder = input("Statements folder filepath [./statement_files]: ") or "./statement_files"
    output_folder = input("Output filepath [./]: ") or "./"

    combined_statements_df = combine_cc_statements(statement_folder)
    cc_dupes = combined_statements_df[combined_statements_df.duplicated(keep=False)]
    # combined_statements_df = combined_statements_df.drop_duplicates()
    stmt_count = len(combined_statements_df.index)
    print(f'Total items processed from statements: {stmt_count}')

    ynab_df = filter_ynab_data(ynab_file)
    ynab_dupes = ynab_df[ynab_df.duplicated(keep=False)]
    # ynab_df = ynab_df.drop_duplicates()
    ynab_count = len(ynab_df.index)
    print(f'Total items processed from YNAB: {ynab_count}')
    matched_df, unmatched_ynab, unmatched_stmts = compare_sheets(ynab_df, combined_statements_df)
    # Remove duplicated rows.
    matched_dupes = matched_df[matched_df.duplicated(keep=False)]
    # matched_df = matched_df.drop_duplicates()
    matched_count = len(matched_df.index)
    unmatched_count = len(unmatched_stmts.index) + len(unmatched_ynab.index)
    print(f'Total: {matched_count + unmatched_count} (matched: {matched_count}, unmatched: {unmatched_count})')

    dupes = pd.concat([ynab_dupes, matched_dupes, cc_dupes])
    print(f'Found {len(dupes.index)} duplicate rows.')

    for df in [matched_df, unmatched_ynab, unmatched_stmts, combined_statements_df]:
        cols = {col: 0.0 for col in df.columns if ("Inflow" in col or "Outflow" in col)}
        # Set any empty Inflow/Outflow columns to 0, the rest to ""
        df.fillna(cols, inplace=True)

    # All values in these columns should be 0.0, because they are the unmatched column in each row.
    matched_df.drop(columns=["Inflow_YNAB", "Outflow_YNAB", "Inflow_Statement", "Outflow_Statement"], inplace=True)
    # If you wanted to drop all columns that only contain 0, you could use:
    # matched_df = matched_df.loc[:, (matched_df != 0).any(axis=0)]

    # Output to CSV, leaving out the index column and substituting '' for any null values
    combined_statements_df.to_csv(f"{output_folder}/Combined_Card_Statements.csv", index=False)
    ynab_df.to_csv(f'{output_folder}/filtered_ynab_data.csv', index=False)
    unmatched_stmts.to_csv("unmatched_statement_rows.csv", index=False)
    unmatched_ynab.to_csv("unmatched_ynab_rows.csv", index=False)
    matched_df.to_csv("matched_rows.csv", index=False)
    dupes.to_csv("duplicate_rows.csv", index=False)
